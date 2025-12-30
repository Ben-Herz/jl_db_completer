import {
  CompletionHandler,
  ICompletionContext,
  ICompletionProvider
} from '@jupyterlab/completer';
import { ISettingRegistry } from '@jupyterlab/settingregistry';
import { fetchPostgresCompletions, ICompletionItem } from './api';

/**
 * Cache entry for PostgreSQL completions.
 */
interface ICacheEntry {
  items: ICompletionItem[];
  timestamp: number;
}

/**
 * PostgreSQL completion provider for JupyterLab.
 *
 * Provides table and column name completions from PostgreSQL databases
 * when editing SQL-like code in notebooks and editors.
 */
export class PostgresCompletionProvider implements ICompletionProvider {
  readonly identifier = 'jl_db_comp:postgres-completer';
  readonly renderer = null;

  private _cache = new Map<string, ICacheEntry>();
  private _cacheTTL = 5 * 60 * 1000; // 5 minutes in milliseconds
  private _settings: ISettingRegistry.ISettings | null = null;
  private _dbUrl = '';
  private _schema = 'public';
  private _enabled = true;

  /**
   * SQL keywords that trigger completion.
   */
  private readonly _sqlKeywords = [
    'select',
    'from',
    'join',
    'where',
    'insert',
    'update',
    'delete',
    'inner',
    'left',
    'right',
    'outer',
    'on',
    'group',
    'order',
    'by',
    'having',
    'into',
    'values',
    'set'
  ];

  /**
   * Create a new PostgresCompletionProvider.
   *
   * @param settings - Optional settings registry to load database configuration
   */
  constructor(settings?: ISettingRegistry.ISettings | null) {
    if (settings) {
      this._settings = settings;
      this._loadSettings();

      settings.changed.connect(() => {
        this._loadSettings();
      });
    }
  }

  /**
   * Load database configuration from settings.
   */
  private _loadSettings(): void {
    if (!this._settings) {
      return;
    }

    this._dbUrl = this._settings.get('databaseUrl').composite as string;
    this._schema = this._settings.get('schema').composite as string;
    this._enabled = this._settings.get('enabled').composite as boolean;
  }

  /**
   * Determine if completions should be shown in the current context.
   *
   * Checks for SQL keywords or context that suggests SQL code.
   */
  async isApplicable(context: ICompletionContext): Promise<boolean> {
    if (!this._enabled) {
      return false;
    }

    // Get editor content from context
    const editor = context.editor;
    if (!editor) {
      return false;
    }

    const text = editor.model.sharedModel.getSource();
    if (!text) {
      return false;
    }

    const textLower = text.toLowerCase();

    // Check if any SQL keyword is present
    return this._sqlKeywords.some(keyword => textLower.includes(keyword));
  }

  /**
   * Fetch completion items for the current context.
   *
   * Uses caching to minimize database calls.
   */
  async fetch(
    request: CompletionHandler.IRequest,
    context: ICompletionContext
  ): Promise<CompletionHandler.ICompletionItemsReply> {
    if (!this._enabled) {
      return { start: request.offset, end: request.offset, items: [] };
    }

    const { text, offset } = request;

    // Extract context: schema, table, and prefix
    const extracted = this._extractContext(text, offset);

    // Create cache key that includes full context
    let cacheKey: string;
    if (extracted.jsonbColumn) {
      // JSONB key completion: table.column->path
      const pathStr = extracted.jsonbPath?.join('.') || '';
      const tablePrefix = extracted.schemaOrTable
        ? `${extracted.schemaOrTable}.`
        : '';
      cacheKey =
        `${tablePrefix}${extracted.jsonbColumn}->${pathStr}.${extracted.prefix}`.toLowerCase();
    } else if (extracted.schema && extracted.tableName) {
      // schema.table.prefix
      cacheKey =
        `${extracted.schema}.${extracted.tableName}.${extracted.prefix}`.toLowerCase();
    } else if (extracted.schemaOrTable) {
      // schema.prefix OR table.prefix (ambiguous)
      cacheKey = `${extracted.schemaOrTable}.${extracted.prefix}`.toLowerCase();
    } else {
      // just prefix
      cacheKey = extracted.prefix.toLowerCase();
    }

    // Check cache first
    const cached = this._getCached(cacheKey);
    if (cached) {
      return this._formatReply(cached, request.offset, extracted.prefix);
    }

    // Fetch from database
    try {
      const items = await fetchPostgresCompletions(
        this._dbUrl || undefined,
        extracted.prefix,
        extracted.schema || this._schema,
        extracted.tableName,
        extracted.schemaOrTable,
        extracted.jsonbColumn,
        extracted.jsonbPath
      );

      // Cache the results
      this._cache.set(cacheKey, {
        items,
        timestamp: Date.now()
      });

      return this._formatReply(items, request.offset, extracted.prefix);
    } catch (error) {
      console.error('Failed to fetch PostgreSQL completions:', error);
      return { start: request.offset, end: request.offset, items: [] };
    }
  }

  /**
   * Extract context from the text: prefix being typed, optional table name, optional schema, and JSONB context.
   *
   * Detects patterns like:
   * - "schema.table.col" → { schema: "schema", tableName: "table", prefix: "col" }
   * - "schema.table." → { schema: "schema", tableName: "table", prefix: "" }
   * - "schema.tab" → { schemaOrTable: "schema", prefix: "tab" }
   * - "schema." → { schemaOrTable: "schema", prefix: "" }
   * - "table.col" → { schemaOrTable: "table", prefix: "col" }
   * - "table." → { schemaOrTable: "table", prefix: "" }
   * - "prefix" → { prefix: "prefix" }
   * - "column_name->" → { jsonbColumn: "column_name", jsonbPath: [], prefix: "" }
   * - "column_name->>'key1'->" → { jsonbColumn: "column_name", jsonbPath: ["key1"], prefix: "" }
   * - "table.column_name->>'key'->" → { schemaOrTable: "table", jsonbColumn: "column_name", jsonbPath: ["key"], prefix: "" }
   *
   * Note: For single-dot patterns (schema. or table.), the backend will determine
   * whether it's a schema (list tables) or table (list columns) by checking the database.
   */
  private _extractContext(
    text: string,
    offset: number
  ): {
    prefix: string;
    tableName?: string;
    schema?: string;
    schemaOrTable?: string;
    jsonbColumn?: string;
    jsonbPath?: string[];
  } {
    const beforeCursor = text.substring(0, offset);

    // JSONB pattern: Detect -> or ->> operators
    // Examples: metadata-> or content -> or patients.metadata->>'key'->
    if (beforeCursor.includes('->')) {
      // Much simpler approach: find the last -> or ->> and work backwards
      // Look for: word characters, optional dot+word, then ->, then anything
      // Pattern: (word.)?word -> rest
      const simpleMatch = beforeCursor.match(/([\w]+\.)?([\w]+)\s*->\s*(.*)$/);

      if (simpleMatch) {
        const tableOrSchema = simpleMatch[1]
          ? simpleMatch[1].slice(0, -1)
          : undefined; // Remove trailing dot
        const columnName = simpleMatch[2];
        const afterOperator = simpleMatch[3];

        // Parse the path after the first ->
        // Example: "'key1'->>'key2'->" or "key1" or ""
        const jsonbPath: string[] = [];
        const pathRegex = /['"]?([\w]+)['"]?\s*->/g;
        let pathMatch;
        while ((pathMatch = pathRegex.exec(afterOperator)) !== null) {
          jsonbPath.push(pathMatch[1]);
        }

        // Get the current prefix (what's being typed after the last ->)
        // Remove any keys that are part of the path
        const lastArrowIndex = afterOperator.lastIndexOf('->');
        let currentPrefix = '';
        if (lastArrowIndex >= 0) {
          currentPrefix = afterOperator
            .substring(lastArrowIndex + 2)
            .trim()
            .replace(/['"]/g, '');
        } else {
          // No nested path, just get whatever is after the ->
          currentPrefix = afterOperator.trim().replace(/['"]/g, '');
        }

        return {
          schemaOrTable: tableOrSchema,
          jsonbColumn: columnName,
          jsonbPath,
          prefix: currentPrefix
        };
      }
    }

    // Three-part pattern: schema.table.column
    const threePartMatch = beforeCursor.match(/([\w]+)\.([\w]+)\.([\w]*)$/);
    if (threePartMatch) {
      return {
        schema: threePartMatch[1],
        tableName: threePartMatch[2],
        prefix: threePartMatch[3]
      };
    }

    // Two-part pattern: could be schema.table OR table.column
    // Backend will determine which by checking if first part is a schema
    const twoPartMatch = beforeCursor.match(/([\w]+)\.([\w]*)$/);
    if (twoPartMatch) {
      return {
        schemaOrTable: twoPartMatch[1],
        prefix: twoPartMatch[2]
      };
    }

    // Single word: could be a table name OR a column name
    // Check if there's a FROM clause in the query to determine context
    const wordMatch = beforeCursor.match(/[\w]+$/);
    const prefix = wordMatch ? wordMatch[0] : '';

    // Look for FROM clause in the entire text (before or after cursor)
    // Match patterns like: FROM table, FROM schema.table, FROM table AS alias
    const fullText = text.toLowerCase();
    const fromMatch = fullText.match(/\bfrom\s+([\w]+\.)?[\w]+/);

    if (fromMatch) {
      // Extract the table name (with optional schema)
      const fromClause = fromMatch[0];
      const tableMatch = fromClause.match(/\bfrom\s+(?:([\w]+)\.)?([\w]+)/);

      if (tableMatch) {
        const schema = tableMatch[1];
        const table = tableMatch[2];

        // If we have a schema, return schema.table pattern
        if (schema) {
          return {
            schema,
            tableName: table,
            prefix
          };
        }

        // Otherwise, return table as schemaOrTable (backend will check if it's a table)
        return {
          schemaOrTable: table,
          prefix
        };
      }
    }

    // No FROM clause found, just return prefix (will suggest tables)
    return {
      prefix
    };
  }

  /**
   * Get cached completion items if still valid.
   */
  private _getCached(prefix: string): ICompletionItem[] | null {
    const key = prefix.toLowerCase();
    const entry = this._cache.get(key);

    if (!entry) {
      return null;
    }

    const age = Date.now() - entry.timestamp;
    if (age > this._cacheTTL) {
      this._cache.delete(key);
      return null;
    }

    return entry.items;
  }

  /**
   * Format completion items into the reply format expected by JupyterLab.
   */
  private _formatReply(
    items: ICompletionItem[],
    offset: number,
    prefix: string
  ): CompletionHandler.ICompletionItemsReply {
    const start = offset - prefix.length;
    const end = offset;

    const formattedItems = items.map(item => {
      let label = item.name;
      let insertText = item.name;

      // Add quotes around JSONB keys
      if (item.type === 'jsonb_key') {
        insertText = `'${item.name}'`;
      }

      // Add table context for columns
      if (item.type === 'column' && item.table) {
        label = `${item.name} (${item.table})`;
      }

      // Add type-specific icon
      let typeIcon = '📊'; // Default for columns
      let sortText = item.name; // Default sort order

      if (item.type === 'table') {
        typeIcon = '📋';
      } else if (item.type === 'view') {
        typeIcon = '👁️';
      } else if (item.type === 'jsonb_key') {
        typeIcon = '🔑';
        // Use 0000 prefix to sort JSONB keys to the top (numbers sort before letters)
        sortText = `0000${item.name}`;
      }

      // Build documentation
      let documentation: string | undefined;
      if (item.type === 'column' && item.dataType && item.table) {
        documentation = `${item.table}.${item.name}: ${item.dataType}`;
      } else if (item.type === 'jsonb_key' && item.keyPath) {
        documentation = `JSONB key: ${item.keyPath.join(' -> ')}`;
      }

      return {
        label: `${typeIcon} ${label}`,
        insertText,
        sortText,
        type: item.type,
        documentation
      };
    });

    return {
      start,
      end,
      items: formattedItems
    };
  }

  /**
   * Clear the completion cache.
   */
  clearCache(): void {
    this._cache.clear();
  }
}
