import {
  CompletionHandler,
  ICompletionContext,
  ICompletionProvider
} from '@jupyterlab/completer';
import { ISettingRegistry } from '@jupyterlab/settingregistry';
import { INotebookTracker } from '@jupyterlab/notebook';
import { KernelMessage } from '@jupyterlab/services';
import {
  fetchPostgresCompletions,
  fetchConnections,
  ICompletionItem
} from './api';

/**
 * Cache entry for PostgreSQL completions.
 */
interface ICacheEntry {
  items: ICompletionItem[];
  timestamp: number;
}

/**
 * Python code to get the active jupysql connection alias and dsn_filename.
 * Returns JSON with connection alias and absolute dsn_filename path.
 */
const GET_JUPYSQL_CONFIG_CODE = `
import json
import os
result = {'connection': '', 'dsn_filename': ''}

# Get active connection
try:
    from sql.connection import ConnectionManager
    conn = ConnectionManager.current
    if conn:
        for alias, c in ConnectionManager.connections.items():
            if c is conn:
                result['connection'] = alias
                break
except:
    pass

# Get dsn_filename from SqlMagic instance
dsn_filename = None
try:
    from sql.magic import SqlMagic
    ip = get_ipython()
    if ip:
        for name, inst in ip.magics_manager.registry.items():
            if isinstance(inst, SqlMagic):
                dsn_filename = inst.dsn_filename
                break
except:
    pass

# Fallback: try to get from config
if not dsn_filename:
    try:
        ip = get_ipython()
        if ip and hasattr(ip, 'config'):
            sql_config = ip.config.get('SqlMagic', {})
            if 'dsn_filename' in sql_config:
                dsn_filename = sql_config['dsn_filename']
    except:
        pass

# Convert to absolute path if we have a dsn_filename
if dsn_filename:
    if not os.path.isabs(dsn_filename):
        # Resolve relative to current working directory
        dsn_filename = os.path.abspath(dsn_filename)
    result['dsn_filename'] = dsn_filename

print(json.dumps(result))
`;

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
  private _notebookTracker: INotebookTracker | null = null;
  private _connectionName = '';
  private _schema = 'public';
  private _enabled = true;
  private _availableConnections: string[] = [];
  private _cachedKernelConfig: {
    connection: string;
    dsnFilename: string;
  } | null = null;
  private _kernelConfigCacheTime = 0;
  private _kernelConfigCacheTTL = 30 * 1000; // 30 seconds cache for kernel config

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
   * @param notebookTracker - Optional notebook tracker to query kernels
   */
  constructor(
    settings?: ISettingRegistry.ISettings | null,
    notebookTracker?: INotebookTracker | null
  ) {
    this._notebookTracker = notebookTracker || null;

    if (settings) {
      this._settings = settings;
      this._loadSettings();

      settings.changed.connect(() => {
        this._loadSettings();
      });
    }

    // Load available connections from backend
    this._loadAvailableConnections();
  }

  /**
   * Load database configuration from settings.
   */
  private _loadSettings(): void {
    if (!this._settings) {
      return;
    }

    this._connectionName = this._settings.get('connectionName')
      .composite as string;
    this._schema = this._settings.get('schema').composite as string;
    this._enabled = this._settings.get('enabled').composite as boolean;
  }

  /**
   * Load available connections from the backend.
   */
  private async _loadAvailableConnections(): Promise<void> {
    try {
      const response = await fetchConnections();
      if (response.status === 'success') {
        this._availableConnections = Object.keys(response.connections);
      }
    } catch (error) {
      console.warn('Failed to load available connections:', error);
    }
  }

  /**
   * Get jupysql configuration from the current notebook's kernel.
   * Returns both the active connection alias and the configured dsn_filename.
   *
   * @returns Object with connection and dsnFilename, or null if not available
   */
  private async _getKernelConfig(): Promise<{
    connection: string;
    dsnFilename: string;
  } | null> {
    // Check cache first
    const now = Date.now();
    if (
      this._cachedKernelConfig &&
      now - this._kernelConfigCacheTime < this._kernelConfigCacheTTL
    ) {
      return this._cachedKernelConfig;
    }

    if (!this._notebookTracker) {
      return null;
    }

    const notebook = this._notebookTracker.currentWidget;
    if (!notebook) {
      return null;
    }

    const kernel = notebook.sessionContext.session?.kernel;
    if (!kernel) {
      return null;
    }

    try {
      const future = kernel.requestExecute({
        code: GET_JUPYSQL_CONFIG_CODE,
        silent: true,
        store_history: false
      });

      const result = await new Promise<{
        connection: string;
        dsnFilename: string;
      } | null>(resolve => {
        let output = '';

        future.onIOPub = (msg: KernelMessage.IIOPubMessage) => {
          if (msg.header.msg_type === 'stream') {
            const content = msg.content as KernelMessage.IStreamMsg['content'];
            if (content.name === 'stdout') {
              output += content.text;
            }
          }
        };

        future.done
          .then(() => {
            try {
              const parsed = JSON.parse(output.trim());
              resolve({
                connection: parsed.connection || '',
                dsnFilename: parsed.dsn_filename || ''
              });
            } catch {
              resolve(null);
            }
          })
          .catch(() => {
            resolve(null);
          });
      });

      // Cache the result
      if (result) {
        this._cachedKernelConfig = result;
        this._kernelConfigCacheTime = now;
      }

      return result;
    } catch (error) {
      console.warn('Failed to get jupysql config from kernel:', error);
      return null;
    }
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
      // Get connection config from kernel (includes connection name and dsn_filename)
      const kernelConfig = await this._getKernelConfig();

      // Get connection: priority is settings -> kernel -> first available
      let connectionName = this._connectionName;
      let connectionsFilePath: string | undefined;

      if (kernelConfig) {
        // Use kernel's dsn_filename if available
        if (kernelConfig.dsnFilename) {
          connectionsFilePath = kernelConfig.dsnFilename;
        }
        // Use kernel's active connection if no settings override
        if (!connectionName && kernelConfig.connection) {
          connectionName = kernelConfig.connection;
        }
      }

      if (!connectionName && this._availableConnections.length > 0) {
        // Fall back to first available connection
        connectionName = this._availableConnections[0];
      }

      const items = await fetchPostgresCompletions(
        connectionName || undefined,
        extracted.prefix,
        extracted.schema || this._schema,
        extracted.tableName,
        extracted.schemaOrTable,
        extracted.jsonbColumn,
        extracted.jsonbPath,
        connectionsFilePath
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
