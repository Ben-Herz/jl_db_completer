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

    // Extract the word being typed (prefix)
    const prefix = this._extractPrefix(text, offset);

    // Check cache first
    const cached = this._getCached(prefix);
    if (cached) {
      return this._formatReply(cached, request.offset, prefix);
    }

    // Fetch from database
    try {
      const items = await fetchPostgresCompletions(
        this._dbUrl || undefined,
        prefix,
        this._schema
      );

      // Cache the results
      this._cache.set(prefix.toLowerCase(), {
        items,
        timestamp: Date.now()
      });

      return this._formatReply(items, request.offset, prefix);
    } catch (error) {
      console.error('Failed to fetch PostgreSQL completions:', error);
      return { start: request.offset, end: request.offset, items: [] };
    }
  }

  /**
   * Extract the word prefix being typed from the text at the given offset.
   */
  private _extractPrefix(text: string, offset: number): string {
    const beforeCursor = text.substring(0, offset);
    const match = beforeCursor.match(/[\w.]+$/);
    return match ? match[0] : '';
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

      // Add table context for columns
      if (item.type === 'column' && item.table) {
        label = `${item.name} (${item.table})`;
      }

      // Add type indicator
      const typeIcon = item.type === 'table' ? '📋' : '📊';

      return {
        label: `${typeIcon} ${label}`,
        insertText,
        type: item.type,
        documentation:
          item.type === 'column' && item.dataType
            ? `${item.table}.${item.name}: ${item.dataType}`
            : undefined
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
