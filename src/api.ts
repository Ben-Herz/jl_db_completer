import { ServerConnection } from '@jupyterlab/services';
import { requestAPI } from './request';

/**
 * Database completion item representing a table, column, or JSONB key.
 */
export interface ICompletionItem {
  name: string;
  type: 'table' | 'column' | 'view' | 'jsonb_key';
  table?: string;
  dataType?: string;
  keyPath?: string[]; // For JSONB keys, the path to this key
}

/**
 * Response from the PostgreSQL completions API endpoint.
 */
export interface ICompletionsResponse {
  status: 'success' | 'error';
  tables: ICompletionItem[];
  columns: ICompletionItem[];
  jsonbKeys?: ICompletionItem[]; // JSONB keys from actual table data
  message?: string;
}

/**
 * Fetch PostgreSQL table and column completions from the server.
 *
 * @param dbUrl - PostgreSQL connection string (optional if using env var)
 * @param prefix - Optional prefix to filter completions
 * @param schema - Database schema name (default: 'public')
 * @param tableName - Optional table name to filter columns (only returns columns from this table)
 * @param schemaOrTable - Ambiguous identifier that could be either a schema or table name (backend will determine)
 * @param jsonbColumn - Optional JSONB column name to extract keys from
 * @param jsonbPath - Optional JSONB path for nested key extraction
 * @returns Array of completion items
 */
export async function fetchPostgresCompletions(
  dbUrl?: string,
  prefix = '',
  schema = 'public',
  tableName?: string,
  schemaOrTable?: string,
  jsonbColumn?: string,
  jsonbPath?: string[]
): Promise<ICompletionItem[]> {
  try {
    const params = new URLSearchParams();
    if (dbUrl) {
      params.append('db_url', encodeURIComponent(dbUrl));
    }
    if (prefix) {
      params.append('prefix', prefix);
    }
    params.append('schema', schema);
    if (tableName) {
      params.append('table', tableName);
    }
    if (schemaOrTable) {
      params.append('schema_or_table', schemaOrTable);
    }
    if (jsonbColumn) {
      params.append('jsonb_column', jsonbColumn);
      if (jsonbPath && jsonbPath.length > 0) {
        params.append('jsonb_path', JSON.stringify(jsonbPath));
      }
    }

    const endpoint = `completions?${params.toString()}`;
    const response = await requestAPI<ICompletionsResponse>(endpoint, {
      method: 'GET'
    });

    if (response.status === 'error') {
      console.error('PostgreSQL completion error:', response.message);
      return [];
    }

    // If JSONB keys requested, return only those
    if (jsonbColumn && response.jsonbKeys) {
      return response.jsonbKeys;
    }

    // Return appropriate results based on context
    if (tableName || schemaOrTable) {
      // If we have table context, prefer columns
      return response.columns.length > 0 ? response.columns : response.tables;
    }

    return [...response.tables, ...response.columns];
  } catch (err) {
    if (err instanceof ServerConnection.ResponseError) {
      const status = err.response.status;
      let detail = err.message;

      if (
        typeof detail === 'string' &&
        (detail.includes('<!DOCTYPE') || detail.includes('<html'))
      ) {
        detail = `HTML error page (${detail.substring(0, 100)}...)`;
      }

      console.error(`PostgreSQL completions API failed (${status}): ${detail}`);
    } else {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      console.error(`PostgreSQL completions API failed: ${msg}`);
    }

    return [];
  }
}
