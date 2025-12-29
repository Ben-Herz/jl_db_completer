import { ServerConnection } from '@jupyterlab/services';
import { requestAPI } from './request';

/**
 * Database completion item representing a table or column.
 */
export interface ICompletionItem {
  name: string;
  type: 'table' | 'column';
  table?: string;
  dataType?: string;
}

/**
 * Response from the PostgreSQL completions API endpoint.
 */
export interface ICompletionsResponse {
  status: 'success' | 'error';
  tables: ICompletionItem[];
  columns: ICompletionItem[];
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
 * @returns Array of completion items
 */
export async function fetchPostgresCompletions(
  dbUrl?: string,
  prefix = '',
  schema = 'public',
  tableName?: string,
  schemaOrTable?: string
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

    const endpoint = `completions?${params.toString()}`;
    const response = await requestAPI<ICompletionsResponse>(endpoint, {
      method: 'GET'
    });

    if (response.status === 'error') {
      console.error('PostgreSQL completion error:', response.message);
      return [];
    }

    // Return appropriate results based on context
    if (tableName || schemaOrTable) {
      // If we have table context, prefer columns
      return response.columns.length > 0
        ? response.columns
        : response.tables;
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
