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
 * @returns Array of completion items
 */
export async function fetchPostgresCompletions(
  dbUrl?: string,
  prefix = '',
  schema = 'public'
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

    const endpoint = `completions?${params.toString()}`;
    const response = await requestAPI<ICompletionsResponse>(endpoint, {
      method: 'GET'
    });

    if (response.status === 'error') {
      console.error('PostgreSQL completion error:', response.message);
      return [];
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
