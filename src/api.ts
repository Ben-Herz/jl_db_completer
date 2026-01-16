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
 * Connection configuration from connections.ini (without password).
 */
export interface IConnectionConfig {
  drivername?: string;
  username?: string;
  host?: string;
  port?: string;
  database?: string;
  has_password?: boolean;
}

/**
 * Response from the connections list API endpoint.
 */
export interface IConnectionsResponse {
  status: 'success' | 'error';
  connections: Record<string, IConnectionConfig>;
  filePath: string | null;
  message?: string;
}

/**
 * Fetch available database connections from connections.ini.
 *
 * @returns Dictionary of connection names to their config
 */
export async function fetchConnections(): Promise<IConnectionsResponse> {
  try {
    const response = await requestAPI<IConnectionsResponse>('connections', {
      method: 'GET'
    });
    return response;
  } catch (err) {
    if (err instanceof ServerConnection.ResponseError) {
      console.error(`Failed to fetch connections: ${err.message}`);
    } else {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      console.error(`Failed to fetch connections: ${msg}`);
    }
    return {
      status: 'error',
      connections: {},
      filePath: null,
      message: 'Failed to fetch connections'
    };
  }
}

/**
 * Fetch PostgreSQL table and column completions from the server.
 *
 * @param connectionName - Connection name from connections.ini
 * @param prefix - Optional prefix to filter completions
 * @param schema - Database schema name (default: 'public')
 * @param tableName - Optional table name to filter columns (only returns columns from this table)
 * @param schemaOrTable - Ambiguous identifier that could be either a schema or table name (backend will determine)
 * @param jsonbColumn - Optional JSONB column name to extract keys from
 * @param jsonbPath - Optional JSONB path for nested key extraction
 * @param connectionsFilePath - Optional custom path to connections.ini file
 * @returns Array of completion items
 */
export async function fetchPostgresCompletions(
  connectionName?: string,
  prefix = '',
  schema = 'public',
  tableName?: string,
  schemaOrTable?: string,
  jsonbColumn?: string,
  jsonbPath?: string[],
  connectionsFilePath?: string
): Promise<ICompletionItem[]> {
  try {
    const params = new URLSearchParams();
    if (connectionName) {
      params.append('connection', connectionName);
    }
    if (connectionsFilePath) {
      params.append('connections_file', connectionsFilePath);
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
