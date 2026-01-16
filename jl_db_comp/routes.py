import json
from urllib.parse import unquote

from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
import tornado

from .connections import (
    find_connections_file,
    get_connection_url,
    list_connections,
)

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


class PostgresCompletionsHandler(APIHandler):
    """Handler for fetching PostgreSQL table and column completions."""

    @tornado.web.authenticated
    def get(self):
        """Fetch completions from PostgreSQL database.

        Query parameters:
        - connection: Connection name from connections.ini (preferred)
        - db_url: URL-encoded PostgreSQL connection string (fallback)
        - prefix: Optional prefix to filter results
        - schema: Database schema (default: 'public')
        - table: Optional table name to filter columns (only returns columns from this table)
        - schema_or_table: Ambiguous identifier - backend determines if it's a schema or table
        """
        if not PSYCOPG2_AVAILABLE:
            self.set_status(500)
            self.finish(json.dumps({
                "status": "error",
                "message": "psycopg2 is not installed. Install with: pip install psycopg2-binary"
            }))
            return

        try:
            connection_name = self.get_argument('connection', None)
            connections_file = self.get_argument('connections_file', None)
            db_url = self.get_argument('db_url', None)
            prefix = self.get_argument('prefix', '').lower()
            schema = self.get_argument('schema', 'public')
            table = self.get_argument('table', None)
            schema_or_table = self.get_argument('schema_or_table', None)
            jsonb_column = self.get_argument('jsonb_column', None)
            jsonb_path_str = self.get_argument('jsonb_path', None)

            # Priority: connection name -> db_url parameter
            if connection_name:
                db_url = get_connection_url(connection_name, connections_file)
                if not db_url:
                    file_info = f" (searched in: {connections_file})" if connections_file else ""
                    self.finish(json.dumps({
                        "status": "error",
                        "tables": [],
                        "columns": [],
                        "jsonbKeys": [],
                        "message": f"Connection '{connection_name}' not found in connections.ini{file_info}"
                    }))
                    return
            elif db_url:
                db_url = unquote(db_url)

            if not db_url:
                self.finish(json.dumps({
                    "status": "success",
                    "tables": [],
                    "columns": [],
                    "jsonbKeys": [],
                    "message": "No connection specified. Configure a connection in connections.ini or provide connection name."
                }))
                return

            # Parse JSON path if provided
            jsonb_path = None
            if jsonb_path_str:
                try:
                    jsonb_path = json.loads(jsonb_path_str)
                except json.JSONDecodeError:
                    jsonb_path = []

            completions = self._fetch_completions(
                db_url, schema, prefix, table, schema_or_table, jsonb_column, jsonb_path
            )
            self.finish(json.dumps(completions))

        except psycopg2.Error as e:
            error_msg = str(e).split('\n')[0]
            self.log.error(f"PostgreSQL error: {error_msg}")
            self.set_status(500)
            self.finish(json.dumps({
                "status": "error",
                "message": f"Database error: {error_msg}",
                "tables": [],
                "columns": []
            }))
        except Exception as e:
            error_msg = str(e)
            self.log.error(f"Completion handler error: {error_msg}")
            self.set_status(500)
            self.finish(json.dumps({
                "status": "error",
                "message": f"Server error: {error_msg}",
                "tables": [],
                "columns": []
            }))

    def _fetch_completions(
        self,
        db_url: str,
        schema: str,
        prefix: str,
        table: str = None,
        schema_or_table: str = None,
        jsonb_column: str = None,
        jsonb_path: list = None
    ) -> dict:
        """Fetch table and column names from PostgreSQL.

        Args:
            db_url: PostgreSQL connection string
            schema: Database schema name
            prefix: Filter prefix (case-insensitive)
            table: Optional table name to filter columns (only returns columns from this table)
            schema_or_table: Ambiguous identifier - determine if it's a schema or table
            jsonb_column: Optional JSONB column to extract keys from
            jsonb_path: Optional path for nested JSONB key extraction

        Returns:
            Dictionary with tables, columns, and jsonbKeys arrays
        """
        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()

            tables = []
            columns = []
            jsonb_keys = []

            # Handle JSONB key extraction
            if jsonb_column:
                jsonb_keys = self._fetch_jsonb_keys(
                    cursor, schema, schema_or_table, jsonb_column, jsonb_path, prefix
                )
                cursor.close()
                return {
                    "status": "success",
                    "tables": [],
                    "columns": [],
                    "jsonbKeys": jsonb_keys
                }

            # Handle schema_or_table: check if it's a schema first, then try as table
            if schema_or_table:
                # First, check if it's a schema
                cursor.execute("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE LOWER(schema_name) = %s
                """, (schema_or_table.lower(),))

                is_schema = cursor.fetchone() is not None

                if is_schema:
                    # It's a schema - fetch tables and views from that schema
                    cursor.execute("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema = %s
                          AND table_type IN ('BASE TABLE', 'VIEW')
                          AND LOWER(table_name) LIKE %s
                        ORDER BY table_name
                    """, (schema_or_table, f"{prefix}%"))

                    tables = [
                        {
                            "name": row[0],
                            "type": "view" if row[1] == 'VIEW' else "table"
                        }
                        for row in cursor.fetchall()
                    ]
                else:
                    # Not a schema - treat as table name, fetch columns from default schema
                    cursor.execute("""
                        SELECT table_name, column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s
                          AND LOWER(table_name) = %s
                          AND LOWER(column_name) LIKE %s
                        ORDER BY ordinal_position
                    """, (schema, schema_or_table.lower(), f"{prefix}%"))

                    columns = [
                        {
                            "name": row[1],
                            "table": row[0],
                            "dataType": row[2],
                            "type": "column"
                        }
                        for row in cursor.fetchall()
                    ]

            # If table is specified with explicit schema, fetch columns from that table
            elif table:
                cursor.execute("""
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND LOWER(table_name) = %s
                      AND LOWER(column_name) LIKE %s
                    ORDER BY ordinal_position
                """, (schema, table.lower(), f"{prefix}%"))

                columns = [
                    {
                        "name": row[1],
                        "table": row[0],
                        "dataType": row[2],
                        "type": "column"
                    }
                    for row in cursor.fetchall()
                ]
            else:
                # No table or schema_or_table specified - fetch tables and views from default schema
                cursor.execute("""
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_type IN ('BASE TABLE', 'VIEW')
                      AND LOWER(table_name) LIKE %s
                    ORDER BY table_name
                """, (schema, f"{prefix}%"))

                tables = [
                    {
                        "name": row[0],
                        "type": "view" if row[1] == 'VIEW' else "table"
                    }
                    for row in cursor.fetchall()
                ]

            cursor.close()

            return {
                "status": "success",
                "tables": tables,
                "columns": columns
            }

        finally:
            if conn:
                conn.close()

    def _fetch_jsonb_keys(
        self,
        cursor,
        schema: str,
        table_name: str,
        jsonb_column: str,
        jsonb_path: list = None,
        prefix: str = ''
    ) -> list:
        """Extract unique JSONB keys from a column in a table.

        Args:
            cursor: Database cursor
            schema: Database schema
            table_name: Table containing the JSONB column (can be None)
            jsonb_column: Name of the JSONB column
            jsonb_path: Optional path for nested keys (e.g., ['user', 'profile'])
            prefix: Filter prefix for keys

        Returns:
            List of JSONB key completion items
        """
        try:
            # If no table specified, find tables with this JSONB column
            if not table_name:
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND LOWER(column_name) = %s
                      AND data_type = 'jsonb'
                    LIMIT 1
                """, (schema, jsonb_column.lower()))

                result = cursor.fetchone()
                if not result:
                    self.log.warning(
                        f"JSONB completion: No JSONB column '{jsonb_column}' found "
                        f"in schema '{schema}'. Verify the column exists and has "
                        f"data_type='jsonb'."
                    )
                    return []

                table_name = result[0]
                self.log.info(
                    f"JSONB completion: Found column '{jsonb_column}' in "
                    f"table '{schema}.{table_name}'"
                )

            # Build the JSONB path expression
            if jsonb_path and len(jsonb_path) > 0:
                # For nested paths: column->>'key1'->>'key2'
                path_expr = jsonb_column
                for key in jsonb_path:
                    path_expr = f"{path_expr}->'{key}'"
            else:
                # For top-level keys: just the column
                path_expr = jsonb_column

            # First, check the data distribution at this path for diagnostics
            diag_query = f"""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT({path_expr}) as non_null_count,
                    COUNT(CASE WHEN jsonb_typeof({path_expr}) = 'object' THEN 1 END) as object_count,
                    COUNT(CASE WHEN jsonb_typeof({path_expr}) = 'array' THEN 1 END) as array_count,
                    COUNT(CASE WHEN jsonb_typeof({path_expr}) IN ('string', 'number', 'boolean') THEN 1 END) as scalar_count
                FROM {schema}.{table_name}
                LIMIT 1000
            """
            cursor.execute(diag_query)
            diag = cursor.fetchone()

            total_rows, non_null, obj_count, arr_count, scalar_count = diag

            if non_null == 0:
                self.log.warning(
                    f"JSONB completion: Column '{jsonb_column}' in "
                    f"'{schema}.{table_name}' has no non-NULL values at "
                    f"path '{path_expr}'. Keys cannot be extracted from NULL data."
                )
                return []

            if obj_count == 0:
                type_info = []
                if arr_count > 0:
                    type_info.append(f"{arr_count} arrays")
                if scalar_count > 0:
                    type_info.append(f"{scalar_count} scalars")
                self.log.warning(
                    f"JSONB completion: Path '{path_expr}' in "
                    f"'{schema}.{table_name}' contains no JSON objects "
                    f"(found: {', '.join(type_info) if type_info else 'only NULL'}). "
                    f"Keys can only be extracted from object types."
                )
                return []

            # Query to extract unique keys
            # LIMIT to 1000 rows for performance (sample the table)
            query = f"""
                SELECT DISTINCT jsonb_object_keys({path_expr})
                FROM {schema}.{table_name}
                WHERE {path_expr} IS NOT NULL
                  AND jsonb_typeof({path_expr}) = 'object'
                LIMIT 1000
            """

            cursor.execute(query)
            keys = cursor.fetchall()

            if len(keys) == 0:
                self.log.warning(
                    f"JSONB completion: No keys found at path '{path_expr}' in "
                    f"'{schema}.{table_name}' despite {obj_count} objects. "
                    f"Objects may be empty {{}}."
                )
                return []

            # Filter by prefix and format results
            result = []
            for row in keys:
                key = row[0]
                if key.lower().startswith(prefix):
                    result.append({
                        "name": key,
                        "type": "jsonb_key",
                        "keyPath": (jsonb_path or []) + [key]
                    })

            self.log.info(
                f"JSONB completion: Found {len(keys)} unique keys at '{path_expr}' "
                f"in '{schema}.{table_name}' (sampled {obj_count} objects)"
            )

            return result

        except psycopg2.Error as e:
            self.log.error(f"JSONB key extraction error: {str(e).split(chr(10))[0]}")
            return []


class JsonbDiagnosticsHandler(APIHandler):
    """Handler for diagnosing JSONB column issues."""

    @tornado.web.authenticated
    def get(self):
        """Get diagnostic information about JSONB columns.

        Query parameters:
        - connection: Connection name from connections.ini (preferred)
        - db_url: URL-encoded PostgreSQL connection string (fallback)
        - schema: Database schema (default: 'public')
        - table: Optional table name to check
        - column: Optional JSONB column name to check
        - jsonb_path: Optional JSON-encoded path array for nested diagnostics
        """
        if not PSYCOPG2_AVAILABLE:
            self.set_status(500)
            self.finish(json.dumps({
                "status": "error",
                "message": "psycopg2 is not installed"
            }))
            return

        try:
            connection_name = self.get_argument('connection', None)
            db_url = self.get_argument('db_url', None)
            schema = self.get_argument('schema', 'public')
            table = self.get_argument('table', None)
            column = self.get_argument('column', None)
            jsonb_path_str = self.get_argument('jsonb_path', None)

            # Priority: connection name -> db_url parameter
            if connection_name:
                db_url = get_connection_url(connection_name)
                if not db_url:
                    self.finish(json.dumps({
                        "status": "error",
                        "message": f"Connection '{connection_name}' not found in connections.ini"
                    }))
                    return
            elif db_url:
                db_url = unquote(db_url)

            if not db_url:
                self.finish(json.dumps({
                    "status": "error",
                    "message": "No connection specified. Configure a connection in connections.ini."
                }))
                return

            jsonb_path = None
            if jsonb_path_str:
                try:
                    jsonb_path = json.loads(jsonb_path_str)
                except json.JSONDecodeError:
                    jsonb_path = []

            diagnostics = self._get_diagnostics(
                db_url, schema, table, column, jsonb_path
            )
            self.finish(json.dumps(diagnostics))

        except psycopg2.Error as e:
            error_msg = str(e).split('\n')[0]
            self.log.error(f"JSONB diagnostics error: {error_msg}")
            self.set_status(500)
            self.finish(json.dumps({
                "status": "error",
                "message": f"Database error: {error_msg}"
            }))
        except Exception as e:
            error_msg = str(e)
            self.log.error(f"JSONB diagnostics error: {error_msg}")
            self.set_status(500)
            self.finish(json.dumps({
                "status": "error",
                "message": f"Server error: {error_msg}"
            }))

    def _get_diagnostics(
        self,
        db_url: str,
        schema: str,
        table: str = None,
        column: str = None,
        jsonb_path: list = None
    ) -> dict:
        """Get diagnostic information about JSONB columns."""
        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()

            result = {
                "status": "success",
                "schema": schema,
                "jsonbColumns": [],
                "columnDiagnostics": None
            }

            # Find all JSONB columns in the schema
            query_params = [schema]
            query = """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND data_type = 'jsonb'
            """
            if table:
                query += " AND LOWER(table_name) = %s"
                query_params.append(table.lower())
            if column:
                query += " AND LOWER(column_name) = %s"
                query_params.append(column.lower())

            query += " ORDER BY table_name, column_name"

            cursor.execute(query, query_params)
            jsonb_columns = cursor.fetchall()

            result["jsonbColumns"] = [
                {"table": row[0], "column": row[1]}
                for row in jsonb_columns
            ]

            # If specific table and column provided, get detailed diagnostics
            if table and column and len(jsonb_columns) > 0:
                actual_table = jsonb_columns[0][0]
                actual_column = jsonb_columns[0][1]

                # Build path expression
                if jsonb_path and len(jsonb_path) > 0:
                    path_expr = actual_column
                    for key in jsonb_path:
                        path_expr = f"{path_expr}->'{key}'"
                else:
                    path_expr = actual_column

                # Get type distribution
                diag_query = f"""
                    SELECT
                        COUNT(*) as total_rows,
                        COUNT({path_expr}) as non_null_count,
                        COUNT(CASE WHEN jsonb_typeof({path_expr}) = 'object' THEN 1 END) as object_count,
                        COUNT(CASE WHEN jsonb_typeof({path_expr}) = 'array' THEN 1 END) as array_count,
                        COUNT(CASE WHEN jsonb_typeof({path_expr}) = 'string' THEN 1 END) as string_count,
                        COUNT(CASE WHEN jsonb_typeof({path_expr}) = 'number' THEN 1 END) as number_count,
                        COUNT(CASE WHEN jsonb_typeof({path_expr}) = 'boolean' THEN 1 END) as boolean_count,
                        COUNT(CASE WHEN jsonb_typeof({path_expr}) = 'null' THEN 1 END) as json_null_count
                    FROM {schema}.{actual_table}
                """
                cursor.execute(diag_query)
                diag = cursor.fetchone()

                result["columnDiagnostics"] = {
                    "table": actual_table,
                    "column": actual_column,
                    "pathExpression": path_expr,
                    "totalRows": diag[0],
                    "nonNullCount": diag[1],
                    "typeDistribution": {
                        "object": diag[2],
                        "array": diag[3],
                        "string": diag[4],
                        "number": diag[5],
                        "boolean": diag[6],
                        "null": diag[7]
                    },
                    "canExtractKeys": diag[2] > 0,
                    "recommendation": self._get_recommendation(diag)
                }

                # If there are objects, get sample keys
                if diag[2] > 0:
                    try:
                        key_query = f"""
                            SELECT DISTINCT jsonb_object_keys({path_expr})
                            FROM {schema}.{actual_table}
                            WHERE {path_expr} IS NOT NULL
                              AND jsonb_typeof({path_expr}) = 'object'
                            LIMIT 20
                        """
                        cursor.execute(key_query)
                        keys = [row[0] for row in cursor.fetchall()]
                        result["columnDiagnostics"]["sampleKeys"] = keys
                    except psycopg2.Error:
                        result["columnDiagnostics"]["sampleKeys"] = []

            cursor.close()
            return result

        finally:
            if conn:
                conn.close()

    def _get_recommendation(self, diag) -> str:
        """Generate a recommendation based on diagnostic data."""
        total, non_null, obj, arr, string, number, boolean, json_null = diag

        if non_null == 0:
            return (
                "All values are NULL. JSONB autocompletion requires non-NULL data. "
                "Check that the column contains actual JSON data."
            )

        if obj == 0:
            types_found = []
            if arr > 0:
                types_found.append(f"{arr} arrays")
            if string > 0:
                types_found.append(f"{string} strings")
            if number > 0:
                types_found.append(f"{number} numbers")
            if boolean > 0:
                types_found.append(f"{boolean} booleans")
            if json_null > 0:
                types_found.append(f"{json_null} JSON nulls")

            return (
                f"No JSON objects found. Found: {', '.join(types_found)}. "
                f"JSONB key extraction only works with object types ({{}}). "
                f"If your data contains arrays, you may need to navigate into "
                f"array elements first."
            )

        return f"JSONB autocompletion should work. Found {obj} objects with extractable keys."


class ConnectionsHandler(APIHandler):
    """Handler for listing available database connections."""

    @tornado.web.authenticated
    def get(self):
        """List available connections from connections.ini.

        Returns:
            JSON response with:
            - connections: Dictionary of available connections (without passwords)
            - file_path: Path to the connections.ini file found
        """
        try:
            connections = list_connections()
            file_path = find_connections_file()

            self.finish(json.dumps({
                "status": "success",
                "connections": connections,
                "filePath": str(file_path) if file_path else None
            }))

        except Exception as e:
            self.log.error(f"Error listing connections: {e}")
            self.set_status(500)
            self.finish(json.dumps({
                "status": "error",
                "message": f"Error reading connections: {str(e)}",
                "connections": {}
            }))


def setup_route_handlers(web_app):
    """Register route handlers with the Jupyter server."""
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]

    completions_route = url_path_join(base_url, "jl-db-comp", "completions")
    diagnostics_route = url_path_join(base_url, "jl-db-comp", "jsonb-diagnostics")
    connections_route = url_path_join(base_url, "jl-db-comp", "connections")

    handlers = [
        (completions_route, PostgresCompletionsHandler),
        (diagnostics_route, JsonbDiagnosticsHandler),
        (connections_route, ConnectionsHandler),
    ]

    web_app.add_handlers(host_pattern, handlers)
