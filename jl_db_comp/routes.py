import json
import os
from urllib.parse import unquote

from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
import tornado

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
        - db_url: URL-encoded PostgreSQL connection string
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
            db_url = self.get_argument('db_url', None)
            prefix = self.get_argument('prefix', '').lower()
            schema = self.get_argument('schema', 'public')
            table = self.get_argument('table', None)
            schema_or_table = self.get_argument('schema_or_table', None)
            jsonb_column = self.get_argument('jsonb_column', None)
            jsonb_path_str = self.get_argument('jsonb_path', None)

            if not db_url:
                db_url = os.environ.get('POSTGRES_URL')
            else:
                db_url = unquote(db_url)

            if not db_url:
                self.finish(json.dumps({
                    "status": "success",
                    "tables": [],
                    "columns": [],
                    "jsonbKeys": [],
                    "message": "No database URL provided"
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
                    return []

                table_name = result[0]

            # Build the JSONB path expression
            if jsonb_path and len(jsonb_path) > 0:
                # For nested paths: column->>'key1'->>'key2'
                path_expr = jsonb_column
                for key in jsonb_path:
                    path_expr = f"{path_expr}->'{key}'"
            else:
                # For top-level keys: just the column
                path_expr = jsonb_column

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

            return result

        except psycopg2.Error as e:
            self.log.error(f"JSONB key extraction error: {str(e).split(chr(10))[0]}")
            return []


def setup_route_handlers(web_app):
    """Register route handlers with the Jupyter server."""
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]

    completions_route = url_path_join(base_url, "jl-db-comp", "completions")
    handlers = [(completions_route, PostgresCompletionsHandler)]

    web_app.add_handlers(host_pattern, handlers)
