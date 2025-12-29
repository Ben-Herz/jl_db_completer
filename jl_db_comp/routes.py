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

            if not db_url:
                db_url = os.environ.get('POSTGRES_URL')
            else:
                db_url = unquote(db_url)

            if not db_url:
                self.finish(json.dumps({
                    "status": "success",
                    "tables": [],
                    "columns": [],
                    "message": "No database URL provided"
                }))
                return

            completions = self._fetch_completions(db_url, schema, prefix, table, schema_or_table)
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

    def _fetch_completions(self, db_url: str, schema: str, prefix: str, table: str = None, schema_or_table: str = None) -> dict:
        """Fetch table and column names from PostgreSQL.

        Args:
            db_url: PostgreSQL connection string
            schema: Database schema name
            prefix: Filter prefix (case-insensitive)
            table: Optional table name to filter columns (only returns columns from this table)
            schema_or_table: Ambiguous identifier - determine if it's a schema or table

        Returns:
            Dictionary with tables and columns arrays
        """
        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()

            tables = []
            columns = []

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


def setup_route_handlers(web_app):
    """Register route handlers with the Jupyter server."""
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]

    completions_route = url_path_join(base_url, "jl-db-comp", "completions")
    handlers = [(completions_route, PostgresCompletionsHandler)]

    web_app.add_handlers(host_pattern, handlers)
