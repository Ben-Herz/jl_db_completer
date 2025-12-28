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

            completions = self._fetch_completions(db_url, schema, prefix)
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

    def _fetch_completions(self, db_url: str, schema: str, prefix: str) -> dict:
        """Fetch table and column names from PostgreSQL.

        Args:
            db_url: PostgreSQL connection string
            schema: Database schema name
            prefix: Filter prefix (case-insensitive)

        Returns:
            Dictionary with tables and columns arrays
        """
        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()

            tables = []
            columns = []

            # Fetch table names
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                  AND LOWER(table_name) LIKE %s
                ORDER BY table_name
            """, (schema, f"{prefix}%"))

            tables = [{"name": row[0], "type": "table"} for row in cursor.fetchall()]

            # Fetch column names with their table names
            cursor.execute("""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND LOWER(column_name) LIKE %s
                ORDER BY table_name, ordinal_position
            """, (schema, f"{prefix}%"))

            columns = [
                {
                    "name": row[1],
                    "table": row[0],
                    "dataType": row[2],
                    "type": "column"
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
