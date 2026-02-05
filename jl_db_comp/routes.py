import hashlib
import json
import threading
import time
from contextlib import contextmanager
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
    import psycopg2.pool
    from psycopg2 import sql as pgsql
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

_pools: dict = {}
_pool_lock = threading.Lock()


@contextmanager
def _pooled_connection(db_url: str):
    """Borrow a connection from a per-URL pool, return it on exit.

    Creates the pool lazily on first use (minconn=1, maxconn=5).
    Sets ``autocommit=True`` since all queries are read-only.
    On ``OperationalError`` the connection is discarded instead of returned.
    """
    conn = None
    discard = False
    try:
        with _pool_lock:
            if db_url not in _pools:
                _pools[db_url] = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1, maxconn=5, dsn=db_url,
                )
        conn = _pools[db_url].getconn()
        conn.autocommit = True
        yield conn
    except psycopg2.OperationalError:
        discard = True
        raise
    finally:
        if conn is not None:
            try:
                with _pool_lock:
                    pool = _pools.get(db_url)
                    if pool:
                        pool.putconn(conn, close=discard)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Metadata cache (TTL = 120 s)
# ---------------------------------------------------------------------------

class _MetadataCache:
    """Thread-safe TTL cache for database metadata."""

    def __init__(self, ttl_seconds: float = 120.0):
        self._ttl = ttl_seconds
        self._data: dict = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        """Return cached value, or ``None`` if missing / expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._data[key]
                return None
            return value

    def put(self, key: str, value):
        """Store *value* with TTL starting now."""
        with self._lock:
            self._data[key] = (value, time.monotonic() + self._ttl)

    def clear(self):
        """Drop every entry."""
        with self._lock:
            self._data.clear()


_cache = _MetadataCache(ttl_seconds=120)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _url_id(db_url: str) -> str:
    """Short hash of a connection URL for use in cache keys."""
    return hashlib.sha256(db_url.encode()).hexdigest()[:12]


def _filter_by_prefix(items: list, prefix: str) -> list:
    """Return items whose ``name`` starts with *prefix* (case-insensitive)."""
    if not prefix:
        return items
    lp = prefix.lower()
    return [item for item in items if item["name"].lower().startswith(lp)]


def _jsonb_path_expr(column: str, path: list = None):
    """Build a safe ``psycopg2.sql.Composable`` for a JSONB path.

    ``_jsonb_path_expr("meta", ["a", "b"])`` produces
    ``"meta"->'a'->'b'``.
    """
    expr = pgsql.Identifier(column)
    for key in (path or []):
        expr = pgsql.SQL("{0}->{1}").format(expr, pgsql.Literal(key))
    return expr


def _jsonb_path_display(column: str, path: list = None) -> str:
    """Human-readable version of the JSONB path (for diagnostics JSON)."""
    result = column
    for key in (path or []):
        result = f"{result}->'{key}'"
    return result


# ---------------------------------------------------------------------------
# Completions handler
# ---------------------------------------------------------------------------

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
        - table: Optional table name to filter columns
        - schema_or_table: Ambiguous identifier
        - jsonb_column: JSONB column for key extraction
        - jsonb_path: JSON-encoded path array for nested JSONB
        - connections_file: Custom path to connections.ini
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

    # -- core logic ---------------------------------------------------------

    def _fetch_completions(
        self,
        db_url: str,
        schema: str,
        prefix: str,
        table: str = None,
        schema_or_table: str = None,
        jsonb_column: str = None,
        jsonb_path: list = None,
    ) -> dict:
        """Fetch table and column names from PostgreSQL.

        Results are cached server-side (120 s TTL).  Prefix filtering is
        done in Python after the cache lookup — SQL queries fetch full
        result sets so that subsequent keystrokes hit the cache.
        """
        uid = _url_id(db_url)

        # --- JSONB key extraction ---
        if jsonb_column:
            cache_key = (
                f"jsonb:{uid}:{schema}:{schema_or_table or ''}:"
                f"{jsonb_column}:{json.dumps(jsonb_path or [])}"
            )
            all_keys = _cache.get(cache_key)
            if all_keys is None:
                with _pooled_connection(db_url) as conn:
                    cur = conn.cursor()
                    all_keys = self._fetch_jsonb_keys(
                        cur, schema, schema_or_table, jsonb_column, jsonb_path,
                    )
                    cur.close()
                _cache.put(cache_key, all_keys)
            return {
                "status": "success",
                "tables": [],
                "columns": [],
                "jsonbKeys": _filter_by_prefix(all_keys, prefix),
            }

        # --- schema_or_table disambiguation ---
        if schema_or_table:
            schema_ck = f"is_schema:{uid}:{schema_or_table.lower()}"
            is_schema = _cache.get(schema_ck)
            if is_schema is None:
                with _pooled_connection(db_url) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT 1 FROM information_schema.schemata "
                        "WHERE LOWER(schema_name) = %s",
                        (schema_or_table.lower(),),
                    )
                    is_schema = cur.fetchone() is not None
                    cur.close()
                _cache.put(schema_ck, is_schema)

            if is_schema:
                return self._tables_in_schema(uid, db_url, schema_or_table, prefix)
            return self._columns_of_table(uid, db_url, schema, schema_or_table, prefix)

        # --- explicit table → columns ---
        if table:
            return self._columns_of_table(uid, db_url, schema, table, prefix)

        # --- default: list tables in schema ---
        return self._tables_in_schema(uid, db_url, schema, prefix)

    # -- helpers ------------------------------------------------------------

    def _tables_in_schema(self, uid, db_url, schema, prefix):
        """Return tables/views in *schema*, filtered by *prefix*."""
        cache_key = f"tables:{uid}:{schema}"
        all_tables = _cache.get(cache_key)
        if all_tables is None:
            with _pooled_connection(db_url) as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT table_name, table_type "
                    "FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "  AND table_type IN ('BASE TABLE', 'VIEW') "
                    "ORDER BY table_name",
                    (schema,),
                )
                all_tables = [
                    {"name": r[0], "type": "view" if r[1] == "VIEW" else "table"}
                    for r in cur.fetchall()
                ]
                cur.close()
            _cache.put(cache_key, all_tables)
        return {
            "status": "success",
            "tables": _filter_by_prefix(all_tables, prefix),
            "columns": [],
        }

    def _columns_of_table(self, uid, db_url, schema, table, prefix):
        """Return columns of *table*, filtered by *prefix*."""
        cache_key = f"columns:{uid}:{schema}:{table.lower()}"
        all_cols = _cache.get(cache_key)
        if all_cols is None:
            with _pooled_connection(db_url) as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT table_name, column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s "
                    "  AND LOWER(table_name) = %s "
                    "ORDER BY ordinal_position",
                    (schema, table.lower()),
                )
                all_cols = [
                    {
                        "name": r[1],
                        "table": r[0],
                        "dataType": r[2],
                        "type": "column",
                    }
                    for r in cur.fetchall()
                ]
                cur.close()
            _cache.put(cache_key, all_cols)
        return {
            "status": "success",
            "tables": [],
            "columns": _filter_by_prefix(all_cols, prefix),
        }

    # -- JSONB key extraction -----------------------------------------------

    def _fetch_jsonb_keys(
        self,
        cursor,
        schema: str,
        table_name: str,
        jsonb_column: str,
        jsonb_path: list = None,
    ) -> list:
        """Extract unique JSONB keys from a column.

        Returns the full (unfiltered) list — the caller applies prefix
        filtering.  Uses bounded sub-queries so that at most 100 rows
        are scanned for diagnostics and 1 000 for key extraction.
        """
        try:
            # If no table specified, find the first table with this JSONB column
            if not table_name:
                cursor.execute(
                    "SELECT table_name "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s "
                    "  AND LOWER(column_name) = %s "
                    "  AND data_type = 'jsonb' "
                    "LIMIT 1",
                    (schema, jsonb_column.lower()),
                )
                result = cursor.fetchone()
                if not result:
                    self.log.warning(
                        f"JSONB completion: No JSONB column '{jsonb_column}' "
                        f"found in schema '{schema}'. Verify the column exists "
                        f"and has data_type='jsonb'."
                    )
                    return []
                table_name = result[0]
                self.log.info(
                    f"JSONB completion: Found column '{jsonb_column}' in "
                    f"table '{schema}.{table_name}'"
                )

            path = _jsonb_path_expr(jsonb_column, jsonb_path)
            sch = pgsql.Identifier(schema)
            tbl = pgsql.Identifier(table_name)

            # Diagnostic sample (100 rows) — bounded via sub-query
            cursor.execute(
                pgsql.SQL(
                    "SELECT "
                    "  COUNT(*), "
                    "  COUNT(jval), "
                    "  COUNT(CASE WHEN jsonb_typeof(jval) = 'object' THEN 1 END), "
                    "  COUNT(CASE WHEN jsonb_typeof(jval) = 'array' THEN 1 END), "
                    "  COUNT(CASE WHEN jsonb_typeof(jval) "
                    "    IN ('string','number','boolean') THEN 1 END) "
                    "FROM (SELECT {0} AS jval FROM {1}.{2} LIMIT 100) sub"
                ).format(path, sch, tbl)
            )
            diag = cursor.fetchone()
            _, non_null, obj_count, arr_count, scalar_count = diag

            if non_null == 0:
                self.log.warning(
                    f"JSONB completion: Column '{jsonb_column}' in "
                    f"'{schema}.{table_name}' has no non-NULL values at "
                    f"path '{_jsonb_path_display(jsonb_column, jsonb_path)}'. "
                    f"Keys cannot be extracted from NULL data."
                )
                return []

            if obj_count == 0:
                type_info = []
                if arr_count > 0:
                    type_info.append(f"{arr_count} arrays")
                if scalar_count > 0:
                    type_info.append(f"{scalar_count} scalars")
                self.log.warning(
                    f"JSONB completion: Path "
                    f"'{_jsonb_path_display(jsonb_column, jsonb_path)}' in "
                    f"'{schema}.{table_name}' contains no JSON objects "
                    f"(found: {', '.join(type_info) if type_info else 'only NULL'}). "
                    f"Keys can only be extracted from object types."
                )
                return []

            # Key extraction — scan at most 1 000 qualifying rows
            cursor.execute(
                pgsql.SQL(
                    "SELECT DISTINCT jsonb_object_keys(jval) "
                    "FROM ("
                    "  SELECT {0} AS jval FROM {1}.{2} "
                    "  WHERE {0} IS NOT NULL "
                    "    AND jsonb_typeof({0}) = 'object' "
                    "  LIMIT 1000"
                    ") sub"
                ).format(path, sch, tbl)
            )
            keys = cursor.fetchall()

            if len(keys) == 0:
                self.log.warning(
                    f"JSONB completion: No keys found at path "
                    f"'{_jsonb_path_display(jsonb_column, jsonb_path)}' in "
                    f"'{schema}.{table_name}' despite {obj_count} objects. "
                    f"Objects may be empty {{}}."
                )
                return []

            self.log.info(
                f"JSONB completion: Found {len(keys)} unique keys at "
                f"'{_jsonb_path_display(jsonb_column, jsonb_path)}' in "
                f"'{schema}.{table_name}' (sampled {obj_count} objects)"
            )

            return [
                {
                    "name": r[0],
                    "type": "jsonb_key",
                    "keyPath": (jsonb_path or []) + [r[0]],
                }
                for r in keys
            ]

        except psycopg2.Error as e:
            self.log.error(f"JSONB key extraction error: {str(e).split(chr(10))[0]}")
            return []


# ---------------------------------------------------------------------------
# JSONB diagnostics handler
# ---------------------------------------------------------------------------

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
        with _pooled_connection(db_url) as conn:
            cursor = conn.cursor()

            result = {
                "status": "success",
                "schema": schema,
                "jsonbColumns": [],
                "columnDiagnostics": None
            }

            # Find all JSONB columns in the schema
            query_params = [schema]
            query = (
                "SELECT table_name, column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = %s "
                "  AND data_type = 'jsonb'"
            )
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

                path_expr = _jsonb_path_expr(actual_column, jsonb_path)
                sch_id = pgsql.Identifier(schema)
                tbl_id = pgsql.Identifier(actual_table)

                # Get type distribution
                diag_query = pgsql.SQL(
                    "SELECT "
                    "  COUNT(*) AS total_rows, "
                    "  COUNT({0}) AS non_null_count, "
                    "  COUNT(CASE WHEN jsonb_typeof({0}) = 'object' THEN 1 END) AS object_count, "
                    "  COUNT(CASE WHEN jsonb_typeof({0}) = 'array' THEN 1 END) AS array_count, "
                    "  COUNT(CASE WHEN jsonb_typeof({0}) = 'string' THEN 1 END) AS string_count, "
                    "  COUNT(CASE WHEN jsonb_typeof({0}) = 'number' THEN 1 END) AS number_count, "
                    "  COUNT(CASE WHEN jsonb_typeof({0}) = 'boolean' THEN 1 END) AS boolean_count, "
                    "  COUNT(CASE WHEN jsonb_typeof({0}) = 'null' THEN 1 END) AS json_null_count "
                    "FROM {1}.{2}"
                ).format(path_expr, sch_id, tbl_id)

                cursor.execute(diag_query)
                diag = cursor.fetchone()

                result["columnDiagnostics"] = {
                    "table": actual_table,
                    "column": actual_column,
                    "pathExpression": _jsonb_path_display(actual_column, jsonb_path),
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
                        key_query = pgsql.SQL(
                            "SELECT DISTINCT jsonb_object_keys(jval) "
                            "FROM ("
                            "  SELECT {0} AS jval FROM {1}.{2} "
                            "  WHERE {0} IS NOT NULL "
                            "    AND jsonb_typeof({0}) = 'object' "
                            "  LIMIT 1000"
                            ") sub"
                        ).format(path_expr, sch_id, tbl_id)
                        cursor.execute(key_query)
                        keys = [row[0] for row in cursor.fetchall()]
                        result["columnDiagnostics"]["sampleKeys"] = keys[:20]
                    except psycopg2.Error:
                        result["columnDiagnostics"]["sampleKeys"] = []

            cursor.close()
            return result

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


# ---------------------------------------------------------------------------
# Connections handler
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------

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
