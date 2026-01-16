"""
Connection management module for jupysql connections.ini integration.

This module handles reading database connection configurations from jupysql's
connections.ini file format and building connection URLs from them.
"""

import configparser
import os
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus


# Default locations to search for connections.ini
DEFAULT_CONNECTIONS_PATHS = [
    Path.home() / '.jupysql' / 'connections.ini',  # jupysql default
    Path.cwd() / 'connections.ini',  # Current working directory
]


def find_connections_file(custom_path: Optional[str] = None) -> Optional[Path]:
    """Find the connections.ini file.

    Args:
        custom_path: Optional custom path to connections.ini file

    Returns:
        Path to the connections.ini file, or None if not found
    """
    # Check custom path first
    if custom_path:
        custom = Path(custom_path)
        if custom.exists():
            return custom

    # Check default locations
    for path in DEFAULT_CONNECTIONS_PATHS:
        if path.exists():
            return path

    return None


def parse_connections_file(file_path: Path) -> dict[str, dict[str, str]]:
    """Parse connections.ini file into a dictionary.

    Args:
        file_path: Path to the connections.ini file

    Returns:
        Dictionary mapping connection names to their configuration
    """
    config = configparser.ConfigParser()
    config.read(file_path)

    connections = {}
    for section in config.sections():
        connections[section] = dict(config[section])

    return connections


def build_connection_url(connection_config: dict[str, str]) -> str:
    """Build a database URL from connection configuration.

    Args:
        connection_config: Dictionary with connection parameters:
            - drivername: database backend (e.g., 'postgresql')
            - username: database user
            - password: database password
            - host: database host
            - port: database port
            - database: database name
            - query: optional query parameters (as string or dict)

    Returns:
        Connection URL string
    """
    drivername = connection_config.get('drivername', 'postgresql')
    username = connection_config.get('username', '')
    password = connection_config.get('password', '')
    host = connection_config.get('host', 'localhost')
    port = connection_config.get('port', '5432')
    database = connection_config.get('database', '')

    # URL encode username and password for special characters
    if username:
        username = quote_plus(username)
    if password:
        password = quote_plus(password)

    # Build the URL
    if username and password:
        auth = f'{username}:{password}@'
    elif username:
        auth = f'{username}@'
    else:
        auth = ''

    url = f'{drivername}://{auth}{host}:{port}/{database}'

    # Handle query parameters if present
    query = connection_config.get('query', '')
    if query:
        # Query could be a string representation of a dict or a plain string
        if isinstance(query, str) and query.startswith('{'):
            # Try to parse as dict-like string
            try:
                import ast
                query_dict = ast.literal_eval(query)
                if isinstance(query_dict, dict):
                    query_str = '&'.join(
                        f'{k}={v}' for k, v in query_dict.items()
                    )
                    url = f'{url}?{query_str}'
            except (ValueError, SyntaxError):
                pass
        elif isinstance(query, str) and query:
            url = f'{url}?{query}'

    return url


def get_connection_url(
    connection_name: str,
    connections_file_path: Optional[str] = None
) -> Optional[str]:
    """Get a connection URL by name from connections.ini.

    Args:
        connection_name: Name of the connection (section name in ini file)
        connections_file_path: Optional custom path to connections.ini

    Returns:
        Connection URL string, or None if connection not found
    """
    file_path = find_connections_file(connections_file_path)
    if not file_path:
        return None

    connections = parse_connections_file(file_path)
    if connection_name not in connections:
        return None

    return build_connection_url(connections[connection_name])


def list_connections(
    connections_file_path: Optional[str] = None
) -> dict[str, dict]:
    """List all available connections from connections.ini.

    Args:
        connections_file_path: Optional custom path to connections.ini

    Returns:
        Dictionary mapping connection names to their config (without password)
    """
    file_path = find_connections_file(connections_file_path)
    if not file_path:
        return {}

    connections = parse_connections_file(file_path)

    # Return connections without exposing passwords
    safe_connections = {}
    for name, config in connections.items():
        safe_config = {k: v for k, v in config.items() if k != 'password'}
        safe_config['has_password'] = 'password' in config
        safe_connections[name] = safe_config

    return safe_connections
