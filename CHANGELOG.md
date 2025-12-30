# Changelog

<!-- <START NEW CHANGELOG ENTRY> -->

## 0.1.0 (2024-12-30)

Initial release of jl_db_comp - PostgreSQL autocomplete for JupyterLab.

### Features

- PostgreSQL table and column name autocompletion
- Schema-aware completion (supports multiple schemas)
- JSONB key completion with nested path navigation
- Automatic FROM clause parsing to suggest columns from referenced tables
- Client-side caching with 5-minute TTL
- Configurable database connection via environment variable or JupyterLab settings
- Smart SQL keyword detection (SELECT, FROM, JOIN, WHERE, etc.)

<!-- <END NEW CHANGELOG ENTRY> -->
