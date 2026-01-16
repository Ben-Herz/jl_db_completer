# jl_db_comp

[![Github Actions Status](https://github.com/Ben-Herz/jl_db_completer/workflows/Build/badge.svg)](https://github.com/Ben-Herz/jl_db_completer/actions/workflows/build.yml)

A JupyterLab extension that provides PostgreSQL table and column name autocompletion when typing SQL queries in notebooks and editors.

## Features

- **Smart SQL Detection**: Automatically activates when SQL keywords (SELECT, FROM, JOIN, etc.) are detected
- **PostgreSQL Integration**: Queries table and column names from PostgreSQL databases
- **Client-Side Caching**: 5-minute TTL cache to minimize database calls
- **Configurable**: Database connection via environment variable or settings
- **Schema Support**: Query specific database schemas (defaults to 'public')

This extension is composed of a Python package named `jl_db_comp`
for the server extension and a NPM package named `jl_db_comp`
for the frontend extension.

## Requirements

- JupyterLab >= 4.0.0
- PostgreSQL database
- Python >= 3.10

## Install

To install the extension, execute:

```bash
pip install jl_db_comp
```

## Configuration

### Database Connection

There are two ways to configure your PostgreSQL database connection:

#### Option 1: Environment Variable (Recommended)

Set the `POSTGRES_URL` environment variable before starting JupyterLab:

```bash
export POSTGRES_URL="postgresql://user:password@localhost:5432/dbname"
jupyter lab
```

#### Option 2: JupyterLab Settings

1. Open JupyterLab Settings (Settings → Settings Editor)
2. Search for "PostgreSQL Database Completer"
3. Configure:
   - **Database URL**: PostgreSQL connection string
   - **Schema**: Database schema to query (default: 'public')
   - **Enabled**: Toggle completions on/off

### Connection String Format

```
postgresql://[user[:password]@][host][:port][/dbname]
```

**Examples:**

```bash
# Local database
postgresql://postgres:example@localhost:5432/ehrexample

# Remote database with SSL
postgresql://user:pass@db.example.com:5432/mydb?sslmode=require
```

## Usage

### Table Name Completion

Type SQL and press **Tab** or **Ctrl+Space** to see table names:

```sql
SELECT * FROM pat<Tab>
```

**Completions will show:**

- 📋 patients
- 📋 patient_visits
- 📋 patient_records

### Column Name Completion (After Table Name)

After typing a table name followed by a dot, you'll see **only the columns from that table**:

```sql
SELECT patients.<Tab>
```

**Completions will show columns from 'patients' table:**

- 📊 patient_id (patients)
- 📊 patient_name (patients)
- 📊 date_of_birth (patients)

```sql
-- Works with table aliases too:
SELECT p.<Tab>
FROM patients p
```

**Completions will show columns from 'p' (if it's a known table)**

### Smart Activation

Autocomplete activates when SQL keywords are detected:

- SELECT, FROM, JOIN, WHERE
- INSERT, UPDATE, DELETE
- GROUP BY, ORDER BY, etc.

### Schema-Aware Completion

Access tables and columns from different schemas:

```sql
-- List tables from a specific schema
SELECT * FROM custom_schema.<Tab>
```

**Completions will show tables and views from 'custom_schema':**

- 📋 users
- 📋 products
- 👁️ active_users (view)

```sql
-- Access columns from schema-qualified table
SELECT custom_schema.users.<Tab>
```

**Completions will show columns from custom_schema.users:**

- 📊 user_id (users)
- 📊 username (users)
- 📊 email (users)

### JSONB Key Completion

For JSONB columns, get autocomplete for JSON object keys after typing `->` or `->>`:

```sql
-- First-level JSONB keys
SELECT metadata-><Tab>
FROM patients
```

**Completions will show keys from the 'metadata' JSONB column:**

- 🔑 allergies
- 🔑 medications
- 🔑 diagnosis

```sql
-- Nested JSONB keys
SELECT metadata->>'diagnosis'-><Tab>
FROM patients
```

**Completions will show nested keys under 'diagnosis':**

- 🔑 code
- 🔑 description
- 🔑 date

**How it works:**

- Extension queries actual table data to extract unique JSONB keys
- Supports nested paths (e.g., `column->>'key1'->>'key2'->`
  )
- Samples up to 1000 rows for performance
- Works with table-qualified columns (e.g., `patients.metadata->`)

### Complete Example

```sql
-- Step 1: Type table name and press Tab (default schema)
SELECT * FROM pat<Tab>
-- → Suggests: patients, patient_visits

-- Step 2: After selecting "patients", type dot and press Tab
SELECT patients.<Tab>
-- → Shows columns: patient_id, patient_name, etc.

-- Step 3: JSONB key completion
SELECT patients.metadata-><Tab>
-- → Shows JSONB keys: allergies, medications, diagnosis

-- Step 4: Nested JSONB keys
SELECT patients.metadata->>'diagnosis'-><Tab>
-- → Shows nested keys: code, description, date

-- Step 5: With different schemas
SELECT
    public.patients.<Tab>,
    custom_schema.users.<Tab>
FROM public.patients
JOIN custom_schema.users ON patients.user_id = users.user_id
-- → "public.patients." shows patient columns
-- → "custom_schema.users." shows user columns

-- Step 6: List tables from specific schema
SELECT * FROM custom_schema.<Tab>
-- → Shows tables and views from custom_schema
```

## Uninstall

To remove the extension, execute:

```bash
pip uninstall jl_db_comp
```

## Troubleshoot

If you are seeing the frontend extension, but it is not working, check
that the server extension is enabled:

```bash
jupyter server extension list
```

If the server extension is installed and enabled, but you are not seeing
the frontend extension, check the frontend extension is installed:

```bash
jupyter labextension list
```

### JSONB Autocompletion Not Working

If JSONB key completion works on one database but not another, check the server logs (terminal running `jupyter lab`) for diagnostic messages when triggering completion.

You can also call the diagnostics endpoint directly:

```
GET /jl-db-comp/jsonb-diagnostics?table=TABLE_NAME&column=COLUMN_NAME
```

This returns the JSONB type distribution and a recommendation. Common causes:

- **All NULL values** - The column has no data
- **Arrays instead of objects** - Keys can only be extracted from `{}` objects, not `[]` arrays
- **Wrong schema** - Add `&schema=your_schema` if not using `public`

## Contributing

### Development install

Note: You will need NodeJS to build the extension package.

The `jlpm` command is JupyterLab's pinned version of
[yarn](https://yarnpkg.com/) that is installed with JupyterLab. You may use
`yarn` or `npm` in lieu of `jlpm` below.

```bash
# Clone the repo to your local environment
# Change directory to the jl_db_comp directory

# Set up a virtual environment and install package in development mode
python -m venv .venv
source .venv/bin/activate
pip install --editable ".[dev,test]"

# Link your development version of the extension with JupyterLab
jupyter labextension develop . --overwrite
# Server extension must be manually installed in develop mode
jupyter server extension enable jl_db_comp

# Rebuild extension Typescript source after making changes
# IMPORTANT: Unlike the steps above which are performed only once, do this step
# every time you make a change.
jlpm build
```

You can watch the source directory and run JupyterLab at the same time in different terminals to watch for changes in the extension's source and automatically rebuild the extension.

```bash
# Watch the source directory in one terminal, automatically rebuilding when needed
jlpm watch
# Run JupyterLab in another terminal
jupyter lab
```

With the watch command running, every saved change will immediately be built locally and available in your running JupyterLab. Refresh JupyterLab to load the change in your browser (you may need to wait several seconds for the extension to be rebuilt).

By default, the `jlpm build` command generates the source maps for this extension to make it easier to debug using the browser dev tools. To also generate source maps for the JupyterLab core extensions, you can run the following command:

```bash
jupyter lab build --minimize=False
```

### Development uninstall

```bash
# Server extension must be manually disabled in develop mode
jupyter server extension disable jl_db_comp
pip uninstall jl_db_comp
```

In development mode, you will also need to remove the symlink created by `jupyter labextension develop`
command. To find its location, you can run `jupyter labextension list` to figure out where the `labextensions`
folder is located. Then you can remove the symlink named `jl_db_comp` within that folder.

### Contributing Features and Fixes

This project uses a **pull request workflow** for all changes. This enables automated changelog generation and streamlined releases.

#### Development Workflow

**1. Create a feature branch:**

```bash
# For new features
git checkout -b feature/descriptive-name

# For bug fixes
git checkout -b fix/bug-description

# For documentation updates
git checkout -b docs/what-you-changed
```

**2. Make your changes:**

- Write code following the guidelines in `AGENTS.md` / `CLAUDE.md`
- Add tests for new functionality
- Update documentation as needed
- Run tests locally: `pytest -vv -r ap --cov jl_db_comp`
- Format code: `jlpm run lint`

**3. Commit and push:**

```bash
git add .
git commit -m "Brief description of changes"
git push origin feature/descriptive-name
```

**4. Create a Pull Request:**

- Go to https://github.com/Ben-Herz/jl_db_completer/pulls
- Click "New pull request"
- Select your feature branch
- **Write a clear PR title** - this becomes the changelog entry!
  - ✅ Good: "Add support for MySQL databases"
  - ✅ Good: "Fix JSONB key completion for nested objects"
  - ❌ Bad: "updates", "fix bug", "changes"
- Add a description explaining what changed and why
- **Add a label** (REQUIRED for CI to pass):
  - `enhancement` - New features
  - `bug` - Bug fixes
  - `documentation` - Documentation updates
  - `maintenance` - Maintenance tasks

  > **Note:** At least one label is required. The "Enforce PR label" CI check will fail if no label is set. You can add labels when creating the PR or afterwards from the PR sidebar.

- Click "Create pull request"

**5. Review and merge:**

- CI checks must pass (build, tests, linting)
- Address any review feedback
- Once approved, merge the PR
- Delete the feature branch after merging

#### Automated Releases

Releases are created using GitHub Actions workflows:

**Step 1: Prepare Release** (Manual workflow)

- Go to **Actions** → **"Step 1: Prep Release"** → **"Run workflow"**
- Optionally specify version (e.g., `0.2.0`, `patch`, `minor`, `major`)
- The workflow will:
  - Bump the version
  - Generate changelog from merged PRs since last release
  - Create a draft GitHub release

**Step 2: Publish Release** (Manual workflow)

- Review the draft release
- Go to **Actions** → **"Step 2: Publish Release"** → **"Run workflow"**
- The workflow will:
  - Publish to PyPI
  - Publish to npm
  - Make the GitHub release public

**Note:** Automated releases require repository secrets for PyPI (`NPM_TOKEN`) and GitHub App credentials. See [RELEASE.md](RELEASE.md) for setup details.

### Testing the extension

#### Server tests

This extension is using [Pytest](https://docs.pytest.org/) for Python code testing.

Install test dependencies (needed only once):

```sh
pip install -e ".[test]"
# Each time you install the Python package, you need to restore the front-end extension link
jupyter labextension develop . --overwrite
```

To execute them, run:

```sh
pytest -vv -r ap --cov jl_db_comp
```

#### Frontend tests

This extension is using [Jest](https://jestjs.io/) for JavaScript code testing.

To execute them, execute:

```sh
jlpm
jlpm test
```

#### Integration tests

This extension uses [Playwright](https://playwright.dev/docs/intro) for the integration tests (aka user level tests).
More precisely, the JupyterLab helper [Galata](https://github.com/jupyterlab/jupyterlab/tree/master/galata) is used to handle testing the extension in JupyterLab.

More information are provided within the [ui-tests](./ui-tests/README.md) README.

## AI Coding Assistant Support

This project includes an `AGENTS.md` file with coding standards and best practices for JupyterLab extension development. The file follows the [AGENTS.md standard](https://agents.md) for cross-tool compatibility.

### Compatible AI Tools

`AGENTS.md` works with AI coding assistants that support the standard, including Cursor, GitHub Copilot, Windsurf, Aider, and others. For a current list of compatible tools, see [the AGENTS.md standard](https://agents.md).
This project also includes symlinks for tool-specific compatibility:

- `CLAUDE.md` → `AGENTS.md` (for Claude Code)

- `GEMINI.md` → `AGENTS.md` (for Gemini Code Assist)

Other conventions you might encounter:

- `.cursorrules` - Cursor's YAML/JSON format (Cursor also supports AGENTS.md natively)
- `CONVENTIONS.md` / `CONTRIBUTING.md` - For CodeConventions.ai and GitHub bots
- Project-specific rules in JetBrains AI Assistant settings

All tool-specific files should be symlinks to `AGENTS.md` as the single source of truth.

### What's Included

The `AGENTS.md` file provides guidance on:

- Code quality rules and file-scoped validation commands
- Naming conventions for packages, plugins, and files
- Coding standards (TypeScript, Python)
- Development workflow and debugging
- Backend-frontend integration patterns (`APIHandler`, `requestAPI()`, routing)
- Common pitfalls and how to avoid them

### Customization

You can edit `AGENTS.md` to add project-specific conventions or adjust guidelines to match your team's practices. The file uses plain Markdown with Do/Don't patterns and references to actual project files.

**Note**: `AGENTS.md` is living documentation. Update it when you change conventions, add dependencies, or discover new patterns. Include `AGENTS.md` updates in commits that modify workflows or coding standards.

### Packaging the extension

See [RELEASE](RELEASE.md)
