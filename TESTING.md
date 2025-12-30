# Testing Guide for jl_db_comp

This guide walks you through testing the PostgreSQL autocomplete extension with your local database.

## Prerequisites

1. PostgreSQL server running on `localhost:5432`
2. Database: `ehrexample`
3. User: `postgres`
4. Password: `example`

## Quick Start

### 1. Set Up Environment

```bash
# Activate virtual environment
source .venv/bin/activate

# Set PostgreSQL connection
export POSTGRES_URL="postgresql://postgres:example@localhost:5432/ehrexample"

# Start JupyterLab
jupyter lab
```

### 2. Create Test Notebook

1. Open JupyterLab in your browser
2. Create a new notebook (File → New → Notebook)
3. In the first cell, type:

```python
import psycopg2

# Test connection
conn = psycopg2.connect("postgresql://postgres:example@localhost:5432/ehrexample")
cursor = conn.cursor()

# List all tables
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
""")

print("Available tables:")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

cursor.close()
conn.close()
```

4. Run this cell to verify database connection and see available tables

### 3. Test Autocomplete

Create a new cell and try the following:

#### Test 1: Table Name Completion

Type this and press **Tab** after typing "pat":

```sql
SELECT * FROM pat
```

**Expected**:

- Completion menu appears
- Shows tables starting with "pat" (e.g., patients, patient_visits)
- Shows ONLY tables, NOT columns
- Tables have 📋 icon

#### Test 2: Column Completion After Table Name

Type this and press **Tab** after the dot:

```sql
SELECT patients.
```

**Expected**:

- Shows ONLY columns from 'patients' table
- Columns have 📊 icon
- Shows data types when hovering

#### Test 3: Column Completion with Prefix

Type this and press **Tab** after "pat":

```sql
SELECT patients.pat
```

**Expected**:

- Shows only columns from 'patients' that start with "pat"
- Example: patient_id, patient_name

#### Test 4: Table Alias Completion

```sql
SELECT p.
FROM patients p
```

Press **Tab** after `p.`

**Expected**:

- Shows columns from 'p' table (if 'p' exists as a table name in database)
- Note: This works if 'p' is an actual table name, not for runtime aliases

#### Test 5: Multiple Table References

```sql
SELECT
    patients.patient_id,
    visits.
FROM patients
JOIN visits ON patients.patient_id = visits.patient_id
```

Press **Tab** after `visits.`

**Expected**:

- Shows columns from 'visits' table only

#### Test 6: No Column Suggestions Without Dot

Type this and press **Tab**:

```sql
SELECT patient
```

**Expected**:

- Shows tables starting with "patient" (if any)
- Does NOT show columns from all tables

#### Test 7: Schema-Aware Table Listing

Type this and press **Tab** after the dot:

```sql
SELECT * FROM information_schema.
```

**Expected**:

- Shows tables and views from 'information_schema' schema
- Tables have 📋 icon
- Views have 👁️ icon (or similar visual indicator)

#### Test 8: Schema-Qualified Column Completion

Type this and press **Tab**:

```sql
SELECT information_schema.tables.
```

**Expected**:

- Shows columns from information_schema.tables
- All columns have 📊 icon

#### Test 9: Multi-Schema Query

Type this and press **Tab** at each dot:

```sql
SELECT
    public.patients.<Tab>,
    custom_schema.users.<Tab>
FROM public.patients
JOIN custom_schema.users
```

**Expected**:

- First Tab shows patient columns from public schema
- Second Tab shows user columns from custom_schema

#### Test 10: Ambiguous Single Dot Resolution

Type this and press **Tab**:

```sql
-- If 'information_schema' is a schema in your database:
SELECT * FROM information_schema.<Tab>
-- Expected: Shows tables from information_schema

-- If 'patients' is a table (not a schema):
SELECT patients.<Tab>
-- Expected: Shows columns from patients table
```

**Expected**:

- Backend checks if identifier is a schema first
- If yes, returns tables/views from that schema
- If no, returns columns from that table in default schema

#### Test 11: JSONB First-Level Keys

Type this and press **Tab** (requires JSONB column):

```sql
SELECT metadata-><Tab>
FROM patients
```

**Expected**:

- Shows unique keys from the 'metadata' JSONB column
- Keys have 🔑 icon
- Only shows if 'metadata' is a JSONB column

#### Test 12: JSONB Nested Keys

Type this and press **Tab**:

```sql
SELECT metadata->>'diagnosis'-><Tab>
FROM patients
```

**Expected**:

- Shows keys nested under the 'diagnosis' key
- Queries actual table data to extract nested structure

#### Test 13: Table-Qualified JSONB Column

Type this and press **Tab**:

```sql
SELECT patients.metadata-><Tab>
FROM patients
```

**Expected**:

- Shows JSONB keys from patients.metadata column
- Works with table qualification

#### Test 14: JSONB Key Prefix Filtering

Type this and press **Tab** after "dia":

```sql
SELECT metadata->>dia<Tab>
FROM patients
```

**Expected**:

- Shows only keys starting with "dia"
- Example: diagnosis, diagnostic_code

## Verification Checklist

### Basic Functionality

- [ ] Extension appears in `jupyter labextension list` as enabled
- [ ] Server extension appears in `jupyter server extension list` as enabled
- [ ] Can connect to PostgreSQL database (test with psycopg2)
- [ ] Autocomplete menu appears when typing SQL

### Table/Column Awareness

- [ ] Table names appear with 📋 icon when NOT after a dot
- [ ] Column names appear with 📊 icon when AFTER a dot (tablename.)
- [ ] Typing "tablename." shows ONLY columns from that table
- [ ] Typing without dot shows ONLY table names from default schema
- [ ] Column completions show table context in label

### Schema Awareness

- [ ] Typing "schemaname." shows tables and views from that schema
- [ ] Typing "schemaname.tablename." shows columns from that table in that schema
- [ ] Views appear with appropriate icon (👁️ or 📋)
- [ ] Default schema (public) works without typing "public."
- [ ] Backend correctly distinguishes between schema names and table names
- [ ] Multi-schema queries work (e.g., public.table1 and custom_schema.table2)

### Performance

- [ ] Completions are cached (second request is faster)
- [ ] Schema detection query is efficient
- [ ] No noticeable delay when typing

### JSONB Functionality (if you have JSONB columns)

- [ ] Typing `column_name->` shows JSONB keys from that column
- [ ] JSONB keys have 🔑 icon
- [ ] Nested JSONB paths work (e.g., `column->>'key1'->`)
- [ ] Table-qualified JSONB works (e.g., `table.column->`)
- [ ] JSONB key prefix filtering works
- [ ] Queries actual table data (samples up to 1000 rows)
- [ ] No errors when querying non-JSONB columns

## Important Notes

### Table Alias Limitation

**Current behavior**: The extension detects `tablename.` patterns but does NOT parse SQL aliases.

Example:

```sql
-- This WILL work if 'p' is an actual table name in your database:
SELECT p.
FROM patients p
```

```sql
-- This will NOT work for alias resolution (shows columns from 'p' table, not 'patients'):
SELECT p.
FROM patients AS p
```

**Why**: Implementing full SQL alias resolution would require:

1. Parsing the entire SQL query to find FROM/JOIN clauses
2. Mapping aliases to actual table names
3. Handling subqueries, CTEs, and complex query structures

This is a future enhancement. For now, use actual table names after the dot for best results.

## Troubleshooting

### Extension Not Loading

```bash
# Check extension status
jupyter labextension list
jupyter server extension list

# If not enabled, run:
jupyter labextension develop . --overwrite
jupyter server extension enable jl_db_comp

# Restart JupyterLab
```

### No Completions Appearing

1. **Check browser console** (F12 or Cmd+Option+I):
   - Look for errors related to `jl_db_comp`
   - Check Network tab for failed API requests

2. **Check JupyterLab server logs**:
   - Look for PostgreSQL connection errors
   - Verify the database URL is correct

3. **Verify SQL keywords**:
   - Completions only activate when SQL keywords are detected
   - Try typing `SELECT` first

### Database Connection Issues

```bash
# Test connection manually
python -c "import psycopg2; conn = psycopg2.connect('postgresql://postgres:example@localhost:5432/ehrexample'); print('Connection successful'); conn.close()"

# Check PostgreSQL is running
psql -h localhost -U postgres -d ehrexample -c "SELECT 1;"
```

### Clearing Cache

The extension caches completions for 5 minutes. To test fresh data:

1. Restart JupyterLab
2. Or wait 5 minutes for cache to expire
3. Or modify `src/provider.ts` to reduce `_cacheTTL`

## Testing Different Scenarios

### Test with Different Schema

```bash
# Set custom schema in environment
export POSTGRES_SCHEMA="custom_schema"
jupyter lab
```

Or configure via JupyterLab settings (Settings → PostgreSQL Database Completer)

### Test Error Handling

1. **Invalid database URL**:

   ```bash
   export POSTGRES_URL="postgresql://invalid:url@localhost:5432/baddb"
   jupyter lab
   ```

   Expected: No errors, completions return empty

2. **No database URL**:

   ```bash
   unset POSTGRES_URL
   jupyter lab
   ```

   Expected: Extension loads but returns no completions

3. **Database connection lost**:
   - Stop PostgreSQL server
   - Try autocomplete
   - Expected: Error logged to console, no completions shown

## Performance Testing

### Measure Cache Performance

```python
import time

# First request (uncached)
start = time.time()
# Type SQL and trigger autocomplete
# Note the time
first_request_time = time.time() - start

# Second request (cached)
start = time.time()
# Type same SQL and trigger autocomplete again
# Note the time
second_request_time = time.time() - start

print(f"First request: {first_request_time:.2f}s")
print(f"Second request (cached): {second_request_time:.2f}s")
print(f"Speedup: {first_request_time / second_request_time:.1f}x")
```

Expected: Second request should be significantly faster (10-100x)

## Development Testing

For rapid testing during development:

```bash
# Terminal 1: Auto-rebuild on changes
jlpm watch

# Terminal 2: Run JupyterLab
export POSTGRES_URL="postgresql://postgres:example@localhost:5432/ehrexample"
jupyter lab

# After making changes:
# 1. Save your TypeScript files
# 2. Wait for watch to rebuild
# 3. Refresh browser (Cmd+R / Ctrl+R)
```

## Reporting Issues

If you encounter problems, collect this information:

1. Extension versions:

   ```bash
   jupyter labextension list | grep jl_db_comp
   jupyter server extension list | grep jl_db_comp
   ```

2. Browser console errors (F12 → Console tab)

3. JupyterLab server logs (from terminal running `jupyter lab`)

4. Database connection details (without password):

   ```bash
   echo $POSTGRES_URL | sed 's/:[^@]*@/:***@/'
   ```

5. PostgreSQL version:
   ```bash
   psql --version
   ```
