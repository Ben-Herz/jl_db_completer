# Quick Start Guide

Get PostgreSQL autocomplete working in JupyterLab in 2 minutes.

## Prerequisites Checklist

- [ ] PostgreSQL running on `localhost:5432`
- [ ] Database `ehrexample` exists
- [ ] User `postgres` with password `example` has access
- [ ] Python 3.10+ installed
- [ ] Virtual environment activated

## Installation (Already Done!)

The extension is already installed in development mode. Verify:

```bash
source .venv/bin/activate
jupyter labextension list | grep jl_db_comp
jupyter server extension list | grep jl_db_comp
```

Both should show "enabled" and "OK".

## Start Testing Now

### Step 1: Set Database Connection

```bash
source .venv/bin/activate
export POSTGRES_URL="postgresql://postgres:example@localhost:5432/ehrexample"
```

### Step 2: Launch JupyterLab

```bash
jupyter lab
```

Your browser should open automatically to `http://localhost:8888`

### Step 3: Test Autocompletion

1. **Create a new notebook**: File → New → Notebook

2. **Test Table Completion** - Type SQL in a cell:

   ```sql
   SELECT * FROM pat
   ```

   Press **Tab** - you should see:
   - 📋 patients
   - 📋 patient_visits
   - 📋 patient_records

3. **Test Column Completion** - After typing a table name and dot:

   ```sql
   SELECT patients.
   ```

   Press **Tab** - you should see only columns from the 'patients' table:
   - 📊 patient_id (patients)
   - 📊 patient_name (patients)
   - etc.

4. **Test with Aliases**:

   ```sql
   SELECT p.
   FROM patients p
   ```

   Press **Tab** after `p.` - if 'p' is recognized as a table name, you'll see its columns

5. **Test Schema-Aware Completion**:

   ```sql
   SELECT * FROM information_schema.
   ```

   Press **Tab** after the dot - you should see:
   - 📋 tables
   - 📋 columns
   - 👁️ views (and other views)
   - All tables/views from the information_schema

6. **Test Schema-Qualified Columns**:

   ```sql
   SELECT information_schema.tables.
   ```

   Press **Tab** - you should see columns from information_schema.tables:
   - 📊 table_name (tables)
   - 📊 table_schema (tables)
   - 📊 table_type (tables)

7. **Test JSONB Keys** (if you have JSONB columns):

   First, create a test table with JSONB data:

   ```python
   import psycopg2
   conn = psycopg2.connect("postgresql://postgres:example@localhost:5432/ehrexample")
   cursor = conn.cursor()

   cursor.execute("""
       CREATE TABLE IF NOT EXISTS patient_data (
           id SERIAL PRIMARY KEY,
           metadata JSONB
       )
   """)

   cursor.execute("""
       INSERT INTO patient_data (metadata) VALUES
       ('{"allergies": ["penicillin"], "diagnosis": {"code": "A01", "description": "Typhoid"}}'),
       ('{"allergies": [], "diagnosis": {"code": "B02", "description": "Shingles"}}')
   """)
   conn.commit()
   cursor.close()
   conn.close()
   ```

   Then test JSONB autocomplete:

   ```sql
   SELECT metadata-><Tab>
   FROM patient_data
   ```

   Press **Tab** - you should see:
   - 🔑 allergies
   - 🔑 diagnosis

   Test nested keys:

   ```sql
   SELECT metadata->>'diagnosis'-><Tab>
   FROM patient_data
   ```

   Press **Tab** - you should see:
   - 🔑 code
   - 🔑 description

## What You Should See

### Successful Autocomplete

When working correctly, you'll see:

```
📋 patients
📋 patient_visits
📊 patient_id (patients)
📊 patient_name (patients)
```

### Completion Menu

- **Icon 📋** = Table
- **Icon 📊** = Column
- **Text format**: `name` for tables, `name (table)` for columns
- **Hover**: Shows data type for columns

## Troubleshooting

### No Completions?

**Check 1: SQL Keywords**

- Completions only activate when SQL keywords are present
- Try typing `SELECT * FROM ` first

**Check 2: Database Connection**

```bash
python -c "import psycopg2; conn = psycopg2.connect('postgresql://postgres:example@localhost:5432/ehrexample'); print('✓ Connected'); conn.close()"
```

**Check 3: Browser Console**

- Press F12 (or Cmd+Option+I on Mac)
- Look for errors in Console tab
- Check Network tab for failed API requests

**Check 4: Server Logs**

- Check the terminal where you ran `jupyter lab`
- Look for PostgreSQL connection errors

### Extension Not Loading?

```bash
# Re-register extension
jupyter labextension develop . --overwrite
jupyter server extension enable jl_db_comp

# Restart JupyterLab (Ctrl+C, then jupyter lab again)
```

### Still Not Working?

1. **Verify PostgreSQL is accessible**:

   ```bash
   psql -h localhost -U postgres -d ehrexample -c "SELECT 1;"
   ```

2. **Check extension status**:

   ```bash
   jupyter labextension list
   jupyter server extension list
   ```

3. **Review full logs**:
   - Browser console (F12 → Console)
   - JupyterLab server terminal output

## Development Mode

For making changes to the extension:

### Terminal 1: Auto-Rebuild

```bash
source .venv/bin/activate
jlpm watch
```

### Terminal 2: Run JupyterLab

```bash
source .venv/bin/activate
export POSTGRES_URL="postgresql://postgres:example@localhost:5432/ehrexample"
jupyter lab
```

After changing TypeScript files:

1. Save the file
2. Wait for `jlpm watch` to rebuild (watch Terminal 1)
3. Refresh browser (Cmd+R or Ctrl+R)

After changing Python files:

1. Save the file
2. Restart JupyterLab (Ctrl+C in Terminal 2, then `jupyter lab` again)

## Example Workflow

Here's a complete example to verify everything works:

### 1. Check Database Tables

In a notebook cell:

```python
import psycopg2

conn = psycopg2.connect("postgresql://postgres:example@localhost:5432/ehrexample")
cursor = conn.cursor()

cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
""")

print("Tables in database:")
for row in cursor.fetchall():
    print(f"  📋 {row[0]}")

cursor.close()
conn.close()
```

### 2. Test Autocomplete

In a new cell, type this and press Tab:

```sql
SELECT * FROM
```

You should see the same tables from step 1 appear in the autocomplete menu.

### 3. Complete a Full Query

```sql
SELECT
    p.patient_id,
    p.name
FROM patients p
WHERE p.
```

Press Tab after `WHERE p.` to see column completions for the patients table.

## Success Criteria

You've successfully set up the extension when:

- ✅ Typing SQL shows autocomplete menu
- ✅ Tables appear with 📋 icon
- ✅ Columns appear with 📊 icon and table context
- ✅ Second autocomplete is faster (caching works)
- ✅ No errors in browser console or server logs

## Next Steps

- **Read full docs**: See `README.md` for detailed configuration
- **Testing guide**: See `TESTING.md` for comprehensive test cases
- **Implementation details**: See `IMPLEMENTATION.md` for architecture
- **Development**: See `CLAUDE.md` for coding standards

## Configuration Options

### Change Database Schema

Via environment:

```bash
export POSTGRES_SCHEMA="custom_schema"
```

Via JupyterLab Settings:

1. Settings → Settings Editor
2. Search "PostgreSQL Database Completer"
3. Set Schema to "custom_schema"

### Change Database Connection

Update the `POSTGRES_URL` environment variable:

```bash
export POSTGRES_URL="postgresql://user:pass@host:port/dbname"
```

Or configure via JupyterLab Settings (same location as above).

## Common SQL Patterns to Test

Try these patterns to verify autocomplete works in different contexts:

```sql
-- Basic SELECT
SELECT * FROM patients

-- JOIN
SELECT * FROM patients p JOIN visits v ON

-- WHERE
SELECT * FROM patients WHERE patient

-- Multiple columns
SELECT patient_id, patient_name, FROM

-- Subquery
SELECT * FROM (SELECT * FROM patients)
```

## Performance Testing

To verify caching is working:

1. Type `SELECT * FROM pat` and press Tab
2. Note the time it takes to show completions (~100-500ms)
3. Delete your typing, then type it again
4. Press Tab again - should be instant (<1ms)

The second request should be noticeably faster because it's served from cache.

## Getting Help

If you're stuck:

1. Check `TESTING.md` for detailed troubleshooting
2. Review `IMPLEMENTATION.md` for how it works
3. Open an issue: https://github.com/Ben-Herz/jl_db_completer/issues

Include:

- Extension versions (`jupyter labextension list`)
- Browser console errors
- JupyterLab server logs
- PostgreSQL version
- Steps to reproduce

## That's It!

You should now have a working PostgreSQL autocomplete in JupyterLab. Happy querying! 🎉
