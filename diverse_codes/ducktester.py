import duckdb

con = duckdb.connect(r"C:\Users\depoplimontj\.databricks\labs\lakebridge_profilers\synapse_assessment\profiler_extract.db")

# List all tables
tables = con.execute("SHOW TABLES").fetchall()
print("Tables:", tables)

# Check row counts for each table
for (table,) in tables:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count} rows")

con.close()