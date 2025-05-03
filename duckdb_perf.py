import duckdb
import glob
import time

start = time.time()

# Get sorted list of Parquet files
files = sorted(glob.glob("parquet_data/*.parquet"))

# Start building base SELECT query
query_parts = []

# Build SELECT and JOIN parts
for i, f in enumerate(files):
    alias = f"t{i}"
    value_col = f"value_{i}"
    subquery = f"(SELECT id, value AS {value_col} FROM '{f}') AS {alias}"

    if i == 0:
        query = f"SELECT * FROM {subquery}"
    else:
        query += f" JOIN {subquery} USING (id)"

# Run the query
con = duckdb.connect()
df = con.execute(query).fetchdf()

end = time.time()

# Output result
print(df.shape)
print("DuckDB horizontal join (by id) time: {:.2f} seconds".format(end - start))
