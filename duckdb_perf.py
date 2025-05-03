import duckdb
import time

# Start timer
start = time.time()

# Read and join all Parquet files in DuckDB
con = duckdb.connect()
query = """
    SELECT *
    FROM parquet_scan('parquet_data/*.parquet') AS a
    INNER JOIN parquet_scan('parquet_data/*.parquet') AS b
    ON a.key = b.key
    LIMIT 1000
"""
result = con.execute(query).fetchdf()
end = time.time()

print("DuckDB join time: {:.2f} seconds".format(end - start))
