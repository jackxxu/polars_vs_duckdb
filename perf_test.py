import glob
import duckdb
import polars as pl
from timer import timer
from data_gen import generate_parquets

generate_parquets(200)

files = sorted(glob.glob("parquet_data/*.parquet"))

@timer
def merge_polars(files):
  # Start with the first LazyFrame
  lf = pl.scan_parquet(files[0]).select(["id", "value"]).rename({"value": "value_0"})

  # Iteratively join the rest on 'id' and rename their 'value' columns
  for i, f in enumerate(files[1:], start=1):
      lf_new = pl.scan_parquet(f).select(["id", "value"]).rename({"value": f"value_{i}"})
      lf = lf.join(lf_new, on="id", how="inner")

  return lf.collect().to_pandas()

@timer
def merge_duckdb(files):
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
  return con.execute(query).fetchdf()

polars_df = merge_polars(files)
duckdb_df = merge_duckdb(files)

# # compare the two dataframes using datacompy
# import datacompy
# compare = datacompy.Compare(polars_df, duckdb_df, on_index=True)
# print(compare.report())