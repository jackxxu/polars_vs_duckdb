import os
import glob
import duckdb
import polars as pl
import pandas as pd
from timer import timer
from data_gen import generate_parquets


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


@timer
def merge_pandas(files):
    # Read first file
    df = pd.read_parquet(files[0])[['id', 'value']].rename(columns={'value': 'value_0'})

    # Iteratively join the rest on 'id' and rename their 'value' columns
    for i, f in enumerate(files[1:], start=1):
        df_new = pd.read_parquet(f)[['id', 'value']].rename(columns={'value': f'value_{i}'})
        df = pd.merge(df, df_new, on='id', how='inner')

    return df

@timer
def stack_pandas():
    parquet_files = glob.glob(os.path.join('parquet_data', '*.parquet'))
    # Read and stack them into a single DataFrame
    return pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)


@timer
def stack_duckdb():
    return duckdb.query("SELECT * FROM 'parquet_data/*.parquet'").to_df()


@timer
def stack_polars():
    files = glob.glob("parquet_data/*.parquet")
    return pl.read_parquet(files)


if __name__ == "__main__":

    generate_parquets(200)
    files = sorted(glob.glob("parquet_data/*.parquet"))

    # Run the functions to see the time taken
    polars_df = merge_polars(files)
    duckdb_df = merge_duckdb(files)
    pandas_df = merge_pandas(files)

    stack_polars()
    stack_pandas()
    stack_duckdb()

    # Check if the dataframes are equal
    assert polars_df.equals(duckdb_df), "DataFrames are not equal"
    assert polars_df.equals(pandas_df), "DataFrames are not equal"
