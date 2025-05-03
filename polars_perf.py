import polars as pl
import glob
import time

start = time.time()

files = sorted(glob.glob("parquet_data/*.parquet"))

# Start with the first LazyFrame
lf = pl.scan_parquet(files[0]).select(["id", "value"]).rename({"value": "value_0"})

# Iteratively join the rest on 'id' and rename their 'value' columns
for i, f in enumerate(files[1:], start=1):
    lf_new = pl.scan_parquet(f).select(["id", "value"]).rename({"value": f"value_{i}"})
    lf = lf.join(lf_new, on="id", how="inner")

# Optionally limit for benchmarking
result = lf.collect()

print(result.shape)

end = time.time()
print("Polars Lazy horizontal join (via key) time: {:.2f} seconds".format(end - start))
