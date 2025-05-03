import polars as pl
import time

# Start timer
start = time.time()

# Lazily scan all Parquet files in the folder (wildcard supported)
lf = pl.scan_parquet("parquet_data/*.parquet")

# Self-join on 'key' (can be changed to join with a second scan if needed)
joined = lf.join(lf, on="key", how="inner").limit(1000)

# Trigger computation and collect result
result = joined.collect()

import ipdb; ipdb.set_trace()

end = time.time()
print("Polars Lazy (scan_parquet) join time: {:.2f} seconds".format(end - start))
