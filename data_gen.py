import pandas as pd
import numpy as np
import os

# Configuration
os.makedirs("parquet_data", exist_ok=True)
num_files = 5
rows_per_file = 1_000_000
np.random.seed(42)

for i in range(num_files):
    df = pd.DataFrame({
        'id': np.arange(i * rows_per_file, (i + 1) * rows_per_file),
        'key': np.random.randint(0, 100000, size=rows_per_file),
        'value': np.random.rand(rows_per_file),
        'category': np.random.choice(['A', 'B', 'C'], size=rows_per_file)
    })
    df.to_parquet(
        f'parquet_data/data_{i}.parquet',
        compression='snappy'  # Options: 'snappy', 'gzip', 'brotli', 'zstd'
    )
