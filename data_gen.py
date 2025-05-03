import pandas as pd
import numpy as np
import os


def generate_parquets(num_files: int):
    # remove the existing parquet_data directory if it exists
    if os.path.exists("parquet_data"):
        import shutil
        shutil.rmtree("parquet_data")

    # Configuration
    os.makedirs("parquet_data", exist_ok=True)
    rows_per_file = 20_000
    np.random.seed(42)

    for i in range(num_files):
        df = pd.DataFrame({
            'id': np.arange(0, rows_per_file),
            'value': np.random.rand(rows_per_file),
        })
        df.to_parquet(
            f'parquet_data/data_{i}.parquet',
            compression='snappy'  # Options: 'snappy', 'gzip', 'brotli', 'zstd'
        )
