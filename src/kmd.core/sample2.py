import pathlib
import re
import shutil
import dask
import polars as pl
from dask import delayed
import time


def read_csv_to_parquet(csv_dir: pathlib.Path, parquet_dir: pathlib.Path) -> None:
    shutil.rmtree(parquet_dir, ignore_errors=True)
    pattern = r'(.+)_(\d{8})\.csv$'
    
    @delayed
    def write_csv_to_parquet(csv_file: pathlib.Path) -> None:
        match = re.match(pattern=pattern, string=csv_file.name)
        if match:
            base_name, date_str = match.groups()
            output_dir: pathlib.Path = parquet_dir / f"{base_name}.delta" / f"date={date_str}"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file: pathlib.Path = output_dir / f"{base_name}.parquet"
            pl.scan_csv(csv_file).collect().write_parquet(output_file)
        else:
            print(f"Skipping file: {csv_file.name} - Filename does not match pattern")
    
    print(list(csv_dir.glob("*.csv")))
    conversion_list = [write_csv_to_parquet(csv_file) for csv_file in csv_dir.glob("**/*.csv")]
    dask.compute(*conversion_list)


def main() -> None:
    start: float = time.time()
    csv_dir = pathlib.Path(r"data/csv")
    parquet_dir = pathlib.Path(r"data/parquet")
    read_csv_to_parquet(csv_dir, parquet_dir)
    end: float = time.time()
    print(f"Time taken: {end - start:.2f} seconds")


if __name__ == "__main__":
    main()
