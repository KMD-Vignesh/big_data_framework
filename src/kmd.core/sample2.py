import pathlib
import dask
import polars as pl
from dask import delayed
import time



def read_csv_to_parquet(csv_dir:pathlib.Path, parquet_dir:pathlib.Path) -> None:
    parquet_dir.mkdir(parents=True, exist_ok=True)
    @delayed
    def write_csv_to_parquet(csv_file:pathlib.Path) -> None:
        df: pl.DataFrame = pl.scan_csv(csv_file).collect()
        output_file: pathlib.Path = parquet_dir / (csv_file.stem + ".parquet")
        df.write_parquet(output_file)
    conversion_list = [write_csv_to_parquet(csv_file) for csv_file in csv_dir.glob("*.csv")]
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
