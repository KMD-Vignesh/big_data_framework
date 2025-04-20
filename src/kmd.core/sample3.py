import os
import json
import polars as pl
import duckdb
from dask import delayed, compute

# Configuration
csv_dir = r'data/csv'
parquet_dir = r'data/parquet'
results_dir = r'data/query_results'
queries_file = r'data/queries/new_scen.json'

EXPORT_CSV = True 

# List of columns to apply format (example)
timestamp_cols: list[str] = ["Date"]
int_cols: list[str] = ["Customer ID"]
string_cols: list[str] = ["Invoice ID"]

# Initialize DuckDB connection
con: duckdb.DuckDBPyConnection = duckdb.connect()

# Step 1: Process CSV files with Dask and Polars
@delayed
def process_csv(file) -> str:
    """
    Process and format a single CSV file, then save it as Parquet.
    """
    df = pl.read_csv(file).with_columns([
        *[pl.col(c).str.strip_chars().str.strptime(dtype=pl.Datetime, format="%Y-%m-%d %H:%M:%S") for c in timestamp_cols if "trans" in file],
        *[pl.col(c).fill_null(0).cast(dtype=pl.Int64) for c in int_cols if "trans" in file],
        *[pl.col(c).cast(pl.String).str.strip_chars().str.to_titlecase() for c in string_cols if "trans" in file],
    ])
    # Save to Parquet
    out_path = os.path.join(parquet_dir, os.path.basename(file).replace(".csv", ".parquet"))
    df.write_parquet(out_path)
    return out_path

def process_files() -> tuple:
    """
    Process all CSV files in parallel using Dask and convert them to Parquet.
    """
    # List CSV files from the directory
    csv_files = [os.path.join(csv_dir, f) for f in os.listdir(csv_dir) if f.endswith(".csv")]
    results = compute(*[process_csv(f) for f in csv_files])
    return results

# Step 2: Create DuckDB Views from Parquet Files
def create_views_from_parquet(parquet_files) -> None:
    """
    Create DuckDB views for each Parquet file.
    """
    for parquet_file in parquet_files:
        table_name = os.path.basename(parquet_file).replace('.parquet', '')
        con.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_file}')")

# Step 3: Load SQL queries from JSON file and run them
def run_queries_from_json(queries_file) -> list:
    """
    Run SQL queries from a JSON file, create views, and optionally export results to CSV.
    """
    with open(queries_file, 'r') as f:
        queries = json.load(f)

    all_results = []
    
    for scenario in queries:
        # Scenario includes multiple queries
        scenario_results = []
        for query_info in scenario['queries']:
            query_name = query_info['name']
            query = query_info['query']
            export_csv = query_info.get('csv_export', False)

            print(f"Running SQL: {query_name}")
            result_df = con.execute(query).fetchdf()

            # Create result view
            result_view_name = f"result_{scenario['id']}_{query_name}"
            con.execute(f"CREATE VIEW {result_view_name} AS SELECT * FROM ({query})")
            scenario_results.append(result_df)

            # Conditionally export to CSV if global export is enabled and query explicitly allows it
            if EXPORT_CSV and export_csv:
                result_df.to_csv(f"{results_dir}/{result_view_name}.csv")

        all_results.append(scenario_results)
    return all_results

# Step 4: Run the full process
if __name__ == '__main__':
    # Step 1: Process CSV files in parallel and save as Parquet
    print("Processing CSV files...")
    processed_parquet_files = process_files()

    # Step 2: Create views in DuckDB
    print("Creating views from Parquet files...")
    create_views_from_parquet(processed_parquet_files)

    # Step 3: Execute queries from JSON
    print("Running SQL queries from JSON...")
    run_queries_from_json(queries_file)

    print("Process complete!")
