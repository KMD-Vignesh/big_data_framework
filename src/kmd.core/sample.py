import json
import os
import time
import polars as pl
import dask
import duckdb

import dask.delayed

def read_csv_to_parquet(input_dir, output_dir):
    """
    Reads CSV files from the input directory and converts them to Parquet files in the output directory.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    @dask.delayed
    def process_file(file_path):
        # Read CSV using Polars LazyFrame
        lazy_frame = pl.scan_csv(file_path)
        # Collect LazyFrame into a DataFrame
        df = lazy_frame.collect()
        # Convert to Parquet
        output_file = os.path.join(output_dir, os.path.basename(file_path).replace('.csv', '.parquet'))
        df.write_parquet(output_file)
        return output_file

    # List all CSV files in the directory
    csv_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.csv')]
    # Process files in parallel
    parquet_files = dask.compute(*[process_file(f) for f in csv_files])
    return parquet_files

def query_parquet_with_duckdb(conn,parquet_files, query):
    """
    Runs SQL queries on Parquet files using DuckDB.
    """
    # Create a DuckDB connection
    # Create a view for each Parquet file
    for i, parquet_file in enumerate(parquet_files):
        view_name = f"parquet_view_{i}"
        conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{parquet_file}'")
    # Run the query
    result = conn.execute(query).fetchall()
    return result

def execute_queries_from_json(conn: duckdb.DuckDBPyConnection, parquet_files: list[str], json_file_path: str) -> dict:
    """
    Executes queries from a JSON file and stores results in views if specified.
    Returns a dictionary of query results.
    """
    # Load queries from JSON
    with open(json_file_path, 'r') as f:
        query_config = json.load(f)
    
    results = {}
    
    # Create initial views for parquet files
    for i, parquet_file in enumerate(parquet_files):
        view_name = f"parquet_view_{i}"
        conn.execute(f"CREATE VIEW IF NOT EXISTS {view_name} AS SELECT * FROM '{parquet_file}'")
    
    # Execute each query
    for query_info in query_config['queries']:
        query_name = query_info['name']
        query = query_info['query']
        create_view = query_info.get('create_view', False)
        
        print(f"\nExecuting query: {query_name}")
        result = conn.execute(query).fetchall()
        results[query_name] = result
        
        # Create view if specified
        if create_view:
            view_name = f"view_{query_name}"
            create_view_query = f"CREATE OR REPLACE VIEW {view_name} AS {query}"
            conn.execute(create_view_query)
            print(f"Created view: {view_name}")
        
        # Print results
        print(f"Results for {query_name}:")
        for row in result[:5]:  # Show first 5 results
            print(row)
    
    return results

if __name__ == "__main__":
    input_directory = r"data/csv"
    output_directory = r"data/parquet"
    queries_file = r"data/queries/scenario_1.json"
    
    start = time.time()
    
    # Step 1: Convert CSV to Parquet
    parquet_files = read_csv_to_parquet(input_directory, output_directory)
    conn: duckdb.DuckDBPyConnection = duckdb.connect()
    
    # Step 2: Execute queries from JSON file
    results = execute_queries_from_json(conn, parquet_files, queries_file)
    
    end = time.time()
    print(f"\nTotal execution time: {end - start:.2f} seconds")
    
    # Clean up
    conn.close()
