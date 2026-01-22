import os
import sys
import duckdb
import glob
import re
from typing import List
from dotenv import load_dotenv
import psutil
import time  # Import time module for delay functionality
import shutil  # Import shutil for file copy functionality
import logging
import os

APILogger = logging.getLogger("APILogger")
if not APILogger.handlers:
    APILogger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    APILogger.addHandler(sh)
    log_dir = os.getenv("output_path", "./output")
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(log_dir, "GasModelcheck.log"), encoding="utf-8")
    fh.setFormatter(formatter)
    APILogger.addHandler(fh)


def sanitize_view_name(path: str, base: str) -> str:
    rel_name = os.path.relpath(path, base)
    return re.sub(r'\W+', '_', rel_name).strip('_')

def find_subdirectories(root_dir: str) -> List[str]:
    subdirectories = []
    for dirpath, dirnames, _ in os.walk(root_dir):
        for dirname in dirnames:
            subdirectories.append(os.path.join(dirpath, dirname))
    return subdirectories

def is_file_locked(filepath: str, retries: int = 5, delay: int = 2) -> int:
    """
    Check if the file is locked by any process. If it is, retry a few times before giving up.
    """
    for attempt in range(retries):
        locked_pid = None
        for proc in psutil.process_iter(['pid', 'open_files']):
            try:
                files = proc.info['open_files']
                if files:
                    for f in files:
                        if os.path.abspath(f.path) == os.path.abspath(filepath):
                            locked_pid = proc.info['pid']
                            break
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if not locked_pid:
            return None  # File is not locked, return None
        print(f"File is locked by PID {locked_pid}. Retrying in {delay} seconds...")
        time.sleep(delay)

    # If we reach here, the file is still locked after retries
    return locked_pid

def prepare_duckdb(model_name: str, verbose_log: bool = False) -> None:
    output_path = os.getenv('output_path')
    if not output_path:
        raise EnvironmentError("Missing 'output_path' in environment variables.")

    model_directory = os.path.join(output_path, f"Model {model_name} Solution")
    if not os.path.exists(model_directory):
        raise FileNotFoundError(f"Expected model output directory not found: {model_directory}")

    duck_file_path = os.path.join(output_path, 'solution_views.ddb')

    lock_pid = is_file_locked(duck_file_path)
    if lock_pid:
        raise RuntimeError(f"Cannot open {duck_file_path}: file is locked by process PID {lock_pid}.")

    os.makedirs(os.path.dirname(duck_file_path), exist_ok=True)

    print(f"Preparing DuckDB views at: {duck_file_path}")
    print(f"Model output found: {model_directory}")

    directories = find_subdirectories(model_directory)
    if not directories:
        print(f"No subdirectories found under {model_directory}")

    canonical_views = {
        'period': 'Period',
        'data': 'data',
        'unit': 'unit'
    }

    with duckdb.connect(duck_file_path) as con:
        created_views = []

        for item in directories:
            path = os.path.join(item, "**", "*.parquet")
            base_name = os.path.basename(item).lower()

            parquet_files = glob.glob(path, recursive=True)
            if not parquet_files:
                print(f"Skipped: No parquet files found in {item}")
                continue

            if base_name == "fullkeyinfo":
                fullkeyinfo_path = path
                continue

            view_name = canonical_views.get(base_name, sanitize_view_name(item, model_directory))
            if not view_name:
                print(f"Skipped: Could not derive valid view name for {item}")
                continue

            try:
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{path}';")
                created_views.append(view_name)
                if verbose_log:
                    print(f"View created: {view_name} -> {path}")
            except Exception as e:
                print(f"Failed to create view {view_name}: {e}")

        if 'fullkeyinfo_path' in locals():
            try:
                unit_columns = []
                try:
                    unit_columns = [col[1] for col in con.execute("PRAGMA table_info(unit);").fetchall()]
                except Exception as e:
                    print(f"Warning: Unable to inspect 'unit' view: {e}")

                if 'UnitId' in unit_columns and 'UnitName' in unit_columns:
                    con.execute(f'''
                        CREATE OR REPLACE VIEW fullkeyinfo AS
                        SELECT fki.*, u.UnitName
                        FROM read_parquet('{fullkeyinfo_path}') fki
                        LEFT JOIN unit u ON fki.UnitId = u.UnitId
                    ''')
                    print("View created: fullkeyinfo (joined with unit)")
                else:
                    print("Warning: 'unit' view missing or missing required columns. Creating fullkeyinfo without join.")
                    con.execute(f'''
                        CREATE OR REPLACE VIEW fullkeyinfo AS
                        SELECT * FROM read_parquet('{fullkeyinfo_path}')
                    ''')
                    print("View created: fullkeyinfo (no join)")
                created_views.append("fullkeyinfo")
            except Exception as e:
                print(f"Failed to create fullkeyinfo view: {e}")
        else:
            print("No fullkeyinfo_path found. Skipping fullkeyinfo view creation.")

        if created_views:
            print(f"{len(created_views)} views created in DuckDB.")
        else:
            print("No views were created. Please ensure parquet files exist in your model solution.")

        memberships_csv = os.path.join(output_path, "memberships_data.csv")
        if os.path.exists(memberships_csv):
            print(f"Loading memberships CSV: {memberships_csv}")
            con.execute("DROP TABLE IF EXISTS memberships;")
            con.execute(f'''
                CREATE TABLE memberships AS 
                SELECT * FROM read_csv_auto('{memberships_csv}', HEADER=TRUE);
            ''')
            print("Memberships table created.")
        else:
            print(f"Members CSV not found at {memberships_csv}, skipping load.")

        for view in ['fullkeyinfo', 'data', 'Period']:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {view};").fetchone()[0]
                print(f"{view} contains {count} rows.")
            except Exception as e:
                print(f"Could not inspect {view}: {e}")

        if verbose_log and created_views:
            print("Previewing created views:")
            for view in created_views:
                try:
                    con.sql(f"SELECT * FROM {view} LIMIT 2").show()
                except Exception as e:
                    print(f"Failed to preview {view}: {e}")

if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        model_name = os.getenv("model_name")
        if not model_name:
            raise ValueError("Environment variable 'model_name' is not set.")
        
        prepare_duckdb(model_name, verbose_log=False)
    except Exception as e:
        print("Prepare DuckDB Views failed:")
        print(e)
    finally:
        print("done")

