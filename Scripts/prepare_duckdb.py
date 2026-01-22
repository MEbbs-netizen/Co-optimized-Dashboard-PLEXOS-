import os
import sys
import duckdb
import glob
import re
from typing import List, Optional
from dotenv import load_dotenv
import psutil
import time  # Import time module for delay functionality
import logging

APILogger = logging.getLogger("APILogger")
if not APILogger.handlers:
    APILogger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    APILogger.addHandler(sh)

    # Log file goes under output_path if provided, otherwise current directory
    log_dir = os.getenv("output_path", ".")
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(log_dir, "GasModelcheck.log"), encoding="utf-8")
    fh.setFormatter(formatter)
    APILogger.addHandler(fh)


def sanitize_view_name(path: str, base: str) -> str:
    rel_name = os.path.relpath(path, base)
    return re.sub(r'\W+', '_', rel_name).strip('_')


def find_subdirectories(root_dir: str) -> List[str]:
    subdirectories: List[str] = []
    for dirpath, dirnames, _ in os.walk(root_dir):
        for dirname in dirnames:
            subdirectories.append(os.path.join(dirpath, dirname))
    return subdirectories


def is_file_locked(filepath: str, retries: int = 5, delay: int = 2) -> Optional[int]:
    """
    Check if the file is locked by any process. If it is, retry a few times before giving up.
    Returns PID if locked after retries, else None.
    """
    for _ in range(retries):
        locked_pid = None
        for proc in psutil.process_iter(['pid', 'open_files']):
            try:
                files = proc.info.get('open_files')
                if files:
                    for f in files:
                        if os.path.abspath(f.path) == os.path.abspath(filepath):
                            locked_pid = proc.info['pid']
                            break
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if not locked_pid:
            return None

        print(f"File is locked by PID {locked_pid}. Retrying in {delay} seconds...")
        time.sleep(delay)

    return locked_pid


def prepare_duckdb(model_name: str, verbose_log: bool = False) -> None:
    """
    Build a SELF-CONTAINED DuckDB file (solution_views.ddb) by MATERIALIZING parquet-backed
    datasets into DuckDB TABLES (not VIEWS). This prevents the deployed Streamlit app from
    trying to read parquet files at runtime (e.g., from Windows C:\\Users\\... paths).
    """
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

    print(f"Preparing DuckDB tables at: {duck_file_path}")
    print(f"Model output found: {model_directory}")

    directories = find_subdirectories(model_directory)
    if not directories:
        print(f"No subdirectories found under {model_directory}")

    canonical_views = {
        'period': 'Period',
        'data': 'data',
        'unit': 'unit'
    }

    fullkeyinfo_path = None

    with duckdb.connect(duck_file_path) as con:
        created_objects: List[str] = []

        for item in directories:
            path = os.path.join(item, "**", "*.parquet")
            base_name = os.path.basename(item).lower()

            parquet_files = glob.glob(path, recursive=True)
            if not parquet_files:
                if verbose_log:
                    print(f"Skipped: No parquet files found in {item}")
                continue

            if base_name == "fullkeyinfo":
                fullkeyinfo_path = path
                continue

            table_name = canonical_views.get(base_name, sanitize_view_name(item, model_directory))
            if not table_name:
                print(f"Skipped: Could not derive valid table name for {item}")
                continue

            try:
                # Materialize parquet into a DuckDB TABLE (self-contained)
                con.execute(f"DROP TABLE IF EXISTS {table_name};")
                con.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{path}');
                """)
                created_objects.append(table_name)
                if verbose_log:
                    print(f"Table created: {table_name} <- {path}")
            except Exception as e:
                print(f"Failed to create table {table_name}: {e}")

        # Materialize fullkeyinfo (optionally joined with unit)
        if fullkeyinfo_path:
            try:
                # Build base table from parquet
                con.execute("DROP TABLE IF EXISTS fullkeyinfo_base;")
                con.execute(f"""
                    CREATE TABLE fullkeyinfo_base AS
                    SELECT * FROM read_parquet('{fullkeyinfo_path}');
                """)

                # Inspect unit table if present
                unit_columns: List[str] = []
                try:
                    unit_columns = [col[1] for col in con.execute("PRAGMA table_info(unit);").fetchall()]
                except Exception as e:
                    print(f"Warning: Unable to inspect 'unit' table: {e}")

                con.execute("DROP TABLE IF EXISTS fullkeyinfo;")

                if 'UnitId' in unit_columns and 'UnitName' in unit_columns:
                    con.execute("""
                        CREATE TABLE fullkeyinfo AS
                        SELECT fki.*, u.UnitName
                        FROM fullkeyinfo_base fki
                        LEFT JOIN unit u ON fki.UnitId = u.UnitId
                    """)
                    print("Table created: fullkeyinfo (joined with unit)")
                else:
                    print("Warning: 'unit' table missing or missing required columns. Creating fullkeyinfo without join.")
                    con.execute("""
                        CREATE TABLE fullkeyinfo AS
                        SELECT * FROM fullkeyinfo_base
                    """)
                    print("Table created: fullkeyinfo (no join)")

                # Cleanup base
                con.execute("DROP TABLE IF EXISTS fullkeyinfo_base;")

                created_objects.append("fullkeyinfo")
            except Exception as e:
                print(f"Failed to create fullkeyinfo table: {e}")
        else:
            print("No fullkeyinfo parquet folder found. Skipping fullkeyinfo creation.")

        if created_objects:
            print(f"{len(created_objects)} tables created in DuckDB.")
        else:
            print("No tables were created. Please ensure parquet files exist in your model solution.")

        # Optional: load memberships CSV into a table (already self-contained)
        memberships_csv = os.path.join(output_path, "memberships_data.csv")
        if os.path.exists(memberships_csv):
            print(f"Loading memberships CSV: {memberships_csv}")
            con.execute("DROP TABLE IF EXISTS memberships;")
            con.execute(f"""
                CREATE TABLE memberships AS
                SELECT * FROM read_csv_auto('{memberships_csv}', HEADER=TRUE);
            """)
            print("Memberships table created.")
        else:
            print(f"Members CSV not found at {memberships_csv}, skipping load.")

        # Quick sanity checks
        for name in ['fullkeyinfo', 'data', 'Period']:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {name};").fetchone()[0]
                print(f"{name} contains {count} rows.")
            except Exception as e:
                print(f"Could not inspect {name}: {e}")

        if verbose_log and created_objects:
            print("Previewing created tables:")
            for name in created_objects:
                try:
                    con.sql(f"SELECT * FROM {name} LIMIT 2").show()
                except Exception as e:
                    print(f"Failed to preview {name}: {e}")


if __name__ == "__main__":
    try:
        load_dotenv()

        model_name = os.getenv("model_name")
        if not model_name:
            raise ValueError("Environment variable 'model_name' is not set.")

        prepare_duckdb(model_name, verbose_log=False)
    except Exception as e:
        print("Prepare DuckDB (tables) failed:")
        print(e)
    finally:
        print("done")
