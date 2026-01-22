import sys
import os
import time
import subprocess
import logging
from dotenv import load_dotenv
from datahub_sync import sync_datahub
from datahub_upload import upload_datahub

# Add the Scripts directory to the system path so that we can run Python scripts from there
scripts_dir = os.path.join(os.getcwd(), 'Scripts')
sys.path.append(scripts_dir)

def run_step(command, name, wait=True):
    print(f"\n=== Running: {name} ===")
    start = time.time()
    if wait:
        result = subprocess.run(command)
        duration = time.time() - start
        print(f"{name} completed in {duration:.2f} seconds.")
        if result.returncode != 0:
            print(f"{name} failed with return code {result.returncode}. Halting pipeline.")
            sys.exit(result.returncode)
        return result.returncode
    else:
        subprocess.Popen(command)
        print(f"{name} launched as a background process.")
        return 0

def assert_file_exists(path, name):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Required file not found: {name} at {path}")
    print(f"Verified: {name} exists at {path}")

def main():
    print("Starting pipeline...")
    load_dotenv()

    model_name = os.getenv("model_name")
    simulation_path = os.getenv("simulation_path", "/simulation")
    reference_db_path = os.path.join(simulation_path, "reference.db")
    output_path = os.getenv("output_path", "./output")

    if not model_name:
        raise EnvironmentError("Missing 'model_name' in .env file.")

    # Initialize unified APILogger
    APILogger = logging.getLogger("APILogger")
    APILogger.addHandler(logging.StreamHandler())
    APILogger.addHandler(logging.FileHandler(os.path.join(output_path, "GasModelcheck.log"), encoding="utf-8"))
    APILogger.setLevel(logging.DEBUG)

    APILogger.info("Syncing with DataHub (download only)...")
    try:
        sync_datahub(APILogger)
        APILogger.info("DataHub sync complete.")
    except Exception as e:
        APILogger.warning(f"DataHub sync failed but continuing. Reason: {e}")

    run_step(["python", "Scripts/run_simulation.py"], "Simulation")  # Updated path
    assert_file_exists(reference_db_path, "reference.db")

    run_step(["python", "Scripts/write_memberships.py"], "Write Memberships")  # Updated path
    run_step(["python", "Scripts/prepare_duckdb.py", model_name], "Prepare DuckDB Views")  # Updated path

    solution_folder = f"Model {model_name} Solution"
    solution_path = os.path.join(output_path, solution_folder)
    parquet_exists = (
        any(f.endswith(".parquet") for f in os.listdir(solution_path))
        if os.path.exists(solution_path)
        else False
    )
    if not parquet_exists:
        raise FileNotFoundError(f"No .parquet files found in {solution_path} before processing.")

    run_step(["python", "Scripts/processing_data.py"], "Data Processing")  # Updated path

    # Run dashboard in non-blocking mode
    APILogger.info("Launching Dashboard in background...")
    try:
        run_step(
            ["python", "-m", "streamlit", "run", "Scripts/postprocess_dashboard.py"],  # Updated path
            "Launch Dashboard",
            wait=False
        )
    except Exception as e:
        APILogger.warning(f"Dashboard launch failed or was interrupted. Continuing. Reason: {e}")

    # Always attempt upload after dashboard launch
    APILogger.info("Proceeding with final DataHub upload...")
    try:
        upload_datahub(APILogger)
        APILogger.info("Final DataHub upload complete.")
    except Exception as e:
        APILogger.error(f"Final DataHub upload failed. Reason: {e}")
        raise

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Pipeline crashed: {e}")
        sys.exit(1)
