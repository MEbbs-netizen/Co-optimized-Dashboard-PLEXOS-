import os
from eecloud.cloudsdk import CloudSDK, SDKBase
from eecloud.models import CommandResponse, Contracts_DatahubMapResponse, Contracts_DatahubCommandResponse
import logging
import threading

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

def run_with_timeout(func, timeout, *args, **kwargs):
    """
    Run a function with a timeout. If it exceeds timeout, raise TimeoutError.
    """
    result = [None]
    exc = [None]

    def wrapper():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            exc[0] = e

    thread = threading.Thread(target=wrapper)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        raise TimeoutError(f"Function `{func.__name__}` timed out after {timeout} seconds.")
    if exc[0]:
        raise exc[0]
    return result[0]

def log_hierarchy(rel_path, APILogger):
    parts = rel_path.split(os.sep)
    indent = ""
    for i, part in enumerate(parts):
        prefix = "├── " if i == len(parts) - 1 else "│   "
        APILogger.info(f"{indent}{prefix}{part}")
        indent += "    "

def sync_datahub(APILogger):
    try:
        cli_path = os.getenv("cloud_cli_path")
        local_path = os.getenv("output_path") or os.getcwd()
        local_base = os.path.basename(local_path.rstrip("/\\"))
        datahub_path = f"BENJAMIN DATAHUB/{local_base}"

        APILogger.info("=== DataHub Sync Start ===")
        APILogger.info(f"CLI Path: {cli_path}")
        APILogger.info(f"Local Directory: {local_path}")
        APILogger.info(f"Remote Path: {datahub_path}")

        if not cli_path or not os.path.exists(cli_path):
            APILogger.error("Skipping sync: CLI path not set or invalid.")
            return

        APILogger.info("Step 1: Initializing CloudSDK...")
        pxc = CloudSDK(cli_path=cli_path)

        APILogger.info("Step 2: Attempting to map local folder to DataHub...")
        try:
            APILogger.info(f"Attempting to map local path: {local_path} to {datahub_path}")
            map_response: list[CommandResponse[Contracts_DatahubMapResponse]] = run_with_timeout(
                pxc.datahub.map_folder,
                60,
                local_path,
                datahub_path,
                print_message=False
            )
            map_data: Contracts_DatahubMapResponse = SDKBase.get_response_data(map_response)

            if map_data is not None:
                APILogger.info(f"Map success: {map_data.Success}, Local: {map_data.LocalPath}, Remote: {map_data.RemotePath}, Patterns: {map_data.Patterns}")
            else:
                APILogger.info("Mapping already exists.")
        except Exception as ex:
            APILogger.warning(f"Mapping skipped or failed: {ex}")

        APILogger.info("Step 3: Syncing local folder with DataHub...")
        try:
            sync_response: list[CommandResponse[Contracts_DatahubCommandResponse]] = run_with_timeout(
                pxc.datahub.sync,
                180,
                local_path_to_sync=local_path,
                print_message=True
            )
            sync_data: Contracts_DatahubCommandResponse = SDKBase.get_response_data(sync_response)
            APILogger.info(f"Sync Status: {sync_data.DatahubCommandStatus.value}")
        except Exception as ex:
            APILogger.error(f"Sync failed: {ex}")
            return

        APILogger.info("Step 4: Verifying local file structure...")
        for root, _, files in os.walk(local_path):
            for fname in files:
                rel_path = os.path.relpath(os.path.join(root, fname), start=local_path)
                log_hierarchy(rel_path, APILogger)

    except Exception as e:
        APILogger.exception(f"DataHub sync failed: {e}")
    finally:
        APILogger.info("=== DataHub Sync End ===\n")

sync_datahub(APILogger)
