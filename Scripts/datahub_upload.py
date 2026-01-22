import os
import shutil
import tempfile
from eecloud.cloudsdk import CloudSDK
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


EXCLUDED_EXTENSIONS = {".log", ".bak", ".csv"}

def is_included_file(rel_path):
    name_lower = rel_path.lower()
    if ".plexoscloud" in name_lower:
        return True
    if "solution" in name_lower or "timeseries" in name_lower:
        return False
    if os.path.splitext(rel_path)[1].lower() in EXCLUDED_EXTENSIONS:
        return False
    return True

def log_hierarchy(rel_path, APILogger):
    parts = rel_path.split(os.sep)
    indent = ""
    for i, part in enumerate(parts):
        prefix = "├── " if i == len(parts) - 1 else "│   "
        APILogger.info(f"{indent}{prefix}{part}")
        indent += "    "

def upload_datahub(APILogger):
    try:
        cli_path = os.getenv("cloud_cli_path")
        local_dir = os.getenv("output_path") or os.getcwd()
        local_base = os.path.basename(local_dir.rstrip("/\\"))
        remote_path = f"Benjamin/{local_base}"

        APILogger.info("=== DataHub Upload Start ===")
        APILogger.info(f"Uploading from: {local_dir}")
        APILogger.info(f"Uploading to: {remote_path}")

        if not cli_path or not os.path.exists(cli_path):
            APILogger.error("Skipping upload: CLI path not set or invalid.")
            return

        pxc = CloudSDK(cli_path=cli_path)

        APILogger.info("Preparing filtered content for upload...")

        with tempfile.TemporaryDirectory() as temp_dir:
            for root, _, files in os.walk(local_dir):
                for fname in files:
                    src_path = os.path.join(root, fname)
                    rel_path = os.path.relpath(src_path, start=local_dir)

                    if ".plexoscloud" in rel_path.lower() or is_included_file(rel_path):
                        dst_path = os.path.join(temp_dir, rel_path)
                        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                        shutil.copy2(src_path, dst_path)
                        log_hierarchy(rel_path, APILogger)

            APILogger.info("Starting upload to DataHub...")
            pxc.datahub.upload(
                local_folder=temp_dir,
                remote_folder=remote_path,
                glob_patterns=["**/*"],
                is_versioned=False,
                print_message=True
            )

        APILogger.info("Upload completed successfully.")

    except Exception as e:
        APILogger.exception(f"DataHub upload failed: {e}")
    finally:
        APILogger.info("=== DataHub Upload End ===\n")

upload_datahub(APILogger)
