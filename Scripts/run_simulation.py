import json
import os
import time
import logging
import re
import glob
from datetime import datetime
from eecloud.cloudsdk import CloudSDK
from eecloud.models import *
import pandas as pd
import openpyxl
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


def safe_filename(name):
    return re.sub(r'[<>:"/\\|?*]', "_", name)


def load_env_file(path=".env"):
    if not os.path.exists(path):
        raise FileNotFoundError(".env file not found.")
    with open(path, "r") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ[key.strip()] = value.strip()


def plexoscloud_exists(base_path):
    return os.path.isdir(os.path.join(base_path, ".plexoscloud"))


def main():
    load_env_file()

    user_name = os.getenv("user_name")
    output_path = os.getenv("output_path")
    study_id = os.getenv("study_id")
    model_name = os.getenv("model_name")
    cloud_cli_path = os.getenv("cloud_cli_path")
    study_name = os.getenv("study_name")

    if not all([user_name, output_path, model_name, cloud_cli_path, study_name]):
        raise EnvironmentError("Missing required environment variables in .env")

    solution_folder_name = f"Model {safe_filename(model_name)} Solution"
    solution_output_path = os.path.join(output_path, solution_folder_name)
    os.makedirs(solution_output_path, exist_ok=True)

    json_path = os.path.join(output_path, "payload.json")
    new_json_path = os.path.join(output_path, "new payload.json")

    APILogger.info(f"{datetime.now()} Initiating...")

    pxc = CloudSDK(cloud_cli_path)
    pxc.auth.check_authentication_status(print_message=True)

    study_folder_exists = plexoscloud_exists(output_path)

    if not study_folder_exists:
        APILogger.info("No .plexoscloud folder found. Checking if study exists in the cloud...")

        command_responses = pxc.study.find_study(
            study_name=study_name,
            print_message=True
        )
        last_command_response = pxc.solution.get_final_response(command_responses)

        if last_command_response and last_command_response.Status == "Success":
            studies_response = last_command_response.EventData
            if studies_response and studies_response.Studies:
                study_id_obj = studies_response.Studies[0].Id
                study_id = study_id_obj.Value
                APILogger.info(f"Found existing study '{study_name}' with ID: {study_id}")

                command_responses = pxc.study.clone_study(
                    study_id=study_id,
                    output_directory_path=output_path,
                    print_message=True
                )
                last_command_response = pxc.solution.get_final_response(command_responses)

                if last_command_response and last_command_response.Status == "Success":
                    study_id = last_command_response.EventData.StudyId
                    os.environ["study_id"] = study_id

                    with open(".env", "r") as f:
                        lines = f.readlines()
                    with open(".env", "w") as f:
                        for line in lines:
                            if not line.strip().startswith("study_id=") and not line.strip().startswith("simulation_path="):
                                f.write(line)
                        f.write(f"study_id={study_id}\n")
                        sim_path = os.path.normpath(os.path.join(output_path, ".plexoscloud", study_id, "Reference"))
                        f.write(f"simulation_path={sim_path}\n")

                    APILogger.info(f"Successfully cloned study. New Study ID: {study_id}")
                else:
                    APILogger.error("Failed to clone existing study.")
                    return
            else:
                APILogger.error(f"Study '{study_name}' not found in the cloud. Exiting.")
                return
        else:
            APILogger.error(f"Study '{study_name}' not found in the cloud. Exiting.")
            return
    else:
        APILogger.info(".plexoscloud folder exists. Skipping study creation or cloning.")

    try:
        pxc.study.pull_latest(study_id, print_message=True)
        APILogger.info("Pulled latest changes from cloud.")
    except Exception as e:
        APILogger.warning(f"Error pulling from cloud: {str(e)}")

    previous_changeset = None
    changeset_response = pxc.study.get_last_changeset_id(study_id)
    if changeset_response and len(changeset_response) > 0:
        previous_changeset = changeset_response[0].EventData.ChangesetId
        APILogger.info(f"Previous ChangeSet ID: {previous_changeset}")
    else:
        APILogger.warning("No previous changeset found.")

    if os.path.exists(output_path):
        os.chdir(output_path)
        APILogger.info(f"Changed working directory to local study path: {output_path}")
    else:
        APILogger.warning(f"Expected local study directory not found at: {output_path}")

    try:
        push_response = pxc.study.push_changeset(study_id, commit_message="Changes pushed from script")
        new_changeset_id = None
        for i, response in enumerate(push_response):
            if hasattr(response, 'EventData') and response.EventData:
                APILogger.info(f"Push Response [{i}]: {repr(response.EventData)}")
                new_changeset_id = getattr(response.EventData, "ChangesetId", None)
        if new_changeset_id and new_changeset_id != previous_changeset:
            changeset_id = new_changeset_id
            APILogger.info(f"Pushed new changeset: {changeset_id}")
        else:
            changeset_id = previous_changeset
            APILogger.info("No new local changes were detected. Changeset unchanged.")
    except Exception as e:
        APILogger.error(f"Error while pushing changeset: {str(e)}")
        changeset_id = previous_changeset

    class CommandResponseEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, CommandResponse):
                return {"data": obj.data}
            return super().default(obj)

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Expected input JSON file not found at: {json_path}")

    with open(json_path, "r") as file:
        my_json = json.load(file)

    my_json["studyId"] = study_id
    if changeset_id:
        my_json["ChangeSetId"] = changeset_id
        my_json["SimulationData"][0]["Uri"] = (
            f"https://studies-eu.energyexemplar.com/1.0/downloads/studies/{study_id}/changesets/{changeset_id}/input-data"
        )
    else:
        my_json["SimulationData"][0]["Uri"] = (
            f"https://studies-eu.energyexemplar.com/1.0/downloads/studies/{study_id}/input-data"
        )

    with open(new_json_path, "w") as output_file:
        json.dump(my_json, output_file, indent=2, cls=CommandResponseEncoder)

    APILogger.info(f"Modified JSON saved to {new_json_path}")

    pxc.simulation.list_simulation_engines(print_message=True)
    command_responses = pxc.simulation.enqueue_simulation(file_path=new_json_path, print_message=True)
    last_command_response = pxc.simulation.get_final_response(command_responses)

    if last_command_response is None or last_command_response.Status != "Success":
        APILogger.error("Failed to enqueue simulation.")
        return

    simulation_id = last_command_response.EventData.SimulationStarted[0].Id.Value
    APILogger.info(f"Simulation triggered. ID: {simulation_id}")

    timeout_limit = 360000
    start_time = time.time()
    simulation_successful = False

    APILogger.info("Monitoring simulation progress...")

    while True:
        command_responses = pxc.simulation.check_simulation_progress(simulation_id=simulation_id, print_message=False)
        last_command_response = pxc.simulation.get_final_response(command_responses)

        if last_command_response and last_command_response.Status == "Success":
            status = last_command_response.EventData.Status
            if status == "CompletedSuccess":
                APILogger.info("Simulation completed successfully.")
                simulation_successful = True
                break
            elif status in ["Failed", "Cancelled"]:
                APILogger.error(f"Simulation failed with status: {status}. Exiting.")
                return
        else:
            APILogger.warning("Failed to retrieve simulation status. Retrying...")

        if time.time() - start_time > timeout_limit:
            APILogger.error("Simulation monitoring timed out.")
            break

        time.sleep(30)

    if simulation_successful:
        solution_ids = []
        command_responses = pxc.solution.get_solution_id(study_id=study_id, model_name=model_name, print_message=True)
        last_command_response = pxc.solution.get_final_response(command_responses)

        if last_command_response and last_command_response.Status == "Success":
            solution_id = last_command_response.EventData.SolutionId
            solution_ids.append(solution_id)
            APILogger.info(f"Found solution ID: {solution_id}")
        else:
            APILogger.warning("Failed to retrieve solution ID.")
            return

        for sid in solution_ids:
            command_responses = pxc.solution.download_solution(
                solution_id=sid,
                output_directory=solution_output_path,
                solution_type="Parquet",
                overwrite=True,
                print_message=True
            )
            last_command_response = pxc.simulation.get_final_response(command_responses)
            if last_command_response and last_command_response.Status == "Success":
                APILogger.info(f"Downloaded solution {sid} to {solution_output_path}")
            else:
                APILogger.warning(f"Failed to download solution {sid}")

        try:
            done_path = os.path.join(solution_output_path, "parquet_ready.done")
            with open(done_path, "w") as done_file:
                done_file.write("done\n")
            APILogger.info(f"Created completion flag: {done_path}")
        except Exception as e:
            APILogger.warning(f"Failed to write parquet_ready.done: {str(e)}")
    else:
        APILogger.warning("Simulation did not complete successfully. No output downloaded.")


if __name__ == "__main__":
    main()
