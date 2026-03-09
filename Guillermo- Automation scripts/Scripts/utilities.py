import os
import time
import logging
from eecloud.cloudsdk import CloudSDK
from eecloud.models import *

def download_solution(APILogger):
    user_name = os.getenv("user_name")
    output_path = os.getenv("output_path")
    study_id = os.getenv("study_id")
    model_name = os.getenv("model_name")
    cloud_cli_path = os.getenv("cloud_cli_path")

    if not all([user_name, output_path, study_id, model_name, cloud_cli_path]):
        raise EnvironmentError("Missing required environment variables in .env")

    solution_folder_name = f"Model {sanitize_filename(model_name)} Solution"
    solution_output_path = os.path.join(output_path, solution_folder_name)
    os.makedirs(solution_output_path, exist_ok=True)

    pxc = CloudSDK(cloud_cli_path)
    pxc.auth.check_authentication_status(print_message=True)

    try:
        pxc.study.pull_latest(study_id, print_message=True)
        APILogger.info("Pulled latest changes from cloud.")
    except Exception as e:
        APILogger.warning(f"Error pulling from cloud: {str(e)}")

    solution_ids = []
    command_responses = pxc.solution.get_solution_id(study_id=study_id, model_name=model_name, print_message=True)
    last_command_response: CommandResponse[Contracts_GetSolutionIdResponse] = pxc.solution.get_final_response(command_responses)

    if last_command_response and last_command_response.Status == "Success":
        solution_id = last_command_response.EventData.SolutionId
        solution_ids.append(solution_id)
        APILogger.info(f"Found solution ID: {solution_id}")
    else:
        APILogger.warning("Failed to retrieve solution ID.")
        return

    APILogger.info(f"Using solution output path: {solution_output_path}")
    solution_type = "Parquet"
    for sid in solution_ids:
        command_responses = pxc.solution.download_solution(
            solution_id=sid,
            output_directory=solution_output_path,
            solution_type=solution_type,
            overwrite=True,
            print_message=True
        )
        last_command_response: CommandResponse[Contracts_DownloadSolution] = pxc.simulation.get_final_response(command_responses)
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


def sanitize_filename(name):
    import re
    return re.sub(r'[<>:"/\\|?*]', "_", name)
