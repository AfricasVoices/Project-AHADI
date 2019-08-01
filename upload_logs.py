import argparse

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

Logger.set_project_name("AHADI")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Uploads output files")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file-path",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("run_id", metavar="run-id",
                        help="Identifier of this pipeline run")
    parser.add_argument("memory_profile_file_path", metavar="memory-profile-file-path",
                        help="Path to the memory profile log file to upload")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    run_id = args.run_id
    memory_profile_file_path = args.memory_profile_file_path

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
        
    memory_profile_upload_location = f"{pipeline_configuration.memory_profile_upload_url_prefix}{run_id}.profile"
    log.info(f"Uploading the memory profile from {memory_profile_file_path} to "
             f"{memory_profile_upload_location}...")
    with open(memory_profile_file_path, "rb") as f:
        google_cloud_utils.upload_file_to_blob(
            google_cloud_credentials_file_path, memory_profile_upload_location, f
        )
