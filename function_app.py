# import azure.functions as func
# import datetime
# import json
# import logging

# app = func.FunctionApp()

# @app.event_grid_trigger(arg_name="azeventgrid")
# def EventGridTrigger(azeventgrid: func.EventGridEvent):
#     logging.info('Python EventGrid trigger processed an event')

import azure.functions as func
import datetime
import logging
import os

from urllib.parse import urlparse, unquote
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

STORAGE_ACCOUNT_NAME = "proservicestorage"

LANDING_CONTAINER = "landingzone"
RAW_CONTAINER = "raw"
ARCHIVE_CONTAINER = "archive"

SOURCE_FOLDER = "bhatbhateni"


@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):

    logging.info("==============================================")
    logging.info("Event Grid trigger started")
    logging.info("==============================================")

    try:

        # -----------------------------------------------------
        # 1. Read Event Grid event
        # -----------------------------------------------------

        event_data = azeventgrid.get_json()

        logging.info(f"Event data: {event_data}")

        blob_url = event_data.get("url")

        if not blob_url:
            logging.error("Blob URL not found in Event Grid event.")
            return

        logging.info(f"Blob URL: {blob_url}")

        # Example:
        #
        # https://proservicestorage.blob.core.windows.net/
        # landingzone/bhatbhateni/sales.csv


        # -----------------------------------------------------
        # 2. Parse blob URL
        # -----------------------------------------------------

        parsed_url = urlparse(blob_url)

        storage_host = parsed_url.netloc

        expected_host = (
            f"{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        )

        # Ignore events from another storage account
        if storage_host.lower() != expected_host.lower():

            logging.warning(
                f"Ignoring event from storage account: "
                f"{storage_host}"
            )

            return


        # Example path:
        #
        # /landingzone/bhatbhateni/sales.csv

        full_path = unquote(parsed_url.path).strip("/")

        path_parts = full_path.split("/")

        if len(path_parts) < 3:

            logging.warning(
                f"Unexpected blob path: {full_path}"
            )

            return


        # -----------------------------------------------------
        # 3. Get container and blob path
        # -----------------------------------------------------

        source_container = path_parts[0]

        source_blob_path = "/".join(path_parts[1:])


        logging.info(
            f"Source container: {source_container}"
        )

        logging.info(
            f"Source blob path: {source_blob_path}"
        )


        # -----------------------------------------------------
        # 4. IMPORTANT:
        #    Process ONLY landingzone
        # -----------------------------------------------------

        if source_container != LANDING_CONTAINER:

            logging.info(
                f"Ignoring event because container is "
                f"'{source_container}'."
            )

            return


        # -----------------------------------------------------
        # 5. We expect:
        #
        # bhatbhateni/file.csv
        #
        # No additional subfolders
        # -----------------------------------------------------

        blob_parts = source_blob_path.split("/")


        if len(blob_parts) != 2:

            logging.warning(
                "File ignored. Expected structure: "
                "landingzone/bhatbhateni/<filename>"
            )

            return


        folder_name = blob_parts[0]
        original_file_name = blob_parts[1]


        # -----------------------------------------------------
        # 6. Only process bhatbhateni folder
        # -----------------------------------------------------

        if folder_name.lower() != SOURCE_FOLDER.lower():

            logging.info(
                f"Ignoring folder: {folder_name}"
            )

            return


        logging.info(
            f"Original filename: {original_file_name}"
        )


        # -----------------------------------------------------
        # 7. Generate UTC timestamp
        #
        # Example:
        # 20260901_123045
        # -----------------------------------------------------

        timestamp = datetime.datetime.now(
            datetime.timezone.utc
        ).strftime("%Y%m%d_%H%M%S")


        # -----------------------------------------------------
        # 8. Add timestamp BEFORE extension
        #
        # sales.csv
        #
        # becomes:
        #
        # sales_20260901_123045.csv
        # -----------------------------------------------------

        if "." in original_file_name:

            file_name_without_extension, extension = (
                original_file_name.rsplit(".", 1)
            )

            raw_file_name = (
                f"{file_name_without_extension}_"
                f"{timestamp}.{extension}"
            )

        else:

            raw_file_name = (
                f"{original_file_name}_{timestamp}"
            )


        logging.info(
            f"Raw filename: {raw_file_name}"
        )


        # -----------------------------------------------------
        # 9. Destination paths
        # -----------------------------------------------------

        # RAW:
        #
        # bhatbhateni/sales_20260901_123045.csv

        raw_blob_path = (
            f"{SOURCE_FOLDER}/{raw_file_name}"
        )


        # ARCHIVE:
        #
        # bhatbhateni/sales.csv
        #
        # Exact original filename

        archive_blob_path = (
            f"{SOURCE_FOLDER}/{original_file_name}"
        )


        logging.info(
            f"RAW destination: "
            f"{RAW_CONTAINER}/{raw_blob_path}"
        )

        logging.info(
            f"ARCHIVE destination: "
            f"{ARCHIVE_CONTAINER}/{archive_blob_path}"
        )


        # -----------------------------------------------------
        # 10. Connect to Azure Storage
        # -----------------------------------------------------

        connection_string = os.environ[
            "AzureWebJobsStorage"
        ]


        blob_service_client = (
            BlobServiceClient.from_connection_string(
                connection_string
            )
        )


        # -----------------------------------------------------
        # 11. Source blob client
        # -----------------------------------------------------

        source_blob_client = (
            blob_service_client.get_blob_client(
                container=LANDING_CONTAINER,
                blob=source_blob_path
            )
        )


        # -----------------------------------------------------
        # 12. Check source exists
        # -----------------------------------------------------

        if not source_blob_client.exists():

            logging.warning(
                f"Source blob no longer exists: "
                f"{source_blob_path}"
            )

            return


        # -----------------------------------------------------
        # 13. Get source properties
        #
        # This lets us retain content type such as:
        # text/csv
        # application/json
        # etc.
        # -----------------------------------------------------

        source_properties = (
            source_blob_client.get_blob_properties()
        )


        # -----------------------------------------------------
        # 14. Download original blob
        # -----------------------------------------------------

        logging.info(
            "Downloading file from landingzone..."
        )


        blob_data = (
            source_blob_client
            .download_blob()
            .readall()
        )


        logging.info(
            f"Downloaded {len(blob_data)} bytes."
        )


        # -----------------------------------------------------
        # 15. RAW blob client
        # -----------------------------------------------------

        raw_blob_client = (
            blob_service_client.get_blob_client(
                container=RAW_CONTAINER,
                blob=raw_blob_path
            )
        )


        # -----------------------------------------------------
        # 16. Upload timestamped file to RAW
        # -----------------------------------------------------

        logging.info(
            "Uploading timestamped file to RAW..."
        )


        raw_blob_client.upload_blob(
            blob_data,
            overwrite=False,
            content_settings=source_properties.content_settings,
            metadata=source_properties.metadata
        )


        logging.info(
            f"RAW upload successful: "
            f"{RAW_CONTAINER}/{raw_blob_path}"
        )


        # -----------------------------------------------------
        # 17. ARCHIVE blob client
        # -----------------------------------------------------

        archive_blob_client = (
            blob_service_client.get_blob_client(
                container=ARCHIVE_CONTAINER,
                blob=archive_blob_path
            )
        )


        # -----------------------------------------------------
        # 18. Copy ORIGINAL file to ARCHIVE
        #
        # Original filename is preserved.
        # -----------------------------------------------------

        logging.info(
            "Moving original file to ARCHIVE..."
        )


        archive_blob_client.upload_blob(
            blob_data,

            # Important:
            # If sales.csv already exists in archive,
            # the latest version replaces it.
            overwrite=True,

            content_settings=source_properties.content_settings,
            metadata=source_properties.metadata
        )


        logging.info(
            f"ARCHIVE upload successful: "
            f"{ARCHIVE_CONTAINER}/{archive_blob_path}"
        )


        # -----------------------------------------------------
        # 19. Delete original from landingzone
        #
        # Do this ONLY AFTER:
        #
        # 1. RAW succeeded
        # 2. ARCHIVE succeeded
        # -----------------------------------------------------

        logging.info(
            "Deleting original file from landingzone..."
        )


        source_blob_client.delete_blob()


        logging.info(
            f"Deleted: "
            f"{LANDING_CONTAINER}/{source_blob_path}"
        )


        # -----------------------------------------------------
        # 20. Success
        # -----------------------------------------------------

        logging.info("==============================================")
        logging.info("FILE PROCESSING SUCCESSFUL")
        logging.info(
            f"SOURCE  : "
            f"{LANDING_CONTAINER}/{source_blob_path}"
        )
        logging.info(
            f"RAW     : "
            f"{RAW_CONTAINER}/{raw_blob_path}"
        )
        logging.info(
            f"ARCHIVE : "
            f"{ARCHIVE_CONTAINER}/{archive_blob_path}"
        )
        logging.info("==============================================")


    except Exception as e:

        logging.exception(
            f"Error processing Event Grid event: {str(e)}"
        )

        # Raising the exception tells Azure Functions
        # that processing failed.
        raise