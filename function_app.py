import datetime
import logging
import os
from urllib.parse import unquote, urlparse

import azure.functions as func
from azure.core import MatchConditions
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

STORAGE_ACCOUNT_NAME = "datastorageacc12"
DATA_CONNECTION_SETTING = "DATA_STORAGE_CONNECTION_STRING"

LANDING_CONTAINER = "landing-zone"
RAW_CONTAINER = "raw"
ARCHIVE_CONTAINER = "archive"

SOURCE_FOLDER = "bhatbhateni"


@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):

    logging.info("========================================")
    logging.info("Event Grid trigger started")
    logging.info("========================================")

    try:
        # -----------------------------------------------------
        # 1. Read Event Grid event
        # -----------------------------------------------------

        event_data = azeventgrid.get_json()
        blob_url = event_data.get("url")

        if not blob_url:
            raise ValueError(
                "Blob URL was not found in the Event Grid event."
            )

        logging.info("Blob URL: %s", blob_url)

        # Expected URL:
        # https://datastorageacc12.blob.core.windows.net/
        # landing-zone/bhatbhateni/Book1.csv

        # -----------------------------------------------------
        # 2. Parse and validate the blob URL
        # -----------------------------------------------------

        parsed_url = urlparse(blob_url)

        expected_host = (
            f"{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        )

        if parsed_url.netloc.lower() != expected_host.lower():

            logging.info(
                "Ignoring event from another storage account: %s",
                parsed_url.netloc,
            )

            return

        full_path = unquote(
            parsed_url.path
        ).strip("/")

        path_parts = full_path.split("/")

        # Expected:
        # landing-zone / bhatbhateni / filename.csv

        if len(path_parts) != 3:

            logging.info(
                "Ignoring unexpected blob path. "
                "Expected landing-zone/bhatbhateni/<filename>. "
                "Received: %s",
                full_path,
            )

            return

        source_container = path_parts[0]
        folder_name = path_parts[1]
        original_file_name = path_parts[2]

        # -----------------------------------------------------
        # 3. Process only landing-zone
        # -----------------------------------------------------

        if (
            source_container.lower()
            != LANDING_CONTAINER.lower()
        ):

            logging.info(
                "Ignoring container: %s",
                source_container,
            )

            return

        # -----------------------------------------------------
        # 4. Process only bhatbhateni
        # -----------------------------------------------------

        if folder_name.lower() != SOURCE_FOLDER.lower():

            logging.info(
                "Ignoring folder: %s",
                folder_name,
            )

            return

        if not original_file_name:

            logging.info(
                "Ignoring event without a filename."
            )

            return

        source_blob_path = (
            f"{folder_name}/{original_file_name}"
        )

        logging.info(
            "Source blob: %s/%s",
            LANDING_CONTAINER,
            source_blob_path,
        )

        # -----------------------------------------------------
        # 5. Create timestamped RAW filename
        # -----------------------------------------------------

        timestamp = datetime.datetime.now(
            datetime.timezone.utc
        ).strftime("%Y%m%d_%H%M%S_%f")

        base_name, separator, extension = (
            original_file_name.rpartition(".")
        )

        if separator and base_name:

            raw_file_name = (
                f"{base_name}_{timestamp}.{extension}"
            )

        else:

            raw_file_name = (
                f"{original_file_name}_{timestamp}"
            )

        raw_blob_path = (
            f"{SOURCE_FOLDER}/{raw_file_name}"
        )

        archive_blob_path = (
            f"{SOURCE_FOLDER}/{original_file_name}"
        )

        logging.info(
            "RAW destination: %s/%s",
            RAW_CONTAINER,
            raw_blob_path,
        )

        logging.info(
            "Archive destination: %s/%s",
            ARCHIVE_CONTAINER,
            archive_blob_path,
        )

        # -----------------------------------------------------
        # 6. Connect to datastorageacc12
        # -----------------------------------------------------

        # Add DATA_STORAGE_CONNECTION_STRING in:
        # Function App → Settings → Environment variables

        connection_string = os.getenv(
            DATA_CONNECTION_SETTING
        )

        if not connection_string:

            raise RuntimeError(
                "Missing Function App environment setting: "
                f"{DATA_CONNECTION_SETTING}"
            )

        blob_service_client = (
            BlobServiceClient.from_connection_string(
                connection_string
            )
        )

        # Confirm the connection string points to the
        # correct storage account.

        connected_account = (
            blob_service_client.account_name
        )

        if (
            connected_account.lower()
            != STORAGE_ACCOUNT_NAME.lower()
        ):

            raise RuntimeError(
                f"{DATA_CONNECTION_SETTING} points to "
                f"'{connected_account}', but it must point "
                f"to '{STORAGE_ACCOUNT_NAME}'."
            )

        logging.info(
            "Connected to storage account: %s",
            connected_account,
        )

        # -----------------------------------------------------
        # 7. Get source blob
        # -----------------------------------------------------

        source_blob_client = (
            blob_service_client.get_blob_client(
                container=LANDING_CONTAINER,
                blob=source_blob_path,
            )
        )

        # A duplicate Event Grid event may arrive after
        # the file has already been processed.

        if not source_blob_client.exists():

            logging.info(
                "Source blob is no longer present. "
                "It may already have been processed: %s/%s",
                LANDING_CONTAINER,
                source_blob_path,
            )

            return

        # -----------------------------------------------------
        # 8. Get properties and download
        # -----------------------------------------------------

        source_properties = (
            source_blob_client.get_blob_properties()
        )

        blob_data = (
            source_blob_client
            .download_blob()
            .readall()
        )

        logging.info(
            "Downloaded %d bytes.",
            len(blob_data),
        )

        # -----------------------------------------------------
        # 9. Upload timestamped copy to RAW
        # -----------------------------------------------------

        raw_blob_client = (
            blob_service_client.get_blob_client(
                container=RAW_CONTAINER,
                blob=raw_blob_path,
            )
        )

        logging.info(
            "Uploading timestamped file to RAW..."
        )

        raw_blob_client.upload_blob(
            blob_data,
            overwrite=False,
            content_settings=(
                source_properties.content_settings
            ),
            metadata=source_properties.metadata,
        )

        logging.info(
            "RAW upload successful: %s/%s",
            RAW_CONTAINER,
            raw_blob_path,
        )

        # -----------------------------------------------------
        # 10. Upload original filename to ARCHIVE
        # -----------------------------------------------------

        archive_blob_client = (
            blob_service_client.get_blob_client(
                container=ARCHIVE_CONTAINER,
                blob=archive_blob_path,
            )
        )

        logging.info(
            "Uploading original file to archive..."
        )

        archive_blob_client.upload_blob(
            blob_data,
            overwrite=True,
            content_settings=(
                source_properties.content_settings
            ),
            metadata=source_properties.metadata,
        )

        logging.info(
            "Archive upload successful: %s/%s",
            ARCHIVE_CONTAINER,
            archive_blob_path,
        )

        # -----------------------------------------------------
        # 11. Delete from landing-zone
        # -----------------------------------------------------
        # This happens only after RAW and archive uploads
        # have completed successfully.

        logging.info(
            "Deleting original file from landing-zone..."
        )

        source_blob_client.delete_blob(
            delete_snapshots="include",
            etag=source_properties.etag,
            match_condition=(
                MatchConditions.IfNotModified
            ),
        )

        # -----------------------------------------------------
        # 12. Success
        # -----------------------------------------------------

        logging.info("========================================")
        logging.info("FILE PROCESSING SUCCESSFUL")
        logging.info(
            "SOURCE DELETED: %s/%s",
            LANDING_CONTAINER,
            source_blob_path,
        )
        logging.info(
            "RAW CREATED: %s/%s",
            RAW_CONTAINER,
            raw_blob_path,
        )
        logging.info(
            "ARCHIVE CREATED: %s/%s",
            ARCHIVE_CONTAINER,
            archive_blob_path,
        )
        logging.info("========================================")

    except Exception:

        logging.exception(
            "Error processing Event Grid event"
        )

        # Mark the Function invocation as failed.
        raise