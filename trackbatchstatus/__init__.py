import logging
import os
import json
import time
import datetime
from azure.storage.blob import BlobServiceClient
from openai import AzureOpenAI


def main(timer):
    """
    Timer Trigger to track batch job progress and save results to batchjob-results container.
    """
    try:
        # Initialize OpenAI client
        client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2024-10-21"
        )

        # Access Azure Blob Storage containers
        response_container = "uploadtoopenai-response"
        result_container = "batchjob-results"
        finalized_container = "finalized-batches"
        connection_string = os.getenv("batchprocessblob_STORAGE")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        response_container_client = blob_service_client.get_container_client(response_container)
        result_container_client = blob_service_client.get_container_client(result_container)
        finalized_container_client = blob_service_client.get_container_client(finalized_container)

        # Iterate over all batch response files
        for blob in response_container_client.list_blobs():
            blob_client = response_container_client.get_blob_client(blob.name)
            response_content = blob_client.download_blob().readall()
            response_data = json.loads(response_content)

            # Extract batch_id
            batch_id = response_data.get("id")
            if not batch_id:
                logging.warning(f"No valid batch ID found in blob: {blob.name}")
                continue

            # Fetch the latest status from Azure OpenAI
            try:
                batch_response = client.batches.retrieve(batch_id)
                status = batch_response.status
                file_id = batch_response.input_file_id
                file_info = client.files.retrieve(file_id)
                logging.info(file_info.model_dump_json(indent=2))
                logging.info(f"Batch Details: {batch_response.model_dump_json(indent=2)}")
                logging.info(f"Batch ID: {batch_id}, Updated Status: {status}")

                # Update the response file with the latest status
                response_data["status"] = status
                blob_client.upload_blob(json.dumps(response_data, indent=4), overwrite=True)

                # Save the batch output or error files if finalized
                if status == "completed":
                    logging.info(f"Batch {batch_id} is completed. Saving output files...")
                    save_batch_file(client, batch_response.output_file_id, result_container_client, batch_id, "output")
                elif status == "failed":
                    logging.info(f"Batch {batch_id} failed. Saving error files...")
                    save_batch_file(client, batch_response.error_file_id, result_container_client, batch_id, "error")

                # Move finalized files to the finalized-batches container
                if status in ("completed", "failed", "canceled"):
                    logging.info(f"Batch {batch_id} has reached a finalized state with status: {status}.")
                    logging.info(f"Initiating move of blob '{blob.name}' to the finalized-batches container in the '{status}' folder...")
                    move_to_finalized(blob_client, finalized_container_client, blob.name, status)
                    logging.info(f"Blob '{blob.name}' successfully moved to finalized-batches/{status}.")
                else:
                    logging.info(f"Batch {batch_id} is still in progress with status: {status}. Retrying in next execution.")

            except Exception as e:
                logging.error(f"Error fetching status for Batch ID {batch_id}: {str(e)}")

    except Exception as e:
        logging.error(f"An error occurred while tracking batch jobs: {str(e)}")



def save_batch_file(client, file_id, container_client, batch_id, file_type):
    """
    Helper function to save batch output or error files to batchjob-results container.
    Saves both .jsonl and .json versions.
    """
    try:
        if not file_id:
            logging.warning(f"No {file_type} file available for Batch ID: {batch_id}.")
            return

        # Retrieve the file content
        logging.info(f"Retrieving {file_type} file for Batch ID: {batch_id}...")
        file_response = client.files.content(file_id)
        raw_responses = file_response.text.strip()

        # Save the .jsonl file
        jsonl_blob_name = f"{batch_id}-{file_type}.jsonl"
        jsonl_blob_client = container_client.get_blob_client(blob=jsonl_blob_name)

        logging.info(f"Saving {file_type} .jsonl responses to {jsonl_blob_name}...")
        jsonl_blob_client.upload_blob(raw_responses, overwrite=True)
        logging.info(f".jsonl file saved successfully for Batch ID: {batch_id}")

    except Exception as e:
        logging.error(f"An error occurred while saving {file_type} file for Batch ID: {batch_id}: {str(e)}")


def convert_to_json(jsonl_content):
    """
    Converts JSONL content to a JSON array.
    
    Args:
        jsonl_content (str): The content of a JSONL file as a string.
    
    Returns:
        str: The content as a JSON array (pretty-printed JSON string).
    """
    try:
        # Split the content into lines and parse each as a JSON object
        json_objects = [json.loads(line) for line in jsonl_content.strip().split("\n")]
        # Convert the list of JSON objects to a JSON array
        json_array = json.dumps(json_objects, indent=4)
        return json_array
    except Exception as e:
        logging.error(f"Error converting JSONL to JSON: {str(e)}")
        raise

def move_to_finalized(blob_client, finalized_container_client, blob_name, status):
    """
    Moves a blob to the finalized-batches container into the appropriate folder based on its status.
    """
    try:
        folder_map = {
            "completed": "completed",
            "failed": "failed",
            "canceled": "canceled"
        }
        folder_name = folder_map.get(status, "others")
        finalized_blob_name = f"{folder_name}/{os.path.basename(blob_name)}"

        logging.info(f"Moving blob {blob_name} to finalized-batches/{folder_name}...")
        finalized_blob_client = finalized_container_client.get_blob_client(finalized_blob_name)

        # Download content from the source blob and upload to the finalized container
        blob_content = blob_client.download_blob().readall()
        finalized_blob_client.upload_blob(blob_content, overwrite=True)

        # Delete the source blob
        blob_client.delete_blob()
        logging.info(f"Blob {blob_name} successfully moved to finalized-batches/{folder_name}.")

    except Exception as e:
        logging.error(f"An error occurred while moving blob {blob_name} to finalized-batches: {str(e)}")