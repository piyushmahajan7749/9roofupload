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

        # Access uploadtoopenai-response and batchjob-results containers
        response_container = "uploadtoopenai-response"
        result_container = "batchjob-results"
        connection_string = os.getenv("batchprocessblob_STORAGE")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        response_container_client = blob_service_client.get_container_client(response_container)
        result_container_client = blob_service_client.get_container_client(result_container)

        # Iterate over all batch response files
        for blob in response_container_client.list_blobs():
            # Extract the blob name
            blob_client = response_container_client.get_blob_client(blob.name)
            response_content = blob_client.download_blob().readall()
            response_data = json.loads(response_content)

            # Extract batch_id
            batch_id = response_data.get("id")
            if not batch_id:
                logging.warning(f"No valid batch ID found in blob: {blob.name}")
                continue

            logging.info(f"Checking status for Batch ID: {batch_id}")
            status = response_data.get("status", "validating")

            # Poll for status updates
            while status not in ("completed", "failed", "canceled"):
                time.sleep(60)  # Wait 60 seconds
                batch_response = client.batches.retrieve(batch_id)
                status = batch_response.status
                logging.info(f"{datetime.datetime.now()} Batch ID: {batch_id}, Status: {status}")

                # Update the status in the response file
                response_data["status"] = status
                blob_client.upload_blob(json.dumps(response_data, indent=4), overwrite=True)

            # Handle final statuses
            if status == "completed":
                logging.info(f"Batch {batch_id} completed. Retrieving output file...")
                save_batch_file(client, batch_response.output_file_id, result_container_client, batch_id, "output")
            elif status == "failed":
                logging.error(f"Batch {batch_id} failed. Dumping detailed response...")
                if hasattr(batch_response, "errors") and batch_response.errors:
                    for error in batch_response.errors.data:
                        logging.error(f"Error code: {error.code}, Message: {error.message}")

                error_file_id = batch_response.error_file_id
                if error_file_id:
                    save_batch_file(client, error_file_id, result_container_client, batch_id, "error")
                else:
                    logging.warning(f"No error file available for Batch ID: {batch_id}.")

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

        # Convert the .jsonl content to .json
        logging.info(f"Converting {file_type} .jsonl to .json for Batch ID: {batch_id}...")
        json_content = convert_to_json(raw_responses)

        # Save the .json file
        json_blob_name = f"{batch_id}-{file_type}.json"
        json_blob_client = container_client.get_blob_client(blob=json_blob_name)

        logging.info(f"Saving {file_type} .json responses to {json_blob_name}...")
        json_blob_client.upload_blob(json_content, overwrite=True)
        logging.info(f".json file saved successfully for Batch ID: {batch_id}")

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

