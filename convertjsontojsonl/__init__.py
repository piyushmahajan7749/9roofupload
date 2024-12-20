import logging
from azure.storage.blob import BlobServiceClient
import json
import os

def main(myblob):
    """
    Azure Function Blob Trigger to transform JSON into JSONL format and save to another container.
    """
    blob_name = myblob.name
    logging.info(f"Processing blob: {blob_name}")

    # Check if the blob has a .json extension
    if not blob_name.lower().endswith('.json'):
        logging.warning(f"Skipping blob '{blob_name}' as it is not a .json file.")
        return

    try:
        # Retrieve the storage connection string
        connection_string = os.getenv("batchprocessblob_STORAGE")
        if not connection_string:
            logging.error("Missing storage connection string 'batchprocessblob_STORAGE'.")
            return

        # Initialize BlobServiceClient
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            logging.info("Successfully connected to Azure Blob Storage.")
        except Exception as e:
            logging.error(f"Failed to initialize BlobServiceClient: {e}")
            return

        # Read the blob content
        try:
            content = myblob.read().decode("utf-8")
            if not content.strip():
                logging.warning(f"Blob '{blob_name}' is empty. Skipping processing.")
                return
            logging.info(f"Successfully read content from blob '{blob_name}'.")
        except Exception as e:
            logging.error(f"Failed to read blob content '{blob_name}': {e}")
            return

        # Parse JSON content
        try:
            json_data = json.loads(content)
            logging.info(f"Successfully parsed JSON content from blob '{blob_name}'.")
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON format in blob '{blob_name}': {e}")
            return

        # Transform JSON content into JSONL format
        try:
            transformed_lines = []
            for i, item in enumerate(json_data):
                transformed_item = {
                    "custom_id": f"task-{i}",
                    "method": "POST",
                    "url": "/chat/completions",
                    "body": {
                        "model": "gpt-4o",
                        "messages": [
                            {"role": "system", "content": "You are an AI assistant that helps people find information."},
                            {"role": "user", "content": item.get("message", "")}
                        ]
                    }
                }
                transformed_lines.append(json.dumps(transformed_item))
            jsonl_content = "\n".join(transformed_lines)
            logging.info("Successfully transformed JSON content to JSONL format.")
        except Exception as e:
            logging.error(f"Failed to transform JSON content: {e}")
            return

        # Write the transformed content to the target container
        try:
            target_container_name = "jsonl-chatfiles"
            filename = os.path.basename(blob_name)
            target_blob_name = filename.replace(".json", ".jsonl")
            target_blob_client = blob_service_client.get_blob_client(
                container=target_container_name, blob=target_blob_name
            )

            target_blob_client.upload_blob(jsonl_content, overwrite=True)
            logging.info(f"Successfully uploaded JSONL file to: {target_container_name}/{target_blob_name}")
        except Exception as e:
            logging.error(f"Failed to upload transformed JSONL blob '{blob_name}': {e}")
            return

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing blob '{blob_name}': {e}")
