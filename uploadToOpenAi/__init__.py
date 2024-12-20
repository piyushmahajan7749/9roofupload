import logging
import os
from azure.storage.blob import BlobServiceClient
from openai import AzureOpenAI
import json
import time
import io

def main(myblob):
    """
    Blob Trigger to upload JSONL file to Azure OpenAI for batch processing and save the response.
    """
    try:
        # Get the blob name
        blob_name = myblob.name
        logging.info(f"Processing blob: {blob_name}")

        # Ensure the file is a JSONL file
        if not blob_name.lower().endswith('.jsonl'):
            logging.warning(f"Skipping blob '{blob_name}' as it is not a .jsonl file.")
            return

        # Read the blob content
        blob_content = myblob.read()
        logging.info(f"Read content from blob '{blob_name}'.")

        # Initialize OpenAI client
        client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2024-10-21"
        )

        # Step 1: Upload the JSONL file to Azure OpenAI
        logging.info("Uploading the JSONL file to Azure OpenAI...")
        filename = os.path.basename(blob_name)
        jsonl_file = io.BytesIO(blob_content)
        jsonl_file.name = filename  # Set filename for proper validation

        file_response = client.files.create(
            file=jsonl_file,
            purpose="batch"
        )
        file_id = file_response.id
        logging.info(f"File uploaded successfully to Azure OpenAI. File ID: {file_id}")

        # Step 2: Poll for file status until 'Processed'
        logging.info("Polling file status until 'Processed'...")
        file_status = "pending"
        while file_status not in ["processed", "failed"]:
            time.sleep(10)  # Wait for 10 seconds before checking again
            file_info = client.files.retrieve(file_id)
            file_status = file_info.status
            logging.info(f"Current file status: {file_status}")

            if file_status == "failed":
                logging.error("File processing failed. Exiting.")
                return

        # Step 3: Submit a batch job
        logging.info("Submitting the batch job to Azure OpenAI...")
        batch_response = client.batches.create(
            input_file_id=file_id,
            endpoint="/chat/completions",
            completion_window="24h",
        )
        batch_id = batch_response.id
        logging.info(f"Batch job submitted successfully. Batch ID: {batch_id}")

        # Step 4: Save full batch response to uploadtoopenai-response container
        logging.info("Saving batch response to uploadtoopenai-response container...")
        container_name = "uploadtoopenai-response"
        response_blob_name = f"{filename.replace('.jsonl', '')}-response.json"
        connection_string = os.getenv("batchprocessblob_STORAGE")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        response_blob_client = blob_service_client.get_blob_client(container=container_name, blob=response_blob_name)

        response_json = json.dumps(batch_response.model_dump(), indent=4)
        response_blob_client.upload_blob(response_json, overwrite=True)
        logging.info(f"Batch response saved successfully: {container_name}/{response_blob_name}")

    except Exception as e:
        logging.error(f"An error occurred while processing blob '{blob_name}': {str(e)}")
