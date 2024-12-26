import logging
import os
import json
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureKeyCredential
from azure.maps.search import MapsSearchClient
from azure.core.exceptions import HttpResponseError

def main(myblob):
    """
    Azure Function Blob Trigger to geocode addresses from processed batch results and save updated data.
    """
    try:
        # Get blob name
        blob_name = myblob.name
        logging.info(f"Processing geocoding for blob: {blob_name}")

        # Read blob content
        content = myblob.read().decode("utf-8")
        data = [json.loads(line) for line in content.strip().split("\n")]

        # Azure Maps setup
        maps_credential = AzureKeyCredential(os.getenv("AZURE_MAPS_KEY"))
        maps_client = MapsSearchClient(credential=maps_credential)

        # Batch geocoding
        batch_size = 10  # Define your batch size here
        batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
        results = []

        for batch in batches:
            batch_request = {"batchItems": []}
            
            for item in batch:
                message_content = item.get("response", {}).get("body", {}).get("choices", [])[0].get("message", {}).get("content", "")
                if message_content:
                    message_data = json.loads(message_content)
                    location = message_data.get("location", "")
                    if location:
                        query = location if "india" in location.lower() else f"{location}, India"
                        logging.debug(f"Appending query: {query}")
                        batch_request["batchItems"].append({"query": query})
                    else:
                        logging.warning(f"No location found for item: {item.get('custom_id')}")
                        message_data["geolocation"] = "no location available"
                        item["response"]["body"]["choices"][0]["message"]["content"] = json.dumps(message_data)
                        results.append(item)
                else:
                    logging.warning(f"No message content found for item: {item.get('custom_id')}")
                    item["response"]["body"]["choices"][0]["message"]["content"] = json.dumps({"geolocation": "no location available"})
                    results.append(item)
            
            try:
                if batch_request["batchItems"]:
                    response = maps_client.get_geocoding_batch(batch_request)
                    
                    if not response.get('batchItems', False):
                        logging.warning("No batchItems in geocoding response")
                        results.extend([None] * len(batch))
                        continue
                    
                    # Process each item in the batch response
                    for item, original_addr in zip(response['batchItems'], batch):
                        if not item.get('features', False):
                            logging.warning(f"No features found for address: {original_addr}")
                            results.append(None)
                            continue
                        
                        try:
                            coordinates = item['features'][0]['geometry']['coordinates']
                            longitude, latitude = coordinates
                            
                            geolocation_obj = {
                                "type": "Point",
                                "coordinates": [longitude, latitude]
                            }
                            message_data = json.loads(original_addr["response"]["body"]["choices"][0]["message"]["content"])
                            message_data["geolocation"] = geolocation_obj
                            original_addr["response"]["body"]["choices"][0]["message"]["content"] = json.dumps(message_data)
                            results.append(original_addr)
                            
                            logging.info(f"Successfully geocoded: {original_addr} -> ({latitude}, {longitude})")
                        except (IndexError, KeyError) as e:
                            logging.error(f"Error processing geocoding result for {original_addr}: {str(e)}")
                            results.append(None)
                            
                else:
                    logging.warning("Batch request is empty, skipping geocoding for this batch.")

            except HttpResponseError as exception:
                if exception.error:
                    logging.error(f"Batch geocoding error - Code: {exception.error.code}, Message: {exception.error.message}")
                else:
                    logging.error(f"Batch geocoding error: {str(exception)}")
                results.extend([None] * len(batch))
            except Exception as e:
                logging.error(f"Unexpected error in batch geocoding: {str(e)}")
                results.extend([None] * len(batch))
    
        # Save updated results to geocoded-results container
        connection_string = os.getenv("batchprocessblob_STORAGE")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        output_container = "geocoded-results"
        output_blob_name = f"{os.path.basename(blob_name).replace('.jsonl', '-geocoded.jsonl')}"
        blob_client = blob_service_client.get_blob_client(container=output_container, blob=output_blob_name)

        logging.info(f"Saving geocoded results to {output_container}/{output_blob_name}")
        blob_client.upload_blob("\n".join([json.dumps(result) for result in results if result]), overwrite=True)
        logging.info("Geocoding results saved successfully.")

    except Exception as e:
        logging.error(f"Error during geocoding for blob '{blob_name}': {str(e)}")

