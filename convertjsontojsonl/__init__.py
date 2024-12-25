import logging
from azure.storage.blob import BlobServiceClient
import json
import os
import re

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
            
            prompt = f"""
            Extract property listing information from this chat message:
            If a field is not available, leave it blank.
            If any of the value is not available that we need to give in the response format then leave it blank.
            If message does not contain the url then leave the url in the response format blank.
            Represents a property listing  with an organization.
            This model captures basic information about a property listing, including the requirement, property type, location, price range, property subtype, additional features, date, name, and contact.

            name : "display_name"
            contact_number = "phone"
            requirement: "Type of requirement: Rent, Sale, Ratio Deal"
            area: "Area in square feet"
            location: "Location of the property"
            price "Price of the property in rupees"
            property_subtype: "Subtype of property based on category:         
            Commercial: Office, Shop, Showroom, School, College, Hospital
            Land: Agricultural land, Commercial land, Industrial land, Residential Plot, Commercial Plot
            Industrial: Factory, Warehouse, Godown
            Hospitality: Hotel, Resort, Farmhouse
            Residential: Hostel, 1bhk Flat, 2bhk flat, 3bhk flat, 4bhk flat, 5bhk flat, 1RK Flat, Studio Apartment, 1bhk house, 2bhk house, 3bhk house, 4bhk house, 5bhk house"
            description: short and concise to the point description of the property. e.g. 2 BHK flat available for sale in Vijay nagar, Indore
            listingDate: str = None
            category: Optional[str] = None
            listing_type: Optional[str] = None
            geolocation: Optional[str] = None
            rating: float = 5.0
            ratings_history: List[Any] = []
            name: Optional[str] = None
            contact_number: Optional[str] = None
            isOwnerListing: Optional[bool] = False
            """

            response_format = {"type": "json_schema", "json_schema": {"name": "property_listing_schema", "schema": {"type": "object", "properties": {"requirement": {"type": "string"}, "description": {"type": "string"}, "area": {"type": "string"}, "location": {"type": "string"}, "price": {"type": "string"},"name": {"type": "string"},"contact_number": {"type": "string"}, "url": {"type": "string"}}, "required": ["requirement", "description", "area", "location", "price", "name", "contact_number", "url"], "additionalProperties": False}, "strict": True}}

            transformed_lines = []
            url_pattern = r"https?://[^\s]+"

            for i, item in enumerate(json_data):
                message = item.get("message", "")
                display_name = item.get("displayName", "")
                phone = item.get("phone", "")
                
                # Extract URL if present
                url_match = re.search(url_pattern, message)
                url = url_match.group(0) if url_match else ""
                
                # Combine fields into user content
                user_content = f"Message: {message}\nDisplay Name: {display_name}\nPhone: {phone}\nURL: {url}"
                
                transformed_item = {
                    "custom_id": f"task-{i}",
                    "method": "POST",
                    "url": "/chat/completions",
                    "body": {
                        "model": "gpt-4o",
                        "messages": [
                            {"role": "system", "content": prompt},
                            {"role": "user", "content": user_content}
                        ],
                        "response_format": response_format
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
