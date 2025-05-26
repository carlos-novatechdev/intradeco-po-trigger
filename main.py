import functions_framework
from google.cloud import storage
from datetime import datetime
import os
from dotenv import load_dotenv

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def gcs_product_order(cloud_event):
    # This function is a placeholder for the GCS product orderer functionality
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    # Ignore already-processed files to prevent recursion
    if "/processed/" in name:
        print(f"Skipping already processed file: {name}")
        return
    
    # List of folders to exclude
    load_dotenv()
    excluded_folders_env = os.getenv("EXCLUDED_FOLDERS", "product_order,contracts")
    EXCLUDED_FOLDERS = [folder.strip() for folder in excluded_folders_env.split(",") if folder.strip()]

    # Skip files if any excluded folder is in the path
    path_folders = name.split('/')[:-1]  # all folders before the filename
    for excluded in EXCLUDED_FOLDERS:
        if excluded in path_folders:
            print(f"Skipping file in excluded folder '{excluded}': {name}")
            return
    
    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(bucket)
    source_blob = bucket.blob(name)

    # Split path and construct processed folder path
    parts = name.split('/')
    filename = parts[-1]
    if len(parts) > 1:
        folder_path = '/'.join(parts[:-1])
        processed_path = f"{folder_path}/processed"
    else:
        processed_path = "processed"
    
    # Add timestamp to filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if '.' in filename:
        name_part, ext = filename.rsplit('.', 1)
        new_filename = f"{name_part}_{timestamp}.{ext}"
    else:
        new_filename = f"{filename}_{timestamp}"

    destination_blob_name = f"{processed_path}/{new_filename}"

    # Copy then delete (simulate move)
    bucket.copy_blob(source_blob, bucket, destination_blob_name)
    source_blob.delete()

    print(f"✅ Moved file from '{name}' to '{destination_blob_name}'")

    return f"Processed file {name} in bucket {bucket} with metageneration {metageneration} at {timeCreated}."
