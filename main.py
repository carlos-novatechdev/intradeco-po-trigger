import functions_framework
from google.cloud import storage
from datetime import datetime

def move_blob(bucket_name, source_blob_name, destination_blob_name):
    """Moves a blob from one bucket to another."""
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(source_blob_name)

    # Copy the blob to the new location
    destination_bucket = storage_client.bucket(bucket_name)
    # destination_blob = destination_bucket.blob(destination_blob_name)
    destination_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

    # Delete the original blob
    source_blob.delete()

    print(f"Blob {source_blob_name} in bucket {bucket_name} moved to {destination_blob_name}.")

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
    EXCLUDED_FOLDERS = ["product_order", "contracts"]

    # Skip files if any excluded folder is in the path
    path_folders = name.split('/')[:-1]  # all folders before the filename
    for excluded in EXCLUDED_FOLDERS:
        if excluded in path_folders:
            print(f"Skipping file in excluded folder '{excluded}': {name}")
            return
    
    # Initialize GCS client
    # client = storage.Client()
    # bucket = client.bucket(bucket)
    # source_blob = bucket.blob(name)

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
    # bucket.copy_blob(source_blob, bucket, destination_blob_name)
    # source_blob.delete()
    move_blob(bucket, name, destination_blob_name)

    print(f"✅ Moved file from '{name}' to '{destination_blob_name}'")

    return f"Processed file {name} in bucket {bucket} with metageneration {metageneration} at {timeCreated}."
