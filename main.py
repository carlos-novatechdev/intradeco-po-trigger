import functions_framework
from google.cloud import storage

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

    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(bucket)
    source_blob = bucket.blob(name)

    # Define new blob path
    path_parts = name.rsplit('/', 1)
    if len(path_parts) == 2:
        base_path, filename = path_parts
        destination_blob_name = f"{base_path}/processed/{filename}"
    else:
        destination_blob_name = f"processed/{name}"

    # Copy then delete (simulate move)
    bucket.copy_blob(source_blob, bucket, destination_blob_name)
    source_blob.delete()

    print(f"✅ Moved file from '{name}' to '{destination_blob_name}'")

    return f"Processed file {name} in bucket {bucket} with metageneration {metageneration} at {timeCreated}."

