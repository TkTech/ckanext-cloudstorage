import logging
import os

from google.cloud import storage
from constants import SERVICE_ACCOUNT_KEY_PATH

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_PATH

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def create_bucket(bucket_name):
    """Creates a new bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.create_bucket(bucket_name)
        log.info("Bucket {} created".format(bucket.name))
    except Exception as e:
        log.error("Error creating bucket: {}".format(e))

def get_bucket_info(bucket_name):
    """Retrieves information about the specified bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        log.info("Bucket Name: {}".format(bucket.name))
        log.info("Storage Class: {}".format(bucket.storage_class))
        log.info("Location: {}".format(bucket.location))
        log.info("Location Type: {}".format(bucket.location_type))
        # Add more properties as needed
    except Exception as e:
        log.error("Error getting bucket info: {}".format(e))

def add_group_iam_permissions(bucket_name, group_email):
    """Add both read and list permissions to a group on a bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        policy = bucket.get_iam_policy()
        viewer_role = "roles/storage.objectViewer"
        policy[viewer_role].add("group:" + group_email)
        bucket.set_iam_policy(policy)

        log.info("Read and list permissions granted to group {} on bucket {}".format(group_email, bucket_name))

    except Exception as e:
        log.error("Error modifying bucket IAM policy: {}".format(e))


def upload_to_gcp(bucket_name, destination_blob_name, source_file_name):
    """
    Uploads a file to the bucket.

    :param bucket_name: Name of your bucket.
    :param destination_blob_name: Blob name to use for the uploaded file.
    :param source_file_name: File to upload.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    try:
        blob.upload_from_filename(source_file_name)
        log.info("File {} uploaded to {}.".format(source_file_name, destination_blob_name))
    except Exception as e:
        log.error("An error occurred: {}".format(e))
