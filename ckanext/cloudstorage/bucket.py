import logging
import os

from google.cloud import storage
from google.cloud.exceptions import NotFound, GoogleCloudError



# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class UploadError(Exception):
    """Custom exception for upload failures."""
    pass

class BucketError(Exception):
    """Custom exception for upload failures."""
    pass


def create_bucket(bucket_name, cloud_storage=None):
    """
    Create a Google Cloud Storage bucket and optionally update CloudStorage instance.

    Args:
        bucket_name (str): The name of the bucket to be created.
        cloud_storage (CloudStorage, optional): Instance to update with the new bucket name.

    Returns:
        bool: True if bucket is created successfully, False if an error occurs.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.create_bucket(bucket_name)
        log.info("Bucket {} created".format(bucket.name))

        if cloud_storage:
            from ckanext.cloudstorage.storage import CloudStorage
            if isinstance(cloud_storage, CloudStorage):
                cloud_storage.container_name = bucket_name

    except Exception as e:
        log.error("Error creating bucket: {}".format(e))
        raise BucketError("Error creating bucket: {}".format(e))

def check_err_response_from_gcp(response, err_msg):
    if "error" in response:
        log.error("{}: {}".format(err_msg, response))
        raise  Exception(response["error"])
    return response

def add_group_iam_permissions(bucket_name, group_email):
    """
    Grant read and list permissions to a group for a specific Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the Google Cloud Storage bucket.
        group_email (str): Email address of the group to grant permissions.

    Returns:
        bool: True if permissions are added successfully, False otherwise.
    """
    storage_client = storage.Client()
    try:
        # Attempt to get the bucket
        bucket = storage_client.get_bucket(bucket_name)
    except NotFound:
        # This block will execute if the bucket is not found
        raise RuntimeError("Bucket '{}' not found.".format(bucket_name))
    except Exception as e:
        # This block will execute for any other exceptions
        raise RuntimeError("An error occurred getting bucket info: {}".format(e))

    policy = bucket.get_iam_policy()
    response = check_err_response_from_gcp(policy, "Error getting Iam policiy")
    log.info("Iam policy {}".format(response))
    
    viewer_role = "roles/storage.objectViewer"
    policy[viewer_role].add("group:" + group_email)
    response = bucket.set_iam_policy(policy)
    response = check_err_response_from_gcp(response, "Error modifying bucket IAM policy")
    log.info("Read and list permissions granted to group {} on bucket {}:  IAM Policy is now:\n{}"
                .format(group_email, bucket_name, response))

def upload_to_gcp_bucket(bucket_name, destination_blob_name, source_file_name):
    """
    Uploads a file to the bucket.

    :param bucket_name: Name of your bucket.
    :param destination_blob_name: Blob name to use for the uploaded file.
    :param source_file_name: File to upload.
    """
    storage_client = storage.Client()

    try:
        # Ensure the source file exists
        if not os.path.exists(source_file_name):
            raise FileNotFoundError("The source file {} does not exist.".format(source_file_name))

        # Get the bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Create a blob object
        blob = bucket.blob(destination_blob_name)

        # Attempt to upload the file
        blob.upload_from_filename(source_file_name)
    except FileNotFoundError as e:
        # Handle the file not found error specifically
        log.error("File not found: {}".format(e))
        raise
    except GoogleCloudError as e:
        # Handle Google Cloud specific exceptions
        log.error("An error occurred with Google Cloud Storage: {}".format(e))
        raise UploadError("Failed to upload {} to {}/{}".format(source_file_name,bucket_name, destination_blob_name))
    except Exception as e:
        # Handle any other exceptions
        log.error("An unexpected error occurred: {}".format(e))
        raise UploadError("Unexpected error during upload: {}".format(e))
   