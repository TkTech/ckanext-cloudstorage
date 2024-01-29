import logging

from google.cloud import storage


# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


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

        return True
    except Exception as e:
        log.error("Error creating bucket: {}".format(e))
        return False

def get_bucket_info(bucket_name):
    """
    Retrieve and return information about the specified Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the Google Cloud Storage bucket.

    Returns:
        dict: Dictionary containing bucket information if successful, empty dict otherwise.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        bucket_info = {
            "Bucket Name": bucket.name,
            "Storage Class": bucket.storage_class,
            "Location": bucket.location,
            "Location Type": bucket.location_type
            # Add more properties here as needed
        }

        log.info("Bucket information retrieved: {}".format(bucket_info))
        return bucket_info
    except Exception as e:
        log.error("Error getting bucket info: {}".format(e))
        return {}

def add_group_iam_permissions(bucket_name, group_email):
    """
    Grant read and list permissions to a group for a specific Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the Google Cloud Storage bucket.
        group_email (str): Email address of the group to grant permissions.

    Returns:
        bool: True if permissions are added successfully, False otherwise.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        policy = bucket.get_iam_policy()
        viewer_role = "roles/storage.objectViewer"
        policy[viewer_role].add("group:" + group_email)
        bucket.set_iam_policy(policy)

        log.info("Read and list permissions granted to group {} on bucket {}"
                 .format(group_email, bucket_name))
        return True
    except Exception as e:
        log.error("Error modifying bucket IAM policy: {}".format(e))
        return False

def upload_to_gcp_bucket(bucket_name, destination_blob_name, source_file_name):
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
