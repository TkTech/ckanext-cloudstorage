from ckanext.cloudstorage.storage import ResourceCloudStorage
import boto

def use_secure_urls():
    return all([
        ResourceCloudStorage.use_secure_urls.fget(None),
        # Currently implemented just AWS version
        'S3' in ResourceCloudStorage.driver_name.fget(None)
    ])
