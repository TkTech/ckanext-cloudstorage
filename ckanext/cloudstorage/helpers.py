from ckanext.cloudstorage.storage import ResourceCloudStorage
import boto

def use_secure_urls():
    return all([
        ResourceCloudStorage.use_secure_urls.fget(None),
        # Currently implemented just AWS version
        'S3' in ResourceCloudStorage.driver_name.fget(None)
    ])


# def get_multipart_credentials():
#     sts = boto.connect_sts();
#     policy=_get_policy(dataset_name)
#     tok = sts.get_session_token(duration=3600)
#     return tok.to_dict()

#     tok = sts.assume_role("arn:aws:iam::148616182266:role/S3MultipartUploadOnly",
#                           (c.user+"@"+config.get('ckan.site_id', ''))[:32],
#                           policy=policy
#                         ).credentials




# self.driver.connection.request('/cloudstorage-test/xxxe?uploads', method='POST')
