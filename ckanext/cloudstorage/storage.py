#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cgi
import mimetypes
import os.path
import urlparse
from ast import literal_eval
from datetime import datetime, timedelta
from tempfile import SpooledTemporaryFile
import logging

from pylons import config
from ckan import model
from ckan.lib import munge
import ckan.plugins as p
import ckan.model as model
from ckan.plugins import toolkit
import ckan.authz as authz
import ckan.logic as logic

from libcloud.storage.types import Provider, ObjectDoesNotExistError
from libcloud.storage.providers import get_driver

from werkzeug.datastructures import FileStorage as FlaskFileStorage

ALLOWED_UPLOAD_TYPES = (cgi.FieldStorage, FlaskFileStorage)

NotAuthorized = logic.NotAuthorized

log = logging.getLogger(__name__)

def _get_underlying_file(wrapper):
    if isinstance(wrapper, FlaskFileStorage):
        return wrapper.stream
    return wrapper.file


class CloudStorage(object):
    def __init__(self):
        """
        Initialize the CloudStorage with a specific storage driver.
        """
        try:
            # Dynamically get the driver class from the Provider.
            driver_class = get_driver(getattr(Provider, self.driver_name))
            # Initialize the driver with the provided options.
            self.driver = driver_class(**self.driver_options)
        except AttributeError:
            raise ValueError("Invalid driver name: {}".format(self.driver_name))
        except Exception as e:
            raise ConnectionError("Failed to initialize driver: {}".format(e))

        self._container = None

    @property
    def container(self):
        """
        Return the currently configured libcloud container.
        """
        if self._container is None:
            self._container = self.driver.get_container(
                container_name=self.container_name
            )

        return self._container

    @property
    def driver_options(self):
        """
        A dictionary of options ckanext-cloudstorage has been configured to
        pass to the apache-libcloud driver.
        """
        return literal_eval(config['ckanext.cloudstorage.driver_options'])

    @property
    def driver_name(self):
        """
        The name of the driver (ex: AZURE_BLOBS, S3) that ckanext-cloudstorage
        is configured to use.


        .. note:: 

            This value is used to lookup the apache-libcloud driver to use
            based on the Provider enum.
        """
        return config['ckanext.cloudstorage.driver']
    
    @property
    def prefix(self):
        """
        The prefix of container or group name
        """
        return config['ckanext.cloudstorage.prefix']


    @property
    def domain(self):
        """
        gcp domain
        """
        return config['ckanext.cloudstorage.domain']

    @property
    def container_name(self):
        """
        The name of the container (also called buckets on some providers)
        ckanext-cloudstorage is configured to use.
        """
        return config['ckanext.cloudstorage.container_name']

    @container_name.setter
    def container_name(self, value):
        """
        Set the name of the container.
        """
        # Optional: Add validation or processing here
        self._container_name = value
        # Optional: Reset or update the container if necessary
        self._container = None

    @property
    def use_secure_urls(self):
        """
        `True` if ckanext-cloudstroage is configured to generate secure
        one-time URLs to resources, `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.use_secure_urls', False)
        )

    @property
    def leave_files(self):
        """
        `True` if ckanext-cloudstorage is configured to leave files on the
        provider instead of removing them when a resource/package is deleted,
        otherwise `False`.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.leave_files', False)
        )


    @property
    def guess_mimetype(self):
        """
        `True` if ckanext-cloudstorage is configured to guess mime types,
        `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.guess_mimetype', False)
        )
    
    @property
    def proxy_download(self):
        """
        If the ckan may stream the object (will use service account to download
        from private storages)
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.proxy_download', False)
        )

    @property
    def can_use_advanced_google(self):
        """
        `True` if the `google-auth` module is installed and
        ckanext-cloudstorage has been configured to use Google, otherwise
        `False`.
        """
        # Are we even using GOOGLE?
        if self.driver_name == 'GOOGLE_STORAGE':
            try:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.driver_options["secret"]
                # Yes? Is the 'google-auth' package available?
                from google.auth import crypt
                assert crypt
                # check six >=1.5
                import six
                assert six.ensure_binary
                return True
            except ImportError:
                # fail fast
                # if we configure a google storage and we have secure_urls,
                # we may want to be sure to have it installed at runtime
                if self.use_secure_urls:
                    raise

        return False


    @property
    def can_use_advanced_azure(self):
        """
        `True` if the `azure-storage` module is installed and
        ckanext-cloudstorage has been configured to use Azure, otherwise
        `False`.
        """
        # Are we even using Azure?
        if self.driver_name == 'AZURE_BLOBS':
            try:
                # Yes? Is the azure-storage package available?
                from azure import storage
                # Shut the linter up.
                assert storage
                return True
            except ImportError:
                pass

        return False

    @property
    def can_use_advanced_aws(self):
        """
        `True` if the `boto` module is installed and ckanext-cloudstorage has
        been configured to use Amazon S3, otherwise `False`.
        """
        # Are we even using AWS?
        if 'S3' in self.driver_name:
            try:
                # Yes? Is the boto package available?
                import boto
                # Shut the linter up.
                assert boto
                return True
            except ImportError:
                pass

        return False


class ResourceCloudStorage(CloudStorage):
    def __init__(self, resource):
            """
            Support for uploading resources to any storage provider
            implemented by the apache-libcloud library.

            :param resource: The resource dict.
            """
            log.info("Initializing ResourceCloudStorage with resource: %s", resource)
            super(ResourceCloudStorage, self).__init__()

            self.resource = resource
            self.filename = None
            self.old_filename = None
            self.file_upload = None

            self._initialize_storage_settings()
            self._handle_file_upload(resource)
            self._handle_clear_upload(resource)

    def _initialize_storage_settings(self):
        """
        Initialize storage settings from the resource.
        """
        self.role = str(self.get_user_role_in_organization()).encode('ascii', 'ignore')
        self.container_name = self.get_container_name_of_current_org()
        self.group_email = self.container_name + "@" + self.domain

    def _handle_file_upload(self, resource):
        """
        Handle the file upload process.
        """
        log.info("Handling file upload for resource: %s", resource)
        upload_field_storage = resource.pop('upload', None)
        multipart_name = resource.pop('multipart_name', None)

        if isinstance(upload_field_storage, (ALLOWED_UPLOAD_TYPES)):
            self._process_file_upload(upload_field_storage, resource)
        elif multipart_name and self.can_use_advanced_aws:
            self._process_multipart_upload(multipart_name, resource)

        log.info("File upload handled successfully for resource: %s", resource)

    def _process_file_upload(self, upload_field_storage, resource):
        """
        Process a standard file upload.
        """
        log.info("Processing file upload: %s", upload_field_storage.filename)
        self.filename = munge.munge_filename(upload_field_storage.filename)
        self.file_upload = _get_underlying_file(upload_field_storage)
        resource['url'] = self.filename
        resource['url_type'] = 'upload'
        log.info("File uploaded successfully: %s", self.filename)

    def _process_multipart_upload(self, multipart_name, resource):
        """
        Process a multipart upload, specifically for AWS.
        """
        resource['url'] = munge.munge_filename(multipart_name)
        resource['url_type'] = 'upload'

    def _handle_clear_upload(self, resource):
        """
        Handle clearing of an upload.
        """
        self._clear = resource.pop('clear_upload', None)
        if self._clear and resource.get('id'):
            self._clear_old_upload(resource)

    def _clear_old_upload(self, resource):
        """
        Clear an old upload when a new file is uploaded.
        """
        old_resource = model.Session.query(model.Resource).get(resource['id'])
        self.old_filename = old_resource.url
        resource['url_type'] = ''

    @property
    def container_name(self):
        """
        Overridden container_name property.
        """
        return self._container_name

    @container_name.setter
    def container_name(self, value):
        """
        Overridden setter for container_name.
        """
        self._container_name = value

    def path_from_filename(self, rid, filename):
        """
        Returns a bucket path for the given resource_id and filename.

        :param rid: The resource ID.
        :param filename: The unmunged resource filename.
        """
        return os.path.join(
            'packages',
            self.package.id,
            'resources',
            rid,
            munge.munge_filename(filename)
        )
    
    def get_container_name_of_current_org(self):
        """
        Generates the container name for the current organization.

        It retrieves the organization from the database using the package's
        owner organization ID and constructs a container name using a predefined
        prefix and the organization's name.

        :return: A string representing the container name.
        """
        log.info("Retrieving container name for current organization")
        owner_org = str(self.package.owner_org).encode('ascii', 'ignore')
        org = model.Session.query(model.Group) \
            .filter(model.Group.id == owner_org).first()

        name = self.prefix + str(org.name).encode('ascii', 'ignore')
        log.info("Container name retrieved: %s", name)
        return name

    def get_user_role_in_organization(self):
        """
        Determines the user's role in the current organization.

        This method retrieves the role of the currently logged-in user in the
        organization that owns the package. It checks the user's membership in
        the organization and returns their role if found.

        :return: A string representing the user's role in the organization, or
                None if the user has no role or is not found.
        """
        org_id = str(self.package.owner_org).encode('ascii', 'ignore')
        user_name = toolkit.c.user
        user_id = authz.get_user_id_for_username(user_name, allow_none=True)
        if not user_id:
            return None
        # get any roles the user has for the group
        q = model.Session.query(model.Member) \
            .filter(model.Member.table_name == 'user') \
            .filter(model.Member.group_id == org_id) \
            .filter(model.Member.state == 'active') \
            .filter(model.Member.table_id == user_id)
        # return the first role we find
        for row in q.all():
            return row.capacity
        return None

    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        if self.filename:
            self._upload_file(id)
        elif self._clear and self.old_filename and not self.leave_files:
            self._delete_old_file(id)

    def _upload_file(self, id):
        """
        Handles the file uploading process.

        :param id: The resource_id.
        """
        if self.can_use_advanced_azure:
            self._upload_to_azure(id)
        else:
            self._upload_to_libcloud(id)

    def _upload_to_azure(self, id):
        """
        Uploads a file to Azure blob storage.

        This method uploads the file associated with the given resource ID
        to Azure Blob Storage, using the configured container and filename.
        It handles content settings for the file based on its MIME type.

        :param id: The resource_id associated with the file to be uploaded.
        :return: The response from the Azure blob service after upload.
        """
        from azure.storage import blob as azure_blob

        blob_service = azure_blob.BlockBlobService(**self.driver_options)
        content_settings = self._get_content_settings_for_azure()

        return blob_service.create_blob_from_stream(
            container_name=self.container_name,
            blob_name=self.path_from_filename(id, self.filename),
            stream=self.file_upload,
            content_settings=content_settings
        )

    def _get_content_settings_for_azure(self):
        """
        Determines the content settings for Azure based on the file's mimetype.
        """
        from azure.storage.blob.models import ContentSettings

        content_settings = None
        if self.guess_mimetype:
            content_type, _ = mimetypes.guess_type(self.filename)
            if content_type:
                content_settings = ContentSettings(content_type=content_type)
        return content_settings

    def _upload_to_libcloud(self, id):
        """
        Uploads a file using the libcloud driver to the configured storage.

        This method handles the file upload process for various cloud storage
        services using the libcloud driver. It supports 'SpooledTemporaryFile' 
        for handling file uploads and streams the file to the designated 
        container and object name based on the resource ID and filename.

        :param id: The resource_id associated with the file to be uploaded.
        """
        # Specific handling for SpooledTemporaryFile
        if isinstance(self.file_upload, SpooledTemporaryFile):
            self.file_upload.next = self.file_upload.next()

        self.container.upload_object_via_stream(
            self.file_upload,
            object_name=self.path_from_filename(id, self.filename)
        )

    def _delete_old_file(self, id):
        """
        Deletes an old file when a new file is uploaded.

        This method is invoked when a previously uploaded file is replaced
        by a new file or a link. It attempts to delete the old file from
        the storage container. If the file does not exist or has already
        been deleted, the method will silently complete without errors.

        :param id: The resource_id associated with the file to be deleted.
        """
        # This is only set when a previously-uploaded file is replace
        # by a link. We want to delete the previously-uploaded file.
        log.info("Deleting old file: %s", self.old_filename)
        try:
            self.container.delete_object(
                self.container.get_object(
                    self.path_from_filename(id, self.old_filename)
                )
            )
            log.info("Old file deleted: %s", self.old_filename)
        except ObjectDoesNotExistError:
            # It's possible for the object to have already been deleted, or
            # for it to not yet exist in a committed state due to an
            # outstanding lease.
            return

    def _generate_azure_url(self, path):
        """
        Generates a signed URL for an Azure Blob Storage object.

        This method creates a URL with a Shared Access Signature (SAS) for
        secure access to a blob in Azure Blob Storage. The SAS is set to expire
        in 1 hour and grants read-only access to the blob.

        :param path: The path to the blob within the Azure container.
        :return: A string representing the SAS URL to the Azure blob.
        """
        from azure.storage import blob as azure_blob

        blob_service = azure_blob.BlockBlobService(
            self.driver_options['key'],
            self.driver_options['secret']
        )

        return blob_service.make_blob_url(
            container_name=self.container_name,
            blob_name=path,
            sas_token=blob_service.generate_blob_shared_access_signature(
                container_name=self.container_name,
                blob_name=path,
                expiry=datetime.utcnow() + timedelta(hours=1),
                permission=azure_blob.BlobPermissions.READ
            )
        )

    def _generate_aws_url(self, path, content_type):
        """
        Generates a signed URL for an AWS S3 object.

        This method creates a presigned URL for an object stored in Amazon S3,
        allowing secure, temporary access. The URL expires in 1 hour and is
        configured for HTTP GET requests. If a content type is provided, it's
        included in the URL's headers.

        :param path: The path to the object in the S3 bucket.
        :param content_type: Optional. The MIME type of the object. Used to set
                            the 'Content-Type' header in the generated URL.
        :return: A string representing the presigned URL to the S3 object.
        """
        from boto.s3.connection import S3Connection
        s3_connection = S3Connection(**self.driver_options)
        generate_url_params = {"expires_in": 3600, "method": "GET",
                            "bucket": self.container_name, "query_auth": True,
                            "key": path}
        if content_type:
            generate_url_params['headers'] = {"Content-Type": content_type}

        return s3_connection.generate_url(**generate_url_params)

    def _generate_public_google_url(self, obj, user_obj, user_email):
        """
        Generates a signed URL for public Google Cloud Storage objects.

        For anonymous users, uses a service account to impersonate a group.
        For authenticated users with admin or editor roles, grants direct access.

        :param obj: The GCS object for which to generate the URL.
        :param user_obj: The user object of the currently logged-in user.
        :param user_email: The email address of the user.
        :return: A signed URL string.
        """
        import ckanext.cloudstorage.google_storage as storage

        if user_obj is None:
            # Use service account for anonymous users
            return storage.generate_signed_url_with_impersonated_user(
                self.driver_options['secret'],
                self.container_name,
                object_name=obj.name,
                impersonate_user=self.group_email,
                expiration=3600
            )
        else:
            if self.role in ("admin", "editor"):
                # Direct signed URL for admin and editor
                return storage.generate_signed_url(
                    self.driver_options['secret'],
                    self.container_name,
                    object_name=obj.name,
                    expiration=3600
                )
            else:
                # Impersonate a user for other roles
                return storage.generate_signed_url_with_impersonated_user(
                    self.driver_options['secret'],
                    self.container_name,
                    object_name=obj.name,
                    impersonate_user=user_email,
                    expiration=3600
                )

    def _generate_private_google_url(self, obj, user_role, user_email):
        """
        Generates a signed URL for private Google Cloud Storage objects.

        Access is based on the user's role. Admin and editor roles are given
        direct access, while member roles are handled through impersonation.

        :param obj: The GCS object for which to generate the URL.
        :param user_role: The role of the user in the organization.
        :param user_email: The email address of the user.
        :return: A signed URL string.
        """
        import ckanext.cloudstorage.google_storage as storage

        if user_role in ("admin", "editor"):
            # Direct signed URL for admin and editor
            return storage.generate_signed_url(
                self.driver_options['secret'],
                self.container_name,
                object_name=obj.name,
                expiration=3600
            )
        elif user_role == "member":
            # Impersonate a user for member role
            return storage.generate_signed_url_with_impersonated_user(
                self.driver_options['secret'],
                self.container_name,
                object_name=obj.name,
                impersonate_user=user_email,
                expiration=3600
            )
        else:
            raise NotAuthorized("User not authorized to read or download this file")

    def _generate_google_url(self, path):
        """
        Generates a signed URL for a Google Cloud Storage object.

        This method creates a signed URL for a GCS object, considering the
        package's privacy status and the user's role. For public packages, it
        either uses a service account for anonymous users or grants direct access
        for admin/editor roles. For private packages, access is based on user roles.
        The URL expires in 1 hour.

        :param path: The path to the object in the GCS bucket.
        :return: A signed URL for the GCS object. Raises NotAuthorized for
                unauthorized users.
        """
        import ckanext.cloudstorage.google_storage as storage

        obj=self.container.get_object(path)
        user_name = toolkit.c.user
        user_obj = toolkit.c.userobj

        is_private_package = self.package.is_private
        user_role = self.role
        user_email = str(user_obj.email).encode('ascii', 'ignore') if user_obj else None

        # For public packages
        if not is_private_package:
           return self._generate_public_google_url(obj,user_obj,user_email)
        # For private packages
        else:
            return self._generate_private_google_url(obj,user_role, user_email)

    def _generate_default_url(self, path):
        """
        Generate a default URL for storage providers that do not require special handling.

        :param path: The path of the object in the storage.
        :returns: A URL for the object or None if not applicable.
        """

        # Find the object for the given key.
        obj = self.container.get_object(path)
        if obj is None:
            return

        try:
            # Attempt to use the provider's CDN URL generation method
            return self.driver.get_object_cdn_url(obj)
        except NotImplementedError:
            # Handle storage providers like S3 or Google Cloud using known URL patterns
            if 'S3' in self.driver_name or 'GOOGLE_STORAGE' in self.driver_name:
                return 'https://{host}/{container}/{path}'.format(
                    host=self.driver.connection.host,
                    container=self.container_name,
                    path=path
                )
            # For Azure and others, check for an 'url' property in the object's extra attributes
            elif 'url' in obj.extra:
                return obj.extra['url']
            # If none of the above, return None or raise an appropriate exception
            else:
                return None  # or raise an appropriate exception

    def get_url_from_filename(self, rid, filename, content_type=None):
        """
        Retrieve a publically accessible URL for the given resource_id
        and filename.

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param rid: The resource ID.
        :param filename: The resource filename.
        :param content_type: Optionally a Content-Type header.

        :returns: Externally accessible URL or None.
        """
        # Find the key the file *should* be stored at.
        path = self.path_from_filename(rid, filename)

        # If advanced azure features are enabled, generate a temporary
        # shared access link instead of simply redirecting to the file.
        if self.can_use_advanced_azure and self.use_secure_urls:
            return self._generate_azure_url(path)
        elif self.can_use_advanced_aws and self.use_secure_urls:
            return self._generate_aws_url(path, content_type)
        elif self.can_use_advanced_google and self.use_secure_urls:
            return self._generate_google_url(path)
        else:
            return self._generate_default_url(path)

    def get_object(self, rid, filename):
        # Find the key the file *should* be stored at.
        path = self.path_from_filename(rid, filename)
        # Find the object for the given key.
        return self.container.get_object(path)

    def get_object_as_stream(self, obj):
        return self.driver.download_object_as_stream(obj) 
        
    @property
    def package(self):
        return model.Package.get(self.resource['package_id'])
