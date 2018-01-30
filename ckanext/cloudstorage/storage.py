#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cgi
import mimetypes
import os.path
import urlparse
from ast import literal_eval
from datetime import timedelta
import datetime
from pylons import config
from ckan import model
from ckan.lib import munge
import ckan.plugins as p

from libcloud.storage.types import Provider, ObjectDoesNotExistError
from libcloud.storage.providers import get_driver


class CloudStorage(object):
    def __init__(self):
        print('cloudstorage init')
        self.driver = get_driver(
            getattr(
                Provider,
                self.driver_name
            )
        )(**self.driver_options)
        self._container = None

    def path_from_filename(self, rid, filename):
        raise NotImplemented

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
    def container_name(self):
        """
        The name of the container (also called buckets on some providers)
        ckanext-cloudstorage is configured to use.
        """
        return config['ckanext.cloudstorage.container_name']

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

    @property
    def can_use_advanced_google_cloud(self):
        """
        `True` if the `google-cloud` module is installed and ckanext-cloudstorage has
        been configured to use Google Cloud Storage, otherwise `False`.
        """
        # Are we even using google cloud?
        if 'GOOGLE_STORAGE' in self.driver_name:
            try:
                # Yes? is the google-cloud-storage package available?
                from google.cloud import storage
                # shut the linter up.
                assert storage
                return True
            except ImportError:
                pass

        return False

    @property
    def guess_mimetype(self):
        """
        `True` if ckanext-cloudstorage is configured to guess mime types,
        `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.guess_mimetype', False)
        )

    def get_object_public_url(self, filename):
        """
        Returns the public url of an object.
        Raises `NotImplementedError` for drivers yet unsupported, or when
        `use_secure_urls` is set to `True`.

        Assumes container is made public.
        """
        if self.driver_name == 'GOOGLE_STORAGE':
            if self.use_secure_urls:
                raise NotImplementedError("Should be pretty easy though!")
            return "https://storage.googleapis.com/{0}/{1}" \
                    .format(self.container_name, self.path_from_filename(filename))
        else:
            raise NotImplementedError("This method hasn't been implemented yet for this driver.")

    def upload_to_path(self, file_path):
        """
        Upload to storage bucket

        :param file_path: File path in storage bucket
        :param old_file_path: File path of old file in storage bucket.
        """

        if self.can_use_advanced_azure:
            from azure.storage import blob as azure_blob
            from azure.storage.blob.models import ContentSettings

            blob_service = azure_blob.BlockBlobService(
                self.driver_options['key'],
                self.driver_options['secret']
            )
            content_settings = None
            if self.guess_mimetype:
                content_type, _ = mimetypes.guess_type(file_path)
                if content_type:
                    content_settings = ContentSettings(
                        content_type=content_type
                    )

            return blob_service.create_blob_from_stream(
                container_name=self.container_name,
                blob_name=file_path,
                stream=self.file_upload,
                content_settings=content_settings
            )
        else:
            self.container.upload_object_via_stream(
                self.file_upload,
                object_name=file_path
            )

    def delete_object_from_path(self, file_path):
        """
        Delete object from cloudstorage at `file_path`
        :param file_path: Path of file to be deletedd
        """
        try:
            self.container.delete_object(
                self.container.get_object(
                    old_file_path
                )
            )
        except ObjectDoesNotExistError:
            # It's possible for the object to have already been deleted, or
            # for it to not yet exist in a committed state due to an
            # outstanding lease.
            return

    def get_url_from_path(self, path):
        """
        Retrieve a publically accessible URL for the given path

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3, Google Storage).

        :param path: The resource path.

        :returns: Externally accessible URL or None.
        """
        # If advanced azure features are enabled, generate a temporary
        # shared access link instead of simply redirecting to the file.
        if self.can_use_advanced_azure and self.use_secure_urls:
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
        elif self.can_use_advanced_aws and self.use_secure_urls:
            from boto.s3.connection import S3Connection
            s3_connection = S3Connection(
                self.driver_options['key'],
                self.driver_options['secret']
            )
            return s3_connection.generate_url(
                expires_in=60 * 60,
                method='GET',
                bucket=self.container_name,
                query_auth=True,
                key=path
            )


        elif self.can_use_advanced_google_cloud and self.use_secure_urls:
            from google.cloud import storage

            client = storage.client.Client.from_service_account_json(
                self.driver_options['secret']
            )

            bucket = client.get_bucket(self.container_name)
            blob = bucket.get_object(path)
            return blob.generate_signed_url(
                expiration=60*60,
                method='GET'
            )

        # Find the object for the given key.
        obj = self.container.get_object(path)
        if obj is None:
            return

        # Not supported by all providers!
        try:
            return self.driver.get_object_cdn_url(obj)
        except NotImplementedError:
            if 'S3' in self.driver_name or 'GOOGLE_STORAGE' in self.driver_name:
                return urlparse.urljoin(
                    'https://' + self.driver.connection.host,
                    '{container}/{path}'.format(
                        container=self.container_name,
                        path=path
                    )
                )
            # This extra 'url' property isn't documented anywhere, sadly.
            # See azure_blobs.py:_xml_to_object for more.
            elif 'url' in obj.extra:
                return obj.extra['url']
            raise



class ResourceCloudStorage(CloudStorage):
    def __init__(self, resource):
        """
        Support for uploading resources to any storage provider
        implemented by the apache-libcloud library.

        :param resource: The resource dict.
        """
        super(ResourceCloudStorage, self).__init__()

        self.filename = None
        self.old_filename = None
        self.file = None
        self.resource = resource

        upload_field_storage = resource.pop('upload', None)
        self._clear = resource.pop('clear_upload', None)
        multipart_name = resource.pop('multipart_name', None)

        # Check to see if a file has been provided
        if isinstance(upload_field_storage, cgi.FieldStorage):
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = upload_field_storage.file
            resource['url'] = self.filename
            resource['url_type'] = 'upload'
        elif multipart_name and self.can_use_advanced_aws:
            # This means that file was successfully uploaded and stored
            # at cloud.
            # Currently implemented just AWS version
            resource['url'] = munge.munge_filename(multipart_name)
            resource['url_type'] = 'upload'
        elif self._clear and resource.get('id'):
            # Apparently, this is a created-but-not-commited resource whose
            # file upload has been canceled. We're copying the behaviour of
            # ckaenxt-s3filestore here.
            old_resource = model.Session.query(
                model.Resource
            ).get(
                resource['id']
            )

            self.old_filename = old_resource.url
            resource['url_type'] = ''

    def path_from_filename(self, rid, filename):
        """
        Returns a bucket path for the given resource_id and filename.

        :param rid: The resource ID.
        :param filename: The unmunged resource filename.
        """
        return os.path.join(
            'resources',
            rid,
            munge.munge_filename(filename)
        )

    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        # If a filename has been provided (a file is being uplaoded) write the
        # file to the appropriate key in the container
        if self.filename:
            file_path = self.path_from_filename(id, self.filename)
            self.upload_to_path(file_path)
        if self._clear and self.old_filename and not self.leave_files:
            old_file_path = self.path_from_filename(id, self.old_filename)
            self.delete_object_from_path(old_file_path)

    def get_url_from_filename(self, id, filename):
        """
        Generate public URL from resource id and filename
        :param id: The resource ID
        :param filename: The resource filename
        """
        path = self.path_from_filename(id, filename)
        return self.get_url_from_path(path)


    @property
    def package(self):
        return model.Package.get(self.resource['package_id'])


class FileCloudStorage(CloudStorage):
    """
    Support upload of general files to cloudstorage.
    """
    def __init__(self, upload_to, old_filename=None):
        super(FileCloudStorage, self).__init__()

        self.filename = None
        self.filepath = None
        self.old_filename = old_filename
        if self.old_filename:
            self.old_filepath = self.path_from_filename(old_filename)

    def path_from_filename(self, filename):
        """
        Returns a bucket path for the given filename.

        :param: filename: The unmunged filename.
        """
        return os.path.join(
            'storage',
            'uploads',
            munge.munge_filename(filename)
        )

    def update_data_dict(self, data_dict, url_field, file_field, clear_field):
        """
        Manipulate data from the data_dict. THis needs to be called before it
        reaches any validators.

        :param url_field: Name of the field where the upload is going to be
        :param file_field: Name of the key where the FieldStorage is kept (i.e.
        the field where the file data actually is).
        :param clear_field: Name of a boolean field which requests the upload
        to be deleted
        """

        self.url = data_dict.get(url_field, '')
        self._clear = data_dict.pop(clear_field, None)
        self.file_field = file_field
        self.upload_field_storage = data_dict.pop(file_field, None)

        if hasattr(self.upload_field_storage, 'filename'):
            self.filename = self.upload_field_storage.filename
            self.filename = str(datetime.datetime.utcnow()) + self.filename
            self.filename = munge.munge_filename_legacy(self.filename)
            self.filepath = self.path_from_filename(self.filename)
            data_dict[url_field] = self.filename
            self.file_upload = self.upload_field_storage.file
        # keep the file if there has been no change
        elif self.old_filename and not self.old_filename.startswith('http'):
            if not self._clear:
                data_dict[url_field] = self.old_filename
            if self._clear and self.url == self.old_filename:
                data_dict[url_field] = ''

    def upload(self, max_size=2):
        """
        Complete the fileupload, or clear an existing upload.

        This should happen just before a commit but after the data has
        been validated and flushed to the db. This is so we do not store
        anything unless the request is actually good.
        :param max_size: ignored
        """
        if self.filename:
            file_path = self.path_from_filename(self.filename)
            return self.upload_to_path(file_path)
        if self._clear and self.old_filename and not self.leave_files:
            old_file_path = self.path_from_filename(self.old_filename)
            self.delete_object_from_path(old_file_path) 

    def get_url_from_filename(self, filename):
        """
        Get public url from filename
        :param filename: name of file
        """
        path = self.path_from_filename(filename)
        return self.get_url_from_path(path)

