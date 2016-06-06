#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import cgi
import os.path
from ast import literal_eval

from pylons import config
from ckan import model
from ckan.lib import munge

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

non_alpha = re.compile(u'[^A-Z]+', re.UNICODE)


class CloudStorage(object):
    def __init__(self):
        self.driver = get_driver(
            getattr(
                Provider,
                config['ckanext.cloudstorage.driver']
            )
        )(
            **literal_eval(
                config['ckanext.cloudstorage.driver_options']
            )
        )
        self.container = self.driver.get_container(
            container_name=config['ckanext.cloudstorage.container_name']
        )

    def path_from_filename(self, rid, filename):
        raise NotImplemented

    def prefix_from_filename(self, filename):
        # We want to prefix our buckets by the first two
        # ascii letters, or 00 if we can't find any. Non-prefixed
        # LIST operations are among the most expensive on all providers,
        # so this lets us do cheaper LISTs.
        prefix = non_alpha.sub('', filename.upper())[:2]
        return prefix if len(prefix) == 2 else '00'


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

        upload_field_storage = resource.pop('upload', None)
        self._clear = resource.pop('clear_upload', None)

        # Check to see if a file has been provided
        if isinstance(upload_field_storage, cgi.FieldStorage):
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = upload_field_storage.file
            resource['url'] = self.filename
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
            '/resources',
            self.prefix_from_filename(filename),
            rid,
            munge.munge_filename(filename)
        )

    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        if self.filename:
            self.container.upload_object_via_stream(
                self.file_upload,
                object_name=self.path_from_filename(
                    id,
                    self.filename
                )
            )
        elif self._clear and self.old_filename:
            # This is only set when a previously-uploaded file is replace
            # by a link. We want to delete the previously-uploaded file.
            self.container.delete_object(
                self.container.get_object(
                    self.path_from_filename(
                        id,
                        self.old_filename
                    )
                )
            )

    def get_url_from_filename(self, rid, filename):
        """
        Retrieve a publically accessible URL for the given resource_id
        and filename.

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param rid: The resource ID.
        :param filename: The resource filename.

        :returns: Externally accessible URL or None.
        """
        # Find the key the file *should* be stored at.
        path = self.path_from_filename(rid, filename)
        # Find the object for the given key.
        obj = self.container.get_object(path)
        if obj is None:
            return

        # This extra 'url' property isn't documented anywhere, sadly.
        # See azure_blobs.py:_xml_to_object for more.
        if 'url' in obj.extra:
            return obj.extra['url']

        # Not supported by all providers!
        return self.driver.get_object_cdn_url(obj)
