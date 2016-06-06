#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckan import plugins
from routes.mapper import SubMapper

from ckanext.cloudstorage import storage


class CloudStoragePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IConfigurable)

    def configure(self, config):
        required_keys = (
            'ckanext.cloudstorage.driver',
            'ckanext.cloudstorage.driver_options',
            'ckanext.cloudstorage.container_name'
        )

        for rk in required_keys:
            if config.get(rk) is None:
                raise RuntimeError(
                    'Required configuration option {0} not found.'.format(
                        rk
                    )
                )

    def get_resource_uploader(self, data_dict):
        # We provide a custom Resource uploader.
        return storage.ResourceCloudStorage(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        # We don't provide misc-file storage (group images for example)
        # Returning None here will use the default Uploader.
        return None

    def before_map(self, map):
        sm = SubMapper(
            map,
            controller='ckanext.cloudstorage.controller:StorageController'
        )

        # Override the resource download controllers so we can do our
        # lookup with libcloud.
        with sm:
            sm.connect(
                'resource_download',
                '/dataset/{id}/resource/{resource_id}/download',
                action='resource_download'
            )
            sm.connect(
                'resource_download',
                '/dataset/{id}/resource/{resource_id}/download/{filename}',
                action='resource_download'
            )

        return map
