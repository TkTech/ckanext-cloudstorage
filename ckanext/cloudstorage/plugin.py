#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckan import plugins
from routes.mapper import SubMapper
import os.path
from ckanext.cloudstorage import storage


class CloudStoragePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IResourceController, inherit=True)

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

    # IResourceController

    def before_delete(self, context, resource, resources):
        # let's get all info about our resource. It somewhere in resources
        # but if there is some possibility that it isn't(magic?) we have
        # `else` clause

        for res in resources:
            if res['id'] == resource['id']:
                break
        else:
            return
        # just ignore simple links
        if res['url_type'] != 'upload':
            return

        # we don't want to change original item from resources, just in case
        # someone will use it in another `before_delete`. So, let's copy it
        # and add `clear_upload` flag
        res_dict = dict(res.items() + [('clear_upload', True)])

        uploader = self.get_resource_uploader(res_dict)

        # to be on the safe side, let's check existence of container
        container = getattr(uploader, 'container', None)
        if container is None:
            return

        # and now uploader removes our file.
        uploader.upload(resource['id'])

        # and all other files linked to this resource
        if not uploader.leave_files:
            upload_path = os.path.dirname(uploader.path_from_filename(resource['id'], 'fake-name'))
            for old_file in uploader.container.iterate_objects():
                if old_file.name.startswith(upload_path):
                    old_file.delete()
