#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckan import plugins
from routes.mapper import SubMapper

from ckanext.cloudstorage import storage
from ckanext.cloudstorage import helpers
import ckanext.cloudstorage.logic.action.multipart as m_action
import ckanext.cloudstorage.logic.auth.multipart as m_auth


class CloudStoragePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IAuthFunctions)

    # IConfigurer

    def update_config(self, config):
        plugins.toolkit.add_template_directory(config, 'templates')
        plugins.toolkit.add_resource('fanstatic', 'cloudstorage')

    # ITemplateHelpers

    def get_helpers(self):
        return dict(
            cloudstorage_use_secure_urls=helpers.use_secure_urls
        )

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

    # IActions

    def get_actions(self):
        return {
            'cloudstorage_initiate_multipart': m_action.initiate_multipart,
            'cloudstorage_upload_multipart': m_action.upload_multipart,
            'cloudstorage_finish_multipart': m_action.finish_multipart,
            'cloudstorage_abort_multipart': m_action.abort_multipart,
        }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'cloudstorage_initiate_multipart': m_auth.initiate_multipart,
            'cloudstorage_upload_multipart': m_auth.upload_multipart,
            'cloudstorage_finish_multipart': m_auth.finish_multipart,
            'cloudstorage_abort_multipart': m_auth.abort_multipart,
        }
