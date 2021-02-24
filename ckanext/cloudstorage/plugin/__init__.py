# -*- coding: utf-8 -*-
import os.path

from ckan import plugins

from ckanext.cloudstorage.logic.action import get_actions
from ckanext.cloudstorage.logic.auth import get_auth_functions

from ckanext.cloudstorage import storage
from ckanext.cloudstorage import helpers

if plugins.toolkit.check_ckan_version("2.9"):
    from ckanext.cloudstorage.plugin.flask_plugin import MixinPlugin
else:
    from ckanext.cloudstorage.plugin.pylons_plugin import MixinPlugin


class CloudStoragePlugin(MixinPlugin, plugins.SingletonPlugin):
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IResourceController, inherit=True)

    # IConfigurer

    def update_config(self, config):
        plugins.toolkit.add_template_directory(config, '../templates')
        plugins.toolkit.add_resource('../fanstatic/scripts', 'cloudstorage-js')

    # ITemplateHelpers

    def get_helpers(self):
        return dict(
            cloudstorage_use_secure_urls=helpers.use_secure_urls,
            cloudstorage_use_multipart_upload=helpers.use_multipart_upload,
            cloudstorage_max_upload_size=helpers.max_upload_size,
        )

    # IConfigurable

    def configure(self, config):

        required_keys = ('ckanext.cloudstorage.driver',
                         'ckanext.cloudstorage.driver_options',
                         'ckanext.cloudstorage.container_name')

        for rk in required_keys:
            if config.get(rk) is None:
                raise RuntimeError(
                    'Required configuration option {0} not found.'.format(rk))

    # IUploader

    def get_resource_uploader(self, data_dict):
        # We provide a custom Resource uploader.
        return storage.ResourceCloudStorage(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        # We don't provide misc-file storage (group images for example)
        # Returning None here will use the default Uploader.
        return None

    # IActions

    def get_actions(self):
        return get_actions()

    # IAuthFunctions

    def get_auth_functions(self):
        return get_auth_functions()

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
        res_dict = dict(list(res.items()) + [('clear_upload', True)])

        uploader = self.get_resource_uploader(res_dict)

        # to be on the safe side, let's check existence of container
        container = getattr(uploader, 'container', None)
        if container is None:
            return

        # and now uploader removes our file.
        uploader.upload(resource['id'])

        # and all other files linked to this resource
        if not uploader.leave_files:
            upload_path = os.path.dirname(
                uploader.path_from_filename(resource['id'], 'fake-name'))

            old_files = uploader.driver.iterate_container_objects(
                uploader.container,
                upload_path
            )

            for old_file in old_files:
                old_file.delete()
