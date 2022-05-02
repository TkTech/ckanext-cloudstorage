# -*- coding: utf-8 -*-
import pytest

import ckan.plugins.toolkit as tk

from ckan.tests import factories, helpers
from ckanext.cloudstorage import utils, storage


@pytest.mark.ckan_config('ckan.plugins', 'cloudstorage')
@pytest.mark.usefixtures('with_driver_options', 'with_plugins')
class TestResourceDownload(object):
    def test_utils_used_by_download_route(self, app, mocker):
        url = tk.url_for('resource.download', id='a', resource_id='b')
        mocker.patch('ckanext.cloudstorage.utils.resource_download')
        app.get(url)
        utils.resource_doewnload.assert_called_once_with('a', 'b', None)

    @pytest.mark.usefixtures('clean_db')
    def test_status_codes(self, app):
        user = factories.User()
        org = factories.Organization()
        dataset = factories.Dataset(private=True, owner_org=org['id'])
        resource = factories.Resource(package_id=dataset['id'])

        env = {'REMOTE_USER': user['name']}
        url = tk.url_for(
            'resource.download', id='a', resource_id='b')
        app.get(url, status=404, extra_environ=env)

        url = tk.url_for(
            'resource.download', id=dataset['id'], resource_id=resource['id'])
        app.get(url, status=401, extra_environ=env)

        helpers.call_action('package_patch', id=dataset['id'], private=False)
        app.get(url, status=302, extra_environ=env, follow_redirects=False)

    @pytest.mark.usefixtures('clean_db')
    def test_download(self, create_with_upload, app):
        filename = 'file.txt'
        resource = create_with_upload('hello world', filename, package_id=factories.Dataset()['id'])
        url = tk.url_for(
            'resource.download',
            id=resource['package_id'],
            resource_id=resource['id'])
        resp = app.get(url, status=302, follow_redirects=False)

        uploader = storage.ResourceCloudStorage(resource)
        expected_url = uploader.get_url_from_filename(resource['id'],
                                                      filename)
        assert resp.headers['location'] == expected_url
