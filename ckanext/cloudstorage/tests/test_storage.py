# -*- coding: utf-8 -*-
import pytest

from six.moves.urllib.parse import urlparse

from ckanext.cloudstorage.storage import CloudStorage, ResourceCloudStorage


@pytest.mark.ckan_config('ckan.plugins', 'cloudstorage')
@pytest.mark.usefixtures('with_driver_options', 'with_plugins')
class TestCloudStorage(object):
    def test_props(self):
        storage = CloudStorage()
        assert storage.driver_options
        assert storage.driver_name
        assert storage.container_name
        assert storage.container
        assert not storage.leave_files
        assert not storage.use_secure_urls
        assert not storage.guess_mimetype


@pytest.mark.ckan_config('ckan.plugins', 'cloudstorage')
@pytest.mark.usefixtures('with_driver_options', 'with_plugins')
class TestResourceCloudStorage(object):
    def test_not_secure_url_from_filename(self, make_resource):
        filename = 'file.txt'
        resource = make_resource('test', filename)
        storage = ResourceCloudStorage(resource)
        url = storage.get_url_from_filename(resource['id'], filename)
        assert storage.container_name in url
        assert not urlparse(url).query

    @pytest.mark.ckan_config('ckanext.cloudstorage.use_secure_urls', True)
    def test_secure_url_from_filename(self, make_resource):
        filename = 'file.txt'
        resource = make_resource('test', filename)
        storage = ResourceCloudStorage(resource)
        if not storage.can_use_advanced_aws or not storage.use_secure_urls:
            pytest.skip('SecureURL not supported')
        url = storage.get_url_from_filename(resource['id'], filename)
        assert urlparse(url).query
