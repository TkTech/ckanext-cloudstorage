import pytest

import ckan.plugins.toolkit as tk


@pytest.mark.ckan_config('ckanext.cloudstorage.container_name', 'test')
@pytest.mark.ckan_config('ckanext.cloudstorage.driver_options', '{}')
@pytest.mark.ckan_config('ckan.plugins', 'cloudstorage')
@pytest.mark.usefixtures('with_plugins')
class TestUseSecureUrls(object):
    @pytest.mark.ckan_config('ckanext.cloudstorage.use_secure_urls', 'true')
    @pytest.mark.ckan_config('ckanext.cloudstorage.driver', 'AZURE_BLOBS')
    def test_unsupported_provider_enabled(self):
        assert not tk.h.cloudstorage_use_secure_urls()

    @pytest.mark.ckan_config('ckanext.cloudstorage.use_secure_urls', 'false')
    @pytest.mark.ckan_config('ckanext.cloudstorage.driver', 'AZURE_BLOBS')
    def test_unsupported_provider_disabled(self):
        assert not tk.h.cloudstorage_use_secure_urls()

    @pytest.mark.ckan_config('ckanext.cloudstorage.use_secure_urls', 'true')
    @pytest.mark.ckan_config('ckanext.cloudstorage.driver', 'S3_US_WEST')
    def test_supported_provider_enabled(self):
        assert tk.h.cloudstorage_use_secure_urls()

    @pytest.mark.ckan_config('ckanext.cloudstorage.use_secure_urls', 'false')
    @pytest.mark.ckan_config('ckanext.cloudstorage.driver', 'S3_US_WEST')
    def test_supported_provider_disabled(self):
        assert not tk.h.cloudstorage_use_secure_urls()
