import pytest

import ckan.plugins.toolkit as tk

_secure_urls = 'ckanext.cloudstorage.use_secure_urls'
_driver = 'ckanext.cloudstorage.driver'
_options = 'ckanext.cloudstorage.driver_options'


@pytest.mark.ckan_config('ckanext.cloudstorage.container_name', 'test')
@pytest.mark.ckan_config('ckan.plugins', 'cloudstorage')
@pytest.mark.usefixtures('with_plugins')
class TestUseSecureUrls(object):
    @pytest.mark.ckan_config(_secure_urls, 'true')
    @pytest.mark.ckan_config(_driver, 'AZURE_BLOBS')
    @pytest.mark.ckan_config(_options, '{}')
    def test_unsupported_provider_enabled(self):
        assert not tk.h.cloudstorage_use_secure_urls()

    @pytest.mark.ckan_config(_secure_urls, 'false')
    @pytest.mark.ckan_config(_driver, 'AZURE_BLOBS')
    @pytest.mark.ckan_config(_options, '{}')
    def test_unsupported_provider_disabled(self):
        assert not tk.h.cloudstorage_use_secure_urls()

    @pytest.mark.ckan_config(_secure_urls, 'true')
    @pytest.mark.ckan_config(_driver, 'S3_US_WEST')
    @pytest.mark.ckan_config(_options, '{}')
    def test_supported_provider_enabled_withoug_host(self):
        assert not tk.h.cloudstorage_use_secure_urls()

    @pytest.mark.ckan_config(_secure_urls, 'true')
    @pytest.mark.ckan_config(_driver, 'S3_US_WEST')
    @pytest.mark.ckan_config(_options, '{"host": "x"}')
    def test_supported_provider_enabled_with_host(self):
        assert tk.h.cloudstorage_use_secure_urls()

    @pytest.mark.ckan_config(_secure_urls, 'false')
    @pytest.mark.ckan_config(_driver, 'S3_US_WEST')
    @pytest.mark.ckan_config(_options, '{}')
    def test_supported_provider_disabled(self):
        assert not tk.h.cloudstorage_use_secure_urls()
