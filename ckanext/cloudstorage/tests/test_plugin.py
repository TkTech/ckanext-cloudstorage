# -*- coding: utf-8 -*-

import pytest

from libcloud.storage.types import ObjectDoesNotExistError

import ckan.plugins as p

from ckan.tests import helpers


@pytest.mark.ckan_config('ckan.plugins', 'cloudstorage')
@pytest.mark.usefixtures('with_driver_options', 'with_plugins')
class TestCloudstoragePlugin(object):

    @pytest.mark.parametrize('option', (
        'ckanext.cloudstorage.driver',
        'ckanext.cloudstorage.driver_options',
        'ckanext.cloudstorage.container_name'))
    def test_required_config(self, ckan_config, monkeypatch, option):
        monkeypatch.delitem(ckan_config, option)
        plugin = p.get_plugin('cloudstorage')
        with pytest.raises(RuntimeError, match='configuration option'):
            plugin.configure(ckan_config)

    @pytest.mark.usefixtures('clean_db')
    def test_before_delete(self, make_resource):
        name = 'test.txt'
        resource = make_resource('hello world', name, name=name)
        plugin = p.get_plugin('cloudstorage')
        uploader = plugin.get_resource_uploader(resource)
        assert uploader.get_url_from_filename(resource['id'], name)
        helpers.call_action('resource_delete', id=resource['id'])
        with pytest.raises(ObjectDoesNotExistError):
            assert uploader.get_url_from_filename(resource['id'], name)
