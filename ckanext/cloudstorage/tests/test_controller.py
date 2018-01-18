import os

from nose.tools import assert_equal, assert_true

import ckan.tests.helpers as helpers
import ckan.tests.factories as factories
import ckanapi

import boto
from moto import mock_s3

class TestStorageController(helpers.FunctionalTestBase):

    def _upload_resource(self):
        factories.Sysadmin(apikey='my-test-key')

        app = self._get_test_app()
        demo = ckanapi.TestAppCKAN(app, apikey='my-test-key')
        factories.Dataset(name='my-dataset')

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        resource = demo.action.resource_create(package_id='my-dataset',
                                               upload=open(file_path),
                                               url='file.txt')
        return resource, demo, app

    @mock_s3
    @helpers.change_config('ckan.site_url', 'http://mytest.ckan.net')
    def test_resource_show_url(self):
        """The resource_show url is expected for uploaded resource file."""

        resource_demo, _ = self._upload_resource()

        # does resource_show have the expected resource file url?
        resource_show = demo.action.resource_show(id=resource['id'])

        expected_url = 'http://mytest.ckan.net/dataset/{0}/resource/{1}/download/data.csv' \
            .format(resource['package_id'], resource['id'])

        assert_equal(resource_show['url'], expected_url)

