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

        resource, demo, _ = self._upload_resource()

        # does resource_show have the expected resource file url?
        resource_show = demo.action.resource_show(id=resource['id'])

        expected_url = 'http://mytest.ckan.net/dataset/{0}/resource/{1}/download/data.csv' \
            .format(resource['package_id'], resource['id'])

        assert_equal(resource_show['url'], expected_url)

    @mock_s3
    def test_resource_download_s3(self):
        """A resource uploaded to S3 ckan be downloaded."""

        resource, demo, app = self._upload_resource()
        resource_show = demo.action.resource_show(id=resource['id'])
        resource_file_url = resource_show['url']

        file_response = app.get(resource_file_url)

        assert_equal(file_response.content_type, 'text/csv')
        assert_true('date,price' in file_response.body)

    @mock_s3
    def test_resource_download_s3_no_filename(self):
        """A resource uploaded can be downloaded when no filename in url."""
        resource, demo, app = self._upload_resource()

        resource_file_url = '/dataset/{0}/resource/{1}/download' \
            .format(resource['package_id'], resource['id'])

        file_response = app.get(resource_file_url)

        assert_equal(file_response.content_type, 'text/csv')
        assert_true('date,price' in file_response.body)

    @mock_s3
    def test_resource_download_url_link(self):
        """A resource with a url (not a file) is redirected correctly."""
        factories.Sysadmin(apikey='my-test-apikey')

        app = self._get_test_app()
        demo = ckanapi.TestAppCKAN(app, apikey='my-test-apikey')
        dataset = factories.Dataset()

        resource = demo.action.resource_create(package_id=dataset['id'],
                                               url='http://example')
        resource_show = demo.action.resource_show(id=resource['id'])
        resource_file_url = '/dataset/{0}/resource/{1}/download' \
            .format(resource['package_id'], resource['id'])
        assert_equal(resource_show['url'], 'http://example')

        conn = boto.connect_s3()
        bucket = conn.get_bucket('my-bucket')
        assert_equal(bucket.get_all_keys(), [])

        # attempt redirect to linked url
        r = app.get(resource_file_url, status=[302, 301])
        assert_equal(r.location, 'http://example')

