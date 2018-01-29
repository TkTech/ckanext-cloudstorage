import os

from nose.tools import assert_equal, assert_true
from mock import patch, create_autospec, MagicMock

import ckan.plugins
import ckan.tests.helpers as helpers
import ckan.tests.factories as factories
from webtest import Upload

from ckan.common import config
import ckanapi
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

from ckanext.cloudstorage.controller import StorageController

google_driver = get_driver(Provider.GOOGLE_STORAGE)


class Uploader(Upload):
    """Extends webtest's Upload class a bit more so it actually stores file data.
    """

    def __init__(self, *args, **kwargs):
        self.file = kwargs.pop('file')
        super(Uplaoder, self).__init__(*args, **kwargs)



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

    @patch('ckanext.cloudstorage.storage.get_driver')
    @helpers.change_config('ckan.site_url', 'http://mytest.ckan.net')
    def test_resource_show_url(self, get_driver):
        """The resource_show url is expected for uploaded resource file."""
        mock_driver = MagicMock(spec=google_driver, name='driver')
        container = MagicMock(name='container')
        mock_driver.get_container.return_value = container
        get_driver.return_value = MagicMock(return_value=mock_driver)

        resource, demo, _ = self._upload_resource()

        # does resource_show have the expected resource file url?
        resource_show = demo.action.resource_show(id=resource['id'])

        expected_url = 'http://mytest.ckan.net/dataset/{0}/resource/{1}/download/data.csv' \
            .format(resource['package_id'], resource['id'])

        assert_equal(resource_show['url'], expected_url)

    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_resource_download_s3(self, get_driver):
        """A resource uploaded to S3 ckan be downloaded."""
        mock_driver = MagicMock(spec=google_driver, name='driver')
        container = MagicMock(name='container')
        mock_driver.get_container.return_value = container
        get_driver.return_value = MagicMock(return_value=mock_driver)

        resource, demo, app = self._upload_resource()
        resource_show = demo.action.resource_show(id=resource['id'])
        resource_file_url = resource_show['url']

        assert_equal(resource_file_url, '{2}/dataset/{0}/resource/{1}/download/data.csv' \
            .format(resource['package_id'], resource['id'], 'http://ckan:5000'))

    @patch('ckanext.cloudstorage.storage.get_driver')
    @patch('ckanext.cloudstorage.controller.h')
    def test_resource_download_s3_no_filename(self, h, get_driver):
        """A resource uploaded can be downloaded when no filename in url."""
        mock_driver = MagicMock(spec=google_driver, name='driver')
        container = MagicMock(name='container')
        mock_driver.get_container.return_value = container
        get_driver.return_value = MagicMock(return_value=mock_driver)

        resource, demo, app = self._upload_resource()

        resource_file_url = '/dataset/{0}/resource/{1}/download' \
            .format(resource['package_id'], resource['id'])

        mock_driver.get_object_cdn_url.return_value = resource_file_url

        file_response = app.get(resource_file_url)

        h.redirect_to.assert_called_with(resource_file_url)

    @patch('ckanext.cloudstorage.storage.get_driver')
    @patch('ckanext.cloudstorage.controller.h')
    def test_resource_download_url_link(self, h, get_driver):
        """A resource with a url (not a file) is redirected correctly."""
        mock_driver = MagicMock(spec=google_driver, name='driver')
        container = MagicMock(name='container')
        mock_driver.get_container.return_value = container
        get_driver.return_value = MagicMock(return_value=mock_driver)
        mock_driver.get_object_cdn_url.return_value = 'http://example'

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

        # attempt redirect to linked url
        r = app.get(resource_file_url)
        h.redirect_to.assert_called_with('http://example')

