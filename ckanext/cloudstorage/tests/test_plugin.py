import os
from nose.tools import assert_equal, assert_raises
from mock import patch, MagicMock

from ckan.tests import helpers, factories
from ckan.lib import helpers as h

import ckanapi

from ckanext.cloudstorage.controller import StorageController
class TestPlugin(helpers.FunctionalTestBase):

    @patch('ckanext.cloudstorage.storage.get_driver')
    @patch('ckanext.cloudstorage.controller.StorageController', spec=StorageController)
    def test_resource_download_calls_ext_method(self, resource_download, get_driver):
        """
        Test `ckanext.cloudstorage.controller.StorageController.resource_download` is called for `resource_download` action.
        """
        app = self._get_test_app()
        demo = ckanapi.TestAppCKAN(app, apikey='my-test-apikey')
        factories.Sysadmin(apikey='my-test-apikey')

        factories.Dataset(name='my-dataset')
        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        resource = demo.action.resource_create(
                package_id='my-dataset',
                upload=open(file_path),
                url='file.txt'
        )

        # proves it's calling the right code, right?
        with assert_raises(TypeError) as exc:
            r = app.get(resource['url'])
        assert_equal(exc.exception.message, "'MagicMock' object is not iterable")
        resource_download.assert_called_once()
