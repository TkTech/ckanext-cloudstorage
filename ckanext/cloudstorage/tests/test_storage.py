import os
from nose.tools import assert_equal, assert_true, assert_false
from mock import create_autospec, patch, MagicMock
import ckanapi
from webtest import Upload

from ckan.tests import helpers, factories
from ckan.plugins import toolkit
from ckanext.cloudstorage.storage import ResourceCloudStorage

from pylons import config

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

google_driver = get_driver(Provider.GOOGLE_STORAGE)
mock_driver = create_autospec(google_driver)


class Uploader(Upload):
    """Extends webtest's Upload class a bit more so it actually stores file data.
    """

    def __init__(self, *args, **kwargs):
        self.file = kwargs.pop('file')
        super(Uplaoder, self).__init__(*args, **kwargs)


class TestS3Uploader(helpers.FunctionalTestBase):

    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_resource_upload(self, get_driver):
        """Test a basic resource file upload."""
        mock_driver = MagicMock(spec=google_driver, name='driver')
        container = MagicMock(name='container')
        mock_driver.get_container.return_value = container
        get_driver.return_value = MagicMock(return_value=mock_driver)
        factories.Sysadmin(apikey='my-test-apikey')

        app = self._get_test_app()
        demo = ckanapi.TestAppCKAN(app, apikey='my-test-apikey')
        factories.Dataset(name='my-dataset')

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        resource = demo.action.resource_create(
            package_id='my-dataset',
            upload=open(file_path),
            url='file.txt'
        )

        key = 'resources/{0}/data.csv' \
            .format(resource['id'])

        args, kwargs = container.upload_object_via_stream.call_args

        assert_equal(kwargs['object_name'], key)
        print('driver method calls', mock_driver.method_calls)
        print('container method calls', container.method_calls)

    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_resource_upload_then_clear(self, get_driver):
        """Test that clearing on upload removes the storage key."""
        mock_driver = MagicMock(spec=google_driver, name='driver')
        container = MagicMock(name='container')
        mock_driver.get_container.return_value = container
        get_driver.return_value = MagicMock(return_value=mock_driver)

        sysadmin = factories.Sysadmin(apikey="my-test-key")

        app = self._get_test_app()
        demo = ckanapi.TestAppCKAN(app, apikey="my-test-key")
        dataset = factories.Dataset(name='my-dataset')

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        resource = demo.action.resource_create(
            package_id='my-dataset',
            upload=open(file_path),
            url='file.txt'
        )

        key = 'resources/{0}/data.csv'.format(resource['id'])

        args, kwargs = container.upload_object_via_stream.call_args
        assert_equal(kwargs['object_name'], key)

        container.get_object.return_value = 'object'

        url = toolkit.url_for(
            controller='package', action='resource_edit', id=dataset['id'], resource_id=resource['id'])
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}
        app.post(url, {'clear_upload': True, 'url': 'http://asdf', 'save': 'save'}, extra_environ=env)

        args, _ = container.get_object.call_args
        path = args[0]
        assert_equal(path, key)
        args, _ = container.delete_object.call_args
        assert_equal(args[0], 'object')

    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_path_from_filename(self, get_driver):
        """path_from_filename returns as expected."""
        dataset = factories.Dataset()
        resource = factories.Resource(package_id=dataset['id'])

        uploader = ResourceCloudStorage(resource)
        returned_path = uploader.path_from_filename(resource['id'], 'myfile.txt')
        assert_equal(returned_path, 'resources/{0}/myfile.txt'.format(resource['id']))

    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_resource_upload_with_url_and_clear(self, get_driver):
        """Test that clearing an upload and using a URL does not crash."""

        sysadmin = factories.Sysadmin(apikey='my-test-key')

        app = self._get_test_app()
        dataset = factories.Dataset(name='my-dataset')

        url = toolkit.url_for(controller='package', action='new_resource', id=dataset['id'])
        env = {'REMOTE_USER': sysadmin['name'].encode('ascii')}

        app.post(url, {'clear_uplaod': True, 'id': '', # empty id from the form
            'url': 'http://asdf', 'save': 'save'}, extra_environ=env)

