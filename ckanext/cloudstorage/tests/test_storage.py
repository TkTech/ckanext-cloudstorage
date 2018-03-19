import os
from nose.tools import assert_equal, assert_true, assert_raises
from mock import create_autospec, patch, MagicMock
import datetime

import ckanapi
from webtest import Upload

from ckan.tests import helpers, factories
from ckan.plugins import toolkit
from ckanext.cloudstorage.storage import ResourceCloudStorage, FileCloudStorage

from pylons import config

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

google_driver = get_driver(Provider.GOOGLE_STORAGE)


class Uploader(Upload):
    """Extends webtest's Upload class a bit more so it actually stores file data.
    """

    def __init__(self, *args, **kwargs):
        self.file = kwargs.pop('file')
        super(Uploader, self).__init__(*args, **kwargs)


class TestResourceUploader(helpers.FunctionalTestBase):

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

    @helpers.change_config('ckanext.cloudstorage.use_secure_urls', True)
    @helpers.change_config('ckanext.cloudstorage.use_secure_urls_for_generics', False)
    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_resource_storage_reads_correct_use_secure_urls_config_option(self, get_driver):
        dataset = factories.Dataset(name='my-dataset')
        resource = factories.Resource(
            package_id=dataset['id'],
        )
        uploader = ResourceCloudStorage(resource)
        assert_true(uploader.use_secure_urls)


class TestFileCloudStorage(helpers.FunctionalTestBase):

    @patch('ckanext.cloudstorage.storage.FileCloudStorage')
    def test_file_upload_calls_FileCloudStorage(self, FileCloudStorage):
        sysadmin = factories.Sysadmin(apikey='apikey')

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        filename = 'image.png'

        img_uploader = Uploader(filename, file=open(file_path))

        with patch('ckanext.cloudstorage.storage.datetime') as mock_date:
            mock_date.datetime.utcnow.returl_value = datetime.datetime(2001, 1, 29)
            context = {'user': sysadmin['name']}
            helpers.call_action('group_create', context=context,
                                name='group',
                                image_upload=img_uploader,
                                image_url=filename,
                                save='save')

        FileCloudStorage.assert_called_once_with('group', None)

    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_group_image_upload(self, get_driver):
        """Test a group image file uplaod."""
        mock_driver = MagicMock(spec=google_driver, name='driver')
        container = MagicMock(name='container')
        mock_driver.get_container.return_value = container
        mock_driver.get_object_cdn_url.return_value = 'http://cdn.url'
        get_driver.return_value = MagicMock(return_value=mock_driver)

        sysadmin = factories.Sysadmin(apikey='my-test-key')

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        filename = 'image.png'

        img_uploader = Uploader(filename, file=open(file_path))

        with patch('ckanext.cloudstorage.storage.datetime') as mock_date:
            mock_date.datetime.utcnow.return_value = \
                datetime.datetime(2001, 1, 29)
            context = {'user': sysadmin['name']}
            helpers.call_action('group_create', context=context,
                                name='my-group',
                                image_upload=img_uploader,
                                image_url=filename,
                                save='save')

        key = "storage/uploads/2001-01-29-000000{0}" \
            .format(filename)

        group = helpers.call_action('group_show', id='my-group')
        print('group', group)

        args, kwargs = container.upload_object_via_stream.call_args
        assert_equal(kwargs['object_name'], unicode(key))

        # app = self._get_test_app()
        # image_file_url = '/uploads/group/{0}'.format(filename)
        # r = app.get(image_file_url)

    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_group_image_upload_then_clear(self, get_driver):
        """Test that clearing an upload calls delete_object"""
        mock_driver = MagicMock(spec=google_driver, name='driver')
        container = MagicMock(name='container')
        mock_driver.get_container.return_value = container
        get_driver.return_value = MagicMock(return_value=mock_driver)

        sysadmin = factories.Sysadmin(apikey='my-test-apikey')

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        file_name = 'image.png'

        img_uploader = Uploader(file_name, file=open(file_path))

        with patch('ckanext.cloudstorage.storage.datetime') as mock_date:
            mock_date.datetime.utcnow.return_value = \
                datetime.datetime(2001, 1, 29)
            context = {'user': sysadmin['name']}
            helpers.call_action('group_create', context=context,
                                name='my-group',
                                image_upload=img_uploader,
                                image_url=file_name)

        object_mock = MagicMock(name='object')
        container.get_object.return_value = object_mock

        helpers.call_action('group_update', context=context,
                            id='my-group', name='my-group',
                            image_url='http://example', clear_upload=True)

        # assert delete object is called
        container.delete_object.assert_called_with(object_mock)

    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_get_object_public_url(self, get_driver):
        """
        Test get_object_public_url returns expected string
        """
        uploader = FileCloudStorage('notused')
        url = uploader.get_object_public_url('file.png')
        assert_equal(url, 'https://storage.googleapis.com/test/storage/uploads/file.png')

    @helpers.change_config('ckanext.cloudstorage.use_secure_urls', False)
    @helpers.change_config('ckanext.cloudstorage.use_secure_urls_for_generics', True)
    @patch('ckanext.cloudstorage.storage.get_driver')
    def test_filestorage_secure_urls_reads_correct_config_option(self, get_driver):
        uploader = FileCloudStorage(None)
        assert_true(uploader.use_secure_urls)

    @helpers.change_config('ckanext.cloudstorage.use_secure_urls', False)
    @helpers.change_config('ckanext.cloudstorage.use_secure_urls_for_generics', True)
    @patch('ckanext.cloudstorage.storage.get_driver')
    @patch('ckanext.cloudstorage.storage.FileCloudStorage.can_use_advanced_azure', False)
    @patch('ckanext.cloudstorage.storage.FileCloudStorage.can_use_advanced_aws', False)
    @patch('ckanext.cloudstorage.storage.FileCloudStorage.can_use_advanced_google_cloud', False)
    def test_path_from_filename_uses_public_url_when_option_is_false(self, get_driver):
        sysadmin = factories.Sysadmin(apikey='my-test-apikey')

        file_path = os.path.join(os.path.dirname(__file__), 'data.csv')
        file_name = 'image.png'

        img_uploader = Uploader(file_name, file=open(file_path))

        with patch('ckanext.cloudstorage.storage.datetime') as mock_date:
            mock_date.datetime.utcnow.return_value = \
                datetime.datetime(2001, 1, 29)
            context = {'user': sysadmin['name']}
            helpers.call_action('group_create', context=context,
                                name='my-group',
                                image_upload=img_uploader,
                                image_url=file_name)

        uploader = FileCloudStorage(None)
        assert_raises(Exception, uploader.get_url_from_filename, 'image.png')
