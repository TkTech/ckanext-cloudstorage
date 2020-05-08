# -*- coding: utf-8 -*-

import pytest
import six

from libcloud.storage.types import ObjectDoesNotExistError

from ckan.tests import factories, helpers
from ckanext.cloudstorage.storage import ResourceCloudStorage
from ckanext.cloudstorage.utils import FakeFileStorage


@pytest.mark.ckan_config('ckan.plugins', 'cloudstorage')
@pytest.mark.usefixtures(
    'with_driver_options', 'with_plugins',
    'with_request_context', 'clean_db')
class TestMultipartUpload(object):

    def test_upload(self):
        filename = 'file.txt'
        res = factories.Resource()
        multipart = helpers.call_action(
            'cloudstorage_initiate_multipart',
            id=res['id'], name='file.txt', size=1024 * 1024 * 5 * 2)
        storage = ResourceCloudStorage(res)
        assert storage.path_from_filename(
            res['id'], filename) == multipart['name']
        with pytest.raises(ObjectDoesNotExistError):
            storage.get_url_from_filename(res['id'], filename)

        fp = six.BytesIO(b'b' * 1024 * 1024 * 5)
        fp.seek(0)
        helpers.call_action(
            'cloudstorage_upload_multipart',
            uploadId=multipart['id'],
            partNumber=1,
            upload=FakeFileStorage(fp, filename))

        with pytest.raises(ObjectDoesNotExistError):
            storage.get_url_from_filename(res['id'], filename)

        fp = six.BytesIO(b'a' * 1024 * 1024 * 5)
        fp.seek(0)
        helpers.call_action(
            'cloudstorage_upload_multipart',
            uploadId=multipart['id'],
            partNumber=2,
            upload=FakeFileStorage(fp, filename))

        with pytest.raises(ObjectDoesNotExistError):
            storage.get_url_from_filename(res['id'], filename)

        result = helpers.call_action(
            'cloudstorage_finish_multipart', uploadId=multipart['id'])
        assert result['commited']
        assert storage.get_url_from_filename(res['id'], filename)
