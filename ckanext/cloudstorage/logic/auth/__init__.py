# -*- coding: utf-8 -*-

from ckanext.cloudstorage.logic.auth import multipart


def get_auth_functions():
    return {
        'cloudstorage_initiate_multipart': multipart.initiate_multipart,
        'cloudstorage_upload_multipart': multipart.upload_multipart,
        'cloudstorage_finish_multipart': multipart.finish_multipart,
        'cloudstorage_abort_multipart': multipart.abort_multipart,
        'cloudstorage_check_multipart': multipart.check_multipart,
        'cloudstorage_clean_multipart': multipart.clean_multipart,
    }
