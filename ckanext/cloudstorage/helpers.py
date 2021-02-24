#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckanext.cloudstorage.storage import ResourceCloudStorage
import ckan.plugins.toolkit as tk

def use_secure_urls():
    return all([
        ResourceCloudStorage.use_secure_urls.fget(None),
        # Currently implemented just AWS version
        'S3' in ResourceCloudStorage.driver_name.fget(None),
        'host' in ResourceCloudStorage.driver_options.fget(None),
    ])


def use_multipart_upload():
    return use_secure_urls()


def max_upload_size():
    return tk.config.get('ckanext.cloudstorage.max_upload_size_gb')
