#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan.lib import base
import ckanext.cloudstorage.utils as utils


class StorageController(base.BaseController):
    def resource_download(self, id, resource_id, filename=None):
        return utils.resource_download(id, resource_id, filename)
