#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path

from pylons import c
from pylons.i18n import _

from ckan import logic, model
from ckan.lib import base, uploader
import ckan.lib.helpers as h
import ckanext.cloudstorage.utils as utils

class StorageController(base.BaseController):
    def resource_download(self, id, resource_id, filename=None):
        return utils.resource_download(id, resource_id, filename)
