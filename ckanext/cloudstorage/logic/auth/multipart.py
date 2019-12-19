#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckan.logic import check_access


def initiate_multipart(context, data_dict):
    return {'success': check_access('resource_create', context, data_dict)}


def upload_multipart(context, data_dict):
    return {'success': check_access('resource_create', context, data_dict)}


def finish_multipart(context, data_dict):
    return {'success': check_access('resource_create', context, data_dict)}


def abort_multipart(context, data_dict):
    return {'success': check_access('resource_create', context, data_dict)}


def check_multipart(context, data_dict):
    return {'success': check_access('resource_create', context, data_dict)}


def clean_multipart(context, data_dict):
    return {'success': False}
