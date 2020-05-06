# -*- coding: utf-8 -*-
import os

import pytest


@pytest.fixture
def with_driver_options(ckan_config, monkeypatch):
    monkeypatch.setitem(
        ckan_config,
        'ckanext.cloudstorage.driver',
        os.getenv('TEST_DRIVER', 'S3'))
    monkeypatch.setitem(
        ckan_config,
        'ckanext.cloudstorage.container_name',
        os.getenv('TEST_CONTAINER', 'cloudstorage-test'))

    monkeypatch.setitem(
        ckan_config,
        'ckanext.cloudstorage.driver_options',
        os.getenv('TEST_DRIVER_OPTIONS', '{}'))
