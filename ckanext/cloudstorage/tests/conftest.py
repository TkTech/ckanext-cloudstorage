# -*- coding: utf-8 -*-
import os

import pytest
from ckanext.cloudstorage import utils


@pytest.fixture
def with_driver_options(ckan_config, monkeypatch):
    """Apply config from env variablies - thus you won't have unstaged
    changes in config file and won't accidentally commit your cloud
    credentials.

    """
    driver = os.getenv('TEST_DRIVER')

    if not driver:
        pytest.skip('TEST_DRIVER is not set')
    monkeypatch.setitem(ckan_config, 'ckanext.cloudstorage.driver', driver)

    container = os.getenv('TEST_CONTAINER')
    if not container:
        pytest.skip('TEST_CONTAINER is not set')
    monkeypatch.setitem(ckan_config,
                        'ckanext.cloudstorage.container_name', container)

    options = os.getenv('TEST_DRIVER_OPTIONS')
    if not options:
        pytest.skip('TEST_DRIVER_OPTIONS is not set')
    monkeypatch.setitem(ckan_config,
                        'ckanext.cloudstorage.driver_options', options)


@pytest.fixture
def clean_db(reset_db):
    """Initialize extension's tables.
    """
    reset_db()
    utils.initdb()
