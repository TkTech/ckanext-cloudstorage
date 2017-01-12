#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='ckanext-cloudstorage',
    version='0.1.1',
    description='Cloud storage for CKAN',
    classifiers=[],
    keywords='',
    author='Tyler Kennedy',
    author_email='tk@tkte.ch',
    url='http://github.com/open-data/ckanext-cloudstorage',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'apache-libcloud'
    ],
    entry_points=(
        """
        [ckan.plugins]
        cloudstorage=ckanext.cloudstorage.plugin:CloudStoragePlugin

        [paste.paster_command]
        cloudstorage=ckanext.cloudstorage.cli:PasterCommand
        """
    ),
)
