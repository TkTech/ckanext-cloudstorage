#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='ckanext-cloudstorage',
    version='0.1.1',
    description='Cloud storage for CKAN',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords=[
        'CKAN',
        'S3',
        'Azure',
        'GoogleCloud'
    ],
    author='Tyler Kennedy',
    author_email='tk@tkte.ch',
    url='http://github.com/open-data/ckanext-cloudstorage',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'apache-libcloud==1.5'
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
