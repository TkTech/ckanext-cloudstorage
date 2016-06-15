#!/usr/bin/env python
# -*- coding: utf-8 -*-
from docopt import docopt
from ckan.lib.cli import CkanCommand

from ckanext.cloudstorage.storage import CloudStorage


USAGE = """ckanext-cloudstorage

Commands:
    - fix-cors       Update CORS rules where possible.

Usage:
    cloudstorage fix-cors <domains>... [--c=<config>]

Options:
    -c=<config>       The CKAN configuration file.
"""


class PasterCommand(CkanCommand):
    summary = 'ckanext-cloudstorage maintence utilities.'
    usage = USAGE

    def command(self):
        self._load_config()
        args = docopt(USAGE, argv=self.args)

        if args['fix-cors']:
            cs = CloudStorage()

            if cs.can_use_advanced_azure:
                from azure.storage import blob as azure_blob
                from azure.storage import CorsRule

                blob_service = azure_blob.BlockBlobService(
                    cs.driver_options['key'],
                    cs.driver_options['secret']
                )

                blob_service.set_blob_service_properties(
                    cors=[
                        CorsRule(
                            allowed_origins=args['<domains>'],
                            allowed_methods=['GET']
                        )
                    ]
                )
                print('Done!')
            else:
                print(
                    'The driver {driver_name} being used does not currently'
                    ' support updating CORS rules through'
                    ' cloudstorage.'.format(
                        driver_name=cs.driver_name
                    )
                )
