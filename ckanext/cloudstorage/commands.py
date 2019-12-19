#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from ckan.lib.cli import CkanCommand
from docopt import docopt

import ckanext.cloudstorage.utils as utils

USAGE = """ckanext-cloudstorage

Commands:
    - fix-cors       Update CORS rules where possible.
    - migrate        Upload local storage to the remote.
    - initdb         Reinitalize database tables.

Usage:
    cloudstorage fix-cors <domains>... [--c=<config>]
    cloudstorage migrate <path_to_storage> [<resource_id>] [--c=<config>]
    cloudstorage initdb [--c=<config>]

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
            _fix_cors(args)
        elif args['migrate']:
            _migrate(args)
        elif args['initdb']:
            _initdb()


def _migrate(args):
    path = args['<path_to_storage>']
    single_id = args['<resource_id>']
    utils.migrate(path, single_id)


def _fix_cors(args):
    msg, _ = utils.fix_cors(args['<domains>'])
    print(msg)


def _initdb():
    utils.initdb()
    print("DB tables are reinitialized")
