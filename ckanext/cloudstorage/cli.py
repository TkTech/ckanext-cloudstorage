# -*- coding: utf-8 -*-

import click
import ckanext.cloudstorage.utils as utils


@click.group()
def cloudstorage():
    """CloudStorage management commands.
    """
    pass


@cloudstorage.command('fix-cors')
@click.argument('domains', nargs=-1)
def fix_cors(domains):
    """Update CORS rules where possible.
    """
    msg, ok = utils.fix_cors(domains)
    click.secho(msg, fg='green' if ok else 'red')


@cloudstorage.command()
@click.argument('path')
@click.argument('resource', required=False)
def migrate(path, resource):
    """Upload local storage to the remote.
    """
    utils.migrate(path, resource)


@cloudstorage.command()
def initdb():
    """Reinitalize database tables.
    """
    utils.initdb()
    click.secho("DB tables are reinitialized", fg="green")


def get_commands():
    return [cloudstorage]
