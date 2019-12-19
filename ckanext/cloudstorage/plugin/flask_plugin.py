# -*- coding: utf-8 -*-

import ckan.plugins as p

from ckanext.cloudstorage.views import get_blueprints
from ckanext.cloudstorage.cli import get_commands


class MixinPlugin(p.SingletonPlugin):
    p.implements(p.IBlueprint)
    p.implements(p.IClick)

    # IBlueprint

    def get_blueprint(self):
        return get_blueprints()

    # IClick

    def get_commands(self):
        return get_commands()
