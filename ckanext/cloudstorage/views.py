# -*- coding: utf-8 -*-

from flask import Blueprint
import ckan.views.resource as resource
import ckanext.cloudstorage.utils as utils

cloudstorage = Blueprint("cloudstorage", __name__)


def download(id, resource_id, filename=None, package_type="dataset"):
    return utils.resource_download(id, resource_id, filename)


cloudstorage.add_url_rule(
    "/dataset/<id>/resource/<resource_id>/download", view_func=download
)
cloudstorage.add_url_rule(
    "/dataset/<id>/resource/<resource_id>/download/<filename>",
    view_func=download,
)


def get_blueprints():
    return [cloudstorage]
