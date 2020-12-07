# -*- coding: utf-8 -*-
from __future__ import print_function
import os.path

from ckan import logic, model
import ckan.plugins.toolkit as tk
from ckan.lib import base, uploader
import ckan.lib.helpers as h
import cgi
import tempfile
from ckan.logic import NotFound
from ckanapi import LocalCKAN

from ckanext.cloudstorage.model import (create_tables, drop_tables)
from ckanext.cloudstorage.storage import (CloudStorage, ResourceCloudStorage)


class FakeFileStorage(cgi.FieldStorage):
    def __init__(self, fp, filename):
        self.file = fp
        self.stream = fp
        self.filename = filename


def initdb():
    drop_tables()
    create_tables()


def fix_cors(domains):
    cs = CloudStorage()

    if cs.can_use_advanced_azure:
        from azure.storage import blob as azure_blob
        from azure.storage import CorsRule

        blob_service = azure_blob.BlockBlobService(cs.driver_options['key'],
                                                   cs.driver_options['secret'])

        blob_service.set_blob_service_properties(
            cors=[CorsRule(allowed_origins=domains, allowed_methods=['GET'])])
        return 'Done!', True
    else:
        return ('The driver {driver_name} being used does not currently'
                ' support updating CORS rules through'
                ' cloudstorage.'.format(driver_name=cs.driver_name)), False


def migrate(path, single_id):
    if not os.path.isdir(path):
        print('The storage directory cannot be found.')
        return

    lc = LocalCKAN()
    resources = {}
    failed = []

    # The resource folder is stuctured like so on disk:
    # - storage/
    #   - ...
    # - resources/
    #   - <3 letter prefix>
    #     - <3 letter prefix>
    #       - <remaining resource_id as filename>
    #       ...
    #     ...
    #   ...
    for root, dirs, files in os.walk(path):
        # Only the bottom level of the tree actually contains any files. We
        # don't care at all about the overall structure.
        if not files:
            continue

        split_root = root.split('/')
        resource_id = split_root[-2] + split_root[-1]

        for file_ in files:
            ckan_res_id = resource_id + file_
            if single_id and ckan_res_id != single_id:
                continue

            resources[ckan_res_id] = os.path.join(root, file_)

    for i, resource in enumerate(iter(list(resources.items())), 1):
        resource_id, file_path = resource
        print('[{i}/{count}] Working on {id}'.format(i=i,
                                                     count=len(resources),
                                                     id=resource_id))
        try:
            resource = lc.action.resource_show(id=resource_id)
        except NotFound:
            print(u'\tResource not found')
            continue
        if resource['url_type'] != 'upload':
            print(u'\t`url_type` is not `upload`. Skip')
            continue

        with open(file_path, 'rb') as fin:
            resource['upload'] = FakeFileStorage(
                fin, resource['url'].split('/')[-1])
            try:
                uploader = ResourceCloudStorage(resource)
                uploader.upload(resource['id'])
            except Exception as e:
                failed.append(resource_id)
                print(u'\tError of type {0} during upload: {1}'.format(
                    type(e), e))

    if failed:
        log_file = tempfile.NamedTemporaryFile(delete=False)
        #log_file.file.writelines(failed)
        print(u'ID of all failed uploads are saved to `{0}`: {1}'.format(
            log_file.name, failed))


def resource_download(id, resource_id, filename=None):
    context = {
        'model': model,
        'session': model.Session,
        'user': tk.c.user or tk.c.author,
        'auth_user_obj': tk.c.userobj
    }

    try:
        resource = logic.get_action('resource_show')(context, {
            'id': resource_id
        })
    except logic.NotFound:
        return base.abort(404, tk._('Resource not found'))
    except logic.NotAuthorized:
        return base.abort(401,
                          tk._('Unauthorized to read resource {0}'.format(id)))

    # This isn't a file upload, so either redirect to the source
    # (if available) or error out.
    if resource.get('url_type') != 'upload':
        url = resource.get('url')
        if not url:
            return base.abort(404, tk._('No download is available'))
        return h.redirect_to(url)

    if filename is None:
        # No filename was provided so we'll try to get one from the url.
        filename = os.path.basename(resource['url'])

    upload = uploader.get_resource_uploader(resource)

    # if the client requests with a Content-Type header (e.g. Text preview)
    # we have to add the header to the signature
    try:
        content_type = getattr(tk.request, "content_type", None)
    except AttributeError:
        content_type = None
    uploaded_url = upload.get_url_from_filename(resource['id'],
                                                filename,
                                                content_type=content_type)

    # The uploaded file is missing for some reason, such as the
    # provider being down.
    if uploaded_url is None:
        return base.abort(404, tk._('No download is available'))

    return h.redirect_to(uploaded_url)
