#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path

from pylons import c
from pylons.i18n import _

from ckan import logic, model
from ckan.lib import base, uploader
import ckan.lib.helpers as h


class StorageController(base.BaseController):
    def resource_download(self, id, resource_id, filename=None):
        context = {
            'model': model,
            'session': model.Session,
            'user': c.user or c.author,
            'auth_user_obj': c.userobj
        }

        try:
            resource = logic.get_action('resource_show')(
                context,
                {
                    'id': resource_id
                }
            )
        except logic.NotFound:
            base.abort(404, _('Resource not found'))
        except logic.NotAuthorized:
            base.abort(401, _('Unauthorized to read resource {0}'.format(id)))

        # This isn't a file upload, so either redirect to the source
        # (if available) or error out.
        if resource.get('url_type') != 'upload':
            url = resource.get('url')
            if not url:
                base.abort(404, _('No download is available'))
            h.redirect_to(url)

        if filename is None:
            # No filename was provided so we'll try to get one from the url.
            filename = os.path.basename(resource['url'])

        upload = uploader.get_resource_uploader(resource)
        file_path = upload.path_from_filename(resource['id'], filename)
        uploaded_url = upload.get_url_from_path(file_path)

        # The uploaded file is missing for some reason, such as the
        # provider being down.
        if uploaded_url is None:
            base.abort(404, _('No download is available'))

        h.redirect_to(uploaded_url)

    def uploaded_file_redirect(self, upload_to, filename):
        '''Redirect static file requests to their location on cloudstorage.'''
        upload = uploader.get_uploader('notused')
        file_path = upload.path_from_filename(filename)
        uploaded_url = upload.get_url_from_path(file_path)
        h.redirect_to(uploaded_url)
