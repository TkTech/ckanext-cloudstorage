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

        # if the client requests with a Content-Type header (e.g. Text preview)
        # we have to add the header to the signature
        try:
            content_type = getattr(c.pylons.request, "content_type", None)
        except AttributeError:
            content_type = None
        uploaded_url = upload.get_url_from_filename(resource['id'], filename,
                                                    content_type=content_type)

        # The uploaded file is missing for some reason, such as the
        # provider being down.
        if uploaded_url is None:
            base.abort(404, _('No download is available'))

        h.redirect_to(uploaded_url)
