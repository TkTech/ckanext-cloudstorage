import ckan.lib.helpers as h
import ckan.model as model
import ckan.plugins.toolkit as toolkit
from ckanext.cloudstorage.storage import ResourceCloudStorage
from ckanext.cloudstorage.model import MultipartUpload, MultipartPart
from sqlalchemy.orm.exc import NoResultFound
import logging

log = logging.getLogger(__name__)


def _get_object_url(uploader, name):
    return '/' + uploader.container_name + '/' + name


def _delete_multipart(upload, uploader):
    resp = uploader.driver.connection.request(
        _get_object_url(uploader, upload.name) + '?uploadId=' + upload.id,
        method='DELETE'
    )
    if not resp.success():
        raise toolkit.ValidationError(resp.error)

    upload.delete()
    upload.commit()
    return resp


def _save_part_info(n, etag, upload):
    try:
        part = model.Session.query(MultipartPart).filter(
            MultipartPart.n == n,
            MultipartPart.upload == upload).one()
    except NoResultFound:
        part = MultipartPart(n, etag, upload)
    else:
        part.etag = etag
    part.save()
    return part


def check_multipart(context, data_dict):
    h.check_access('cloudstorage_check_multipart', data_dict)
    id = toolkit.get_or_bust(data_dict, 'id')
    try:
        upload = model.Session.query(MultipartUpload).filter_by(
            resource_id=id).one()
    except NoResultFound:
        return
    upload_dict = upload.as_dict()
    upload_dict['parts'] = model.Session.query(MultipartPart).filter(
        MultipartPart.upload==upload).count()
    return {'upload': upload_dict}


def initiate_multipart(context, data_dict):
    h.check_access('cloudstorage_initiate_multipart', data_dict)
    id, name, size = toolkit.get_or_bust(data_dict, ['id', 'name', 'size'])

    uploader = ResourceCloudStorage({'multipart_name': name})
    res_name = uploader.path_from_filename(id, name)

    upload_object = MultipartUpload.by_name(res_name)

    if upload_object is not None:
        _delete_multipart(upload_object, uploader)
        upload_object = None

    if upload_object is None:
        for old_upload in model.Session.query(MultipartUpload).filter_by(
                resource_id=id):
            _delete_multipart(old_upload, uploader)

        _rindex = res_name.rfind('/')
        if ~_rindex:
            try:
                name_prefix = res_name[:_rindex]
                for cloud_object in uploader.container.iterate_objects():
                    if cloud_object.name.startswith(name_prefix):
                        log.info('Removing cloud object: %s' % cloud_object)
                        cloud_object.delete()
            except Exception as e:
                log.error('[delete from cloud] %s' % e)

        resp = uploader.driver.connection.request(
            _get_object_url(uploader, res_name) + '?uploads',
            method='POST'
        )
        if not resp.success():
            raise toolkit.ValidationError(resp.error)

        upload_id = resp.object.find(
            '{%s}UploadId' % resp.object.nsmap[None]).text
        upload_object = MultipartUpload(upload_id, id, res_name, size, name)

        upload_object.save()
    return upload_object.as_dict()

    return {'UploadId': upload_id}


def upload_multipart(context, data_dict):
    h.check_access('cloudstorage_upload_multipart', data_dict)
    upload_id, part_number, part_content = toolkit.get_or_bust(
        data_dict, ['uploadId', 'partNumber', 'upload'])

    uploader = ResourceCloudStorage({})
    upload = model.Session.query(MultipartUpload).get(upload_id)

    # import pdb; pdb.set_trace()
    resp = uploader.driver.connection.request(
        _get_object_url(
            uploader, upload.name) + '?partNumber={0}&uploadId={1}'.format(
                part_number, upload_id),
        method='PUT',
        data=bytearray(part_content.file.read())
    )
    if resp.status != 200:
        raise toolkit.ValidationError('Upload failed: part %s' % part_number)

    _save_part_info(part_number, resp.headers['etag'], upload)
    return {
        'partNumber': part_number,
        'ETag': resp.headers['etag']
    }


def finish_multipart(context, data_dict):
    h.check_access('cloudstorage_finish_multipart', data_dict)
    upload_id = toolkit.get_or_bust(data_dict, 'uploadId')
    upload = model.Session.query(MultipartUpload).get(upload_id)
    chunks = [
        (part.n, part.etag)
        for part in model.Session.query(MultipartPart).filter_by(
            upload_id=upload_id).order_by(MultipartPart.n)
    ]
    uploader = ResourceCloudStorage({})
    try:
        obj = uploader.container.get_object(upload.name)
        obj.delete()
    except:
        pass
    uploader.driver._commit_multipart(
        _get_object_url(uploader, upload.name),
        upload_id,
        chunks)
    upload.delete()
    upload.commit()
    print chunks


def abort_multipart(context, data_dict):
    h.check_access('cloudstorage_abort_multipart', data_dict)
    id = toolkit.get_or_bust(data_dict, ['id'])
    uploader = ResourceCloudStorage({})

    resource_uploads = MultipartUpload.resource_uploads(id)

    aborted = []
    for upload in resource_uploads:
        _delete_multipart(upload, uploader)

        aborted.append(upload.id)

    model.Session.commit()

    return aborted
