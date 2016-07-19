ckan.module('cloudstorage-multipart-upload', function($, _) {
    'use strict';

    return {
        options: {
            cloud: 'S3',
            i18n: {
                resource_create: _('Resource has been created.'),
                resource_update: _('Resource has been updated.'),
                undefined_upload_id: _('Undefined uploadId.'),
                upload_completed: _('Upload completed.'),
            }
        },

        _partNumber: 1,

        _uploadId: null,
        _packageId: null,
        _resourceId: null,

        initialize: function() {
            $.proxyAll(this, /_on/);
            this.options.packageId = this.options.packageId.slice(1);
            this._form = this.el.closest('form');
            this._file = $('#field-image-upload');
            this._url = $('#field-image-url');
            this._save = $('[name=save]');
            this._id = $('input[name=id]');
            this._progress = $('<div>', {
                class: 'hide controls progress progress-striped active'
            })
            this._bar = $('<div>', {class:'bar'});

            this._progress.append(this._bar);
            this._progress.insertAfter(this._file.parent().parent());

            var self = this;

            this._file.fileupload({
                url: this.sandbox.client.url('/api/action/cloudstorage_upload_multipart'),
                replaceFileInput: false,
                maxChunkSize: 5 * 1024 * 1024,
                add: this._onFileUploadAdd,
                progressall: this._onFileUploadProgress,
                done: this._onFinishUpload,
                fail: this._onUploadFail,
                always: this._onAnyEndedUpload,
                formData: this._onGenerateAdditionalData,
                submit: this._onUploadFileSubmit
            })

            this._save.on('click', this._onSaveClick);

            // switch (this.options.cloud) {
            //     case 'S3':
            //     default:
            //         this._onPrepareUpload = this._onPrepareUpload;
            //         this._onUploadStarted = this._onUploadStarted;
            //         this._onUploadSlice = this._onUploadSlice;
            //         this._onAbortUpload = this._onAbortUpload;
            // }
        },

        _onUploadFail: function (e, data) {
            console.log(arguments);
            this._onHandleError('Upload fail');

        },

        _onUploadFileSubmit: function (event, data) {
            if (!this._uploadId) {
                this._onDisableSave(false);
                this.sandbox.notify(
                    'Upload error',
                    this.i18n('undefined_upload_id'),
                    'error'
                )
                return false;
            }
            this._setProgress(0, this._bar);
            this._setProgressType('info', this._progress);
            this._progress.show('slow');
        },

        _onGenerateAdditionalData: function (form) {
            return [
                {
                    name: 'partNumber',
                    value: this._partNumber++
                },
                {
                    name: 'uploadId',
                    value: this._uploadId
                },

            ]
        },

        _onAnyEndedUpload: function () {
            this._partNumber = 1;
        },

        _onFileUploadAdd: function (event, data) {

            var chunkSize = $(event.target).fileupload('option', 'maxChunkSize');

            while (data.files[0].size / chunkSize > 10000) chunkSize *= 2;

            $(event.target).fileupload('option', 'maxChunkSize', chunkSize)

            this.el.off('multipartstarted.cloudstorage');
            this.el.on('multipartstarted.cloudstorage', function () {
                data.submit();
            });
        },

        _onFileUploadProgress: function (event, data) {
            var progress = 100 / (data.total / data.loaded);
            this._setProgress(progress, this._bar);
        },

        _onSaveClick: function(event, pass) {
            if (pass || !window.FileList || !this._file || !this._file.val()) {
                return;
            }
            event.preventDefault();
            var file = this._file[0].files[0];

            try{
                this._onDisableSave(true);
                this._onSaveForm(file);
            } catch(error){
                console.log(error);
                this._onDisableSave(false);
            }

            // this._form.trigger('submit', true);
        },

        _onSaveForm: function(file) {
            var self = this;
            var formData = this._form.serializeArray().reduce(
                function (result, item) {
                    result[item.name] = item.value;
                    return result;
            }, {});

            formData['multipart_name'] = file.name;
            formData['url'] = file.name;
            formData['package_id'] = this.options.packageId;
            var action = formData.id ? 'resource_update' : 'resource_create';
            var url = this._form.attr('action') || window.location.href;
            this.sandbox.client.call(
                'POST',
                action,
                formData,
                function (data) {
                    var result = data.result;
                    self._packageId = result.package_id;
                    self._resourceId = result.id;

                    self._id.val(result.id)
                    self.sandbox.notify(
                        result.id,
                        self.i18n(action, {id: result.id}),
                        'success'
                    )
                    self._onPerformUpload(file);
                },
                function (err, st, msg) {
                    self.sandbox.notify(
                        'Error',
                        msg,
                        'error'
                    )
                    self._onHandleError('Unable to save resource');
                }
            );

        },


        _onPerformUpload: function(file) {
            var id = this._id.val();
            var self = this;
            this._onPrepareUpload(file, id).then(
                function (data) {
                    self._uploadId = data.result.id;
                    self.el.trigger('multipartstarted.cloudstorage');
                },
                function (err) {
                    console.log(err);
                    self._onHandleError('Unable to initiate multipart upload');
                }
            );

        },

        _onPrepareUpload: function(file, id) {
            return $.ajax({
                method: 'POST',
                url: this.sandbox.client.url('/api/action/cloudstorage_initiate_multipart'),
                data: JSON.stringify({
                    id: id,
                    name: file.name,
                    size: file.size
                })
            });

        },

        _onAbortUpload: function(id) {
            var self = this;
            this.sandbox.client.call(
                'POST',
                'cloudstorage_abort_multipart',
                {
                    id: id
                },
                function (data) {
                    console.log(data);
                },
                function (err) {
                    console.log(err);
                    self._onHandleError('Unable to abort multipart upload');
                }
            );

        },

        _onFinishUpload: function() {
            var self = this;
            this.sandbox.client.call(
                'POST',
                'cloudstorage_finish_multipart',
                {
                    'id': this._uploadId
                },
                function (data) {
                    self.sandbox.notify(
                        'Success',
                        self.i18n('upload_completed'),
                        'success'
                    )
                    self._progress.hide('fast')
                    self._onDisableSave(false);
                    if (self._resourceId && self._packageId){
                        // self._form.remove();
                        var redirect_url = self.sandbox.url(
                            '/dataset/' +
                            self._packageId +
                            '/resource/' +
                            self._resourceId);
                        self._form.attr('action', redirect_url);

                        self._form.submit();
                    }
                },
                function (err) {
                    console.log(err);
                    self._onHandleError('Unable to finish multipart upload');
                }
            );
            this._setProgressType('success', this._progress);
        },

        _onDisableSave: function (value) {
            this._save.attr('disabled', value);
        },

        _setProgress: function (progress, bar) {
            bar.css('width', progress + '%');
        },

        _setProgressType: function (type, progress) {
            progress
                .removeClass('progress-success progress-danger progress-info')
                .addClass('progress-' + type);
        },

        _onHandleError: function (msg) {
            console.log(msg);
            this._onDisableSave(false);
        }

    }
})


