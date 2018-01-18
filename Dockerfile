FROM thedataplace/ckan:2.7.2

USER root

WORKDIR $CKAN_VENV/src
RUN ckan-pip install -r ckan/dev-requirements.txt && \
    ckan-pip install ckanapi boto moto

# install cloudstorage plugin
RUN git clone https://github.com/TkTech/ckanext-cloudstorage 
RUN git clone https://github.com/okfn/ckanext-s3filestore
WORKDIR $CKAN_VENV/src/ckanext-cloudstorage
RUN sh $CKAN_VENV/bin/activate && $CKAN_VENV/bin/python setup.py develop

WORKDIR $CKAN_VENV/src/ckanext-s3filestore
RUN ckan-pip install -r requirements.txt -r dev-requirements.txt

COPY test_entrypoint.sh  $CKAN_VENV/src/ckanext-cloudstorage/test_entrypoint.sh
RUN cp -v $CKAN_VENV/src/ckanext-cloudstorage/test_entrypoint.sh /test_entrypoint.sh && \
    chmod +x /test_entrypoint.sh

ENTRYPOINT ["/test_entrypoint.sh"]

USER ckan

CMD ["ckan-paster", "serve", "/etc/ckan/ckan.ini"]
