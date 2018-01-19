FROM thedataplace/ckan:2.7.2

USER root

WORKDIR $CKAN_VENV/src
RUN ckan-pip install -r ckan/dev-requirements.txt

# install cloudstorage plugin
RUN git clone https://github.com/TkTech/ckanext-cloudstorage 
RUN git clone https://github.com/okfn/ckanext-s3filestore
WORKDIR $CKAN_VENV/src/ckanext-cloudstorage
RUN sh $CKAN_VENV/bin/activate && $CKAN_VENV/bin/python setup.py develop

# install dev dependencies
RUN ckan-pip install --upgrade \
    enum34 \
    boto==2.38.0 \
    moto==0.4.4 \
    ckanapi==3.5

COPY test_entrypoint.sh  $CKAN_VENV/src/ckanext-cloudstorage/test_entrypoint.sh
RUN cp -v $CKAN_VENV/src/ckanext-cloudstorage/test_entrypoint.sh /test_entrypoint.sh && \
    chmod +x /test_entrypoint.sh

ENTRYPOINT ["/test_entrypoint.sh"]

USER ckan

CMD ["ckan-paster", "serve", "/etc/ckan/ckan.ini"]
