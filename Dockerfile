FROM ckan:2.7.2

# install cloudstorage plugin
WORKDIR $CKAN_VENV/src
RUN echo $CKAN_VENV/src
RUN git clone https://github.com/TkTech/ckanext-cloudstorage 
WORKDIR $CKAN_VENV/src/ckanext-cloudstorage
RUN $CKAN_VENV/bin/python setup.py develop

