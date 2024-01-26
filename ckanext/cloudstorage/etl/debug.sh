# run this script inside etl folder for debugging purpose
# Don't forget to add these 3 arguments: organization, apikey , and configuration file
# test_ckan_1 32597cc5-8918-496c-a3e9-604de0d4fc0e /etc/ckan/production.ini
python -m debugpy --log-to-stderr --wait-for-client --listen 0.0.0.0:5675 etl_run.py
