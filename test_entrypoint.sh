#!/bin/sh
set -e

abort () {
  echo "$@" >&2
  exit 1
}

echo "test_entrypoint"
echo "$@"

# execute stuff before ckan entrypoint
cd $CKAN_VENV/src/ckan
ckan-paster datastore set-permissions -c test-core.ini | PGPASSWORD=ckan psql -h db

echo "configure solr"
curl 'http://solr:8983/solr/admin/cores?action=CREATE&name=ckan&instanceDir=/etc/solr/ckan'

echo "exiting test_entrypoint.sh"
# hand over control to ckan-entrypoint, including CMD args
sh /ckan-entrypoint.sh "$@"

exec "$@"

