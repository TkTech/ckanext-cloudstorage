import logging
import argparse

from ckanext.cloudstorage.etl.gcp_utils import create_id_token_and_auth_session
from ckanext.cloudstorage.etl.org_group_manager import OrganizationGroupManager
from ckanext.cloudstorage.etl.ckan_manager import CKANManager
from ckanext.cloudstorage.etl.bucket_utils import upload_to_gcp
from constants import CKAN_BASE_URL, SERVICE_ACCOUNT_KEY_PATH, GCP_BASE_URL

# Logging Configuration
log = logging.getLogger('ETL')
log.setLevel(logging.DEBUG)
# Check if handlers are already configured
if not log.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)
# Prevent log messages from being propagated to the root log
log.propagate = False

def run(organization, ckan_api_key):
    ckan_manager = CKANManager(CKAN_BASE_URL, ckan_api_key)
    org_members = ckan_manager.get_all_organization_members()
    orgs_with_desc = ckan_manager.get_organizations_with_descriptions()
    active_users = ckan_manager.get_active_users()

    service_account_key_path = SERVICE_ACCOUNT_KEY_PATH
    auth_session = create_id_token_and_auth_session(service_account_key_path)

    manager = OrganizationGroupManager(auth_session, GCP_BASE_URL)
    log.info("Process each organization to create groups and add members")
    log.info("="*100)
    responses = manager.process_organizations(orgs_with_desc, org_members, active_users)

    log.info("Retrieve all organizations, their packages, and resources.")
    log.info("="*100)
    data = ckan_manager.get_data_for_single_org(organization)
    if data:
        ckan_manager.process_resources(data, upload_to_gcp)
    else:
        log.error("Failed to retrieve data.")


def main():
    parser = argparse.ArgumentParser(description='Run ETL process for a specific organization.')
    parser.add_argument('organization', help='Name of the organization')
    parser.add_argument('ckan_api_key', help='ckan api key value')
    args = parser.parse_args()

    run(args.organization, args.ckan_api_key)

if __name__ == '__main__':
    main()
