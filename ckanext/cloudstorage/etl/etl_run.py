import logging
import argparse
from constants import ConfigurationManager
import os

from ckanext.cloudstorage.authorization import create_id_token_and_auth_session
from ckanext.cloudstorage.etl.org_group_manager import OrganizationGroupManager
from ckanext.cloudstorage.etl.ckan_manager import CKANManager
from ckanext.cloudstorage.bucket import upload_to_gcp_bucket
    

# Logging Configuration
log = logging.getLogger('ETL')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)
log.propagate = False

def run():
    parser = argparse.ArgumentParser(description='Run ETL process for a specific organization.')
    parser.add_argument('organization', help='Name of the organization')
    parser.add_argument('ckan_api_key', help='ckan api key value')
    parser.add_argument('config_file', help='Configuration ini file')
    args = parser.parse_args()

    # Load configuration
    ConfigurationManager.load_config(args.config_file)

    log.info("="*100)
    CKAN_ROOT_PATH = ConfigurationManager.get_config_value('app:main', 'ckan.root_path', 
                                     "CKAN ROOT PATH not defined in production.ini")
    log.info("CKAN_ROOT_PATH = {}".format(CKAN_ROOT_PATH))
    
    CKAN_BASE_URL = ConfigurationManager.get_config_value('app:main', 'ckan.site_url', 
                                     "CKAN site URL not defined in production.ini") + CKAN_ROOT_PATH
    log.info("CKAN_BASE_URL = {}".format(CKAN_BASE_URL))
    
    SERVICE_ACCOUNT_KEY_PATH = ConfigurationManager.get_config_value('app:main', 'ckanext.cloudstorage.service_account_key_path', 
                                                "CKAN cloudstorage service account path not defined in production.ini")
    log.info("SERVICE_ACCOUNT_KEY_PATH = {}".format(SERVICE_ACCOUNT_KEY_PATH))
    
    GCP_BASE_URL = ConfigurationManager.get_config_value('app:main', 'ckanext.cloudstorage.gcp_base_url', 
                                        "CKAN cloudstorage GCP base URL not defined in production.ini")
    log.info("GCP_BASE_URL = {}".format(GCP_BASE_URL))
    
    STORAGE_DIR = ConfigurationManager.get_config_value('app:main', 'ckan.storage_path', 
                                    "CKAN storage path not defined in production.ini")
    log.info("STORAGE_DIR = {}".format(STORAGE_DIR))

    PREFIX = ConfigurationManager.get_config_value('app:main', 'ckanext.cloudstorage.prefix', 
                                    "CKAN cloudstorage prefix not defined in production.ini")
    log.info("PREFIX = {}".format(PREFIX))

    DOMAIN = ConfigurationManager.get_config_value('app:main', 'ckanext.cloudstorage.domain', 
                                    "CKAN cloudstorage domain not defined in production.ini")
    log.info("DOMAIN = {}".format(DOMAIN))
    log.info("="*100)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_PATH

    ckan_manager = CKANManager(CKAN_BASE_URL, STORAGE_DIR, args.ckan_api_key, PREFIX)
    
    org_members = ckan_manager.get_members_for_single_org(args.organization)
    orgs_with_desc = ckan_manager.get_organization_description(args.organization)
    active_users = ckan_manager.get_active_users()
    log.info("="*100)
    log.info("org_members: {}".format(org_members))
    log.info("orgs_with_desc: {}".format(orgs_with_desc))
    log.info("="*100)

    service_account_key_path = SERVICE_ACCOUNT_KEY_PATH
    auth_session = create_id_token_and_auth_session(service_account_key_path)

    manager = OrganizationGroupManager(auth_session, GCP_BASE_URL, DOMAIN, PREFIX)
    log.info("Process {} to create groups and add members".format(args.organization))
    log.info("="*100)
    if orgs_with_desc and org_members and active_users:
        manager.process_organizations(orgs_with_desc, org_members, active_users)
        log.info("Sucessfully process organization {}.".format(args.organization))
        log.info("="*100)
    else:
        log.warning("Not all data available for organization {}. Skipping...".format(args.organization))
        exit(-1)

    log.info("Retrieve packages, and resources for {}".format(args.organization))
    log.info("="*100)
    data = ckan_manager.get_data_for_single_org(args.organization)
    if data:
        ckan_manager.process_resources(data, upload_to_gcp_bucket)
    else:
        log.error("Failed to retrieve data.")


if __name__ == '__main__':
    run()
