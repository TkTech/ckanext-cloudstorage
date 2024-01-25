"""
constants.py

This module is responsible for reading configuration values from the CKAN
production.ini file. It retrieves specific settings related to the CKAN instance,
such as the base URL, API key, service account path, and GCP FAO base URL.
"""

import logging
import configparser

# Configure Logging
log = logging.getLogger('Config')

# Initialize ConfigParser and read production.ini
config = configparser.ConfigParser()
config.read('/etc/ckan/production.ini')

class ConfigNotFound(Exception):
    """Exception raised for missing configuration.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super(ConfigNotFound, self).__init__(self.message)


def get_config_value(section, option, error_msg):
    """
    Retrieve a configuration value from the production.ini file.
    
    Args:
        section (str): The section in the ini file where the setting is located.
        option (str): The option name of the setting to retrieve.
        error_msg (str): The error message to log if the setting is not found.
    
    Returns:
        str: The value of the configuration setting, or None if not found.
    """
    try:
        return config.get(section, option)
    except configparser.NoOptionError:
        log.error(error_msg)
        raise ConfigNotFound(message=error_msg)


log.info("="*100)
# Access the constants with centralized function
CKAN_BASE_URL = get_config_value('app:main', 'ckan.site_url', 
                                 "CKAN site URL not defined in production.ini")
log.info("CKAN_BASE_URL = {}".format(CKAN_BASE_URL))

CKAN_API_KEY = get_config_value('app:main', 'ckanext.cloudstorage.ckan_api_key', 
                           "CKAN API key not defined in production.ini")
log.info("CKAN_API_KEY = {}".format(CKAN_API_KEY))

SERVICE_ACCOUNT_KEY_PATH = get_config_value('app:main', 'ckanext.cloudstorage.service_account_key_path', 
                                            "CKAN cloudstorage service account path not defined in production.ini")
log.info("SERVICE_ACCOUNT_KEY_PATH = {}".format(SERVICE_ACCOUNT_KEY_PATH))

GCP_BASE_URL = get_config_value('app:main', 'ckanext.cloudstorage.gcp_base_url', 
                                    "CKAN cloudstorage GCP base URL not defined in production.ini")

STORAGE_DIR = get_config_value('app:main', 'ckan.storage_path', 
                                    "CKAN storage path not defined in production.ini")
log.info("GCP_BASE_URL = {}".format(GCP_BASE_URL))
log.info("="*100)
