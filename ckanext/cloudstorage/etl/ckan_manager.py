import requests
import logging
import json
import os


# Logging Configuration
log = logging.getLogger('CKAN_Manager')

class CKANManager:
    """
    A class to manage interactions with a CKAN instance.

    Attributes:
        ckan_url (str): URL of the CKAN instance.
        api_key (str): API key for authentication (optional).
    """
    ORGANIZATION_USER_MEMBERS_LIST = '/api/3/action/organization_show?id={}&include_users=true'
    ORGANIZATION_LIST_ENDPOINT = '/api/3/action/organization_list'
    ORGANIZATION_DESC_ENDPOINT = '/api/3/action/organization_list?all_fields=true'
    ORGANIZATION_SHOW_ENDPOINT = '/api/3/action/organization_show?id={}&include_datasets=true'
    PACKAGE_SHOW_ENDPOINT = '/api/3/action/package_show?id={}'
    USER_LIST_ENDPOINT = '/api/3/action/user_list?all_fields=true'

    def __init__(self, ckan_url, ckan_storage_dir, api_key=None):
        """
        Initialize the CKANManager with CKAN instance URL and API key.

        Args:
            ckan_url (str): URL of the CKAN instance.
            api_key (str): API key for authentication (optional).
        """
        self.ckan_url = ckan_url
        self.api_key = api_key
        self.ckan_storage_dir = ckan_storage_dir

    def get_request(self, url):
        """
        Perform a GET request to a specified URL.

        Args:
            url (str): The URL to make the GET request to.

        Returns:
            dict: JSON response data or None in case of an error.
        """
        headers = {'Authorization': self.api_key} if self.api_key else {}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            log.error("Request error: {0}".format(e))
            return None

    def download_file(self, url, save_path):
        """
        Download a file from a given URL and save it to a specified path.

        Args:
            url (str): URL of the file to download.
            save_path (str): Path where the file will be saved.
        """
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192): 
                    file.write(chunk)
            return True
        except requests.RequestException as e:
            log.error("Error downloading file: {}".format(e))
            return False

    def get_active_users(self):
        """
        Retrieves all active users of the CKAN instance with their usernames and emails.

        Returns:
            dict: A dictionary mapping usernames to their emails.
        """
        endpoint = self.ckan_url.rstrip('/') + self.USER_LIST_ENDPOINT
        data = self.get_request(endpoint)

        if data and data.get('success'):
            log.info("Successfully retrieved all users.")
            active_users = {user['name']: user['email'] for user in data['result'] if user['state'] == 'active'}
            return active_users
        else:
            log.error("Failed to retrieve users.")
            return None

    def get_all_organizations(self):
        """
        Retrieve all organizations from the CKAN instance.

        Returns:
            list: List of organizations or None if an error occurs.
        """
        endpoint = self.ckan_url.rstrip('/') + self.ORGANIZATION_LIST_ENDPOINT
        data = self.get_request(endpoint)

        if data and data.get('success'):
            log.info("Successfully retrieved organizations.")
            return data['result']
        else:
            log.error("Failed to retrieve organizations.")
            return None

    def get_organizations_with_descriptions(self):
        """
        Retrieves all organizations with their names and descriptions.

        Returns:
            dict: A dictionary with organization names as keys and their descriptions as values.
        """
        endpoint = self.ckan_url.rstrip('/') + self.ORGANIZATION_DESC_ENDPOINT
        data = self.get_request(endpoint)

        if data and data.get('success'):
            log.info("Successfully retrieved all organizations.")
            return {org['name']: org['description'] for org in data['result']}
        else:
            log.error("Failed to retrieve organizations.")
            return None

    def get_organization_members(self, organization_name):
        """
        Retrieves all user members of a specific CKAN organization with their roles.
        """
        endpoint = self.ckan_url.rstrip('/') + self.ORGANIZATION_USER_MEMBERS_LIST.format(organization_name)
        data = self.get_request(endpoint)

        if data and data.get('success'):
            log.info("Successfully retrieved user members of {}.".format(organization_name))
            users = data['result']['users']
            return {user['name']: user['capacity'] for user in users}
        else:
            log.error("Failed to retrieve organization members for {}.".format(organization_name))
            return None

    def get_all_organization_members(self):
        """
        Retrieves all user members with their roles for each CKAN organization.

        Returns:
            dict: A dictionary with organization names as keys and dicts of user members
                  and their roles as values.
        """
        organizations = self.get_all_organizations()
        all_members = {}

        if not organizations:
            log.error("No organizations found or failed to retrieve organizations.")
            return None

        for org_name in organizations:
            if org_name:
                members = self.get_organization_members(org_name)
                if members:
                    all_members[org_name] = members

        return all_members

    def get_packages_for_organization(self, organization):
        """
        Retrieve all packages for a given organization.

        Args:
            organization (str): Name of the organization.

        Returns:
            list: List of packages or None if an error occurs.
        """
        endpoint = self.ckan_url.rstrip('/') + self.ORGANIZATION_SHOW_ENDPOINT.format(organization)
        data = self.get_request(endpoint)

        if data and data.get('success'):
            log.info("Successfully retrieved packages for {0}.".format(organization))
            return [dataset['name'] for dataset in data['result']['packages']]
        else:
            log.error("Failed to retrieve packages for {0}.".format(organization))
            return None

    def get_resources_for_package(self, package_id):
        """
        Retrieve all resources for a given package.

        Args:
            package_id (str): ID of the package.

        Returns:
            list: List of resources or None if an error occurs.
        """
        endpoint = self.ckan_url.rstrip('/') + self.PACKAGE_SHOW_ENDPOINT.format(package_id)
        data = self.get_request(endpoint)

        if data and data.get('success'):
            log.info("Successfully retrieved resources for package {0}.".format(package_id))
            return data['result']['resources']
        else:
            log.error("Failed to retrieve resources for package {0}.".format(package_id))
            return None

    def get_all_resources_for_packages(self, packages):
        """
        Retrieve all resources for a list of packages.

        Args:
            packages (list): List of package IDs.

        Returns:
            dict: Dictionary mapping package IDs to their resources.
        """
        all_resources = {}
        for package in packages:
            resources = self.get_resources_for_package(package)
            all_resources[package] = resources or []
        return all_resources

    def get_all_data(self):
        """
        Retrieve all organizations, their packages, and resources.

        Returns:
            dict: Data containing organizations, packages, and resources.
        """
        organizations_data = {}
        organizations = self.get_all_organizations()

        if not organizations:
            return None

        for organization in organizations:
            packages = self.get_packages_for_organization(organization)
            if packages:
                resources = self.get_all_resources_for_packages(packages)
                organizations_data[organization] = resources

        return organizations_data

    def get_data_for_single_org(self, organization):
        """
        Retrieve all  packages, and resources for single organization

        Returns:
            dict: Data containing organization, packages, and resources.
        """
        organizations_data = {}

        if organization == "":
            return None

        packages = self.get_packages_for_organization(organization)
        if packages:
            resources = self.get_all_resources_for_packages(packages)
            organizations_data[organization] = resources

        return organizations_data

    def save_to_json(self, data, filename):
        """
        Save data to a JSON file.

        Args:
            data (dict): The data to be saved.
            filename (str): Name of the file to save the data in.
        """
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
        log.info("Data saved to {0}.".format(filename))

    def ensure_dir(self, file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def check_resource_directories(self, base_path, resource_id):
        """
        Check if specific subdirectories based on a resource ID exist in the base path.

        :param base_path: The base directory path.
        :param resource_id: The resource ID to create the subdirectory paths.
        """
        if len(resource_id) < 6:
            log.error("Resource ID is too short.")
            return

        first_dir = resource_id[:3]
        second_dir = resource_id[3:6]

        first_path = os.path.join(base_path, first_dir)
        second_path = os.path.join(first_path, second_dir)

        if not os.path.isdir(first_path):
            log.error("Directory does not exist: {}".format(first_path))
        elif not os.path.isdir(second_path):
            log.error("Directory does not exist: {}".format(second_path))
        else:
            log.info("Both directories exist: {}, {}".format(first_path, second_path))
            return True
        return False

    def delete_file(self, file_path):
        """
        Deletes a file from the given file path.

        Args:
            file_path (str): The path of the file to be deleted.
        """
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                log.info("File '{}' successfully deleted.".format(file_path))
            else:
                log.warning("File '{}' not found.".format(file_path))
        except Exception as e:
            log.error("Error occurred while deleting file '{}': {}".format(file_path, e))

    def process_resources(self, data, upload_to_gcp):
        """
        Processes resources in the provided data, downloads files, uploads to GCP, and deletes local copies.

        Args:
            data (dict): A dictionary containing organization, package, and resource details.
            upload_to_gcp (function): Function to upload file to GCP.
        """
        for organization, packages in data.items():
            for package_id, resources in packages.items():
                for resource in resources:
                    if resource:
                        resource_id = resource.get("id", "")
                        url = resource.get("url", "")
                        if url:
                            file_name = url.split('/')[-1]
                            # Get the current working directory
                            current_directory = os.getcwd()

                            # Get the parent directory
                            parent_directory = os.path.dirname(current_directory)
                            download_dir = os.path.join(parent_directory, "")
                            log.info("download_dir: {}".format(download_dir))
                            self.ensure_dir(download_dir)

                            file_path = os.path.join(download_dir, file_name)
                            log.info("file_path: {}".format(file_path))
                        
                            if url.startswith(self.ckan_url):
                                base_resource_dir= "{}/resources/".format(self.ckan_storage_dir)
                                prefix = "fao-catalog-"
                                bucket_name = prefix + organization
                                destination_blob_name = os.path.join(
                                    'packages',
                                    package_id,
                                    'resources',
                                    resource_id,
                                    file_name
                                )
                                if self.check_resource_directories(base_resource_dir, resource_id) is False:
                                    success = self.download_file(url, file_path)
                                    if success:
                                        log.info("Downloaded {} to {}".format(file_name, file_path))
                                        # upload file to bucket
                                        upload_to_gcp(bucket_name, destination_blob_name, file_path)
                                        self.delete_file(file_path)
                                    else:
                                        log.error("Failed to download {}".format(url))
                                else:
                                    # build full path resource on file system
                                    first_dir = resource_id[:3]
                                    second_dir = resource_id[3:6]
                                    resource = resource_id[6:]
                                    first_path = os.path.join(base_resource_dir, first_dir)
                                    second_path = os.path.join(first_path, second_dir)
                                    full_resource_path = os.path.join(second_path, resource)
                                    log.info("the full resource path on file system: {}".format(full_resource_path))
                                    # upload file to bucket
                                    upload_to_gcp(bucket_name, destination_blob_name, full_resource_path)

                            else:
                                log.warning('Skipping external URL: {}'.format(url))
