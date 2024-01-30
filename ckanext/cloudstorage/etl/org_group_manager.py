import logging

from ckanext.cloudstorage.group_service import CreateGroupsCommand
from ckanext.cloudstorage.group_service import GetGroupsCommand
from ckanext.cloudstorage.group_service import AddMemberGroupCommand

# Configure Logging
log = logging.getLogger('OrganizationGroupManager')


class ProcessOrganizationError(Exception):
    pass


class AddingMembersError(Exception):
    pass


class OrganizationGroupManager:
    """
    Manages group creation and member addition for each CKAN organization.
    """

    def __init__(self, auth_session, base_url, domain, prefix=""):
        """
        Initialize the manager with an authenticated session and base URL.
        
        Args:
            auth_session: Authenticated session for making API calls.
            base_url: Base URL for the group service API.
        """
        self.auth_session = auth_session
        self.base_url = base_url
        self.domain = domain
        self.prefix = prefix

    def process_organizations(self, orgs_with_desc, org_members, active_users):
        """
        Process each organization to create groups and add members.

        Args:
            orgs_with_desc (dict): A list of organizations to process.
        """
        responses = []
        try:
            for organization, description in orgs_with_desc.items():
                log.info("organization : {}".format(organization))
                group_name = self.prefix + organization
                group_email = self.prefix + organization + "@" + self.domain
                log.info("group_email : {}".format(group_email))
                payload = {
                    "name": group_name,
                    "email": group_email,
                    "description": description
                }
                get_group_response = self.get_group(group_email)
                if get_group_response["success"] == True:
                    if get_group_response[u"response"][u"status"] == 200:
                        log.warning("Group <{}> already exists.".format(group_name))
                    elif get_group_response[u"response"][u"status"] == 404:
                        log.info("Group <{}>  does not exist yet.".format(group_name))
                        self.create_group(payload)
                    else:
                        log.error("Group <{}> has not been created.".format(group_name))

                members_response = self.add_members(organization, group_email, org_members, active_users)
                responses.append(members_response)
                return {"success": True, "response": responses}
        except Exception as e:
            log.error("Error processing organization {0}: {1}".format(organization, e))
            raise ProcessOrganizationError("Error processing organization {0}: {1}".format(organization, e))
        

    def create_group(self, payload):
        """
        Create a group for the organization.

        Args:
            organization (dict): Organization data.
        """
        create_group_url = self.base_url + '/groups'
        create_group_command = CreateGroupsCommand(self.auth_session, create_group_url, payload)
        return create_group_command.execute()

    def get_group(self, group_email):
        """
        Retrieve a group for the organization.

        Args:
            organization (dict): Organization data.
        """
        get_group_url = self.base_url + "/groups/{}/members".format(group_email)
        get_group_command = GetGroupsCommand(self.auth_session, get_group_url)
        return get_group_command.execute()

    def add_members(self, organization, group_email, org_members, active_users):
        """
        Add members to the organization's group.

        Args:
            organization (dict): Organization data.
        """
        add_member_url = self.base_url + '/groups/{}/members'.format(group_email)
        member_responses = []
        try:
            for org, users_roles in org_members.items():
                if org == organization:
                    for username, role in users_roles.items():
                        member_email = active_users.get(username, '')
                        log.info("Adding {} to group {}.".format(member_email, group_email))
                        # Mapping of internal roles to GCP group roles
                        map_role = {"admin": "MANAGER", "editor": "MEMBER", "member": "MEMBER", "sysadmin": "OWNER"}
                        payload = {"email": member_email, "role": map_role[role], "deliverySettings": "NONE"}
                        add_member_command = AddMemberGroupCommand(self.auth_session, add_member_url, payload)
                        member_response = add_member_command.execute()
                        member_responses.append(member_response)
                    return member_responses
        except Exception as e:
            raise AddingMembersError("Error adding member to group: {}".format(e))
