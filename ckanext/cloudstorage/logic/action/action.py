# encoding: utf-8
import logging
import ckan.plugins as plugins
import ckan.logic as logic
from ckanext.cloudstorage.logic.auth.auth import can_create_gcp_group
from ckanext.cloudstorage.logic.auth.auth import can_delete_gcp_group
from ckanext.cloudstorage.logic.auth.auth import can_delete_member_from_gcp_group
from ckanext.cloudstorage.logic.auth.auth import can_create_member_from_gcp_group
from ckanext.cloudstorage.storage import CloudStorage
from ckanext.cloudstorage.fao_group_service import CreateGroupsCommand
from ckanext.cloudstorage.fao_group_service import DeleteGroupsCommand
from ckanext.cloudstorage.fao_group_service import AddMemberGroupCommand
from ckanext.cloudstorage.fao_group_service import UpdateMemberGroupCommand
from ckanext.cloudstorage.fao_group_service import GetMemberGroupCommand
from ckanext.cloudstorage.fao_group_service import DeleteMemberGroupCommand
from ckanext.cloudstorage.exception import GCPGroupCreationError
from ckanext.cloudstorage.exception import GCPGroupDeletionError
from ckanext.cloudstorage.exception import GCPGroupMemberAdditionError
from ckanext.cloudstorage.exception import GCPGroupMemberUpdateError
from ckanext.cloudstorage.exception import GCPGroupMemberRemovalError
from ckanext.cloudstorage.authorization import create_id_token_and_auth_session
from ckan.common import _, request


log = logging.getLogger(__name__)


# Define some shortcuts
# Ensure they are module-private so that they don't get loaded as available
# actions in the action API.
_check_access = logic.check_access
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
ValidationError = logic.ValidationError

cloud_storage = CloudStorage()
service_account_key_path = cloud_storage.driver_options["secret"]
group_email = cloud_storage.driver_options["key"]

@plugins.toolkit.chained_action
def organization_create(next_auth, context, data_dict):
    """
    Extends CKAN organization creation with optional GCP group workspace creation.

    This function facilitates the creation of a Google Cloud Platform (GCP) group
    workspace alongside a CKAN organization. It checks for the 'gcp_group_create' flag
    in 'data_dict'. If true and the user is authorized, a GCP group workspace is
    created using the organization's name and description.

    The function validates mandatory fields 'name' and 'description', checks user
    authorization, and uses service account credentials for GCP group workspace creation.
    It handles GCP API responses and errors, adjusting 'gcp_group_create' flag and
    raising exceptions as needed.

    Args:
        next_auth (function): The next authorization function in the chain.
        context (dict): Context including environmental settings and user info.
        data_dict (dict): Data for organization creation, including 'name', 'description',
                          and 'gcp_group_create'.

    Returns:
        dict: Modified 'data_dict' after processing, passed to the next function in chain.
    """

    log.info("Starting organization creation process with GCP group workspace option.")

    url = "https://gcp.fao.org/groups-service/api/v1/groups"
    
    # Validate mandatory fields: 'name' and 'description'
    name = str(data_dict.get('name')).encode('ascii', 'ignore')
    if name is None:
        log.error("Missing 'name' in data_dict")
        raise ValidationError({'name': _('Missing value')})
    
    description = str(data_dict.get('description')).encode('ascii', 'ignore')
    if description is None:
        log.error("Missing 'description' in data_dict")
        raise ValidationError({'description': _('Missing value')})

    log.info("Validated input data: name=%s, description=%s", name, description)

    # Authorization check for GCP group workspace creation
    if not can_create_gcp_group(context['auth_user_obj']):
        log.warning("User %s unauthorized to create GCP group workspace", context['auth_user_obj'])
        raise NotAuthorized(_('User not authorized to create an organization or a gcp group workspace.'))
    
    username = str(context['user']).encode('ascii', 'ignore')
    model = context['model']
    user = model.User.get(username)
    member_email = str(user.email).encode('ascii', 'ignore')
    role = "sysadmin"
    prefix = "fao-catalog-"
    group_email = prefix + name + "@fao.org"
    group_name = prefix + name

    log.info("Preparing to create GCP group workspace: %s", group_email)

    # Create GCP group workspace if authorized
    try:
        auth_session = create_id_token_and_auth_session(service_account_key_path)
        payload = {
            "name": group_name,
            "email": group_email,
            "description": description
        }
        create_group = CreateGroupsCommand(auth_session, url, payload)
        response = create_group.execute()
        log.info("GCP group %s created successfully.", group_email)
    except Exception as e:
        log.error("Error during GCP group creation: %s", e)
        raise GCPGroupCreationError(message="Error during GCP group creation: {}".format(e))
    
    # Add sysadmin user as owner of gcp group
    try:
        map_role = {"sysadmin": "OWNER"}
        url = "https://gcp.fao.org/groups-service/api/v1/groups/{}/members".format(group_email)
        payload = {"email": member_email, "role": map_role[role], "deliverySettings": "NONE"}
        add_member_to_gcp = AddMemberGroupCommand(auth_session, url, payload)
        response = add_member_to_gcp.execute()
        log.info("Added member %s to GCP group %s as %s.", member_email, group_email, map_role[role])

    except Exception as e:
        log.error("Error adding member %s to GCP group %s: %s", member_email, group_email, e)
        raise GCPGroupMemberAdditionError(member_email, group_name)

    log.info("Organization creation with GCP group workspace completed successfully.")
    
    # Proceed to the next action in the chain
    return next_auth(context, data_dict)

@plugins.toolkit.chained_action
def organization_update(next_auth, context, data_dict):
    """
    Updates an organization's details after performing authorization checks.

    This method is a chained action for updating organization details. It first
    checks if the organization exists and if the user has the authorization to
    edit it. If the organization does not exist or the user is not authorized,
    it raises a ValidationError. Otherwise, it passes the call to the next
    authorization function in the chain.

    :param next_auth: The next authorization function in the chain.
    :param context: The context dictionary containing relevant information.
    :param data_dict: The data dictionary containing organization details.
    :return: The result of the next authorization function.
    :raises ValidationError: If the organization does not exist or the user
                             is not authorized to edit it.
    """
    model = context['model']
    name = data_dict.get('name')
    title = data_dict.get('title')

    org = model.Group.get(name)
    if org is None:
        import ckan.lib.helpers as h
        h.flash_error("You are not authorized to edit or update organization name")
        raise ValidationError("You are not authorized to edit or update organization name")

    return next_auth(context,data_dict)

@plugins.toolkit.chained_action            
def organization_delete(next_auth, context, data_dict):
    """
    Deletes an organization and its corresponding GCP group workspace.

    This function handles the deletion of a CKAN organization and its associated Google
    Cloud Platform (GCP) group workspace. It checks if the user is authorized and
    proceeds with the deletion process.

    Args:
        next_auth (function): Next authorization function in the chain.
        context (dict): Context dictionary with environment and user information.
        data_dict (dict): Data dictionary containing organization ID.

    Raises:
        ValidationError: If the organization ID is missing.
        NotAuthorized: If user lacks permission to delete the organization or GCP group.
        GCPGroupDeletionError: For errors during GCP group deletion.

    Returns:
        dict: Result from the next authorization function in the chain.
    """
    log.info("Starting organization deletion process with corresponding GCP group workspace.")

    # Extract the organization ID from the provided data
    group_id = str(data_dict.get('id')).encode('ascii', 'ignore')
    if group_id is None:
        log.error("Missing 'id' in data_dict")
        raise ValidationError({'id': _('Missing value')})

    # Retrieve the organization object using the model from the context
    model = context['model']
    org = model.Group.get(group_id)
    
    # Encode the organization name to ASCII, ignoring non-ASCII characters
    group_name = str(org.name).encode('ascii', 'ignore')

    # Construct the email for the GCP group using the group name
    prefix = "fao-catalog-"
    group_email = prefix + group_name + "@fao.org"
    url = "https://gcp.fao.org/groups-service/api/v1/groups/{}".format(group_email)

    log.info("Prepared to delete GCP group workspace: %s", group_email)

    # Check if the user is authorized to delete the GCP group workspace
    if not can_delete_gcp_group(context['auth_user_obj']):
        log.warning("User %s unauthorized to delete GCP group workspace", context['auth_user_obj'])
        raise NotAuthorized(_('User not authorized to delete an organization or a gcp group workspace.'))
    
    try:
        # Create an authenticated session
        auth_session = create_id_token_and_auth_session(service_account_key_path)    
        # Create a command to delete the group and execute it
        delete_group = DeleteGroupsCommand(auth_session, url)
        response = delete_group.execute()
        log.info("GCP group %s deleted successfully.", group_email)
    except Exception as e:
        # Handle exceptions during GCP group deletion
        log.error("Error when trying to delete GCP group %s: %s", group_email, e)
        raise GCPGroupDeletionError(message="Error when trying to delete GCP group: {}".format(e))
    
    log.info("Organization deletion with corresponding GCP group workspace completed successfully.")

    # Proceed to the next authorization function
    return next_auth(context, data_dict)



@plugins.toolkit.chained_action
def organization_member_create(next_auth, context, data_dict):
    """
    Creates or updates a member in a GCP group workspace based on their role.

    This function handles the creation or update of a member in a Google Cloud Platform (GCP) 
    group workspace. It sets the member's role and adds them to the group if they do not exist.

    Args:
        next_auth: The next authorization function in the chain.
        context: The context dictionary containing environment and user information.
        data_dict: A dictionary containing the member's information.

    Raises:
        NotAuthorized: If the user is not authorized to create/update a member in GCP group.
        GCPGroupMemberUpdateError: If there is an error updating the member in GCP group.
        GCPGroupMemberAdditionError: If there is an error adding the member to GCP group.

    Returns:
        The result of the next authorization function.
    """

    log.info("Starting process to create or update a member in a GCP group workspace.")

    # Extracting member information from the provided data
    role = data_dict.get("role")
    if role is None:
        log.error("Missing 'role' in data_dict")
        raise ValidationError({'role': _('Missing value')})
    role = str(data_dict.get("role")).encode('ascii', 'ignore')
    
    org_id = data_dict.get("id")
    if org_id is None:
        log.error("Missing 'id' in data_dict")
        raise ValidationError({'id': _('Missing value')})
    org_id = str(data_dict.get("id")).encode('ascii', 'ignore')
    
    username = str(context['user']).encode('ascii', 'ignore')

    log.info("Member data extracted: role=%s, username=%s, org_id=%s", role, username, org_id)

    # Retrieve the user object using the model from the context
    model = context['model']
    user = model.User.get(username)

    if not user:
        log.error("User not found in the model: %s", username)
        raise NotFound(_('User not found.'))

    # Mapping of internal roles to GCP group roles
    map_role = {"admin": "MANAGER", "editor": "MEMBER", "member": "MEMBER", "sysadmin": "OWNER"}

    # Construct the group email using the group name from context
    prefix = "fao-catalog-"
    group_name = str(context["group"].name).encode('ascii', 'ignore')
    group_email = prefix + group_name + "@fao.org"
    member_email = str(user.email).encode('ascii', 'ignore')

    log.info("GCP group email constructed: %s", group_email)

    # Authorization check for creating/updating member in GCP group
    if not can_create_member_from_gcp_group(context['auth_user_obj'], username, org_id):
        log.warning("User %s unauthorized to create or update a member in GCP group workspace", context['auth_user_obj'])
        raise NotAuthorized(_('User not authorized to create or update a member within gcp group workspace.'))

    # Construct the URL for GCP group service and check if the member exists
    url = "https://gcp.fao.org/groups-service/api/v1/groups/{}/members/{}".format(group_email, member_email)
    auth_session = create_id_token_and_auth_session(service_account_key_path)   
    get_member_from_gcp = GetMemberGroupCommand(auth_session, url)

    try:
        response = get_member_from_gcp.execute()
        log.info("Response received for member existence check: %s", response)
    except Exception as e:
        log.error("Error checking for member existence in GCP group: %s", e)
        raise Exception("Error checking for member existence in GCP group: {}".format(e))

    if response[u"status"] == 200:
        # If member exists, update their role
        try:
            payload = {"email": member_email, "role": map_role[role], "deliverySettings": "NONE"}
            update_member = UpdateMemberGroupCommand(auth_session, url, payload)
            response = update_member.execute()
            log.info("Member %s updated in GCP group %s with role %s.", member_email, group_email, map_role[role])
        except Exception as e:
            log.error("Error updating member %s in GCP group %s: %s", member_email, group_email, e)
            raise GCPGroupMemberUpdateError(member_email, group_name)
    else:
        # If member does not exist, add them to the group
        try:
            url = "https://gcp.fao.org/groups-service/api/v1/groups/{}/members".format(group_email)
            payload = {"email": member_email, "role": map_role[role], "deliverySettings": "NONE"}
            add_member_to_gcp = AddMemberGroupCommand(auth_session, url, payload)
            response = add_member_to_gcp.execute()
            log.info("Member %s added to GCP group %s with role %s.", member_email, group_email, map_role[role])
        except Exception as e:
            log.error("Error adding member %s to GCP group %s: %s", member_email, group_email, e)
            raise GCPGroupMemberAdditionError(member_email, group_name)

    log.info("Process to create or update a member in a GCP group workspace completed successfully.")

    return next_auth(context, data_dict)


@plugins.toolkit.chained_action
def organization_member_delete(next_auth, context, data_dict):
    """
    Deletes a member from a GCP group workspace based on organization and user ID.

    This function is responsible for removing a member from a GCP group workspace. It
    utilizes the member's user ID and the organization ID for this operation. Proper 
    authorization is required to execute this function.

    Args:
        next_auth (function): Next authorization function in the chain.
        context (dict): Context containing environment and user information.
        data_dict (dict): Dictionary with the member's user ID and organization ID.

    Raises:
        ValidationError: If the 'id' field is missing in data_dict.
        NotAuthorized: If user lacks permission to delete the member from GCP group.
        GCPGroupMemberRemovalError: For errors during removal of the member from GCP group.

    Returns:
        dict: Result from the next authorization function in the chain.
    """

    log.info("Starting process to delete a member from a GCP group workspace.")

    # Extracting member and organization information
    username = str(context['user']).encode('ascii', 'ignore')

    group_id = data_dict.get("id")
    if group_id is None:
        log.error("Missing 'id' in data_dict")
        raise ValidationError({'id': _('Missing value')})
    group_id = str(group_id).encode('ascii', 'ignore')

    log.info("Extracted member and organization information: username=%s, group_id=%s", username, group_id)

    # Retrieve user and group objects using the model from the context
    model = context['model']
    user = model.User.get(username)
    group = model.Group.get(group_id)
    group_name = str(group.name).encode('ascii', 'ignore')
    prefix = "fao-catalog-"
    group_email = prefix + group_name + "@fao.org"
    member_email = str(user.email).encode('ascii', 'ignore')

    log.info("Prepared member and group emails for deletion: group_email=%s, member_email=%s", group_email, member_email)

    # Authorization check for deleting member from GCP group
    if not can_delete_member_from_gcp_group(context['auth_user_obj'], username, group_id):
        log.warning("User %s unauthorized to delete a member of GCP group workspace", context['auth_user_obj'])
        raise NotAuthorized(_('User not authorized to delete a member of a gcp group workspace.'))

    # Construct the URL for GCP group service and execute the delete command
    try:
        url = "https://gcp.fao.org/groups-service/api/v1/groups/{}/members/{}".format(group_email, member_email)
        auth_session = create_id_token_and_auth_session(service_account_key_path)    
        delete_member_from_gcp = DeleteMemberGroupCommand(auth_session, url)
        response = delete_member_from_gcp.execute()
        log.info("Member %s deleted from GCP group %s successfully.", member_email, group_email)
    except Exception as e:
        log.error("Error when trying to delete member %s from GCP group %s: %s", member_email, group_email, e)
        raise GCPGroupMemberRemovalError(member_email, group_name)
    
    log.info("Member deletion from GCP group workspace completed successfully.")

    return next_auth(context, data_dict)
