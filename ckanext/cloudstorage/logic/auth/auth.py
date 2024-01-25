import ckan.logic as logic
import ckan.authz as authz
import ckan.plugins.toolkit as t
_ = t._
c = t.c
import ckan.model as model
import ckan.logic as logic

ValidationError = logic.ValidationError
NotFound = logic.NotFound


def users_role_for_group_or_org(user_name, org_id):
    ''' Returns the user's role for the group. (Ignores privileges that cascade
    in a group hierarchy.)

    '''

    user_id = authz.get_user_id_for_username(user_name, allow_none=True)
    if not user_id:
        return None
    # get any roles the user has for the group
    q = model.Session.query(model.Member) \
        .filter(model.Member.table_name == 'user') \
        .filter(model.Member.group_id == org_id) \
        .filter(model.Member.state == 'active') \
        .filter(model.Member.table_id == user_id)
    # return the first role we find
    for row in q.all():
        return row.capacity
    return None

def is_sysadmin(username):
    authz.is_sysadmin(username)

def is_admin_of_org(username, org_id):
    ''' Returns True is username is admin of an organization '''
    return users_role_for_group_or_org(username, org_id) == 'admin'

def can_create_gcp_group(user):
    return user.sysadmin

def can_delete_gcp_group(user):
    return user.sysadmin

def can_create_member_from_gcp_group(user, username, org_id):
    return user.sysadmin or is_admin_of_org(username, org_id)

def can_delete_member_from_gcp_group(user, username, org_id):
    return user.sysadmin or is_admin_of_org(username, org_id)
