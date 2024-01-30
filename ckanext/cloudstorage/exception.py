class GCPGroupDeletionError(Exception):
    """Exception raised for errors in the GCP group deletion process.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error occurred during GCP group deletion"):
        self.message = message
        super(GCPGroupDeletionError, self).__init__(self.message)


class GCPGroupCreationError(Exception):
    """Exception raised for errors in the GCP group deletion process.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error occurred during GCP group creation"):
        self.message = message
        super(GCPGroupCreationError, self).__init__(self.message)
        

class GCPGroupMemberAdditionError(Exception):
    """
    Exception raised for errors in adding a member to a GCP group.

    Attributes:
        message (str): Explanation of the error
        member_email (str): Email of the member being added
        group_name (str): Name of the GCP group
    """

    def __init__(self, member_email, group_name, message=None):
        self.member_email = member_email
        self.group_name = group_name
        if message is None:
            message = "Failed to add member {} to GCP group {}.".format(member_email, group_name)
        super(GCPGroupMemberAdditionError, self).__init__(message)


class GCPGroupMemberRemovalError(Exception):
    """
    Exception raised for errors in removing a member from a GCP group.

    Attributes:
        member_email (str): Email of the member being removed.
        group_name (str): Name of the GCP group.
        message (str): Explanation of the error.
    """

    def __init__(self, member_email, group_name, message=None):
        self.member_email = member_email
        self.group_name = group_name
        if message is None:
            message = "Failed to remove member {} from GCP group {}.".format(member_email, group_name)
        super(GCPGroupMemberRemovalError, self).__init__(message)


class GetMemberGroupCommandError(Exception):
    """Exception raised for errors retrieving member info from group.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error occurred retrieving member info from group"):
        self.message = message
        super(GetMemberGroupCommandError, self).__init__(self.message)


class GetGroupCommandError(Exception):
    """Exception raised for errors retrieving group info.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error occurred retrieving group info"):
        self.message = message
        super(GetGroupCommandError, self).__init__(self.message)



class GCPGroupMemberUpdateError(Exception):
    """
    Exception raised for errors in updating a member's information in a GCP group.

    Attributes:
        member_email (str): Email of the member being updated.
        group_name (str): Name of the GCP group.
        message (str): Explanation of the error.
    """

    def __init__(self, member_email, group_name, message=None):
        self.member_email = member_email
        self.group_name = group_name
        if message is None:
            message = "Failed to update member {} in GCP group {}.".format(member_email, group_name)
        super(GCPGroupMemberUpdateError, self).__init__(message)
