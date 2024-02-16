from time import sleep
import logging
from abc import ABCMeta, abstractmethod

from ckanext.cloudstorage.bucket import create_bucket
from ckanext.cloudstorage.bucket import add_group_iam_permissions
from ckanext.cloudstorage.exception import GCPGroupCreationError
from ckanext.cloudstorage.exception import GCPGroupMemberAdditionError
from ckanext.cloudstorage.exception import GCPGroupDeletionError
from ckanext.cloudstorage.exception import GetMemberGroupCommandError
from ckanext.cloudstorage.exception import GCPGroupMemberUpdateError
from ckanext.cloudstorage.exception import GCPGroupMemberRemovalError
from ckanext.cloudstorage.exception import GetGroupMembersCommandError

log = logging.getLogger(__name__)

class Command:
    """
    Abstract base class for all commands, requiring an execute method.
    """

    __metaclass__ = ABCMeta
    
    @abstractmethod
    def execute(self):
        pass


class CreateGroupsCommand(Command):
    """
    Concrete command to create groups using a POST request.
    """
    def __init__(self, auth_session, url, payload, cloud_storage=None):
        self.auth_session = auth_session
        self.url = url
        self.payload = payload
        self.cloud_storage = cloud_storage

    def execute(self):
        try:
            response = self.auth_session.post(url=self.url, json=self.payload)
            response.raise_for_status()  # Raises an exception for 4XX/5XX responses

            create_bucket(bucket_name=self.payload["name"], cloud_storage=self.cloud_storage)
            # Wait 20s (required for Group OWNER registration)
            sleep(20)

            add_group_iam_permissions(
                bucket_name=self.payload["name"],
                group_email=self.payload["email"]
            )

        except Exception as e:
            if response.json()[u"status"] == 409:
                log.warning("Group  {} already exists".format(self.payload["name"]))
                return
            log.error("Error in group creation or bucket setup: {}".format(e))
            raise GCPGroupCreationError("Error in group creation or bucket setup: {}".format(e))

class GetGroupMembersCommand(Command):
    """
    Concrete command to retrieve group members using a GET request.
    """
    def __init__(self, auth_session, url):
        self.auth_session = auth_session
        self.url = url

    def execute(self):
        try:
            response = self.auth_session.get(url=self.url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return {"success": True, "response": response.json()}
        except Exception as e:
            if response.json()[u"status"] == 404:
                log.warning("Group members not found: {}".format(e))
                return {"success": True, "response": response.json()}
            log.error("Unexpected Error retrieving group memebers: {}".format(e))
            raise GetGroupMembersCommandError("Unexpected Error retrieving group: {}".format(e))


class AddMemberGroupCommand(Command):
    """
    Concrete command to add a member to a group using a POST request.
    """
    def __init__(self, auth_session, url, payload):
        self.auth_session = auth_session
        self.url = url
        self.payload = payload

    def execute(self):
        try:
            response = self.auth_session.post(url=self.url, json=self.payload)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return {"success": True, "response": response.json()}
        except Exception as e:
            if response.json()[u"status"] == 409:
                log.warning("member {} already exists".format(self.payload["email"]))
                return
            log.error("Unexpected error when adding member to a group: {}".format(e))
            raise GCPGroupMemberAdditionError("Unexpected error when adding member to a group: {}".format(e))


class GetMemberGroupCommand(Command):
    """
    Concrete command to retrieve a member from a group using a GET request.
    """
    def __init__(self, auth_session, url):
        self.auth_session = auth_session
        self.url = url

    def execute(self):
        try:
            response = self.auth_session.get(url=self.url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return {"success": True, "response": response.json()}
        except Exception as e:
            if response.json()[u"status"] == 404:
                log.warning("Group member not found: {}".format(e))
                return {"success": True, "response": response.json()}
            log.error("Unexpected error when retrieving a member from a group: {}".format(e))
            raise GetMemberGroupCommandError("Unexpected error when retrieving a member from a group: {}".format(e))


class UpdateMemberGroupCommand(Command):
    """
    Concrete command to update a member that belongs to group using a PUT request.
    """
    def __init__(self, auth_session, url, payload):
        self.auth_session = auth_session
        self.url = url
        self.payload = payload

    def execute(self):
        try:
            response = self.auth_session.put(url=self.url, json=self.payload)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return True
        except Exception as e:
            log.error("Unexpected error when updating member info to group: {}".format(e))
            raise GCPGroupMemberUpdateError("Unexpected error when updating member info to group: {}".format(e))


class DeleteMemberGroupCommand(Command):
    """
    Concrete command to delete a member from a group using a DELETE request.
    """
    def __init__(self, auth_session, url):
        self.auth_session = auth_session
        self.url = url

    def execute(self):
        try:
            response = self.auth_session.delete(url=self.url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return True
        except Exception as e:
            log.error("Unexpected error when deleting member from a group: {}".format(e))
            raise GCPGroupMemberRemovalError("Unexpected error when deleting member from a group: {}".format(e))


class DeleteGroupsCommand(Command):
    """
    Concrete command to delete a group using a DELETE request.
    """
    def __init__(self, auth_session, url):
        self.auth_session = auth_session
        self.url = url

    def execute(self):
        try:
            response = self.auth_session.delete(url=self.url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return True
        except Exception as e:
            log.error("Unexpected error when deleting group: {}".format(e))
            raise GCPGroupDeletionError("Unexpected error when deleting group: {}".format(e))


class APIInvoker:
    """
    Invoker class to execute a series of commands.
    """
    def __init__(self):
        self.commands = []

    def add_command(self, command):
        self.commands.append(command)

    def run(self):
        responses = []
        for command in self.commands:
            responses.append(command.execute())
        return responses
