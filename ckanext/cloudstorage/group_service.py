from time import sleep
import logging
from abc import ABCMeta, abstractmethod

from ckanext.cloudstorage.bucket import create_bucket
from ckanext.cloudstorage.bucket import add_group_iam_permissions

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

            bucket_create = create_bucket(bucket_name=self.payload["name"], cloud_storage=self.cloud_storage)
            # Wait 20s (required for Group OWNER registration)
            sleep(20)

            if bucket_create:
                permission = add_group_iam_permissions(
                    bucket_name=self.payload["name"],
                    group_email=self.payload["email"]
                )
                if permission:
                    return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            log.error("Error in group creation or bucket setup: {}".format(e))
            return False

class GetGroupsCommand(Command):
    """
    Concrete command to retrieve groups using a GET request.
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
            log.error("Unexpected Error retrieving groups list: {}".format(e))
            return {"success": False, "response": None}


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
            log.error("Unexpected error when adding member to a group: {}".format(e))
            return {"success": False, "response": response.json()}


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
            log.error("Unexpected error when retrieving a member from a group: {}".format(e))
            return {"success": False}


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
            return False


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
            return False


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
            return False


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
