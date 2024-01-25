from time import sleep
from abc import ABCMeta, abstractmethod

from ckanext.cloudstorage.bucket import create_bucket
from ckanext.cloudstorage.bucket import add_group_iam_permissions

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
    def __init__(self, auth_session, url, payload):
        self.auth_session = auth_session
        self.url = url
        self.payload = payload

    def execute(self):
        response = self.auth_session.post(url=self.url, json=self.payload)
        # TODO: maybe create a bucket in meantime
        # wait 20s (this because to register the Group OWNER Google takes about 20s)
        sleep(20)
        create_bucket(bucket_name=self.payload["name"])
        add_group_iam_permissions(
            bucket_name=self.payload["name"],
            group_email=self.payload["email"]
        )
        return response.json()


class GetGroupsCommand(Command):
    """
    Concrete command to retrieve groups using a GET request.
    """
    def __init__(self, auth_session, url):
        self.auth_session = auth_session
        self.url = url

    def execute(self):
        response = self.auth_session.get(url=self.url)
        return response.json()


class AddMemberGroupCommand(Command):
    """
    Concrete command to add a member to a group using a POST request.
    """
    def __init__(self, auth_session, url, payload):
        self.auth_session = auth_session
        self.url = url
        self.payload = payload

    def execute(self):
        response = self.auth_session.post(url=self.url, json=self.payload)
        return response.json()

class GetMemberGroupCommand(Command):
    """
    Concrete command to retrieve a member from a group using a GET request.
    """
    def __init__(self, auth_session, url):
        self.auth_session = auth_session
        self.url = url

    def execute(self):
        response = self.auth_session.get(url=self.url)
        return response.json()


class UpdateMemberGroupCommand(Command):
    """
    Concrete command to add a member to a group using a PUT request.
    """
    def __init__(self, auth_session, url, payload):
        self.auth_session = auth_session
        self.url = url
        self.payload = payload

    def execute(self):
        response = self.auth_session.put(url=self.url, json=self.payload)
        return response.json()

class DeleteMemberGroupCommand(Command):
    """
    Concrete command to delete a member from a group using a DELETE request.
    """
    def __init__(self, auth_session, url):
        self.auth_session = auth_session
        self.url = url

    def execute(self):
        response = self.auth_session.delete(url=self.url)
        return response.json()


class DeleteGroupsCommand(Command):
    """
    Concrete command to delete a group using a DELETE request.
    """
    def __init__(self, auth_session, url):
        self.auth_session = auth_session
        self.url = url

    def execute(self):
        response = self.auth_session.delete(url=self.url)
        return response.json()


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


def create_gcp_group(auth_session, payload, url="https://gcp.fao.org/groups-service/api/v1/groups"):
    """
    Sends a POST request to a GCP API to create a new group.

    This function is specifically designed for interacting with Google Cloud Platform (GCP) APIs.
    It uses an authenticated session to send a POST request, facilitating the creation of groups
    within GCP services. The function is useful for automating group management tasks in GCP.

    :param auth_session: An instance of an authenticated session, typically created using
                         GCP credentials. This session includes authentication tokens and is
                         used to securely communicate with GCP APIs.
    :param url: The URL of the GCP API endpoint. This string should correspond to the
                specific GCP service and operation for group creation.
    :param payload: A dictionary containing the data required by the GCP API for creating
                    a new group. This typically includes group attributes and settings.

    :return: A dictionary representing the JSON response from the GCP API. The response
             generally includes information about the newly created group or details about
             any errors that occurred during the request process.
    """
    response = auth_session.post(
        url=url,
        json=payload
    )
    return response.json()

