from time import sleep
from abc import ABCMeta, abstractmethod

from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

from ckanext.cloudstorage.etl.bucket_utils import create_bucket
from ckanext.cloudstorage.etl.bucket_utils import add_group_iam_permissions

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


def create_id_token_and_auth_session(service_account_json_file, target_audience="https://groups.fao.org"):
    """
    Generates an ID token using a GCP service account and makes a POST request.

    This function creates an ID token using Google Cloud Platform service account credentials,
    and returns an authorized session for making HTTP requests, particularly POST requests.

    :param service_account_json_file: Path to the service account key file in JSON format.
                                      It contains credentials for the service account.
    :param target_audience: The intended audience (URL) for the ID token. This specifies
                            the target service or API that the token is intended for.
    
    :return: An instance of `AuthorizedSession` with ID token credentials. This session
             can be used for authenticated HTTP requests to the specified target audience.
    """
    # Load the service account credentials and create an ID token
    credentials = service_account.IDTokenCredentials.from_service_account_file(
        service_account_json_file, 
        target_audience=target_audience
    )

    # Create an authorized session using the credentials
    auth_session = AuthorizedSession(credentials)

    return auth_session
