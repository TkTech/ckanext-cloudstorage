from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession


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
