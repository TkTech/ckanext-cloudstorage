import binascii
import collections
import datetime
import hashlib
from six.moves.urllib.parse import quote

# Required for Google Cloud authentication
from google.oauth2 import service_account

# Required for handling Python 2 and 3 compatibility issues
import six

def generate_signed_url(
    service_account_file,  # Path to the service account file
    bucket_name,           # Name of the GCS bucket
    object_name,           # Name of the object in the bucket
    subresource=None,      # Optional subresource of the object
    expiration=604800,     # Expiration time of the URL in seconds (default 7 days)
    http_method="GET",     # HTTP method for the access (default GET)
    query_parameters=None, # Additional query parameters for the URL
    headers=None           # HTTP headers for the request
):
    """
    Generate a signed URL to access a Google Cloud Storage object.

    This function creates a signed URL that provides temporary access to a specific object
    in a Google Cloud Storage bucket. The URL will be valid for the specified duration.

    Args:
        service_account_file (str): Path to the service account JSON file.
        bucket_name (str): Name of the Google Cloud Storage bucket.
        object_name (str): Name of the object in the bucket.
        subresource (str, optional): Subresource of the object, if any. Defaults to None.
        expiration (int, optional): Time in seconds until the URL expires. Defaults to 604800 (7 days).
        http_method (str, optional): HTTP method for the access. Defaults to "GET".
        query_parameters (dict, optional): Additional query parameters. Defaults to None.
        headers (dict, optional): HTTP headers for the request. Defaults to None.

    Returns:
        str: A signed URL for accessing the specified storage object.

    Raises:
        ValueError: If the expiration time exceeds 7 days (604800 seconds).
    """

    # Validate expiration time
    if expiration > 604800:
        raise ValueError("Expiration time can't be longer than 604800 seconds (7 days).")

    # Encode the object name for URL
    escaped_object_name = quote(six.ensure_binary(object_name), safe=b"/~")
    canonical_uri = "/{}".format(escaped_object_name)

    # Get the current time in UTC
    datetime_now = datetime.datetime.utcnow()
    request_timestamp = datetime_now.strftime('%Y%m%dT%H%M%SZ')
    datestamp = datetime_now.strftime('%Y%m%d')

    # Load Google credentials
    google_credentials = service_account.Credentials.from_service_account_file(
        service_account_file
    )
    client_email = google_credentials.service_account_email
    credential_scope = "{}/auto/storage/goog4_request".format(datestamp)
    credential = "{}/{}".format(client_email, credential_scope)

    # Set default headers if not provided
    if headers is None:
        headers = dict()
    host = "{}.storage.googleapis.com".format(bucket_name)
    headers["host"] = host

    # Create canonical headers string
    canonical_headers = ""
    ordered_headers = collections.OrderedDict(sorted(headers.items()))
    for k, v in ordered_headers.items():
        lower_k = str(k).lower()
        strip_v = str(v).strip()
        canonical_headers += "{}:{}\n".format(lower_k, strip_v)

    # Create signed headers string
    signed_headers = ";".join(ordered_headers.keys()).lower()

    # Set default query parameters if not provided
    if query_parameters is None:
        query_parameters = dict()
    query_parameters.update({
        "X-Goog-Algorithm": "GOOG4-RSA-SHA256",
        "X-Goog-Credential": credential,
        "X-Goog-Date": request_timestamp,
        "X-Goog-Expires": str(expiration),
        "X-Goog-SignedHeaders": signed_headers
    })
    if subresource:
        query_parameters[subresource] = ""

    # Create canonical query string
    canonical_query_string = "&".join(
        "{}={}".format(quote(str(k), safe=''), quote(str(v), safe=''))
        for k, v in sorted(query_parameters.items())
    ).rstrip('&')

    # Create canonical request
    canonical_request = "\n".join([
        http_method,
        canonical_uri,
        canonical_query_string,
        canonical_headers,
        signed_headers,
        "UNSIGNED-PAYLOAD",
    ])

    # Hash the canonical request
    canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

    # Create the string to sign
    string_to_sign = "\n".join([
        "GOOG4-RSA-SHA256",
        request_timestamp,
        credential_scope,
        canonical_request_hash,
    ])

    # Sign the string to sign using the service account's private key
    signature = binascii.hexlify(
        google_credentials.signer.sign(string_to_sign.encode())
    ).decode()

    # Construct the final signed URL
    scheme_and_host = "https://{}".format(host)
    signed_url = "{}{}?{}&x-goog-signature={}".format(
        scheme_and_host, canonical_uri, canonical_query_string, signature
    )

    return signed_url
