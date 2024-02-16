# ckanext-cloudstorage

`ckanext-cloudstorage` is a plugin for CKAN that enhances its capabilities by enabling the use of various cloud storage services. It supports integration with over 15 different storage providers, including Amazon S3, Google Cloud Storage, and Azure, via [libcloud][]. This flexibility allows CKAN[CKAN][] to leverage the robustness and scalability of these cloud storage solutions

## Features

- **Google Storage bucket integration**: You have the ability to upload files to Google Cloud Platform (GCP) Bucket Storage and download files from GCP Workspace Storage.
- **GCP group management**: In GCP Workspace, you have the capability to administer groups efficiently. This includes creating and deleting       groups, as well as adding and removing members from these groups...
- **Manage IAM permission**: ou have the capability to set IAM permissions for GCP Storage Buckets and configure group permissions, allowing for effective management of access control to these storage resources

Most libcloud-based providers should work out of the box, but only those listed
below have been tested:

| Provider | Uploads | Downloads | Secure URLs (private resources) |
| --- | --- | --- | --- |
| Google Bucket | YES | YES | YES (if `google-auth` and `six>=1.5` is installed) 


## Prerequisites

- Python 2.7
- Google Workspace domain with admin access
- Service account with domain-wide delegation and the necessary permissions


## Installation

Fork the repository[repository][https://github.com/ccancellieri/ckanext-cloudstorage] and clone  to your local machine and switch to `google-cloud-support` branch


## Setup
After installing `ckanext-cloudstorage`, add it to your list of plugins in
your `.ini`:

```bash
    ckan.plugins = stats cloudstorage

```

If you haven't already, setup [CKAN file storage][ckanstorage] or the file
upload button will not appear.

Every driver takes two options, regardless of which one you use. Both
the name of the driver and the name of the container/bucket are
case-sensitive:

```bash
    ckanext.cloudstorage.driver = GOOGLE_STORAGE
    ckanext.cloudstorage.container_name = demo
```

You can find a list of driver names [here][storage] (see the `Provider
Constant` column.)

Each driver takes its own setup options. See the [libcloud][] documentation.
These options are passed in using `driver_options`, which is a Python dict.
For most drivers, this is all you need:

```bash
    ckanext.cloudstorage.driver_options = {"key": "<your public key>", "secret": "<your secret key>"}
```

### What are "Secure URLs"?

"Secure URLs" are a method of preventing access to private resources. By
default, anyone that figures out the URL to your resource on your storage
provider can download it. Secure URLs allow you to disable public access and
instead let ckanext-cloudstorage generate temporary, one-use URLs to download
the resource. This means that the normal CKAN-provided access restrictions can
apply to resources with no further effort on your part, but still get all the
benefits of your CDN/blob storage.
```bash
    ckanext.cloudstorage.use_secure_urls = True
```
This option also enables multipart uploads, but you need to create database tables
first. Run next command from extension folder:
    `paster cloudstorage initdb -c /etc/ckan/default/production.ini `

With that feature you can use `cloudstorage_clean_multipart` action, which is available
only for sysadmins. After executing, all unfinished multipart uploads, older than 7 days,
will be aborted. You can configure this lifetime, example:

```bash
     ckanext.cloudstorage.max_multipart_lifetime  = 7
```

## Install the require dependencies

from `ckanext-cloudstorage` folder execute the activate your virtual environment  and run the command below:
```bash
pip install -r requirements.txt
```

## Migrating From FileStorage

If you already have resources that have been uploaded and saved using CKAN's
built-in FileStorage, cloudstorage provides an easy migration command.
Simply setup cloudstorage as explained above, enable the plugin, and run the
migrate command. Provide the path to your resources on-disk (the
`ckan.storage_path` setting in your CKAN `.ini`), and
cloudstorage will take care of the rest. Ex:

Before running etl script make sure you have setup this config values :

```bash
ckanext.cloudstorage.service_account_key_path= {PATH_TO_SECRET_KEY_FILE}
ckanext.cloudstorage.gcp_base_url= {GCP_BASE_URL}
ckan.site_url= {SITE_URL}
ckan.root_path= {ROOT_PATH}
ckan.storage_path={STORAGE_PATH}
ckanext.cloudstorage.prefix={PREFIX}
ckanext.cloudstorage.domain={DOMAIN}
```

from `ckanext-cloudstorage` folder execute this command:

```bash
cd ckanext/cloudstorage/etl
```

and then from `etl` folder run the command below:

```bash

python etl_run.py organization_name ckan_api_key configuration_file
```
- Replace `organization_name` with the actual name of the organization you want to process.
- Replace `ckan_api_key` with the actual sysadmin api key of your ckan instance.
- Replace `configuration_file` with the path of your production.ini file.


## Notes

1. You should disable public listing on the cloud service provider you're
   using, if supported.
2. Currently, only resources are supported. This means that things like group
   and organization images still use CKAN's local file storage.
3. Make sure the vm instance has the correct scopes. If not use this command below to set right scopes:

    ```bash
    gcloud beta compute instances set-scopes [INSTANCE_NAME] --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/devstorage.full_control [--zone=[ZONE]]

    ```
    and restart the vm instance after to allow changes to be applied.
    
4. Check if scopes has been apply correctly by using this command below:

    ```bash

    gcloud compute instances describe [INSTANCE_NAME] --format='get(serviceAccounts[].scopes[])'

    ```

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements
- [Google APIs Client Library for Python](https://github.com/googleapis/google-api-python-client)</s>
- [libcloud](https://libcloud.apache.org/)
- [ckan](http://ckan.org/)
- [storage](https://libcloud.readthedocs.io/en/latest/storage/supported_providers.html)
- [ckanstorage](http://docs.ckan.org/en/latest/maintaining/filestore.html#setup-file-uploads)
