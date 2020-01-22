# ckanext-cloudstorage

Implements support for using S3, Azure, or any of 15 different storage
providers supported by [libcloud][] to [CKAN][].

# Setup

After installing `ckanext-cloudstorage`, add it to your list of plugins in
your `.ini`:

    ckan.plugins = stats cloudstorage

If you haven't already, setup [CKAN file storage][ckanstorage] or the file
upload button will not appear.

Every driver takes two options, regardless of which one you use. Both
the name of the driver and the name of the container/bucket are
case-sensitive:

    ckanext.cloudstorage.driver = AZURE_BLOBS
    ckanext.cloudstorage.container_name = demo

You can find a list of driver names [here][storage] (see the `Provider
Constant` column.)

Each driver takes its own setup options. See the [libcloud][] documentation.
These options are passed in using `driver_options`, which is a Python dict.
For most drivers, this is all you need:

    ckanext.cloudstorage.driver_options = {"key": "<your public key>", "secret": "<your secret key>"}

# Support

Most libcloud-based providers should work out of the box, but only those listed
below have been tested:

| Provider | Uploads | Downloads | Secure URLs (private resources) |
| --- | --- | --- | --- |
| Azure    | YES | YES | YES (if `azure-storage` is installed) |
| AWS S3   | YES | YES | YES (if `boto` is installed) |
| Rackspace | YES | YES | No |

# What are "Secure URLs"?

"Secure URLs" are a method of preventing access to private resources. By
default, anyone that figures out the URL to your resource on your storage
provider can download it. Secure URLs allow you to disable public access and
instead let ckanext-cloudstorage generate temporary, one-use URLs to download
the resource. This means that the normal CKAN-provided access restrictions can
apply to resources with no further effort on your part, but still get all the
benefits of your CDN/blob storage.

    ckanext.cloudstorage.use_secure_urls = 1

This option also enables multipart uploads, but you need to create database tables
first. Run next command from extension folder:
    `paster cloudstorage initdb -c /etc/ckan/default/production.ini `

With that feature you can use `cloudstorage_clean_multipart` action, which is available
only for sysadmins. After executing, all unfinished multipart uploads, older than 7 days,
will be aborted. You can configure this lifetime, example:

     ckanext.cloudstorage.max_multipart_lifetime  = 7

# Migrating From FileStorage

If you already have resources that have been uploaded and saved using CKAN's
built-in FileStorage, cloudstorage provides an easy migration command.
Simply setup cloudstorage as explained above, enable the plugin, and run the
migrate command. Provide the path to your resources on-disk (the
`ckan.storage_path` setting in your CKAN `.ini` + `/resources`), and
cloudstorage will take care of the rest. Ex:

    paster cloudstorage migrate <path to files> -c ../ckan/development.ini

# Notes

1. You should disable public listing on the cloud service provider you're
   using, if supported.
2. Currently, only resources are supported. This means that things like group
   and organization images still use CKAN's local file storage.

# FAQ

- *DataViews aren't showing my data!* - did you setup CORS rules properly on
  your hosting service? ckanext-cloudstorage can try to fix them for you automatically,
  run:

        paster cloudstorage fix-cors <list of your domains> -c=<CKAN config>

- *Help! I can't seem to get it working!* - send me a mail! tk@tkte.ch

[libcloud]: https://libcloud.apache.org/
[ckan]: http://ckan.org/
[storage]: https://libcloud.readthedocs.io/en/latest/storage/supported_providers.html
[ckanstorage]: http://docs.ckan.org/en/latest/maintaining/filestore.html#setup-file-uploads
