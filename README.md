# Utility Package for CPD Services
This package is a collection of frequently used utility functions for data science related services on Cloud Pak for Data, covering:

- CPD catalog (the base for CAMs)
- storage volumes
- WS
- WML
- WMLA
- WOS

Some methods are higher level wrappers based on existing methods from the official SDK packages (e.g., to list all data assets in a WML deployment space), while the others are a python wrapper based on API methods (e.g., for storage volume service).

## Dependencies
- ibm-cloud-sdk-core==3.10.1
- ibm-watson-openscale>=3.0.14

## Installation
### To use packaged utilities:
Download the `.tar.gz` archive and install the package from source:
```
pip install cpd_sdk_plus-1.1.tar.gz 
```
Then you can import the utility methods in a similar way, for example:
```
from cpd_sdk_plus import storage_volume_utils as sv
```

### To use in a WML deployment environment:
1. Register the source file in zip format as a package extension:
```
from cpd_sdk_plus import wml_sdk_utils as wml_util

client = wml_util.get_client(space_id='********')

# Create package extension
meta_prop_pkg_extn = {
    client.package_extensions.ConfigurationMetaNames.NAME: "cpd_sdk_plus_1.1",
    client.package_extensions.ConfigurationMetaNames.DESCRIPTION: "Pkg extension for custom lib",
    client.package_extensions.ConfigurationMetaNames.TYPE: "pip_zip"
}

pkg_extn_details = client.package_extensions.store(meta_props=meta_prop_pkg_extn, file_path="cpd_sdk_plus-1.1.zip")
pkg_extn_uid = client.package_extensions.get_uid(pkg_extn_details)
```
2. Create a custom software specification and add the package extension
```
# Create software specification and add custom library
base_sw_spec_uid = client.software_specifications.get_uid_by_name("runtime-22.1-py3.9")

meta_prop_sw_spec = {
    client.software_specifications.ConfigurationMetaNames.NAME: "py39_cpd_sdk_util",
    client.software_specifications.ConfigurationMetaNames.DESCRIPTION: "py39 software specification for cpd sdk util",
    client.software_specifications.ConfigurationMetaNames.BASE_SOFTWARE_SPECIFICATION: {"guid": base_sw_spec_uid}
}

sw_spec_details = client.software_specifications.store(meta_props=meta_prop_sw_spec)
sw_spec_uid = client.software_specifications.get_uid(sw_spec_details)

client.software_specifications.add_package_extension(sw_spec_uid, pkg_extn_uid)
```
3. Now, your deployments using software specification `py39_cpd_sdk_util` will be able to import and use this package the same way as in the development environment.

## How to Contribute
`DCO` is suggested to be used. See [here](https://wiki.linuxfoundation.org/dco) for details on how to do it.

## Contributors
Many scripts are co-developed by 
- Rich Nieto (rich.nieto@ibm.com) 
- Drew McCalmont (drewm@ibm.com)

