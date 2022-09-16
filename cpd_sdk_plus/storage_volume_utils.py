import requests
import urllib
import os
import pandas as pd
import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from . import *

# unlike an cli tool, the credentials in a package needs to be grabbed each time a function is called
# otherwise newly assigned values won't be reflected as the following code is only executed when
# importing the library
# USER_ACCESS_TOKEN = os.getenv('USER_ACCESS_TOKEN')
# USERNAME = os.getenv('CPD_USERNAME')
# APIKEY = os.getenv('CPD_APIKEY')
# BASE_URL = os.getenv('CPD_BASE_URL',os.getenv('RUNTIME_ENV_APSX_URL'))

LISTINSTANCES = '/zen-data/v3/service_instances?addon_type=volumes'
LISTFILES = '/zen-volumes/{volume_display_name}/v1/volumes/directories/{path_encoded}'
FILES = '/zen-volumes/{volume_display_name}/v1/volumes/files/{path_encoded}'

HEADERS_POST = {'Content-Type':'application/json',
                'Authorization':'Bearer {access_token}'}
HEADERS_GET = {'Authorization':'Bearer {access_token}'}
HEADERS_PUT = {'Authorization':'Bearer {access_token}'}

def list_storage_volumes(params = {'limit':1000}, return_df=False, return_raw=False, verbose=0):
    
    BASE_URL = os.getenv('CPD_BASE_URL',os.getenv('RUNTIME_ENV_APSX_URL'))
    access_token = validate_authorization(BASE_URL,os.getenv('CPD_USERNAME'),os.getenv('CPD_APIKEY'),
                                          os.getenv('USER_ACCESS_TOKEN'))
    
    requests_args = {'url': BASE_URL+LISTINSTANCES,
                     'headers': fill_in_access_token(HEADERS_GET,access_token),
                     'params': params,
                     'verify': False}
    if verbose > 0:
        print(requests_args['url'])
    out = run_with_fault_tolerance(requests.get,requests_args,status_code_pass=[200],return_raw=True)
    
    if out.status_code == 200:
        if return_raw:
            return out
        elif return_df:
            return pd.json_normalize(out.json()['service_instances'])
        else:
            return {x['display_name']:{'id':x['id'], 'mount_path':x['metadata']['mount_path']} for x in out.json()['service_instances']}
    else:
        return out

def list_files(path, volume_display_name=None, 
               params = {'include_details':'true','recursive':'true'}, 
               return_df=False, files_only=False, most_recent=None, file_extensions=None,verbose=0,
               return_raw=False):
    """
    Given dir path on storage volume, return a list of files and metadata.
    
    params: "include_details"="true" will be automatically added if 
               - parameter files_only is True, OR
               - parameter most_recent is not None
               - parameter file_extensions is not None
    most_recent: an integer in day to filter the most recent files/directories
    file_extensions: a list of acceptable file extensions, e.g., ['.png','.json']
                     this will force files_only to be true
    return_raw: return the raw response, overwrites return_df
    """
    volume_display_name = validate_storage_volume(volume_display_name,'VOLUME_DISPLAY_NAME')
    BASE_URL = os.getenv('CPD_BASE_URL',os.getenv('RUNTIME_ENV_APSX_URL'))
    access_token = validate_authorization(BASE_URL,os.getenv('CPD_USERNAME'),os.getenv('CPD_APIKEY'),
                                          os.getenv('USER_ACCESS_TOKEN'))

    if path == '':
        path = '/' # '' triggers error; use / to inspect the root folder

    if files_only or most_recent is not None or file_extensions is not None:
        params['include_details'] = 'true'
        
    if file_extensions is not None:
        files_only=True
    
    requests_args = {'url': BASE_URL+LISTFILES.format(volume_display_name=volume_display_name, path_encoded=encode_path(path)),
                     'headers': fill_in_access_token(HEADERS_GET,access_token),
                     'params': params,
                     'verify': False}
    if verbose > 0:
        print(requests_args['url'])
    out = run_with_fault_tolerance(requests.get,requests_args,status_code_pass=[200,404],return_raw=True)

    if return_raw:
        return out
    else:
        if out.status_code == 404:
            return None
        else:
            out = out.json().get('responseObject').get('directoryContents')

            if files_only:
                out = [metadata for metadata in out if metadata['type']=='file']

            if file_extensions is not None:
                out = [metadata for metadata in out if metadata['file_extension'] in file_extensions]

            if most_recent is not None:
                dt_current = datetime.datetime.now()

                out_recent = []
                for metadata in out:
                    days_passed = (dt_current - datetime.datetime.strptime(metadata['last_modified'], '%a, %d %b %Y %H:%M:%S %Z')).days
                    if days_passed <= most_recent:
                        out_recent.append(metadata)
                out = out_recent.copy()

            if return_df:
                return pd.DataFrame(out)
            else:
                return out

def upload(path_source, path_target=None,
           volume_display_name=None, keep_source_folder_structure=True):
    """
    Upload a local file to a storage volume.
    
    path_source: the path in your "local"/"current" environment for the file to be uploaded
                 can handle a directory:
                   storage volumes api does not support uploading a whole directory in one request
                   send one upload request per file
    path_target: the path in storage volume as the destination
                 if a folder is provided instead of a filename, the file(s) will be uploaded to this folder
                 with sub-folder structure remained
    keep_source_folder_structure: whether to keep the same folder structure for the target destination
                                  example of using True vs. using False:
                                    path_source = 'abc/def.g'
                                    #1 path_target not specified
                                      final target path = '/mnts/<volume>/abc/def.g' vs. '/mnts/<volume>/def.g'
                                    #2 path_target = 'xyz' (a folder)
                                      final target path = '/mnts/<volume>/xyz/abc/def.g' vs. '/mnts/<volume>/xyz/def.g'
                                    #3 path target = 'xyz/z.z'
                                      final target path = '/mnts/<volume>/xyz/z.z' vs. '/mnts/<volume>/xyz/z.z'
    """
    volume_display_name = validate_storage_volume(volume_display_name,'VOLUME_DISPLAY_NAME')
    BASE_URL = os.getenv('CPD_BASE_URL',os.getenv('RUNTIME_ENV_APSX_URL'))
    access_token = validate_authorization(BASE_URL,os.getenv('CPD_USERNAME'),os.getenv('CPD_APIKEY'),
                                          os.getenv('USER_ACCESS_TOKEN'))
    
    if not os.path.exists(path_source):
        raise Exception(f'Local path {path_source} does not exist.')
        
    # later may want to modify the string in uptream (here) than downstream (file_or_dir)
#     if path_source.startswith('./'):
#         path_source = path_source[2:]
    
    if os.path.isfile(path_source):
        l_path_source = [path_source]
    else:
        l_path_source = list_files_local(path_source)
    
    for path_source in l_path_source:
        path_target_final = path_source if path_target is None else path_target
        path_target_type = file_or_dir(path_target_final,volume_display_name)

        if path_target_type == 'directory':
            if keep_source_folder_structure:
                path_target_final = f"{path_target_final}/{path_source}"
            else:
                path_target_final = f"{path_target_final}/{os.path.basename(path_source)}"
        else:
            if not keep_source_folder_structure:
                path_target_final = f"{os.path.basename(path_source)}"
                
        requests_args = {'url': BASE_URL+FILES.format(volume_display_name=volume_display_name, 
                                                      path_encoded=encode_path(path_target_final)),
                         'headers': fill_in_access_token(HEADERS_PUT,access_token),
                         'files': {'upFile':(path_source,open(path_source, 'rb'))},
                         'verify': False}

        run_with_fault_tolerance(requests.put,requests_args)
        
        print(f'Success, file uploaded to {path_target_final}')
    

def upload_batch(l_path_source, path_target=None,
                 volume_display_name=None):
    """
    l_path_source: a list of paths to single files, or directories, or a mix of both
    """
    volume_display_name = validate_storage_volume(volume_display_name,'VOLUME_DISPLAY_NAME')
    
    for path_source in l_path_source:
        upload(path_source,path_target,volume_display_name)
    

def download(path_source, path_target=None,
             volume_display_name=None, params={},verbose=0):
    """
    Download a file from storage volume to local environment.
    
    path_source: the path in storage volume for the file to be downloaded
                 can handle a directory:
                   storage volumes api supports downloading a whole directory in one request
                   the directory will be downloaded as a zip file unless other format is specified
    path_target: the path in your "local"/"current" environment as the destination
                 if a folder is provided instead of a filename, the file(s) will be downloaded to this folder
                 with sub-folder structure remained
    params: use {"compress":"zip"} if you want to download a folder as an archive
            {"compress":"zip"} will be used anyway if a folder is provided in path_source
    """
    volume_display_name = validate_storage_volume(volume_display_name,'VOLUME_DISPLAY_NAME')
    BASE_URL = os.getenv('CPD_BASE_URL',os.getenv('RUNTIME_ENV_APSX_URL'))
    access_token = validate_authorization(BASE_URL,os.getenv('CPD_USERNAME'),os.getenv('CPD_APIKEY'),
                                          os.getenv('USER_ACCESS_TOKEN'))
    
    if path_source[-1] == '/':
        path_source = path_source[:-1]
    
    path_source_type = file_or_dir(path_source,volume_display_name)
    if path_source_type == 'directory':
        params['compress'] = 'zip'

    if path_target is None:
        path_target = path_source
    else:
        if os.path.isdir(path_target):
            path_target = os.path.join(path_target,path_source)
    
    requests_args = {'url': BASE_URL+FILES.format(volume_display_name=volume_display_name, path_encoded=encode_path(path_source)),
                     'headers': fill_in_access_token(HEADERS_GET,access_token),
                     'params': params,
                     'verify': False}
    
    out = run_with_fault_tolerance(requests.get,requests_args,
                                   return_raw=True)
    
    if (os.path.dirname(path_target) != ''): # a parent dir will be '' if the path doesn't have one ('abc')
        os.makedirs(os.path.dirname(path_target),exist_ok=True)

    if 'compress' in params:
        path_target += f".{params['compress']}"

    with open(path_target,'wb') as f:
        f.write(out.content)
    
    if verbose > 0:
        print(f'Success, file downloaded to {path_target}')


def download_batch(l_path_source, path_target=None,
                   volume_display_name=None, params={},verbose=0):
    """
    l_path_source: a list of paths to single files, or directories, or a mix of both
    """
    volume_display_name = validate_storage_volume(volume_display_name,'VOLUME_DISPLAY_NAME')
    
    for path_source in l_path_source:
        download(path_source,path_target,volume_display_name,params,verbose)



#### utils ####
def list_files_local(path):
    """
    List all files recursively.
    """
    return [os.path.join(dp, f) for dp, dn, fns in os.walk(path) for f in fns]


# def fill_in_default_if_none(var,env_var=None,default=None):
#     """
#     Used to dynamically determine the default value of an argument.
#     In this way, the default value is not determined at the time the module is
#     imported, but at the time when the function is called.
    
#     var: variable to examine
#     env_var: the name of environment variable to pull the default value from
#     default: if env_var is not defined in this function, or the corresponding 
#     environment variable does not exist, fill with this default value
#     """
#     if var is None:
#         if env_var is not None:
#             return os.getenv(env_var,default)
#         else:
#             return default
#     else:
#         return var


def encode_path(path):
    path_quoted = urllib.parse.quote(path, safe='')
    
    # urllib.parse.quote does not convert _ to %5F
    return path_quoted


def fill_in_access_token(headers,access_token):
    return {k:v.format(access_token=access_token) for k,v in headers.items()}



def file_or_dir(path, volume_display_name=None):
    """
    Examines whether an EXISTING path points to a file or a directory on storage volume.
    If the path does not exist, calls file_or_dir_guess() to make a guess.
    
    path: do NOT end with /
    """
    volume_display_name = validate_storage_volume(volume_display_name,'VOLUME_DISPLAY_NAME')
    
    path_parent = os.path.dirname(path)
    path_parent = '/' if path_parent == '' else path_parent
    if path_parent.startswith('.'): # the storage volume api doesn't recognize "./"
        path_parent = path_parent[2:]
    path_base = os.path.basename(path)
    
    l_meta = list_files(path_parent, volume_display_name,{'include_details':'true','recursive':'false'})
    
    if l_meta is None: # this happens when the parent path does not exist in storage volume
        print(f'{path} cannot be found on storage volume (yet), returned the guessed type')
        return file_or_dir_guess(path)
    
    l_meta_filtered = [meta for meta in l_meta if meta['path'] == path_base]

    if len(l_meta_filtered) == 0: # this happens when the parent path exists, but the file does not
        print(f'{path} cannot be found on storage volume (yet), returned the guessed type')
        return file_or_dir_guess(path)
    elif len(l_meta_filtered) == 1:
        return l_meta_filtered[0]['type']
    else:
        raise Exception(f'FAILED: have multiple entries, {l_meta_filtered}; either check the code of this function')
        
def file_or_dir_local(path):
    """
    Check whether a local EXISTING path points ot a folder or a file.
    """
    if os.path.isdir(path):
        return 'directory'
    else:
        return 'file'

def file_or_dir_guess(path):
    """
    Guess whether a path looks like a file or a directory.
    Use it when you have to get some sense before the path gets created.
    It simply checks whether there is dot in the last portion of the path.
    """
    if '.' in path.split('/')[-1]:
        return 'file'
    else:
        return 'directory'

def validate_storage_volume(volume_display_name,env_var):
    volume_display_name = os.getenv(env_var) if volume_display_name is None else volume_display_name
    if volume_display_name is None:
        raise Exception(f'Volume display name {volume_display_name} is not set. Either use the volume_display_name parameter in the function or provide the value in environment variable {env_var}.')
    else:
        d_storage_volume = list_storage_volumes()
        if volume_display_name in d_storage_volume.keys():
            return volume_display_name
        else:
            raise Exception(f"Volume display name {volume_display_name} cannot be found in storage volume list. Please verify whether there is a storage volume with this name.")
