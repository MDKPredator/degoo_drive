#!/usr/bin/python3
# encoding: utf-8
'''
Degoo API -- An API interface to interact with a Degoo cloud drive

Degoo lack a command line client, they lack a Linux client, but they
expose a GraphQL API which their web app (and probably their phone app)
communicate with. This is a reverse engineering based on observations of
those communications aand a Python client implementation.

@author:     Bernd Wechner

@copyright:  2020. All rights reserved.

@license:    The Hippocratic License 2.1

@contact:    YndlY2huZXJAeWFob28uY29t    (base64 encoded)
@deffield    updated: Updated
'''
import base64
import datetime
import hashlib
import json
import mimetypes
import os
import sys
import time
from shutil import copyfile

import humanfriendly
import humanize
import jwt
import requests
import wget
from appdirs import user_config_dir
from clint.textui.progress import Bar as ProgressBar
from dateutil import parser, tz
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor


class upload_in_chunks(object):
    def __init__(self, filename, chunksize=1 << 13):
        self.filename = filename
        self.chunksize = chunksize
        self.totalsize = os.path.getsize(filename)
        self.readsofar = 0

    def __iter__(self):
        with open(self.filename, 'rb') as file:
            while True:
                data = file.read(self.chunksize)
                if not data:
                    sys.stderr.write("\n")
                    break
                self.readsofar += len(data)
                percent = self.readsofar * 1e2 / self.totalsize
                sys.stderr.write("\r{percent:3.0f}%".format(percent=percent))
                yield data

    def __len__(self):
        return self.totalsize


# An Error class for Degoo functions to raise if need be
class DegooError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super().__init__(type(self))
        self.msg = msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg


class DegooConfig:
    def __init__(self, config_dir=None, email=None, password=None, refresh_token=None):
        # Get the path to user configuration diectory for this app
        if config_dir is None:
            config_dir = user_config_dir("degoo")

        # Local config and state files
        global cred_file
        cred_file = os.path.join(config_dir, "credentials.json")
        if email and password:
            with open(cred_file, "w") as file:
                file.write(json.dumps({"Username": email, "Password": password}))

        global cwd_file
        cwd_file = os.path.join(config_dir, "cwd.json")

        global keys_file
        keys_file = os.path.join(config_dir, "keys.json")
        if refresh_token:
            with open(keys_file, "r") as jsonFile:
                data = json.load(jsonFile)

            # Token will be requested when needed
            data['Token'] = ''
            data['RefreshToken'] = refresh_token

            with open(keys_file, "w") as jsonFile:
                json.dump(data, jsonFile)

        global DP_file
        DP_file = os.path.join(config_dir, "default_properties.txt")

        # Ensure the user configuration directory exists
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

# URLS

# A string to prefix CLI commands with (configurable, and used by 
# build.py     - to make the commands and,
# commands.py  - to implement the commands 
command_prefix = "degoo_"

# The URLS that the Degoo API relies upon
URL_LOGIN = "https://rest-api.degoo.com/login"
URL_API = "https://production-appsync.degoo.com/graphql"
URL_ACCESS_TOKEN = "https://rest-api.degoo.com/access-token"
URL_ACCESS_TOKEN_V2 = "https://rest-api.degoo.com/access-token/v2"

# Get the path to user configuration diectory for this app
conf_dir = user_config_dir("degoo")

# Local config and state files
cred_file = os.path.join(conf_dir, "credentials.json")
cwd_file = os.path.join(conf_dir, "cwd.json")
keys_file = os.path.join(conf_dir, "keys.json")
DP_file = os.path.join(conf_dir, "default_properties.txt")
overlay4_file = os.path.join(conf_dir, "overlay4.txt")

# A local cache of Degoo items and contents, to speed up successive queries for them
# BY convention we have Degoo ID 0 as the root directory and the API returns no
# properties for that so we dummy some up for local use to give it the appearance 
# of a root directory.
__CACHE_ITEMS__ = {0: {
                        "ID": 0,
                        "ParentID": None,
                        "Name": "/",
                        "FilePath": "/",
                        "Category": None,
                        "CategoryName": "Root",
                        }
                    }
__CACHE_CONTENTS__ = {}

FILE_CHILDREN_LIMIT = 100

api = None

###########################################################################
# Support functions

def ddd(ID, Path):
    '''
    A Degoo Directory Dictionary (ddd).
    
    A convenient way to represent a current working directory so it can be 
    saved, restored and communicated. 
    
    :param ID:    The Degoo ID of a a Degoo Item
    :param Path:  The Path of that same Item  
    '''
    return {"ID": ID, "Path": Path}

def split_path(path):
    '''
    Given a path string, splits it into a list of elements.
    
    os.sep is used to split the path and appears in none of the elements,
    with the exception of parts[0] which is equal to os.sep for an absolute 
    path and not for relative path. 
    
    :param path:  A file path string
    '''
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts

def absolute_remote_path(path):
    '''
    Convert a give path strig to an absolute one (if it's relative).
     
    :param path: The path to convert.
    :returns: The absolute version of path 
    '''
    global CWD
    if path and path[0] == os.sep:
        return os.path.abspath(path.rstrip(os.sep))
    else:
        return os.path.abspath(os.path.join(CWD["Path"], path.rstrip(os.sep)))

def wait_until_next(time_of_day, verbose=0):
    '''
    Wait until the specified time. Uses Python sleep() which uses no CPU as rule
    and works on a basis liek Zeno's dichotomy paradox, by sleeping to the half 
    way point in successive trials unil we're with 0.1 second of the target.
    
    Used herein for scheduling uploads and downloads.  
     
    :param time_of_day: A time of day as time.time_struct.
    '''
    now = time.localtime()
    if now < time_of_day:
        today = datetime.datetime.now().date()
        until = datetime.datetime.combine(today, datetime.datetime.fromtimestamp(time.mktime(time_of_day)).time())
    else:
        tomorrow = datetime.datetime.now().date() + datetime.timedelta(days=1)
        until = datetime.datetime.combine(tomorrow, datetime.datetime.fromtimestamp(time.mktime(time_of_day)).time())
    
    if verbose > 0:
        print(f"Waiting until {until.strftime('%A, %d/%m/%Y %H:%M:%S')}")
    
    while True:
        diff = (until - datetime.datetime.now()).total_seconds()
        if diff < 0: return       # In case end_datetime was in past to begin with
        
        if verbose > 1:
            print(f"Waiting for {humanfriendly.format_timespan(diff/2)} seconds")
            
        time.sleep(diff/2)
        if diff <= 0.1: return

###########################################################################
# Load the current working directory, if available

CWD = ddd(0, "/")
if os.path.isfile(cwd_file):
    with open(cwd_file, "r") as file:
        CWD = json.loads(file.read())

# Logging in is a prerequisite to using the API (a pre API step). The
# login function reads from the configure cred_file and writes keys the
# API needs to the keys_file.

def login():
    '''
    Logs into a Degoo account. 
    
    The login is lasting, i.e. does not seem to expire (not subjet to any autologout)
    
    This function leans on:
    
    Degoo account credentials stored in a file `cred_file` 
    (by default ~/.config/degoo/credentials.json) This file shoud contain
    a JSON dict with elements Username and Password akin to:
        {"Username":"username","Password":"password"}
    This file should be secure readable by the user concerned, as it affords
    access to that users Degoo account.
        
    URL_login which is the URL it posts the credentials to.
    
    The reply provides a couple of keys that must be provided with each subsequent
    API call, as authentication, to prove we're logged in. These are written in JSON
    format to keys_file which is by default: ~/.config/degoo/keys.json
    
    TODO: Support logout as well (which will POST a logout request and remove these keys)
    
    :returns: True if successful, false if not
    '''
    CREDS = {}
    if os.path.isfile(cred_file):
        with open(cred_file, "r") as file:
            CREDS = json.loads(file.read())

    if CREDS:
        response = requests.post(URL_LOGIN, data=json.dumps(CREDS))

        if response.ok:
            rd = json.loads(response.text)

            keys = {"Token": rd["Token"], "RefreshToken": rd["RefreshToken"], "x-api-key": api.API_KEY}

            with open(keys_file, "w") as file:
                file.write(json.dumps(keys))

            return True
        else:
            return False
    else:
        with open(cred_file, "w") as file:
            file.write(json.dumps({"Username": "<your Degoo username here>", "Password": "<your Degoo password here>"}))

        print(f"No login credentials available. Please add account details to {cred_file}", file=sys.stderr)

    if not os.path.isfile(DP_file):
        source_file = os.path.basename(DP_file)
        if os.path.isfile(source_file):
            copyfile(source_file, DP_file)
        else:
            print(f"No properties are configured or available. If you can find the supplied file '{source_file}' copy it to '{DP_file}' and try again.")


###########################################################################
# Bundle all the API interactions into an API class


class API:
    # Empirically determined, largest value degoo supports for the Limit 
    # on the Limit parameter to the getFileChildren5 operation. It's used
    # for paging, and if more items exist there'll be a NextToken returned.
    # TODO: Determine the syntax and use of that NextToken so that paged 
    # results can be fecthed reliably as well. For now we just make calls 
    # with the max limit and avoid dealing with paging. 
    LIMIT_MAX = int('1'*31, 2)-1

    # This appears to be an invariant key that the API expects in the header
    # x-api-key:
    API_KEY = "da2-vs6twz5vnjdavpqndtbzg3prra"
    
    # Keys needed to interact with the API. Provided during login.
    KEYS = None
    
    # Known Degoo API item categories
    CATS = {  0: "File",
              1: "Device",
              2: "Folder",
              3: "Image",
              4: "Video",
              5: "Music",
              6: "Document",
             10: "Recycle Bin",
           }
    
    # The types of folder we see on a Degoo account
    # These are characteristically different kinds of item to Degoo
    # But we will try to provide a more unifor folder style interface 
    # to them herein. 
    folder_types = ["Folder", "Device", "Recycle Bin"]
    
    # A guess at the plans available
    PLANS = { 0: "Free 100 GB",
              1: "Pro 500 GB",
              2: "Ultimate 10 TB",
              3: "Ultimate Stackcommerce offer 10 TB"
            }
    
    # Width of a Category field in text output we produce
    # Should be wide enough to handle the longest entry in CATS
    # Updated in __init_/
    CATLEN = 10
    
    # Width of Name field for text output we produce
    # Used when listing files, updated to teh width needed to display
    # the longest filename. Updated by getFileChildren5 when it returns
    # a list of filenames.
    NAMELEN = 20
    
    # A list of Degoo Item properties. The three API calls:
    #    getOverlay4
    #    getFileChildren5
    #    getFilesFromPaths
    # all want a list of explicit propeties it seems, that they will 
    # return. We want them all basically, and the superset of all known 
    # properties that Degoo Items have should be stored in DP_file, 
    # which is by default:
    #
    # ~/.config/degoo/default_properties.txt
    #
    # A sample file should accompany this script. One property per
    # line in the file. 
    # 
    # TODO: Are there any further properties? It would be nice for 
    # example if we could ask for the checksum that is canculated
    # when the file is upladed and provided to SetUploadFile2.
    PROPERTIES = ""
    OVERLAY4 = ""
    
    def __init__(self):
        '''
        Reads config and state files to intitialise the API.
        
        Specifically:
        
            Loads authentication keys from key_file if available (the product of 
            loging in)
            
            Loads the superset of known degoo item properties that we can ask 
            for when sending queries to the remote API.
            
            Sets CATLEN to the length fo the longest CAT name.
        '''
        if os.path.isfile(keys_file):
            with open(keys_file, "r") as file:
                keys = json.loads(file.read())
                if "Token" not in keys:
                    raise DegooError("'Token' key not found")
                if not keys["RefreshToken"]:
                    raise DegooError("'RefreshToken' key not found")
                if not keys["x-api-key"]:
                    raise DegooError("'x-api-key' key not found")
        else:
            raise DegooError(f"File {keys_file} not found")
                
        self.KEYS = keys
        
        if os.path.isfile(DP_file):
            with open(DP_file, "r") as file:
                self.PROPERTIES = file.read()
        else:
            raise DegooError(f"File {DP_file} not found")

        if os.path.isfile(overlay4_file):
            with open(overlay4_file, "r") as file:
                self.OVERLAY4 = file.read()
        else:
            raise DegooError(f"File {overlay4_file} not found")

        self.CATLEN = max([len(n) for _,n in self.CATS.items()])

        global api
        api = self

    def _human_readable_times(self, creation, modification, upload):
        '''
        Given three Degoo timestamps converts them to human readable 
        text strings. These three timestamps are provided for every
        Degoo item.
        
        :param creation:        The time of creation 
        :param modification:    The time of last modifciation 
        :param upload:          The time of last upload  

        :returns:               A tuple of 3 strings.        
        '''
        date_format = "%Y-%m-%d %H:%M:%S"
        no_date = "Unavailable"

        # Add a set of Human Readable timestamps
        if creation:
            c_time = creation
            c_datetime = parser.parse(c_time)
            c_dt = c_datetime.strftime(date_format)
        else:
            c_dt = no_date

        if modification:
            m_secs = int(modification)/1000
            m_datetime = datetime.datetime.utcfromtimestamp(m_secs)
            m_dt = m_datetime.strftime(date_format)
        else:
            m_dt = no_date

        if upload:
            u_secs = int(upload)/1000
            u_datetime = datetime.datetime.utcfromtimestamp(u_secs)
            u_dt = u_datetime.strftime(date_format)
        else:
            u_dt = no_date
        
        return (c_dt, m_dt, u_dt)

    def _request_access_token(self):
        keys = {"RefreshToken": self.KEYS["RefreshToken"]}
        response = requests.post(URL_ACCESS_TOKEN_V2, data=json.dumps(keys))

        if response.ok:
            rd = json.loads(response.text)

            keys = {"Token": rd["AccessToken"], "RefreshToken": self.KEYS["RefreshToken"], "x-api-key": api.API_KEY}
            self.KEYS["Token"] = rd["AccessToken"]

            with open(keys_file, "w") as file:
                file.write(json.dumps(keys))

            return True
        else:
            return False
    
    def _get_token(self):
        expired_time = 0
        if self.KEYS["Token"]:
            try:
                deserialized = jwt.decode(
                    self.KEYS["Token"],
                    options={
                        "verify_signature": False,
                        "verify_aud": False,
                        "verify_nbf": False})
                expired_time = deserialized['exp']
            except jwt.ExpiredSignatureError:
                pass
        else:
            print('Token does not found. Requesting access token')
            self._request_access_token()

        if not self.KEYS["RefreshToken"] or (expired_time != 0 and datetime.datetime.today().timestamp() > expired_time):
            print('Refresh token does not found, or token expired. Requesting access token')
            self._request_access_token()

        return self.KEYS["Token"]

    def check_sum(self, filename, blocksize=65536):
        '''
        When uploading files Degoo uses a 2 step process:
            1) Get Authorisation from the Degoo API - provides metadate needed for step 2
            2) Upload the file to a nominated URL (in practice this appears to be Google Cloud Services)
            
        The upload to Google Cloud services wants a checksum for the file (for upload integrity assurance)
        
        This appears to a base64 encoded SHA1 hash of the file. Empirically this, with a little dressing
        appears to function. The SHA1 hash seems to use a hardcoded string as a seed (based on JS analysis)
                
        :param filename:    The name of the file (full path so it can be read)
        :param blocksize:   Optionally a block size used for reading the file 
        '''
        Seed = bytes([13, 7, 2, 2, 15, 40, 75, 117, 13, 10, 19, 16, 29, 23, 3, 36])
        Hash = hashlib.sha1(Seed)
        with open(filename, "rb") as f:
            for block in iter(lambda: f.read(blocksize), b""):
                Hash.update(block)
                
        cs = list(bytearray(Hash.digest()))
        
        # On one test file we now have:
        # [82, 130, 147, 14, 109, 84, 251, 153, 64, 39, 135, 7, 81, 9, 21, 80, 203, 120, 35, 150]
        # and need to encode this to:
        # [10, 20, 82, 130, 147, 14, 109, 84, 251, 153, 64, 39, 135, 7, 81, 9, 21, 80, 203, 120, 35, 150, 16, 0]
        # Which is four bytes longer, prepended by a word and appended by a word.
        # JS code inspection is non conclusive, it's well obfuscated Webpack JS alas.
        #
        # But a hypothesis is:
        #
        # 10, len(checksum), checksum, 16, type
        # And type is always 0 for file uploads.
        #
        # This passes all tests so far. But remains an hypthesis and not well understood.
        #
        # TODO: Can we move this from an hypothesis to a conclusion?
        CS = [10, len(cs)] + cs + [16, 0]
        
        # Finally, Degoo base64 encode is cehcksum.
        checksum = base64.b64encode(bytes(CS)).decode()
        
        return  checksum

    def rename_file(self, file_id, new_name):
        """
        Rename a file or folder

        :param file_id Id of file or directory
        :param new_name: New name of file or folder
        :return: Message with result of operation
        """

        func = f"setRenameFile(Token: $Token, FileRenames: $FileRenames)"
        query = f"mutation SetRenameFile($Token: String!, $FileRenames: [FileRenameInfo]!) {{ {func} }}"

        request = {"operationName": "SetRenameFile",
                   "variables": {
                       "Token": self._get_token(),
                       "FileRenames": [{
                           "ID": file_id,
                           "NewName": new_name
                       }]
                   },
                   "query": query
                   }

        header = {"x-api-key": self.KEYS["x-api-key"]}

        response = requests.post(URL_API, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getUserInfo failed with: {message}")
            else:
                return rd["data"]['setRenameFile']
        else:
            raise DegooError(f"renameFile failed with: {response}")

    def mv(self, file_id, new_parent_id):
        """
        Move a file or folder to new destination

        :param file_id Id of file or directory
        :param new_parent_id: Id of destination path
        :return: Message with result of operation
        """
        func = f"setMoveFile(Token: $Token, Copy: $Copy, NewParentID: $NewParentID, FileIDs: $FileIDs)"
        query = f"mutation SetMoveFile($Token: String!, $Copy: Boolean, $NewParentID: String!, $FileIDs: [String]!) {{ {func} }}"

        request = {"operationName": "SetMoveFile",
                   "variables": {
                       "Token": self._get_token(),
                       "NewParentID": new_parent_id,
                       "FileIDs": [
                           file_id
                       ]
                   },
                   "query": query
                   }

        header = {"x-api-key": self.KEYS["x-api-key"]}

        response = requests.post(URL_API, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getUserInfo failed with: {message}")
            else:
                return rd["data"]['setMoveFile']
        else:
            raise DegooError(f"renameFile failed with: {response}")
    
    def getUserInfo(self, humanise=True):
        '''
        A Degoo Graph API call: gets information about the logged in user.
         
        :param humanise:  If true converts some properties into a human readable format.
        '''
        args = "\n".join(["Name", "Email", "Phone", "AvatarURL", "AccountType", "UsedQuota", "TotalQuota", "__typename"])
        func = f"getUserInfo(Token: $Token) {{ {args} }}"
        query = f"query GetUserInfo($Token: String!) {{ {func} }}"
        
        request = { "operationName": "GetUserInfo",
                    "variables": {
                        "Token": self._get_token()
                    },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)
            
            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getUserInfo failed with: {message}")
            else: 
                properties = rd["data"]["getUserInfo"]
            
            if properties:
                if humanise:
                    properties['AccountType'] = self.PLANS.get(properties['AccountType'], properties['AccountType'])
                    properties['UsedQuota'] = humanize.naturalsize(int(properties['UsedQuota']))
                    properties['TotalQuota'] = humanize.naturalsize(int(properties['TotalQuota']))
                    del properties['__typename']
                
                return properties
            else:
                return {}
        else:
            raise DegooError(f"getUserInfo failed with: {response}")

    def getOverlay4(self, degoo_id):
        '''
        A Degoo Graph API call: gets information about a degoo item identified by ID.
        
        A Degoo item can be a file or folder but may not be limited to that (see self.CATS). 
         
        :param degoo_id: The ID of the degoo item.
        '''
        args = f"{self.OVERLAY4}"
        func = f"getOverlay4(Token: $Token, ID: $ID) {{ {args} }}"
        query = f"query GetOverlay4($Token: String!, $ID: IDType!) {{ {func} }}"

        request = { "operationName": "GetOverlay4",
                    "variables": {
                        "Token": self._get_token(),
                        "ID": {"FileID": degoo_id}
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)
            
            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getOverlay4 failed with: {message}")
            else: 
                properties = rd["data"]["getOverlay4"]
            
            if properties:
                
                # Add the Category Name
                cat = self.CATS.get(properties['Category'], f"Category {properties['Category']}")
                properties["CategoryName"] = cat
                
                # Fix the FilePath from Degoo incompleteness to a complete path from root.
                if cat in ["Device", "Recycle Bin"]:
                    if cat == "Device":
                        properties["FilePath"] = f"{os.sep}{properties['Name']}"
                    elif cat == "Recycle Bin":
                        dns = device_names()
                        properties["FilePath"] = f"{os.sep}{dns[properties['DeviceID']]}{os.sep}Recycle Bin"
                else:
                    # FilePath includes neither the Device name nor Recylce Bin alas. We 
                    # patch those in here to provide a FilePath that is complete and 
                    # compariable with the web interface UX. 
                    binned = properties["IsInRecycleBin"]
                    dns = device_names()
                    prefix = dns[properties['DeviceID']]+os.sep+"Recycle Bin" if binned else dns[properties['DeviceID']] 
                    properties["FilePath"] = f"{os.sep}{prefix}{properties['FilePath'].replace('/',os.sep)}"
        
                # Convert ID and Sizeto an int. 
                properties["ID"] = int(properties["ID"])
                properties["ParentID"] = int(properties["ParentID"]) if properties["ParentID"] else 0
                properties["MetadataID"] = int(properties["MetadataID"])
                properties["Size"] = int(properties["Size"])

                # Add a set of Human Readable time stamps based om the less readable API timestamps
                times = self._human_readable_times(properties['CreationTime'], properties['LastModificationTime'], properties['LastUploadTime'])

                properties["Time_Created"]      = times[0]
                properties["Time_LastModified"] = times[1]
                properties["Time_LastUpload"]   = times[2]
                
                return properties
            else:
                return {}
        else:
            raise DegooError(f"getOverlay4 failed with: {response.text}")
    
    def getFileChildren5(self, dir_id, next_token=None):
        '''
        A Degoo Graph API call: gets the contents of a Degoo directory (the children of a Degoo item that is a Folder)
        
        :param dir_id: The ID of a Degoo Folder item (might work for other items too?)
        :param next_token: Token for next page
        
        :returns: A list of property dictionaries, one for each child, contianing the properties of that child.
        '''
        args = f"Items {{ {self.PROPERTIES} }} NextToken"
        func = f"getFileChildren5(Token: $Token, ParentID: $ParentID, AllParentIDs: $AllParentIDs, Limit: $Limit, Order: $Order, NextToken: $NextToken) {{ {args} }}"
        query = f"query GetFileChildren5($Token: String!, $ParentID: String, $AllParentIDs: [String], $Limit: Int!, $Order: Int!, $NextToken: String) {{ {func} }}"

        request = { "operationName": "GetFileChildren5",
                    "variables": {
                        "Token": self._get_token(),
                        "ParentID": -1 if dir_id == 0 else f"{dir_id}",
                        "Limit": FILE_CHILDREN_LIMIT,
                        "Order": 3,
                        "NextToken": next_token if next_token else ''
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)

            if 'errors' in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                if "Invalid input!" in messages:
                    print(f"WARNING: Degoo Directory with ID {dir_id} apparently does not to exist!", file=sys.stderr)
                    return []   
                else:
                    message = '\n'.join(messages)
                    raise DegooError(f"getFileChildren5 failed with: {message}")
            else:
                items = rd["data"]["getFileChildren5"]["Items"]
                
                if items:
                    # Fix FilePath by prepending it with a Device name.and converting 
                    # / to os.sep so it becomes a valid os path as well.
                    if dir_id == 0:
                        for i in items:
                            i["FilePath"] = f"{os.sep}{i['Name']}"
                            i["CategoryName"] = self.CATS.get(i['Category'], i['Category'])
                    else:
                        # Get the device names if we're not getting a root dir
                        # device_names calls back here (i.e. uses the getFileChildren5 API call)
                        # with dir_id==0, to get_file the devices. We only need device names to prepend 
                        # paths with if we're looking deeper than root.
                        dns = device_names()
                        
                        for i in items:
                            binned = i["IsInRecycleBin"]
                            cat = self.CATS.get(i['Category'], f"Category {i['Category']}")
                            i["CategoryName"] = cat 
                            
                            # Fix the FilePath from Degoo incompleteness to a complete path.
                            if cat in ["Device", "Recycle Bin"]:
                                if cat == "Device":
                                    i["FilePath"] = f"{os.sep}{i['Name']}"
                                elif cat == "Recycle Bin":
                                    i["FilePath"] = f"{os.sep}{dns[i['DeviceID']]}{os.sep}Recycle Bin"
                            else:
                                # FilePath includes neither the Device name nor Recylce Bin alas. We 
                                # patch those in here to provide a FilePath that is complete and 
                                # compariable with the web interface UX. 
                                binned = i["IsInRecycleBin"]
                                prefix = dns[i['DeviceID']]+os.sep+"Recycle Bin" if binned else dns[i['DeviceID']] 
                                i["FilePath"] = f"{os.sep}{prefix}{i['FilePath'].replace('/',os.sep)}"
            
                    # Convert ID to an int. 
                    for i in items:
                        i["ID"] = int(i["ID"])
                        i["ParentID"] = int(i["ParentID"]) if i["ParentID"] else 0
                        i["MetadataID"] = int(i["MetadataID"])
                        i["Size"] = int(i["Size"])

                        i['isFolder'] = i.get("CategoryName") in api.folder_types
                        
                        # Add a set of Human Readable time stamps based om the less readable API timestamps
                        times = self._human_readable_times(i['CreationTime'], i['LastModificationTime'], i['LastUploadTime'])
        
                        i["Time_Created"]      = times[0]
                        i["Time_LastModified"] = times[1]
                        i["Time_LastUpload"]   = times[2]
                    
                    self.NAMELEN = max([len(i["Name"]) for i in items])

                    next = rd["data"]["getFileChildren5"]["NextToken"]  # @ReservedAssignment
                    if next:
                        next_page = self.getFileChildren5(dir_id, next)
                        items.extend(next_page)
                
                return items
        else:
            raise DegooError(f"getFileChildren5 failed with {response}: {response.text}")

    def getFilesFromPaths(self, device_id, path=""):
        '''
        A Degoo Graph API call: Not sure what this API call is for to be honest. 
        
        It's called after an upload for some reason. But if seems to offer nothing of value.
        
        :param device_id: A Degoo device ID
        :param path:      Don't know what this is.
        '''
        args = f"{self.PROPERTIES}"
        func = f"getFilesFromPaths(Token: $Token, FileIDPaths: $FileIDPaths) {{ {args} }}"
        query = f"query GetFilesFromPaths($Token: String!, $FileIDPaths: [FileIDPath]!) {{ {func} }}"
        
        request = { "operationName": "GetFilesFromPaths",
                    "variables": {
                        "Token": self._get_token(),
                        "FileIDPaths": [{
                            "DeviceID": device_id,
                            "Path": path,
                            "IsInRecycleBin": False
                        }]
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getFilesFromPaths failed with: {message}")
            else:
                # Debugging output
                print("Response Headers:", file=sys.stderr)
                for h in response.headers:
                    print(f"\t{h}: {response.headers[h]}", file=sys.stderr)
                
                rd = json.loads(response.text)
                items = rd["data"]["getFilesFromPaths"]
                return items
        else:            
            raise DegooError(f"getFilesFromPaths failed with: {response}")

    def setDeleteFile5(self, degoo_id):
        '''
        A Degoo Graph API call: Deletes a Degoo item identified by ID. It is moved to the Recycle Bin 
        for the device it was on, and this is not a secure delete. It must be expolicitly deleted 
        from the Recylce bin to be a secure delete.
        
        #TODO: support an option to find and delete the file in the recycle bin,  
        
        :param degoo_id: The ID of a Degoo item to delete.
        '''
        func = f"setDeleteFile5(Token: $Token, IsInRecycleBin: $IsInRecycleBin, IDs: $IDs)"
        query = f"mutation SetDeleteFile5($Token: String!, $IsInRecycleBin: Boolean!, $IDs: [IDType]!) {{ {func} }}"
    
        request = { "operationName": "SetDeleteFile5",
                    "variables": {
                        "Token": self._get_token(),
                        "IDs": [{ "FileID": degoo_id }],
                        "IsInRecycleBin": False,
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"setDeleteFile5 failed with: {message}")
            else:
                return response.text
        else:
            raise DegooError(f"setDeleteFile5 failed with: {response}")

    def setUploadFile3(self, name, parent_id, size="0", checksum="CgAQAg"):
        '''
        A Degoo Graph API call: Appears to create a file in the Degoo filesystem. 
        
        Directories are created with this alone, but files it seems are not stored 
        on the Degoo filesystem at all, just their metadata is, the actual file contents
        are stored on a Google Cloud Service. 
        
        To Add a file means to call getBucketWriteAuth4 to start the process, then
        upload the file content, then finally, create the Degoo file item that then 
        points to the actual file data with a URL. setUploadFile3 does not return 
        that UTL, but getOverlay4 does.
        
        :param name:        The name of the file
        :param parent_id:   The Degoo ID of the Folder it will be placed in  
        :param size:        The files size
        :param checksum:    The files checksum (see self.check_sum)
        '''
        func = f"setUploadFile3(Token: $Token, FileInfos: $FileInfos)"
        query = f"mutation SetUploadFile3($Token: String!, $FileInfos: [FileInfoUpload3]!) {{ {func} }}"
    
        # The size is 0 and checksum is "CgAQAg" when creating folders. 
        #    This seems consistent.
        #
        # For file uploads we need Size and Checksum to be right.
        #    Size is easy (the size of the file should be supplied_
        #    Checksum is a little harder, but it should be supplied ready to plug un here.
        #
        # In practice it turns out Degoo use a SHA1 checksum seeded with what looks
        # like a hardcoded string, and then prefixing it and appending it with some
        # metadata and then encoding it base64. Phew. Hence we leave it to the caller 
        # to provide the checksum.
        request = { "operationName": "SetUploadFile3",
                    "variables": {
                        "Token": self._get_token(),
                        "FileInfos": [{
                            "Checksum": checksum,
                            "Name": name,
                            "CreationTime": int(1000*time.time()),
                            "ParentID": parent_id,
                            "Size": size
                        }]
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
#             print("Degoo Response Headers:", file=sys.stderr)
#             for h in response.headers:
#                 print(f"\t{h}: {response.headers[h]}", file=sys.stderr)
#             print("Degoo Response Content:", file=sys.stderr)
#             print(json.dumps(json.loads(response.content), indent=4), file=sys.stderr)        
#             print("", file=sys.stderr)
            
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"setUploadFile3 failed with: {message}")
            else:
                contents = self.getFileChildren5(parent_id)
                for c in contents:
                    if c['Name'] == name:
                        c['isFolder'] = c.get("CategoryName") in api.folder_types
                        __CACHE_ITEMS__[c['ID']] = c
                        break

                ids = {f["Name"]: int(f["ID"]) for f in contents}
                if not name in ids:
                    obj = get_item(parent_id)
                    print(f"WARNING: Failed to find {name} in {obj['FilePath']} after upload.", file=sys.stderr)
                return ids[name]
        
        else:
            raise DegooError(f"setUploadFile3 failed with: {response}")
        
    def getBucketWriteAuth4(self, dir_id, filename, size, checksum):
        '''
        A Degoo Graph API call: Appears to kick stat the file upload process.
        
        Returns crucial information for actually uploading the file. 
        Not least the URL to upload it to! 
        
        :param dir_id:
        '''
        kv = " {Key Value}"
        args = "\n".join(["PolicyBase64", "Signature", "BaseURL", "KeyPrefix", "AccessKey"+kv, "ACL",  "AdditionalBody"+kv])
        func = f"getBucketWriteAuth4(Token: $Token, ParentID: $ParentID, StorageUploadInfos: $StorageUploadInfos) {{ AuthData {{ {args} }} Error }}"
        query = f"query GetBucketWriteAuth4($Token: String!, $ParentID: String!, $StorageUploadInfos: [StorageUploadInfo2]) {{ {func} }}"
        
        request = { "operationName": "GetBucketWriteAuth4",
                    "variables": {
                        "Token": self._get_token(),
                        "ParentID": f"{dir_id}",
                        "StorageUploadInfos": [{
                            "Checksum": checksum,
                            "FileName": filename,
                            "Size": size
                        }]
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)

            if len(rd['data']['getBucketWriteAuth4']) > 0 and rd['data']['getBucketWriteAuth4'][0]['Error'] is not None:
                messages = []
                for error in rd['data']['getBucketWriteAuth4']:
                    messages.append(error["Error"])
                message = '\n'.join(messages)
                raise DegooError(f"getBucketWriteAuth4 failed with: {message}")
            else:
                # The index 0 suggests maybe if we upload multiple files we get_file mutipple WriteAuths back
                RD = rd["data"]["getBucketWriteAuth4"][0]
                
                return RD
        else:
            raise DegooError(f"getBucketWriteAuth4 failed with: {response}")

    def getSchema(self): 
        '''
        Experimental effort to probe the GRaphQL Schema that Degoo provide.
        
        Not successful yet alas.
        '''
        request = '{"query":"{\n\t__schema: {\n queryType {\n fields{\n name\n }\n }\n }\n}"}'
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if not response.ok:
            raise DegooError(f"getSchema failed with: {response.text}")
        else:
            return response
    
###########################################################################
# Command functions - these are entry points for the CLI tools

def userinfo():
    '''
    Returns information about the logged in user
    '''
    return api.getUserInfo()    

def mkpath(path, verbose=0):
    '''
    Analagous to Linux "mkdir -p", creating all necessary parents along the way.
    
    :param name: A name or path. If it starts with {os.sep} it's interpreted 
                 from root if not it's interpreted from CWD.
    '''
    dirs = split_path(path)        
        
    if dirs[0] == os.sep:
        current_dir = 0
        dirs.pop(0)  # We don't have to make the root dir
    else:
        current_dir = CWD["ID"]

    for d in dirs:
        # If the last char in path is os.sep then the last item in dirs is empty
        if d:
            current_dir = mkdir(d, current_dir, verbose)
            
    return get_item(current_dir)["FilePath"]

def mkdir(name, parent_id=None, verbose=0, dry_run=False):
    '''
    Makes a Degoo directory/folder or device
    
    :param name: The name of a directory/folder to make in the CWD or nominated (by id) parent
    :param parent_id: Optionally a the degoo ID of a parent. If not specified the CWD is used.
    '''
    if parent_id == None and "Path" in CWD:
        parent_id = get_item(CWD["Path"]).get("ID", None)
    
    if parent_id:
        contents = get_children(parent_id)
        existing_names = [f["Name"] for f in contents]
        ids = {f["Name"]: int(f["ID"]) for f in contents}
        if name in existing_names:
            if verbose>0:
                print(f"{name} already exists")
            return ids[name]
        else:
            if not dry_run:
                ID = api.setUploadFile3(name, parent_id)
            else:
                # Dry run, no ID created
                ID = None 
                
            if verbose>0:
                print(f"Created directory {name} with ID {ID}")
                
            return ID
    else:
        raise DegooError(f"mkdir: No parent_id provided.")


def rename(path_file_folder, new_name):
    """
    Rename a file or folder

    :param path_file_folder: Path to file or folder
    :param new_name: New name of file or folder
    :return: Message with result of operation
    """
    old_name = path_file_folder[path_file_folder.rfind('/') + 1:] if '/' in path_file_folder else path_file_folder
    if old_name == new_name:
        raise DegooError(f"rename: Old name and new name \"{new_name}\" cannot be the same")

    if isinstance(path_file_folder, int):
        file_id = path_file_folder
    elif isinstance(path_file_folder, str):
        file_id = path_id(path_file_folder)
    else:
        raise DegooError(f"rm: Illegal file: {path_file_folder}")

    result = api.rename_file(file_id, new_name)
    update_item(file_id)
    return result


def mv(path_file_folder, new_path):
    """
    Move a file or folder

    :param path_file_folder: Path to file or folder
    :param new_path: New path to move the file or folder
    :return: Message with result of operation
    """
    if not is_folder(new_path):
        raise DegooError(f"mv: The target path is not a folder")

    source_path = path_file_folder if is_folder(path_file_folder) else path_file_folder[:path_file_folder.rfind('/')]

    if source_path == new_path:
        raise DegooError(f"mv: The target path cannot be the same as the source path")

    if isinstance(path_file_folder, int):
        file_id = path_file_folder
    elif isinstance(path_file_folder, str):
        file_id = path_id(path_file_folder)
    else:
        raise DegooError(f"rm: Illegal file: {path_file_folder}")

    if isinstance(new_path, int):
        new_parent_id = new_path
    elif isinstance(new_path, str):
        new_parent_id = path_id(new_path)
    else:
        raise DegooError(f"rm: Illegal destination folder: {new_path}")

    result = api.mv(file_id, new_parent_id)
    update_item(file_id)
    return result


def rm(file):
    '''
    Deletes (Removes) a nominated file from the Degoo filesystem.
    
    Unless the remote server deletes the actual file content from the cloud server this is not
    secure of course. In fact it supports trash and removing the file or folder, moves it to
    the Recycle Bin.
         
    :param file: Either a string which specifies a file or an int which provides A Degoo ID.
    '''
    if isinstance(file, int):
        file_id = file
    elif isinstance(file, str):
        file_id = path_id(file)
    else:
        raise DegooError(f"rm: Illegal file: {file}")

    path = api.getOverlay4(file_id)["FilePath"]
    api.setDeleteFile5(file_id)

    if file_id in __CACHE_ITEMS__:
        del __CACHE_ITEMS__[file_id]

    return path

def cd(path):
    '''
    Change the current working directory (in the Degoo filesystem)
    
    :param path: an absolute or relative path. 
    '''
    CWD = get_dir(path)
    with open(cwd_file, "w") as file:
        file.write(json.dumps(CWD))
    return CWD

def device_names():
    '''
    Returns a dictionary of devices, keyed on Degoo ID, containing the name of the device.
    
    Top level folders in the Degoo Filesystem are called devices.
    
    TODO: Degoo's web interface does not currently allow creation of devices even when licensed to. 
    Thus we have no way of working out an API call that does so and we're stuck with devices they 
    give us (even when licensed to have as many as you like). 
    '''
    devices = {}
    root = get_children(0)
    for d in root:
        if d['CategoryName'] == "Device":
            devices[int(d['DeviceID'])] = d['Name']
    return devices 

def device_ids():
    '''
    Returns a dictionary of devices, keyed on name, containing the Degoo ID of the device.
    
    Top level folders in the Degoo Filesystem are called devices.    
    '''
    devices = {}
    root = get_children(0)
    for d in root:
        if d['CategoryName'] == "Device":
            devices[d['Name']] = int(d['DeviceID'])
    return devices 

def get_dir(path=None):
    '''
    Returns a Degoo Directory Dictionary (ddd) for the specified directory.
    
    Is impartial actually, and works for Files and Folders alike. 
    
    :param path: The path (absolute or relative) of a Degoo item. If not specified returns the current working directory.
    '''
    if path:
        item = get_item(path)
    else:
        # Trust the CWD Path more than the ID
        # A known weakness is if we delete a folder and recreate 
        # it the web interface it's recreated with a new ID. By checking
        # the path and getting the ID we confirm it's real.
        item = get_item(CWD['Path'])
    return ddd(item["ID"], item["FilePath"])

def get_parent(degoo_id):
    '''
    Given the Degoo ID returns the Degoo Directory Dictionary (ddd) for the parent directory.
    
    :param degoo_id: The Degoo ID of an item.
    '''
    props = get_item(degoo_id)
    parent = props.get("ParentID", None)
    
    if not parent is None:
        parent_props = get_item(parent)
        return ddd(parent_props.get("ID", None), parent_props.get("FilePath", None))
    else:
        return None    

def path_str(degoo_id):
    '''
    Returns the FilePath property of a Degoo item.
    
    :param degoo_id: The Degoo ID of the item.
    '''
    props = get_item(degoo_id)
    return props.get("FilePath", None)

def parent_id(degoo_id):
    '''
    Returns the Degoo ID of the parent of a Degoo Item.
     
    :param degoo_id: The Degoo ID of the item concerned.
    '''
    props = get_item(degoo_id)
    return props.get("ParentID", None)

def path_id(path):
    '''
    Returns the Degoo ID of the object at path (Folder or File, or whatever). 
    
    If an int is passed just returns that ID but if a str is passed finds the ID and returns it.
    
    if no path is specified returns the ID of the Current Working Directory (CWD).
    
    :param path: An int or str or None (which ask for the current working directory)
    '''
    return get_item(path)["ID"] 

def is_folder(path):
    '''
    Returns true if the remote Degoo item referred to by path is a Folder
    
    :param path: An int or str or None (for the current working directory)
    '''
    return get_item(path)["CategoryName"] in api.folder_types

def get_item(path=None, verbose=0, recursive=False):
    '''
    Return the property dictionary representing a nominated Degoo item.
    
    :param path: An int or str or None (for the current working directory)
    '''
    def props(degoo_id):
        # The root is special, it returns no properties from the degoo API
        # We dummy some up for internal use:
        if degoo_id not in __CACHE_ITEMS__:
            __CACHE_ITEMS__[degoo_id] = api.getOverlay4(degoo_id)
        
        return __CACHE_ITEMS__[degoo_id]

    if path is None:
        if CWD:
            path = CWD["ID"] # Current working directory if it exists
        else:
            path = 0 # Root directory if nowhere else
    elif isinstance(path, str):
        abs_path = absolute_remote_path(path)

        paths = {item["FilePath"]: item for _,item in __CACHE_ITEMS__.items()}
        
        if not recursive and abs_path in paths:
            return paths[abs_path]
        else: 
            parts = split_path(abs_path) # has no ".." parts thanks to absolute_remote_path
            
            if parts[0] == os.sep:
                part_id = 0
                parts.pop(0)
            else:
                part_id = CWD["ID"]
            
            for p in parts:
                if verbose>1:
                    print(f"get_item: getting children of {part_id} hoping for find {p}")
                    
                contents = get_children(part_id)
                ids = {f["Name"]: int(f["ID"]) for f in contents}
                if p in ids:
                    part_id = ids[p]
                else:
                    raise DegooError(f"{p} does not exist in {path_str(part_id)}")
                    
            # Now we have the item ID we can call back with an int part_id.
            return get_item(part_id, verbose, recursive)

    # If recursing we pass in a prop dictionary
    elif recursive and isinstance(path, dict):
        path = path.get("ID", 0)
        
    if isinstance(path, int):
        item = props(path)
        
        if recursive:
            items = {item["FilePath"]: item}
            
            if item["CategoryName"] in api.folder_types:
                if verbose>1:
                    print(f"Recursive get_item descends to {item['FilePath']}")
                
                children = get_children(item)
                
                if verbose>1:
                    print(f"\tand finds {len(children)} children.")
                
                for child in children:
                    if child["CategoryName"] in api.folder_types:
                        items.update(get_item(child, verbose, recursive))
                    else:
                        items[child["FilePath"]] = child
            elif verbose>1:
                print(f"Recursive get_item stops at {item['FilePath']}. Category: {item['CategoryName']}")

            return items
        else:
            return item
    else:
        raise DegooError(f"Illegal path: {path}")

def get_children(directory=None):
    '''
    Returns a list of children (as a property dictionary) of a dominated directory. 
     
    :param directory: The path (absolute of relative) of a Folder item, 
                        the property dictionary representing a Degoo Folder or 
                        None for the current working directory, 
    
    :returns: A list of property dictionaries, one for each child, contianing the properties of that child.
    '''
    if directory is None: 
        if CWD["ID"]:
            dir_id = CWD["ID"]
        else:
            dir_id = 0
    elif isinstance(directory, dict):
        dir_id = directory.get("ID", 0)
    elif isinstance(directory, int):
        dir_id = directory
    elif isinstance(directory, str):
        dir_id = path_id(directory)
    else:
        raise DegooError(f"get_children: Illegal directory: {directory}")
        
    if dir_id not in __CACHE_CONTENTS__:
        __CACHE_CONTENTS__[dir_id] = api.getFileChildren5(dir_id)
        # Having the props of all children we cache those too
        # Can overwrite existing cache as this fetch is more current anyhow 
        for item in __CACHE_CONTENTS__[dir_id]:            
            __CACHE_ITEMS__[item["ID"]] = item
            
    return __CACHE_CONTENTS__[dir_id]

def has_changed(local_filename, remote_path, verbose=0):
    '''
    Determines if a local local_filename has changed since last upload.
     
    :param local_filename: The local local_filename ((full or relative remote_path)
    :param remote_path:    The Degoo path it was uploaded to (can be a Folder or a File, either relative or abolute remote_path)
    :param verbose:        Print useful tracking/diagnostic information
    
    :returns: True if local local_filename has chnaged since last upload, false if not.
    '''
    # We need the local local_filename name, size and last modification time
    Name = os.path.basename(local_filename) 
    Size = os.path.getsize(local_filename)
    LastModificationTime = datetime.datetime.fromtimestamp(os.path.getmtime(local_filename)).astimezone(tz.tzlocal())

    # Get the files' properties either from the folder it's in or the file itself
    # Depending on what was specified in remote_path (the containing folder or the file)
    if is_folder(remote_path):
        files = get_children(remote_path)
        
        sizes = {f["Name"]: int(f["Size"]) for f in files}
        times = {f["Name"]: int(f["LastUploadTime"]) for f in files}

        if Name in sizes:
            Remote_Size = sizes[Name] 
            LastUploadTime = datetime.datetime.utcfromtimestamp(int(times[Name])/1000).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
        else:
            Remote_Size = 0
            LastUploadTime = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
    else:
        props =  get_item(remote_path)
        
        if props:
            Remote_Size = props["Size"]
            LastUploadTime = datetime.datetime.utcfromtimestamp(int(props["LastUploadTime"])/1000).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
        else:
            Remote_Size = 0
            LastUploadTime = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
        
    if verbose>0:
        print(f"{local_filename}: ")
        print(f"\tLocal size: {Size}")
        print(f"\tRemote size: {Remote_Size}")
        print(f"\tLast Modified: {LastModificationTime}")
        print(f"\tLast Uploaded: {LastUploadTime}")
        
    # We only have size and upload time available at present
    # TODO: See if we can coax the check_sum we calculated for upload out of Degoo for testing again against the local check_sum. 
    return Size != Remote_Size or LastModificationTime > LastUploadTime

def get_file(remote_file, local_directory=None, verbose=0, if_missing=False, dry_run=False, schedule=False, onlyUrl=False):
    '''
    Downloads a specified remote_file from Degoo. 
    
    :param remote_file: An int or str or None (for the current working directory)
    :param local_directory: The local directory into which to drop the downloaded file
    :param verbose:    Print useful tracking/diagnostic information
    :param if_missing: Only download the remote_file if it's missing locally (i.e. don't overwrite local files)
    :param dry_run:    Don't actually download the remote_file ... 
    :param schedule:   Respect the configured schedule (i.e download only when schedule permits) 
    
    :returns: the FilePath property of the downloaded remote_file.
    '''
    item = get_item(remote_file)
    
    # If we landed here with a directory rather than remote_file, just redirect
    # to the approproate downloader.
    if item["CategoryName"] in api.folder_types:
        return get_directory(remote_file)

    # Small text files has content directly
    data = item.get('Data', None)
    if data:
        return base64.b64decode(data)

    # Try the Optimized URL first I guess
    URL = item.get("OptimizedURL", None)
    if not URL:
        URL = item.get("URL", None)
        if not URL:
            item = update_item(item["ID"])
            URL = item.get("URL", None)

    if onlyUrl:
        return URL

    Name = item.get("Name", None)
    Path = item.get("FilePath", None)
    Size = item.get('Size', 0)

    # Remember the current working directory
    cwd = os.getcwd()
    
    if local_directory is None:
        dest_file = os.path.join(os.getcwd(), Name)
    elif os.path.isdir(local_directory):
        os.chdir(local_directory)
        dest_file = os.path.join(local_directory, Name)
    else:
        raise DegooError(f"get_file: '{local_directory}' is not a directory.")

    if URL and Name:
        if not if_missing or not os.path.exists(dest_file):
            # We use wget which outputs a progress bar to stdout.
            # This is the only method that writes to stdout because wget does so we do.
            if verbose>0:
                if dry_run:
                    print(f"Would download {Path} to {dest_file}")
                else:
                    print(f"Downloading {Path} to {dest_file}")
                
            # Note:
            #
            # This relies on a special version of wget at:
            #
            #    https://github.com/bernd-wechner/python3-wget
            #
            # which has an upstream PR are:
            #
            #    https://github.com/jamiejackherer/python3-wget/pull/4
            #
            # wget has a bug and while it has a nice progress bar it renders garbage 
            # for Degoo because the download source fails to set the content-length 
            # header. We know the content length from the Degoo metadata though and so
            # can specify it manually.
            #
            # The Degoo API also rejects the User-Agent that urllib provides by default
            # and we need to set one it accepts. Anything works in fact just not the one
            # python-urllib uses, which seems to be blacklisted.  
            
            if not dry_run:
                _ = wget.download(URL, out=Name, size=Size, headers={'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/83.0.4103.61 Chrome/83.0.4103.61 Safari/537.36"})
            
                # The default wqet progress bar leaves cursor at end of line.
                # It fails to print a new line. This causing glitchy printing
                # Easy fixed, 
                print("")
    
            # Having downloaded the file chdir back to where we started
            os.chdir(cwd)
            
            return item["FilePath"]
        else:
            if verbose>1:
                if dry_run:
                    print(f"Would NOT download {Path}")
                else:
                    print(f"Not downloading {Path}")
    else:
        raise DegooError(f"{Path} apparantly has no URL to download from.")

def get_directory(remote_folder, local_directory=None, verbose=0, if_missing=False, dry_run=False, schedule=False):
    '''
    Downloads a Directory and all its contents (recursively).
    
    :param remote_folder: An int or str or None (for the current working directory)
    :param local_directory: The local directory into which to drop the downloaded folder
    :param verbose:    Print useful tracking/diagnostic information
    :param if_missing: Only download files missing locally (i.e don't overwrite local files)
    :param dry_run:    Don't actually download the file ... 
    :param schedule:   Respect the configured schedule (i.e download only when schedule permits) 
    '''
    item = get_item(remote_folder)
    
    # If we landed here with a file rather than folder, just redirect
    # to the approproate downloader.
    if not item["CategoryName"] in api.folder_types:
        return get_file(remote_folder)
    
    dir_id = item['ID']

    # Remember the current working directory
    cwd = os.getcwd()
    
    if local_directory is None:
        pass # all good, just use cwd
    elif os.path.isdir(local_directory):
        os.chdir(local_directory)
    else:
        raise DegooError(f"get_file: '{local_directory}' is not a directory.")
    
    # Make the target direcory if needed    
    try:
        os.mkdir(item['Name'])
    except FileExistsError:
        # Not a problem if the remote folder already exists
        pass
    
    # Step down into the new directory for the downloads
    os.chdir(item['Name'])
    
    # Fetch and classify all Degoo drive contents of this remote folder
    children = get_children(dir_id)

    files = [child for child in children if not child["CategoryName"] in api.folder_types]
    folders = [child for child in children if child["CategoryName"] in api.folder_types]

    # Download files
    for f in files:
        try:
            get_file(f['ID'], local_directory, verbose, if_missing, dry_run, schedule)
            
        # Don't stop on a DegooError, report it but keep going.
        except DegooError as e:
            if verbose>0:
                print(e, file=sys.stderr)

    # Make the local folders and download into them
    for f in folders:
        get_directory(f['ID'], local_directory, verbose, if_missing, dry_run, schedule)

    # Having downloaded all the items in this remote folder chdir back to where we started
    os.chdir(cwd)

def get(remote_path, local_directory=None, verbose=0, if_missing=False, dry_run=False, schedule=False):
    '''
    Downloads a file or folder from Degoo.
    
    :param remote_path: The file or folder (a degoo path) to download
    :param local_directory: The local directory into which to drop the download
    :param verbose:    Print useful tracking/diagnostic information
    :param if_missing: Only download the file if it's missing (i.e. don't overwrite local files)
    :param dry_run:    Don't actually download the file ... 
    :param schedule:   Respect the configured schedule (i.e download only when schedule permits) 
    '''
    item = get_item(remote_path)

    if item["CategoryName"] in api.folder_types:
        return get_directory(item['ID'], local_directory, verbose, if_missing, dry_run, schedule)
    else:
        return get_file(item['ID'], local_directory, verbose, if_missing, dry_run, schedule)

def put_file(local_file, remote_folder, verbose=0, if_changed=False, dry_run=False, schedule=False):
    '''
    Uploads a local_file to the Degoo cloud store.

    TODO: See if we can get a progress bar on this, like we do for the download wget.
    TODO: https://medium.com/google-cloud/google-cloud-storage-signedurl-resumable-upload-with-curl-74f99e41f0a2

    :param local_file:     The local file ((full or relative remote_folder)
    :param remote_folder:  The Degoo folder upload it to (must be a Folder, either relative or abolute path)
    :param verbose:        Print useful tracking/diagnostic information
    :param if_changed:     Only upload the local_file if it's changed
    :param dry_run:        Don't actually upload the local_file ...
    :param schedule:       Respect the configured schedule (i.e upload only when schedule permits)

    :returns: A tuple containing the Degoo ID, Remote file path and the download URL of the local_file.
    '''
    dest = get_item(remote_folder)
    dir_id = dest["ID"]
    dir_path = dest["FilePath"]

    if not is_folder(dir_id):
        raise DegooError(f"put_file: {remote_folder} is not a remote folder!")

    if verbose>1:
        print(f"Asked to upload {local_file} to {dir_path}: {if_changed=} {dry_run=}")

    # Upload only if:
    #    if_changed is False and dry_run is False (neither is true)
    #    if_changed is True and has_changed is true and dry_run is False
    if (not if_changed or has_changed(local_file, remote_folder, verbose-1)):
        if dry_run:
            if verbose>0:
                print(f"Would upload {local_file} to {dir_path}")
        else:
            if verbose>0:
                print(f"Uploading {local_file} to {dir_path}")
            # The steps involved in an upload are 4 and as follows:
            #
            # 1. Call getBucketWriteAuth4 to get the URL and parameters we need for upload
            # 2. Post to the BaseURL provided by that
            # 3. Call setUploadFile3 to inform Degoo it worked and create the Degoo item that maps to it
            # 4. Call getOverlay4 to fetch the Degoo item this created so we can see that worked (and return the download URL)

            filename = os.path.basename(local_file)
            MimeTypeOfFile = mimetypes.guess_type(filename)[0]

            # This one is a bit mysterious. The Key seems to be made up of 4 parts
            # separated by /. The first two are provided by getBucketWriteAuth4 as
            # as the KeyPrefix, the next appears to be the local_file extension, and the
            # last an apparent filename that is consctucted as checksum.extension.
            # Odd, to say the least.
            Type = os.path.splitext(local_file)[1][1:]
            Checksum = api.check_sum(local_file)

            # We need filesize
            Size = os.path.getsize(local_file)

            #################################################################
            ## STEP 1: getBucketWriteAuth4

            # Get the Authorisation to write to this directory
            # Provides the metdata we need for the upload
            result = api.getBucketWriteAuth4(dir_id, filename, Size, Checksum)

            #################################################################
            ## STEP 2: POST to BaseURL

            # Then upload the local_file to the nominated URL
            BaseURL = result["AuthData"]["BaseURL"]

            # We now POST to BaseURL and the body is the local_file but all these fields too
            Signature = result["AuthData"]["Signature"]
            AccessKey = result["AuthData"]["AccessKey"]["Key"]
            AccessValue = result["AuthData"]["AccessKey"]["Value"]
            CacheControl = result["AuthData"]["AdditionalBody"][0]["Value"]  # Only one item in list not sure why indexed
            Policy = result["AuthData"]["PolicyBase64"]
            ACL = result["AuthData"]["ACL"]
            KeyPrefix = result["AuthData"]["KeyPrefix"]  # Has a trailing /

            if Type:
                Key = "{}{}/{}.{}".format(KeyPrefix, Type, Checksum, Type)
            else:
                # TODO: When there is no local_file extension, the Degoo webapp uses "unknown"
                # This requires a little more testing. It seems to work with any value.
                Key = "{}{}/{}.{}".format(KeyPrefix, "@", Checksum, "@")

            # Now upload the local_file
            parts = [
                ('key', (None, Key)),
                ('acl', (None, ACL)),
                ('policy', (None, Policy)),
                ('signature', (None, Signature)),
                (AccessKey, (None, AccessValue)),
                ('Cache-control', (None, CacheControl)),
                ('Content-Type', (None, MimeTypeOfFile)),
                ('file', (os.path.basename(local_file), open(local_file, 'rb'), MimeTypeOfFile))
            ]

            # Perform the upload
            multipart = MultipartEncoder(fields=dict(parts))

            heads = {"ngsw-bypass": "1", "content-type": multipart.content_type, "content-length": str(multipart.len)}

            callback = create_callback(multipart)
            monitor = MultipartEncoderMonitor(multipart, callback)

            response = requests.post(BaseURL, data=monitor, headers=heads)

            # We expect a 204 status result, which is silent acknowledgement of success.
            if response.ok and response.status_code == 204:

                if verbose > 1:
                    print("Google Response Headers:")
                    for h in response.headers:
                        print(f"\t{h}: {response.headers[h]}")
                    print("Google Response Content:")
                    if response.content:
                        print(json.dumps(json.loads(response.content), indent=4))
                    else:
                        print("\tNothing, Nil, Nada, Empty")
                    print("")

#                 # Empirically the download URL seems fairly predictable from the inputs we have.
#                 # with two caveats:
#                 #
#                 # The expiry time is a little different. It's 14 days from now all right but
#                 # now being when setUploadFile3 on the server has as now and not the now we have
#                 # here.
#                 #
#                 # The Signature is new, it's NOT the Signature that getBucketWriteAuth4 returned
#                 # nor any obvious variation upon it (like base64 encoding)
#                 #
#                 # After some testing it's clear that the signature is a base64 encoded signature
#                 # and is generated using a Degoo private key from the URL, which we can't predict.
#                 #
#                 # In fact Google's feedback on a faulty signature is:
#                 # <Error>
#                 #     <Code>SignatureDoesNotMatch</Code>
#                 #     <Message>
#                 #         The request signature we calculated does not match the signature you provided. Check your Google secret key and signing method.
#                 #     </Message>
#                 #     <StringToSign>
#                 #         GET 1593121729 /degoo-production-large-local_file-us-east1.degoo.me/gCkuIp/tISlDA/ChT/gXKXPh2ULNAtufkHfMQ+hE0CSRAA
#                 #     </StringToSign>
#                 # </Error>
#                 #
#                 # That is the Signature provided needs signing using the Degoo private key.
#                 #
#                 # I'd bet that setUploadFile3 given he checksum can build that string and
#                 # using the Degoo private key generate a signature. But alas it doestn't
#                 # return one and so we need to use getOverlay4 to fetch it explicitly.
#                 expiry = str(int((datetime.datetime.utcnow() + datetime.timedelta(days=14)).timestamp()))
#                 expected_URL = "".join([
#                             BaseURL.replace("storage-upload.googleapis.com/",""),
#                             Key,
#                             "?GoogleAccessId=", GoogleAccessId,
#                             "&Expires=", expiry,
#                             "&Signature=", Signature,
#                             "&use-cf-cache=true"]) # @UnusedVariable

                #################################################################
                ## STEP 3: setUploadFile3

                degoo_id = api.setUploadFile3(os.path.basename(local_file), dir_id, Size, Checksum)

                #################################################################
                ## STEP 4: getOverlay4

                props = api.getOverlay4(degoo_id)

                Path = props['FilePath']
                URL = props['URL']

#                 if not URL:
#                     if verbose>0:
#                         print("EXPERIMENT: Trying fallback URL.")
#                     # This won't work!
#                     URL = expected_URL

                return (degoo_id, Path, URL)
            else:
                error_message = str(response.status_code) + ' ' + response.text
                raise DegooError(f"Upload failed with: {error_message}")
    else:
        if dry_run and verbose:
            print(f"Would NOT upload {local_file} to {dir_path} as it has not changed since last upload.")

        children = get_children(dir_id)
        props = {child['Name']: child for child in children}

        filename = os.path.basename(local_file)
        if filename in props:
            ID = props[filename]['ID']
            Path = props[filename]['FilePath']
            URL = props[filename]['URL']

        return (ID, Path, URL)

def put_directory(local_directory, remote_folder, verbose=0, if_changed=False, dry_run=False, schedule=False):
    '''
    Uploads a local directory recursively to the Degoo cloud store.

    :param local_directory: The local directory (full or relative remote_folder) 
    :param remote_folder:    The Degoo folder to upload it to (must be a Folder, either relative or abolute path)
    :param verbose: Print useful tracking/diagnostic information
    :param if_changed: Uploads only files changed since last upload  
    :param dry_run: Don't actually upload anything ...
    :param schedule:   Respect the configured schedule (i.e upload only when schedule permits) 

    :returns: A tuple containing the Degoo ID and the Remote file path
    '''
    IDs = {}
    
    target_dir = get_dir(remote_folder)
    (target_junk, target_name) = os.path.split(local_directory)
    
    Root = target_name
    IDs[Root] = mkdir(target_name, target_dir['ID'], verbose-1, dry_run)
    
    for root, dirs, files in os.walk(local_directory):
        # if local directory contains a head that is included in root then we don't want
        # it when we're making dirs on the remote (cloud) drive and remembering the
        # IDs of those dirs.
        if target_junk:
            relative_root = root.replace(target_junk+os.sep, "", 1)
        else:
            relative_root = root
        
        for name in dirs:
            Name = os.path.join(relative_root, name)
            
            IDs[Name] = mkdir(name, IDs[relative_root], verbose-1, dry_run)
            
        for name in files:
            Name = os.path.join(relative_root, name)
            
            put_file(Name, IDs[relative_root], verbose, if_changed, dry_run, schedule)
    
    # Directories have no download URL, they exist only as Degoo metadata
    return (IDs[Root], target_dir["Path"])
            
def put(local_path, remote_folder, verbose=0, if_changed=False, dry_run=False, schedule=False):
    '''
    Uplads a file or folder to the Degoo cloud store
    
    :param local_path: The path (absolute or relative) of a local file or folder
    :param remote_folder: The Degoo path to upload it to (must be a Folder, either relative or absolute path)
    :param verbose: Print useful tracking/diagnostic information
    :param if_changed: Uploads only files changed since last upload  
    :param schedule:   Respect the configured schedule (i.e upload only when schedule permits) 
    '''
    isFile = os.path.isfile(local_path)
    isDirectory = os.path.isdir(local_path)
    
    if isDirectory:
        return put_directory(local_path, remote_folder, verbose, if_changed, dry_run, schedule)
    elif isFile:
        return put_file(local_path, remote_folder, verbose, if_changed, dry_run, schedule)
    else:
        return None
    

def ls(directory=None, long=False, recursive=False):
    if recursive:
        props = get_item(directory)
        print(f"{props['FilePath']}:")
        
    items = get_children(directory)
    
    for i in items:
        if long:
            times = f"c:{i['Time_Created']}\tm:{i['Time_LastModified']}\tu:{i['Time_LastUpload']}"
            print(f"{i['ID']}\t{i['CategoryName']:{api.CATLEN}s}\t{i['Name']:{api.NAMELEN}s}\t{times}")
        else:
            print(f"{i['Name']}")
            
    if recursive:
        print('')
        for i in items:
            if i['CategoryName'] in api.folder_types:
                ls(i['ID'], long, recursive)
        

def tree(dir_id=0, show_times=False, _done=[], show_tree=False, mode='lazy'):
    T = "├── "
    I = "│   "
    L = "└── "
    E = "    "

    # Print name of the root item in the tree
    if not _done:
        props = get_item(dir_id)
        name = props.get("FilePath", "")
        if show_tree:
            print(name)

    kids = get_children(dir_id)
    
    if kids:
        last_id = kids[-1]['ID']
        for kid in kids:
            ID = kid['ID']
            name = kid.get("Name", "")
            cat = kid.get("CategoryName", kid.get("Category", None))
            kid['isFolder'] = kid.get("CategoryName") in api.folder_types
            
            postfix = ""
            if show_times:               
                postfix = f" (c:{kid['Time_Created']}, m:{kid['Time_LastModified']}, u:{kid['Time_LastUpload']})"
            
            prefix = "".join([E if d else I for d in _done])
            prefix = prefix + (L if ID == last_id else T)

            if show_tree:
                print(prefix + name + postfix)
            
            if cat in api.folder_types and mode == 'eager':
                new_done = _done.copy()
                new_done.append(ID == last_id)
                tree(ID, show_times, new_done, mode=mode)


def get_cached_items():
    return __CACHE_ITEMS__


def tree_cache(dir_id=0, show_times=False, _done=[], mode='lazy'):
    global __CACHE_CONTENTS__
    global __CACHE_ITEMS__

    root_path = {
        "ID": 0,
        "ParentID": None,
        "Name": "/",
        "FilePath": "/",
        "Category": None,
        "CategoryName": "Root",
    }
    __CACHE_CONTENTS__ = {}
    __CACHE_ITEMS__ = {0: root_path, 1: root_path}
    tree(dir_id, show_times, _done, mode=mode)
    return __CACHE_ITEMS__


def get_url_file(remote_file):
    return get_file(remote_file, onlyUrl=True)


def update_item(file_id):
    item = api.getOverlay4(file_id)
    item['isFolder'] = item.get("CategoryName") in api.folder_types
    __CACHE_ITEMS__[file_id] = item
    return item


def create_callback(encoder):
    encoder_len = encoder.len
    bar = ProgressBar(expected_size=encoder_len, filled_char='#', hide=False)

    def callback(monitor):
        bar.show(monitor.bytes_read)

    return callback
