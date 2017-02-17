from __future__ import print_function
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.files import GoogleDriveFile, FileNotUploadedError, ApiRequestError
from base64 import b64decode, b64encode

from Encryption import RSAAESEncryption
from CCBackEnd import *
import sys
import re
import xmltodict

# Ipython autocomplete
try:
    # >=ipython-1.0
    from IPython import get_ipython
except ImportError:
    try:
        # support >=ipython-0.11, <ipython-1.0
        from IPython.core.ipapi import get as get_ipython
    except ImportError:
        # support <ipython-0.11
        try:
            from IPython.ipapi import get as get_ipython
        except ImportError:
            print('Not ipython')

if sys.version_info.major > 2:
    raw_input = input  # future compatibility


class NotS3Error(RuntimeError):
    """This is not an S3 backend"""


class GDriveClient(CCBackEnd):
    """
    Google Drive client based on PyDrive, which is based on google apis.
    To use, you need to enable gdrive APIs and make an OAuth2 id
    """

    @staticmethod
    def Authenticate(secrets, credentials):
        """
        Authenticates with Gdrive
        @param secrets: 		str, name of client secrets file
        @param credentials: 	str, name of saved credentials file
        @return:	GoogleAuth object with authentications
        """
        authenticator = GoogleAuth()
        authenticator.settings = {
            'client_config_backend': 'file',
            'client_config_file':    secrets,
            'save_credentials':      False,
            'oauth_scope':           ['https://www.googleapis.com/auth/drive']
        }

        # get credentials
        authenticator.LoadCredentialsFile(credentials)
        if authenticator.credentials is None:  # no credentials
            response = raw_input('No credentials. Authenticate with web browser? [y]/n > ')
            if response.lower() in ['y', 'yes'] or len(response) == 0:
                authenticator.LocalWebserverAuth()
            else:
                authenticator.CommandLineAuth()
        elif authenticator.access_token_expired:  # stale credentials
            authenticator.Refresh()
        else:
            authenticator.Authorize()

        authenticator.SaveCredentialsFile(credentials)  # update/save credentials

        return authenticator

    def __init__(self, secrets = 'client_secrets.json', credentials = 'gdrive-credentials.txt', config = None):
        """
        Constructor
        @param secrets:			str, name of client secrets file
        @param credentials: 	str, name of saved credentials file
        @param config:			str, name of config file, if given will override other settings
        """

        # fields
        self.drive = None
        self.current_directory_object = None
        self.dir = None
        self.service = None
        self.keyFolder = None
        self.rootID = None

        if not config:
            ### Authentication
            authenticator = GDriveClient.Authenticate(secrets, credentials)

            ### Get actual drive client
            self.initialise_drive(authenticator)
        else:
            self.load_config(config)

        # if running in ipython, hook in autocomplete
        try:
            self.hook_ipython_completer()
        except Exception as e:
            print(e.__str__())

    def initialise_drive(self, authenticator):
        """
        Initializes internal gdrive client
        @param authenticator: 	authenticated GoogleAuth instance
        @return:
        """
        self.drive = GoogleDrive(authenticator)
        self.current_directory_object = self.get_file_by_ID('root')  # cmd-like movement in google drive structure
        self.rootID = self.currentDirectoryID  # remember root ID
        self.dir = '/'
        self.list_objects(True)  # this call inits self.drive.auth.service, which exposes lower-level api calls
        self.service = self.drive.auth.service  # less typing XD

    def load_config(self, file_name):
        """
        Load a config file
        @param file_name: 	str, name of file
        @return:
        """
        with open(file_name) as configFile:
            config = xmltodict.parse(configFile.read())

        authenticator = self.Authenticate(config['DriveClient-Config']['Authentication']['Client-Secrets'], config['DriveClient-Config']['Authentication']['Credentials'])
        self.initialise_drive(authenticator)

        if bool(config['DriveClient-Config']['FileIO']['@Encryption']):
            self.encryptor = RSAAESEncryption(config['DriveClient-Config']['FileIO']['Encryption']['RSAkey'])
            self.keyFolder = config['DriveClient-Config']['FileIO']['Encryption']['KeyFolder']

    def save_config(self, file_name):
        """
        Saves the current configs
        @param file_name:
        @return:
        """

    @property
    def currentDirectoryID(self):
        """
        ID of the current object
        @return: 	str
        """
        if self.current_directory_object.metadata['id'] == self.rootID:
            return 'root'
        else:
            return self.current_directory_object.metadata['id']

    #### Navigation commands

    @property
    def pwd(self):
        """
        Prints current working directory, no quotes attached
        @return:
        """
        print(self.dir)
        return None

    def list_directory(self, path, limit):
        if path is not None:
            path = re.sub('^\./', '', path)
        curDirObj = self.current_directory_object
        if path is not None and len(path) > 0:
            if not self.cd(path):
                return
        list = self.list_objects(namesOnly = True)
        if path is not None:
            self.current_directory_object = curDirObj
            self.rebuild_current_path()
        return list

    def ls(self, directory = None):
        """
        Prints the current folder's contents
        @return: Nothing
        """
        if directory is not None:
            directory = re.sub('^\./', '', directory)
        curDirObj = self.current_directory_object
        if directory is not None and len(directory) > 0:
            if not self.cd(directory):
                return
        list = self.list_objects()
        for item in list:
            print('{} {}'.format('d' if item['mimeType'] == 'application/vnd.google-apps.folder' else ' ', item['title']))
        if directory is not None:
            self.current_directory_object = curDirObj
            self.rebuild_current_path()

    def cd(self, directory = None, make_if_not_exist = False, isID = False):
        """
        Changes the current directory
        @param directory:			str, where to?
        @param make_if_not_exist:	bool, should the directory be made if it does not exist? (Mainly used for uploading)
        @param isID:				bool, is the specified directory an id instead of a name? Can CD immediately there.
        @return:	bool, success of action
        """
        if directory is None or directory == '.':
            print(self.dir)
            return True

        if isID:
            try:
                self.current_directory_object = self.get_file_by_ID(directory)
                self.rebuild_current_path()
                return True
            except FileNotFoundError:
                return False

        directory = re.sub('^\./', '', directory)

        remainder = None
        if '/' in directory:  # not a simple change, so we just process the first one only each turn
            directory = re.sub('/$', '', directory)
            if '/' in directory:  # if it wasn't just a ending slash
                dirs = re.split('/', directory, 1)
                directory = dirs[0]
                remainder = dirs[1]

        if directory == '..':  # move up one
            if self.current_directory_object['parents'][0]['isRoot']:
                self.current_directory_object = self.get_file_by_ID('root')
            else:
                self.current_directory_object = self.get_file_by_ID(self.current_directory_object['parents'][0]['id'])
        elif directory in ['/', '']:  # change to root
            self.current_directory_object = self.get_file_by_ID('root')
        else:  # move down one
            directories = self.drive.ListFile({'q': 'title="{}" and "{}" in parents and mimeType = \'application/vnd.google-apps.folder\' and trashed=false'.format(directory, self.currentDirectoryID)}).GetList()
            if len(directories) < 1:
                if not make_if_not_exist:
                    print('No such directory: {}'.format(directory))
                    return False
                else:
                    directories.append(self.get_file_by_ID(self.mkdir(directory)))
            if len(directories) > 1:
                print('For some reason there\'s more than one result')
                for item in directories:
                    print('{}\t{}'.format(item['id'], item['title']))
                return False
            self.current_directory_object = directories[0]

        self.rebuild_current_path()
        # recursively cd to the innermost folder
        if remainder is not None:
            return self.cd(remainder)
        return True

    def mkdir(self, folder_name):
        """
        Make directory
        @param folder_name:	str, name of new folder
        @return:			str, id of new folder
        """
        # name formatting
        folder_name = re.sub('^\./', '', folder_name)

        # not current dir
        curDirObj = None
        if '/' in folder_name:
            curDirObj = self.current_directory_object
            tokens = re.split('/', folder_name)
            for i in range(len(tokens) - 1):
                if not self.cd(tokens[i], True):
                    return None
            folder_name = tokens[-1]

        directories = self.drive.ListFile({'q': 'title="{}" and "{}" in parents and mimeType = \'application/vnd.google-apps.folder\' and trashed=false'.format(folder_name, self.currentDirectoryID)}).GetList()
        if len(directories) > 0:
            print('Folder already exists')
            return None

        folder = self.drive.CreateFile({'title': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [{'id': self.currentDirectoryID}]})
        folder.Upload()

        if curDirObj is not None:
            self.current_directory_object = curDirObj
            self.rebuild_current_path()
        return folder.metadata['id']

    def move(self, file, destination, sb = None, db = None, o = None):
        """
        Moves a file to another folder
        @param file:
        @param destination:
        @return:
        """

        # TODO: renaming in mv
        originDir = re.match('.*/', file)
        destDir = re.match('.*/', destination)
        if originDir is destDir:
            print('renaming using move is not supported yet. use the .rename method')
            return False
        if originDir.group(0) == destDir.group(0):
            print('renaming using move is not supported yet. use the .rename method')
            return False

        return self.update_metadata(file, {'parents': [{'id': self.get_ID_by_name(destination)}]})

    def rename(self, file, newName):
        """
        Renames a file
        @param file:		str, name of file
        @param newName:		str, new name
        @return:
        """

        # TODO: name validation
        if '/' in newName:
            print('No slashes in name, please')
            return False

        return self.update_metadata(file, {'title': newName})

    def copy(self, original_name, copy_name, sb = None, db = None, o = None):
        """
        Copys a file
        @param original_name: 	str, name of file
        @param copy_name: 		str, name of copy
        @return:
        """
        # TODO: directory copying and deep/shallowness
        try:
            original = self.get_file_by_name(original_name)
            self.upload_stream(copy_name, original.content)
            copyID = self.get_ID_by_name(copy_name)
            if 'properties' in original.metadata.keys() and len(original.metadata['properties'].keys()) > 0:
                for key in original.metadata['properties'].keys():
                    if not self.insert_property(copyID, key, original.metadata['properties'][key]):
                        print('Property copy for {} failed'.format(key))
            return True
        except Exception as e:
            print (e.__str__())
            return False

    def delete(self, file_name, recursive = False, delete = False):
        """
        Deletes a file on drive. Works for both files and folders. Will apply action to everything inside folder.
        @param file_name:		str, name of file to delete
        @param recursive: 		bool, recursive delete folder?
        @param delete:			bool, if true, hard deletes, if false, trashes the file
        @return:				bool, success of action
        """
        try:
            fileID = self.get_ID_by_name(file_name)
            f = self.drive.CreateFile({'id': fileID})
            f.FetchMetadata()
            if f.metadata['mimeType'] == 'application/vnd.google-apps.folder' and not recursive:
                print('Is folder and recursive delete is not selected')
                return False
            if delete:
                f.delete()
            else:
                f.Trash()  # trashed files still exist so the association count remains the same
            return True
        except FileNotFoundError:
            print('File not found')
            return False

    #### File IO functions

    def upload_file(self, file_name, cloud_name = None, permissions = None):
        """
        Uploads a file/file-like object to the current directory.
        Will automatically multi-part
        @param file_name:			str, name of local file
        @param cloud_name:			str, name on drive for file
        @param permissions:			None, for signature compatibility with S3
        @return: bool, upload success
        """
        if cloud_name is None:
            cloud_name = file_name

        if type(file_name) != str:
            # assume this is a file_name-like object
            self.upload_stream(file_name, cloud_name)

        # cloud_name formatting
        cloud_name = re.sub('^\./', '', cloud_name)

        # not current dir
        curDirObj = None
        if '/' in cloud_name:
            curDirObj = self.current_directory_object
            tokens = re.split('/', cloud_name)
            for i in range(len(tokens) - 1):
                if not self.cd(tokens[i], True):
                    return False
            cloud_name = tokens[-1]

        # metadata
        metadata = {}
        metadata['title'] = cloud_name
        metadata['parents'] = [{'id': self.currentDirectoryID}]

        # try to upload the file_name
        newFile = self.drive.CreateFile(metadata)
        newFile.Upload()
        try:
            newFile.SetContentFile(file_name)
            newFile.Upload()
        except Exception as e:
            print('Error uploading file_name:\n{}'.format(e.__str__()))
            newFile.delete()
            return False

        if curDirObj is not None:
            self.current_directory_object = curDirObj
            self.rebuild_current_path()

        return True

    def upload_stream(self, fileObj, name, properties = None, permissions = None):
        """
        Uploads a file from a file-like object in memory that has a .read() function
        Will automatically multi-part
        @param name:			str, cloudName on drive for file
        @param fileObj:			file-like, stream object with data
        @param properties:		dict, custom metadata values
        @param permissions:		None, for signature compatibility with S3
        @return:				bool, success of action
        """
        # cloudName formatting
        name = re.sub('^./', '', name)

        # not current dir
        curDirObj = None
        if '/' in name:
            curDirObj = self.current_directory_object
            tokens = re.split('/', name)
            for i in range(len(tokens) - 1):
                if not self.cd(tokens[i], True):
                    return False
            name = tokens[-1]

        # metadata
        metadata = {}
        metadata['title'] = name
        metadata['parents'] = [{'id': self.currentDirectoryID}]

        newFile = self.drive.CreateFile(metadata)
        newFile.Upload()
        try:
            newFile.content = fileObj
            newFile.Upload()
        except Exception as e:
            print('Error uploading stream:\n{}'.format(e.__str__()))
            newFile.delete()
            return False

        if properties is not None:
            for key in properties.keys():
                if not self.insert_property(newFile.metadata['id'], key, properties[key]):
                    print('Property insertion for {} failed'.format(key))

        if curDirObj is not None:
            self.current_directory_object = curDirObj
            self.rebuild_current_path()

        return True

    def upload_multipart(self, stream, cloudName, properties = None, permissions = None):
        return self.upload_stream(stream, cloudName, properties, permissions)


    def Download(self, driveFile, localFile = None):
        """
        Download file to disk
        @param driveFile:	str, cloudName of file on g drive
        @param localFile:	str, cloudName of file to save to, if None, will use same cloudName as g-drive file
        @return:
        """

        try:
            f = self.drive.CreateFile({'id': self.get_ID_by_name(driveFile)})
        except FileNotFoundError:
            print('File not found')
            return False

        f.FetchMetadata()
        if localFile is None:
            localFile = f.metadata['title']
        f.GetContentFile(localFile)

        return True

    def download_stream(self, driveFile):
        """
        Download file to memory
        @param driveFile:	str, cloudName of file on g-drive
        @return:			StringIO object if mode == 'r', ByteIO if 'b'
        @raise FileNotFoundError:	 when file is not found
        """

        try:
            f = self.drive.CreateFile({'id': self.get_ID_by_name(driveFile)})
        except FileNotFoundError:
            print('File not found')
            raise FileNotFoundError

        f.FetchMetadata()
        properties = self.get_file_properties(f.metadata['id'])
        if 'key' in properties:
            properties['key'] = self.get_encryption_key(f.metadata['id'])

        if f.content is None:
            f.FetchContent()
        return CloudStream(f.content, properties)

    #### Misc helper functions

    def check_ID_exists(self, id):
        """
        Checks if a file with the given id exists on google drive
        @param id:
        @return:
        """
        metadata = {'id': id}
        item = GoogleDriveFile(self.drive.auth, metadata, uploaded = True)
        try:
            item.FetchMetadata()
            return True
        except (FileNotUploadedError, ApiRequestError):
            return False
        print('This line should not be executed.')

    def check_file_exists(self, file_name, bucketName):
        """
        Checks if a file with the given cloudName exists on google drive
        @param file_name:
        @param bucketName:		useless, for compatibility reasons with s3 client
        @return:
        """
        try:
            self.get_ID_by_name(file_name)
            return True
        except FileNotFoundError:
            return False
        print('This line should not be executed.')


    def rebuild_current_path(self):
        """
        After a cd, rebuilds the current working directory string
        @return:
        """
        if self.currentDirectoryID == 'root':
            self.dir = '/'
            return

        self.dir = ''
        thisDir = self.current_directory_object
        self.dir = '/' + thisDir['title'] + self.dir
        while (not thisDir['parents'][0]['isRoot']):
            thisDir = self.get_file_by_ID(self.current_directory_object['parents'][0]['id'])
            self.dir = '/' + thisDir['title'] + self.dir
        self.dir = str(self.dir)

    def list_objects(self, namesOnly = False, includeTrash = False):
        """
        Gets a list of items in the current folder
        @param namesOnly:		bool, if true, returns a list of strings of names, if false returns full drive file objects
        @param includeTrash:	bool, look in trashed files?
        @return: list<GoogleDriveFile>|list<str>
        """
        if namesOnly:
            files = self.drive.ListFile({'q': "'{}' in parents and trashed={}".format(self.currentDirectoryID, str(includeTrash).lower())}).GetList()
            out = []
            for f in files:
                out.append(f['title'])
            return out
        return self.drive.ListFile({'q': "'{}' in parents and trashed={}".format(self.currentDirectoryID, str(includeTrash).lower())}).GetList()

    def get_file_by_name(self, name):
        """
        Gets the file corresponding to a cloudName
        @param name:
        @return:
        """
        return self.get_file_by_ID(self.get_ID_by_name(name))

    def get_file_by_ID(self, id):
        """
        Gets a file by google's ID for it
        @param id: 	str, id of object
        @return:	GoogleDriveFile
        """
        metadata = {'id': id}
        item = GoogleDriveFile(self.drive.auth, metadata, uploaded = True)
        try:
            item.FetchMetadata()
        except (FileNotUploadedError, ApiRequestError):
            print('File does not exist')
            return None
        return item

    def get_ID_by_name(self, file_name):
        """
        Gets a file's ID by its cloudName
        @param file_name:	str, file cloudName
        @return:	str, id of the file
        @raise FileNotFoundError when file not found
        """

        drive_file = re.sub('^\./', '', file_name)

        if file_name == '/':
            return 'root'

        # not current dir
        curDirObj = None
        if '/' in drive_file:
            curDirObj = self.current_directory_object
            tokens = re.split('/', drive_file)
            if ('' in tokens):
                tokens.remove('')
            for i in range(len(tokens) - 1):
                if not self.cd(tokens[i]):
                    raise FileNotFoundError
            drive_file = tokens[-1]

        items = self.list_objects()
        item = None
        for i in items:
            if i['title'] == drive_file:
                item = i
                break

        if curDirObj is not None:
            self.current_directory_object = curDirObj
            self.rebuild_current_path()

        if item is not None:
            return item['id']
        else:
            raise FileNotFoundError

    def insert_property(self, id, key, value, visibility = 'PUBLIC'):
        """
        Adds a custom property to a file
        @param id:			str, id of file to add property to
        @param key:			str, key of property
        @param value:		str, value of property
        @param visibility:	'PUBLIC'|'PRIVATE'
        @return:
        """

        if visibility not in ['PUBLIC', 'PRIVATE']:
            raise ValueError('Bad visibility value')

        if key == 'key':
            return self.store_encryption_key(id, value)

        body = {'key': key, 'value': value, visibility: visibility}
        # print('Adding {}: {}'.format(key, value))

        try:
            self.service.properties().insert(fileId = id, body = body).execute()
            return True
        except Exception as e:
            print('Error: {}'.format(e.__str__()))
            return False

    def get_file_properties(self, id):
        """
        Gets the properties, i.e. custom metadata, of a file
        @param id:	str, id of the file
        @return:	dict
        """
        try:
            f = self.get_file_by_ID(id)
            if f is None:
                print('File does not exist')
            properties = {}
            f.FetchMetadata()
            if 'properties' in f.metadata.keys():
                for p in f.metadata['properties']:
                    properties[p['key']] = p['value']
            return properties
        except Exception as e:
            print('Error: {}'.format(e.__str__()))
            return None

    def store_encryption_key(self, fileID, key64, chunkSize = 96):
        """
        Stores a key to the custom properties of a file
        @param fileID:		str, id of file to store key to
        @param key65:		str, RSA encrypted key in base64 encoding
        @param chunkSize:	int,  bytes per chunk of key as a metadata entry
        @return:
        """
        if chunkSize > 114:
            print('Chunk size set to 114 because of limitations of metadata size')
            chunkSize = 114
        nChuncks = len(key64) / chunkSize + (len(key64) % chunkSize > 0)
        self.insert_property(fileID, 'keyChunks', str(nChuncks))
        self.insert_property(fileID, 'key', 'in chunks')
        for i in range(nChuncks):
            start = i * chunkSize
            end = (i + 1) * chunkSize
            end = end if end < len(key64) else len(key64)
            if not self.insert_property(fileID, 'keyChunk{}'.format(i), key64[start:end]):
                print('Key upload failed')
                return

    def get_encryption_key(self, fileID):
        """
        Gets the stored and chunked key
        @param fileID:	str, id of file
        @return: RSA encrypted key encoded in base 64
        """
        key64 = ''
        properties = self.get_file_properties(fileID)
        if 'keyChunks' not in properties:
            print('No stored key')
            return None
        else:
            nChunks = int(properties['keyChunks'])
            for i in range(nChunks):
                key64 += properties['keyChunk{}'.format(i)]
            return key64

    def update_metadata(self, file_name, metadata):
        """
        Updates the metadata on a file
        @param file_name: 	str, cloudName of file (not id)
        @param metadata: 	dict, updated values
        @return: 	bool, success of action
        """
        try:
            drive_file = self.service.files().get(fileId = self.get_ID_by_name(file_name)).execute()
            for key in metadata.keys():
                drive_file[key] = metadata[key]
            self.service.files().update(fileId = self.get_ID_by_name(file_name), body = drive_file).execute()
            return True
        except FileNotFoundError:
            print('File not found')
            return False
        except Exception as e:
            print('Update failed')
            print(e.__str__())
            return False

    @property
    def size(self):
        files = self.drive.ListFile({'q': "trashed=false"}).GetList()
        sizes = [f.metadata['size'] for f in files]
        return sum(sizes)

    def completer(self, context, event):
        """
        IPython autocompleter calculations.
        @param context: 	I have no idea what this is
        @param event: 		ipy's event info about what the user typed, etc
        @return:
        """
        # print(event.line)
        path = re.sub('(?:.*\=)?(.+?)(\.(ls|cd|Download.?|rm|mv|rename|CheckFileExists|CheckIDExists))\([\'"]', '', event.line)  # remove the user command, leaving only the path
        path = re.sub('.+[\'"]\s?,\s?[\'"]', '', path)  # for cases like mv where there may be a second argument, removes the first arg if complete
        # path = re.sub('^\./', '', path)

        if '/' in path:
            # print('/ foound')
            directory = re.match('^.*/', path).group(0)
            path = re.search('[^/]+$', path)
            if path is not None:
                path = path.group(0)
            # print('\n{}\n{}\n'.format(directory, path))

            curDirObj = self.current_directory_object
            if directory is not None:
                if not self.cd(directory):  # if the directory doesn't actually exist, return empty list
                    return []
            items = self.list_objects(True)
            if directory is not None:
                self.current_directory_object = curDirObj
                self.rebuild_current_path()
            out = []
            for item in items:
                out.append(directory + item)
            # print(out)
            return out
        else:
            items = self.list_objects(True)

            if path is None:
                return items
            out = []
            for item in items:
                # if re.match(path, item) is not None:	# Ipython takes care of filtering the matches
                out.append(item)
            return out

    def hook_ipython_completer(self):
        """
        Hooks the autocompleter into ipython
        @return:
        """
        ipython = get_ipython()
        if ipython is None:
            return
        ipython.set_hook('complete_command', self.completer, re_key = r'(?:.*\=)?(.+?)(\.(ls|cd|Download|rm|DownloadRawArray|DownloadStream|mv|rename|CheckFileExists|CheckIDExists))\([\'"].*')

    #### For more informative errors
    def check_bucket_exists(self):
        raise NotS3Error

    def create_bucket(self):
        raise NotS3Error

    def set_current_bucket(self):
        raise NotS3Error

    def get_bucket(self):
        raise NotS3Error

    def show_all_buckets(self):
        raise NotS3Error

    def get_s3_object(self, o, b):
        raise NotS3Error

    def get_current_bucket_size(self):
        raise NotS3Error

    def _get_bucket_name(self, b):
        raise NotS3Error