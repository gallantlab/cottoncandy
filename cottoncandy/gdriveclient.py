from __future__ import print_function
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.files import GoogleDriveFile, FileNotUploadedError, ApiRequestError
from .backend import *
import sys
import re

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


class GDriveClient(CCBackEnd):
    """
    Google Drive client based on PyDrive, which is based on google apis.
    To use, you need to enable gdrive APIs and make an OAuth2 id
    """

    @staticmethod
    def Authenticate(secrets, credentials):
        """

        Parameters
        ----------
        secrets : str
            path to client secrets json file
        credentials : str
            path to credentials file
        Returns
        -------
        authenticator : GoogleAuth
            google drive authentication object
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
            response = raw_input('No credentials. Authenticate with local web browser? [y]/n > ')
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

    def __init__(self, secrets='client_secrets.json', credentials='gdrive-credentials.txt'):
        """

        Parameters
        ----------
        secrets : str
            path to client secrets json file
        credentials : str
            path to credentials file

        Returns
        -------

        """

        super(GDriveClient, self).__init__()

        # fields
        self.drive = None
        self.current_directory_object = None
        self.dir = None
        self.service = None
        self.rootID = None

        ### Authentication
        authenticator = GDriveClient.Authenticate(secrets, credentials)

        ### Get actual drive client
        self.initialise_drive(authenticator)

        # if running in ipython, hook in autocomplete
        try:
            self.hook_ipython_completer()
        except Exception as e:
            print(e.__str__())

    def initialise_drive(self, authenticator):
        """

        Parameters
        ----------
        authenticator : GoogleAuth
            authenticated GoogleAuth object

        Returns
        -------

        """
        self.drive = GoogleDrive(authenticator)
        self.current_directory_object = self.get_file_by_ID('root')  # cmd-like movement in google drive structure
        self.rootID = self.current_directory_id  # remember root ID
        self.dir = '/'
        self.list_objects(True)  # this call inits self.drive.auth.service, which exposes lower-level api calls
        self.service = self.drive.auth.service  # less typing XD

    @property
    def current_directory_id(self):
        """
        ID of current directory
        Returns
        -------
        id : str
            Google UUID
        """
        if self.current_directory_object.metadata['id'] == self.rootID:
            return 'root'
        else:
            return self.current_directory_object.metadata['id']

    #### Navigation commands

    @property
    def pwd(self):
        """
        Prints current working directory

        Returns
        -------
        None
        """
        print(self.dir)
        return None

    def list_directory(self, path, limit):
        if path is not None:
            path = re.sub('^\./', '', path)
        current_directory = self.current_directory_object
        if path is not None and len(path) > 0:
            if not self.cd(path):
                return
        list = self.list_objects(namesOnly = True)
        if path is not None:
            self.current_directory_object = current_directory
            self.rebuild_current_path()
        return list

    def ls(self, directory=None):
        """
        Prints contents of a folder

        Parameters
        ----------
        directory : str
            folder to list, if None defaults to pwd

        Returns
        -------
        None
        """
        if directory is not None:
            directory = re.sub('^\./', '', directory)
        current_directory = self.current_directory_object
        if directory is not None and len(directory) > 0:
            if not self.cd(directory):
                return
        list = self.list_objects()
        for item in list:
            print('{} {}'.format('d' if item['mimeType'] == 'application/vnd.google-apps.folder' else ' ', item['title']))
        if directory is not None:
            self.current_directory_object = current_directory
            self.rebuild_current_path()

    def cd(self, directory=None, make_if_not_exist=False, isID=False):
        """
        Changes directory

        Parameters
        ----------
        directory : str
            path
        make_if_not_exist : bool
            make the directory if it doesn't exist?
        isID : bool
            is `directory` a Google UUID?
        Returns
        -------
        : bool
            success of cd operation
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
            directories = self.drive.ListFile({'q': 'title="{}" and "{}" in parents and mimeType = \'application/vnd.google-apps.folder\' and trashed = false'.format(directory, self.current_directory_id)}).GetList()
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

        Parameters
        ----------
        folder_name : str
            name of folder to make

        Returns
        -------
        id : str
            Google UUID of new directory

        """
        # name formatting
        folder_name = re.sub('^\./', '', folder_name)

        # not current dir
        current_directory = None
        if '/' in folder_name:
            current_directory = self.current_directory_object
            tokens = re.split('/', folder_name)
            for i in range(len(tokens) - 1):
                if not self.cd(tokens[i], True):
                    return None
            folder_name = tokens[-1]

        directories = self.drive.ListFile({'q': 'title="{}" and "{}" in parents and mimeType = \'application/vnd.google-apps.folder\' and trashed=false'.format(folder_name, self.current_directory_id)}).GetList()
        if len(directories) > 0:
            print('Folder already exists')
            return None

        folder = self.drive.CreateFile({'title': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [{'id': self.current_directory_id}]})
        folder.Upload()

        if current_directory is not None:
            self.current_directory_object = current_directory
            self.rebuild_current_path()
        return folder.metadata['id']

    def move(self, file, destination, sb=None, db=None, o=None):
        """

        Parameters
        ----------
        file : str
            file to move
        destination : str
            destination folder

        Returns
        -------
        : bool
            success of move operation
        """

        # TODO: renaming in mv
        origin = re.match('.*/', file)
        destination = re.match('.*/', destination)
        if origin is destination:
            print('renaming using move is not supported yet. use the .rename method')
            return False
        if origin.group(0) == destination.group(0):
            print('renaming using move is not supported yet. use the .rename method')
            return False

        return self.update_metadata(file, {'parents': [{'id': self.get_ID_by_name(destination)}]})

    def rename(self, original_name, new_name):
        """

        Parameters
        ----------
        original_name : str
            file to rename
        new_name : str
            new name

        Returns
        -------
        : bool
            success of operation
        """

        # TODO: name validation
        if '/' in new_name:
            print('No slashes in name, please')
            return False

        return self.update_metadata(original_name, {'title': new_name})

    def copy(self, original_name, copy_name, sb=None, db=None, o=None):
        """

        Parameters
        ----------
        original_name : str
            name of origin
        copy_name : str
            name/path of copy

        Returns
        -------

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

    def delete(self, file_name, recursive=False, delete=False):
        """

        Parameters
        ----------
        file_name : str
            name of file/folder to delete
        recursive : bool
            recursively delete a folder?
        delete : bool
            hard delete files?

        Returns
        -------
        : bool
            success of operation
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

    def upload_file(self, file_name, cloud_name=None, permissions=None):
        """
        Uploads from file on disk

        Parameters
        ----------
        file_name : str
            path to file on disk to upload
        cloud_name : str
            path/name for cloud file

        Returns
        -------
        : bool
            success of operation
        """
        if cloud_name is None:
            cloud_name = file_name

        if type(file_name) != str:
            # assume this is a file_name-like object
            self.upload_stream(file_name, cloud_name)

        # cloud_name formatting
        cloud_name = re.sub('^\./', '', cloud_name)

        # not current dir
        current_directory = None
        if '/' in cloud_name:
            current_directory = self.current_directory_object
            tokens = re.split('/', cloud_name)
            for i in range(len(tokens) - 1):
                if not self.cd(tokens[i], True):
                    return False
            cloud_name = tokens[-1]

        # metadata
        metadata = {}
        metadata['title'] = cloud_name
        metadata['parents'] = [{'id': self.current_directory_id}]

        # try to upload the file_name
        newfile = self.drive.CreateFile(metadata)
        newfile.Upload()
        try:
            newfile.SetContentFile(file_name)
            newfile.Upload()
        except Exception as e:
            print('Error uploading file_name:\n{}'.format(e.__str__()))
            newfile.delete()
            return False

        if current_directory is not None:
            self.current_directory_object = current_directory
            self.rebuild_current_path()

        return True

    def upload_stream(self, stream, name, properties=None, permissions=None):
        """
        Upload a stream with a .read() method

        Parameters
        ----------
        stream : stream
            stream to upload
        name : str
            name to use for cloud file
        properties : dict
            custom metadata

        Returns
        -------
        : bool
            success of operation
        """
        # cloudName formatting
        name = re.sub('^./', '', name)

        # not current dir
        current_directory = None
        if '/' in name:
            current_directory = self.current_directory_object
            tokens = re.split('/', name)
            for i in range(len(tokens) - 1):
                if not self.cd(tokens[i], True):
                    return False
            name = tokens[-1]

        # metadata
        metadata = {}
        metadata['title'] = name
        metadata['parents'] = [{'id': self.current_directory_id}]

        newfile = self.drive.CreateFile(metadata)
        newfile.Upload()
        try:
            newfile.content = stream
            newfile.Upload()
        except Exception as e:
            print('Error uploading stream:\n{}'.format(e.__str__()))
            newfile.delete()
            return False

        if properties is not None:
            for key in properties.keys():
                if not self.insert_property(newfile.metadata['id'], key, properties[key]):
                    print('Property insertion for {} failed'.format(key))

        if current_directory is not None:
            self.current_directory_object = current_directory
            self.rebuild_current_path()

        return True

    def upload_multipart(self, stream, cloud_name, properties=None, permissions=None):
        return self.upload_stream(stream, cloud_name, properties, permissions)


    def download_to_file(self, drive_file, local_file=None):
        """
        Download a file to disk

        Parameters
        ----------
        drive_file : str
            name of file to download
        local_file : str
            name to use on disk, if None, uses same name

        Returns
        -------
        : bool
            success of operation
        """

        try:
            f = self.drive.CreateFile({'id': self.get_ID_by_name(drive_file)})
        except FileNotFoundError:
            print('File not found')
            return False

        f.FetchMetadata()
        if local_file is None:
            local_file = f.metadata['title']
        f.GetContentFile(local_file)

        return True

    def download_stream(self, drive_file):
        """
        Downloads a file to memory

        Parameters
        ----------
        drive_file : str
            name of file to download

        Returns
        -------
        : CloudStream
            object in memory of downloaded data

        Raises
        ------
        FileNotFoundError
        """

        try:
            f = self.drive.CreateFile({'id': self.get_ID_by_name(drive_file)})
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

        Parameters
        ----------
        id : str
            Google UUID

        Returns
        -------
        : bool
            existence of ID on drive
        """
        metadata = {'id': id}
        item = GoogleDriveFile(self.drive.auth, metadata, uploaded = True)
        try:
            item.FetchMetadata()
            return True
        except (FileNotUploadedError, ApiRequestError):
            return False
        print('This line should not be executed.')

    def check_file_exists(self, file_name, bn):
        """

        Parameters
        ----------
        file_name : str
            path on cloud to check

        Returns
        -------
        : bool
            existence of file
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
        """
        if self.current_directory_id == 'root':
            self.dir = '/'
            return

        self.dir = ''
        this_directory = self.current_directory_object
        self.dir = '/' + this_directory['title'] + self.dir
        while (not this_directory['parents'][0]['isRoot']):
            this_directory = self.get_file_by_ID(self.current_directory_object['parents'][0]['id'])
            self.dir = '/' + this_directory['title'] + self.dir
        self.dir = str(self.dir)

    def list_objects(self, namesOnly=False, trashed=False):
        """
        Gets a list of all files in current directory

        Parameters
        ----------
        namesOnly : bool
            Returns names only?
        trashed : bool
            Look in trash folder instead?

        Returns
        -------
        : list<str> | list<GoogleDriveFile>
            List of files
        """
        if namesOnly:
            files = self.drive.ListFile({'q': "'{}' in parents and trashed={}".format(self.current_directory_id, str(trashed).lower())}).GetList()
            out = []
            for f in files:
                out.append(f['title'])
            return out
        return self.drive.ListFile({'q': "'{}' in parents and trashed={}".format(self.current_directory_id, str(trashed).lower())}).GetList()

    def get_file_by_name(self, name):
        """
        Gets the GoogleDriveFile for a file
        Parameters
        ----------
        name : str
            name of file to get

        Returns
        -------
        item : GoogleDriveFile
            object for the file
        """
        return self.get_file_by_ID(self.get_ID_by_name(name))

    def get_file_by_ID(self, id):
        """
        Gets the GoogleDriveFile for a file

        Parameters
        ----------
        id : str
            Google UUID

        Returns
        -------
        item : GoogleDriveFile
            object for the file
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
        Gets the Google UUID for a file

        Parameters
        ----------
        file_name : str
            name of file to get

        Returns
        -------
        id : str
            Google UUID
        """

        drive_file = re.sub('^\./', '', file_name)

        if file_name == '/':
            return 'root'

        # not current dir
        current_directory = None
        if '/' in drive_file:
            current_directory = self.current_directory_object
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

        if current_directory is not None:
            self.current_directory_object = current_directory
            self.rebuild_current_path()

        if item is not None:
            return item['id']
        else:
            raise FileNotFoundError

    def insert_property(self, id, key, value, visibility='PUBLIC'):
        """
        Adds a custom property to a file

        Parameters
        ----------
        id : str
            Google UUID of file
        key : str
            name of the custom property
        value : str
            value of the custom property
        visibility : 'PUBLIC'|'PRIVATE'
            visibility of the property

        Returns
        -------
        : bool
            operation success

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
        Gets the properties for a file

        Parameters
        ----------
        id : str
            Google UUID of file

        Returns
        -------
        properties : dict
            custom metadata
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

    def store_encryption_key(self, fileID, key64, chunk_size=96):
        """
        Stores a base64-encoded encryption key

        Parameters
        ----------
        fileID : str
            file UUID to store this to
        key64 : str
            base-64 encoded key
        chunk_size : int
            size in chars to store the key in (there are limits to property lengths)

        Returns
        -------

        """
        if chunk_size > 114:
            print('Chunk size set to 114 because of limitations of metadata size')
            chunk_size = 114
        nChuncks = len(key64) / chunk_size + (len(key64) % chunk_size > 0)
        self.insert_property(fileID, 'keyChunks', str(nChuncks))
        self.insert_property(fileID, 'key', 'in chunks')
        for i in range(nChuncks):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            end = end if end < len(key64) else len(key64)
            if not self.insert_property(fileID, 'keyChunk{}'.format(i), key64[start:end]):
                print('Key upload failed')
                return

    def get_encryption_key(self, fileID):
        """
        Gets the encryption key from the properties for a file

        Parameters
        ----------
        fileID : str
            Google UUID

        Returns
        -------
        key64 : str
            base-64 encoded encryption key
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
        Updates the custom properties for a file

        Parameters
        ----------
        file_name : str
            Name of file
        metadata : dict
            new metadata

        Returns
        -------
        : bool
            operation success
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
        Ipython autocomplete hook

        Parameters
        ----------
        context : ???
            ???
        event : ???
            eventargs

        Returns
        -------
        : list<str>
            autocompelte options
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

            current_directory = self.current_directory_object
            if directory is not None:
                if not self.cd(directory):  # if the directory doesn't actually exist, return empty list
                    return []
            items = self.list_objects(True)
            if directory is not None:
                self.current_directory_object = current_directory
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
        """
        ipython = get_ipython()
        if ipython is None:
            return
        ipython.set_hook('complete_command', self.completer, re_key = r'(?:.*\=)?(.+?)(\.(ls|cd|Download|rm|DownloadRawArray|DownloadStream|mv|rename|CheckFileExists|CheckIDExists))\([\'"].*')