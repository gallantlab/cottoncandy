from abc import ABCMeta, abstractmethod

class FileNotFoundError(RuntimeError):
    """File not found error"""

class CCBackEnd(object):
    """
    Interface for cottoncandy backends
    """
    __metaclass__ = ABCMeta
    def __init__(self):
        pass

    ## Basic File IO
    @abstractmethod
    def check_file_exists(self, file_name, bucket_name):
        """Checks whether a file exists on the cloud

        Parameters
        ----------
        file_name : str
            file on cloud
        bucket_name : str
            (s3) bucket to check in

        Returns
        -------
        bool
        """
        pass

    @abstractmethod
    def upload_stream(self, stream, cloud_name, metadata, permissions):
        """Uploads a stream object with a .read() function

        Parameters
        ----------
        stream : stream
            streaming object
        cloud_name : str
            name to use on cloud
        metadata : dict
            custom metadata for this file
        permissions : str?
            permissions for this file

        Returns
        -------
        bool, upload success
        """
        pass

    @abstractmethod
    def upload_file(self, file_name, cloud_name, permissions):
        """Uploads a file from disk

        Parameters
        ----------
        file_name : str
            name of file to upload
        cloud_name : str
            name to use on the cloud
        permissions : str?
            permissions for this file

        Returns
        -------
        bool, upload success
        """
        pass

    @abstractmethod
    def upload_multipart(self, stream, cloud_name, metadata, permissions, buffersize, verbose):
        """Multi-part upload for large stream objects

        Parameters
        ----------
        stream : stream
            streaming object
        cloud_name : str
            name to use on cloud
        metadata : dict
            custom metadata
        permissions : str?
            permissions for this file
        buffersize : int
            s3 uploading buffersize
        verbose : bool
            s3 verbosity

        Returns
        -------
        bool, upload success
        """
        pass

    @abstractmethod
    def download_stream(self, cloud_name):
        """Downloads a object to an in-memory stream

        Parameters
        ----------
        cloud_name : str
            name of object to download

        Returns
        -------
        CloudStream object
        """
        pass

    @abstractmethod
    def download_to_file(self, cloud_name, file_name):
        """Downloads an object directly to disk

        Parameters
        ----------
        cloud_name : str
            name of object to download
        file_name : str
            name on disk to use

        Returns
        -------
        bool, download success
        """
        pass

    ## Basic File management

    @abstractmethod
    def list_directory(self, path, limit):
        """Lists the content of a directory

        Parameters
        ----------
        path : str
            path on the cloud to get contents for

        Returns
        -------

        """
        pass

    @abstractmethod
    def list_objects(self):
        """Gets all objects contained by backend

        Returns
        -------

        """
        pass

    @abstractmethod
    def copy(self, source, destination, source_bucket, destination_bucket, overwrite):
        """Copies an object

        Parameters
        ----------
        source : str
            origin path
        destination : str
            destination path
        source_bucket : str
            (s3) origin bucket
        destination_bucket : str
            (s3) destination bucket
        overwrite : bool
            overwrite if destination exists?

        Returns
        -------
        bool, copy success
        """
        pass

    @abstractmethod
    def move(self, source, destination, source_bucket, destination_bucket, overwrite):
        """Moves an object

        Parameters
        ----------
        source
        destination
        source_bucket
        destination_bucket
        overwrite

        Returns
        -------

        """
        pass

    @abstractmethod
    def delete(self, file_name, recursive=False, delete=False):
        """Deletes an object

        Parameters
        ----------
        file_name : str
            name of cloud object to delete
        recursive : bool
            recursively delete directory?
        delete : bool
            (gdrive) hard delete the file(s)?

        Returns
        -------

        """
        pass

    @property
    @abstractmethod
    def size(self):
        """Size of stored cloud items in bytes

        Returns
        -------
        int
        """
        pass

class CloudStream(object):
    """
    A simple unified representation of an object downloaded from the cloud.
     .content is a streaming object with a .read() function
     .metadata is a dictionary of the custom metadata of this object

    TODO: unified metadata
    """
    def __init__(self, stream, metadata):
        self.content = stream
        self.metadata = metadata
