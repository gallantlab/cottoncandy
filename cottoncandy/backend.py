from abc import ABCMeta, abstractmethod
from typing import NamedTuple, BinaryIO, Optional


class FileNotFoundError(RuntimeError):
    """File not found error"""


class CloudStream(NamedTuple):
    """
    A simple unified representation of an object downloaded from the cloud.
     .content is a streaming object with a .read() function
     .metadata is a dictionary of the custom metadata of this object

    TODO: unified metadata
    """
    content: BinaryIO
    metadata: dict[str, str]


class CCBackEnd:
    """
    Interface for cottoncandy backends
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    ## Basic File IO
    @abstractmethod
    def check_file_exists(self, cloud_name: str, bucket_name: Optional[str] = None) -> bool:
        """Checks whether a file exists on the cloud

        Parameters
        ----------
        cloud_name : str
            file on cloud
        bucket_name : str
            (s3) bucket to check in

        Returns
        -------
        bool
        """
        pass

    @abstractmethod
    def upload_stream(self, stream: BinaryIO, cloud_name: str, metadata: dict[str, str], permissions: Optional[str], threads: int) -> None:
        """Uploads a stream object with a .read() function

        Parameters
        ----------
        threads
        stream : stream
            streaming object
        cloud_name : str
            name to use on cloud
        metadata : dict
            custom metadata for this file
        permissions : str?
            permissions for this file
        threads : int
            number of threads to use

        Returns
        -------
        None
        """
        pass

    @abstractmethod
    def upload_file(self, file_name: str, cloud_name: Optional[str] = None, permissions: Optional[str] = None, threads: int = 1) -> None:
        """Uploads a file from disk

        Parameters
        ----------
        threads
        file_name : str
            name of file to upload
        cloud_name : str
            name to use on the cloud
        permissions : str?
            permissions for this file
        threads : int
            number of threads to use

        Returns
        -------
        None
        """
        pass


    @abstractmethod
    def download_stream(self, cloud_name: str, threads: int) -> CloudStream:
        """Downloads a object to an in-memory stream

        Parameters
        ----------
        threads
        cloud_name : str
            name of object to download
        threads : int
            number of threads to use

        Returns
        -------
        CloudStream object
        """
        pass

    @abstractmethod
    def download_to_file(self, cloud_name: str, file_name: str, threads: int) -> None:
        """Downloads an object directly to disk

        Parameters
        ----------
        threads
        cloud_name : str
            name of object to download
        file_name : str
            name on disk to use
        threads : int
            number of threads to use

        Returns
        -------
        None
        """
        pass

    ## Basic File management

    @abstractmethod
    def list_directory(self, path: str, limit: int) -> list[str]:
        """Lists the content of a directory

        Parameters
        ----------
        path : str
            path on the cloud to get contents for

        Returns
        -------
        list[str]
        """
        pass

    @abstractmethod
    def list_objects(self) -> list[str]:
        """Gets all objects contained by backend

        Returns
        -------

        """
        pass

    @abstractmethod
    def copy(self, source: str, destination: str, source_bucket: Optional[str] = None, destination_bucket: Optional[str] = None, overwrite: bool = False):
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
    def move(self, source: str, destination: str, source_bucket: Optional[str] = None, destination_bucket: Optional[str] = None, overwrite: bool = False) -> bool:
        """Moves an object

        Parameters
        ----------
        source: str
            origin path
        destination: str
            destination path
        source_bucket: str
            (s3) origin bucket
        destination_bucket: str
            (s3) destination bucket
        overwrite: bool
            overwrite if destination exists?

        Returns
        -------
        bool, move success
        """
        pass

    @abstractmethod
    def delete(self, cloud_name: str, recursive: bool = False, delete: bool = False) -> bool:
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
        bool, delete success
        """
        pass

    @property
    @abstractmethod
    def size(self) -> int:
        """Size of stored cloud items in bytes

        Returns
        -------
        int
        """
        pass
