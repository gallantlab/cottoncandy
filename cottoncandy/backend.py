from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, BinaryIO, Iterable, NamedTuple


class CloudStream(NamedTuple):
    """
    A simple unified representation of an object downloaded from the cloud.
     .content is a streaming object with a .read() function
     .metadata is a dictionary of the custom metadata of this object

    TODO: unified metadata
    """
    content: BinaryIO
    metadata: dict[str, str]


class CCBackEnd(ABC):
    """
    Interface for cottoncandy backends

    Error handling follows the standard library's filesystem functions.
    Every method raises on failure and never reports failure through its
    return value. Use the built-in :class:`OSError` subclasses:
    :class:`FileNotFoundError` for a missing object, :class:`FileExistsError`
    when the destination exists and ``overwrite`` is False, and
    :class:`IsADirectoryError` when a directory is removed without
    ``recursive``. Methods that change state return None, except ``copy``
    and ``move``, which return the destination name (like ``shutil.copy``
    and ``shutil.move``).
    """

    @property
    @abstractmethod
    def bucket_name(self) -> str | None:
        """Name of the current bucket or backing path."""

    ## Basic File IO
    @abstractmethod
    def check_file_exists(self, cloud_name: str, bucket_name: str | None = None) -> bool:
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

    @abstractmethod
    def upload_stream(self, stream: BinaryIO, cloud_name: str, metadata: dict[str, str],
                      permissions: str | None, threads: int) -> None:
        """Uploads a stream object with a .read() function

        Parameters
        ----------
        stream : stream
            streaming object
        cloud_name : str
            name to use on cloud
        metadata : dict
            custom metadata for this file
        permissions : str or None
            permissions for this file
        threads : int
            number of threads to use
        """

    @abstractmethod
    def upload_file(self, file_name: str, cloud_name: str | None = None,
                    permissions: str | None = None, threads: int = 1) -> None:
        """Uploads a file from disk

        Parameters
        ----------
        file_name : str
            name of file to upload
        cloud_name : str
            name to use on the cloud
        permissions : str or None
            permissions for this file
        threads : int
            number of threads to use

        Raises
        ------
        FileNotFoundError
            If ``file_name`` does not exist.
        """

    @abstractmethod
    def download_stream(self, cloud_name: str, threads: int) -> CloudStream:
        """Downloads a object to an in-memory stream

        Parameters
        ----------
        cloud_name : str
            name of object to download
        threads : int
            number of threads to use

        Returns
        -------
        CloudStream object

        Raises
        ------
        FileNotFoundError
            If the object does not exist.
        """

    @abstractmethod
    def download_to_file(self, cloud_name: str, file_name: str, threads: int) -> None:
        """Downloads an object directly to disk

        Parameters
        ----------
        cloud_name : str
            name of object to download
        file_name : str
            name on disk to use
        threads : int
            number of threads to use

        Raises
        ------
        FileNotFoundError
            If the object does not exist.
        """

    ## Basic File management

    @abstractmethod
    def list_directory(self, path: str, limit: int) -> list[str]:
        """Lists the content of a directory

        Parameters
        ----------
        path : str
            path on the cloud to get contents for
        limit : int
            maximum number of entries to return (backends may ignore it)

        Returns
        -------
        list[str]
        """

    @abstractmethod
    def list_objects(self, **kwargs: Any) -> Iterable[Any]:
        """Gets all objects contained by backend

        The keyword arguments and the type of each item are
        backend-specific: the s3 backend yields ``boto3`` object
        summaries, the local backend yields object names.

        Returns
        -------
        Iterable of objects
        """

    @abstractmethod
    def copy(self, source: str, destination: str, source_bucket: str | None = None,
             destination_bucket: str | None = None, overwrite: bool = False) -> str:
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
        str
            Name of the destination object.

        Raises
        ------
        FileNotFoundError
            If ``source`` does not exist.
        FileExistsError
            If ``destination`` exists and ``overwrite`` is False.
        """

    @abstractmethod
    def move(self, source: str, destination: str, source_bucket: str | None = None,
             destination_bucket: str | None = None, overwrite: bool = False) -> str:
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
        str
            Name of the destination object.

        Raises
        ------
        FileNotFoundError
            If ``source`` does not exist.
        FileExistsError
            If ``destination`` exists and ``overwrite`` is False.
        """

    @abstractmethod
    def delete(self, cloud_name: str, recursive: bool = False, delete: bool = False) -> None:
        """Deletes an object

        Parameters
        ----------
        cloud_name : str
            name of cloud object to delete
        recursive : bool
            recursively delete directory?
        delete : bool
            (gdrive) hard delete the file(s)?

        Raises
        ------
        FileNotFoundError
            If nothing exists under ``cloud_name``.
        IsADirectoryError
            If ``cloud_name`` is a directory and ``recursive`` is False.

        Notes
        -----
        The s3 backend does not implement this and raises RuntimeError:
        S3 deletion is done by ``FileSystemInterface.rm``, which uses ``glob``.
        """

    @property
    @abstractmethod
    def size(self) -> int:
        """Size of stored cloud items in bytes

        Returns
        -------
        int
        """
