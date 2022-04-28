import os
import json
import glob
import shutil

try:
    from cStringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO

from .backend import CCBackEnd, CloudStream
from .utils import sanitize_metadata


class LocalClient(CCBackEnd):
    """
    Client interface for local file system.

    Handle metadata in CouldStream objects by storing a json file (.meta.json).
    """

    def __init__(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)
        self.path = path

    def check_file_exists(self, cloud_name, bucket_name):
        """Checks whether a file exists on the cloud

        Parameters
        ----------
        cloud_name : str
            file on cloud
        bucket_name : str
            For a local client, the bucket is just a directory path.
            If None, use self.path.

        Returns
        -------
        bool
        """
        if bucket_name is None:
            bucket_name = self.path
        return os.path.isfile(os.path.join(bucket_name, cloud_name))

    def upload_stream(self, stream, cloud_name, metadata, permissions):
        """Uploads a stream object with a .read() function

        Parameters
        ----------
        stream : stream
            streaming object, e.g. StringIO(json.dumps(my_dict)) (no metadata)
        cloud_name : str
            name to use on cloud
        metadata : dict
            custom metadata for this file
        permissions : ignored

        Returns
        -------
        bool, upload success
        """
        file_name = os.path.join(self.path, cloud_name)
        auto_makedirs(file_name)
        with open(file_name, 'wb') as local_file:
            local_file.write(stream.read())

        metadata_file_name = file_name + ".meta.json"
        auto_makedirs(metadata_file_name)
        with open(metadata_file_name, 'w') as local_file:
            json.dump(metadata, local_file, indent=4)

    def upload_file(self, file_name, cloud_name, permissions):
        """Uploads a file from disk

        Parameters
        ----------
        file_name : str
            name of file to upload
        cloud_name : str
            name to use on the cloud
        permissions : ignored

        Returns
        -------
        bool, upload success
        """
        self.copy(
            source=file_name,
            destination=os.path.join(self.path, cloud_name),
            source_bucket=None,
            destination_bucket=None,
            overwrite=True,
        )

    def upload_multipart(self, stream, cloud_name, metadata, permissions,
                         buffersize, verbose):
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
        raise NotImplementedError

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
        file_name = os.path.join(self.path, cloud_name)
        with open(file_name, 'rb') as local_file:
            content = StringIO()
            content.write(local_file.read())  # load in memory
            content.seek(0)

        metadata_file_name = file_name + ".meta.json"
        if os.path.isfile(metadata_file_name):
            with open(metadata_file_name, 'r') as local_file:
                metadata = json.load(local_file)
        else:
            metadata = dict()

        return CloudStream(content, sanitize_metadata(metadata))

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
        return self.copy(
            source=os.path.join(self.path, cloud_name),
            destination=file_name,
            source_bucket=None,
            destination_bucket=None,
            overwrite=True,
        )

    def list_directory(self, path, limit):
        """Lists the content of a directory

        Parameters
        ----------
        path : str
            path on the cloud to get contents for
        limit : ignored

        Returns
        -------

        """
        path = os.path.join(self.path, path)
        results = glob.glob(os.path.join(path, "*"))
        results = self._remove_path_and_metadata(results)
        return results

    def list_objects(self):
        """Gets all objects contained by backend

        Returns
        -------

        """
        results = [
            os.path.join(dirpath, filename)
            for dirpath, dirnames, filenames in os.walk(
                os.path.expanduser(self.path)) for filename in filenames
        ]
        results = self._remove_path_and_metadata(results)
        return results

    def copy(self, source, destination, source_bucket, destination_bucket,
             overwrite):
        """Copies an object

        Parameters
        ----------
        source : str
            origin path
        destination : str
            destination path
        source_bucket : ignored
        destination_bucket : ignored
        overwrite : bool
            overwrite if destination exists

        Returns
        -------
        bool, copy success
        """
        if overwrite is False:
            raise NotImplementedError()
        if source_bucket is None:
            source_bucket = self.path
        if destination_bucket is None:
            destination_bucket = source_bucket
        source = os.path.join(source_bucket, source)
        destination = os.path.join(destination_bucket, destination)
        auto_makedirs(destination)
        return shutil.copy(source, destination)

    def move(self, source, destination, source_bucket, destination_bucket,
             overwrite):
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
        if overwrite is False:
            raise NotImplementedError()
        if source_bucket is None:
            source_bucket = self.path
        if destination_bucket is None:
            destination_bucket = source_bucket
        source = os.path.join(source_bucket, source)
        destination = os.path.join(destination_bucket, destination)
        auto_makedirs(destination)
        shutil.move(source, destination)

    def delete(self, cloud_name, recursive=False, delete=False):
        """Deletes an object

        Parameters
        ----------
        cloud_name : str
            name of cloud object to delete
        recursive : bool
            recursively delete directory?
        delete : bool
            ignored

        Returns
        -------

        """
        cloud_name = os.path.join(self.path, cloud_name)
        if os.path.isfile(cloud_name):
            os.remove(cloud_name)
        else:
            if recursive:
                shutil.rmtree(cloud_name)
            else:
                os.rmdir(cloud_name)

    @property
    def size(self):
        """Size of stored cloud items in bytes

        Returns
        -------
        int
        """
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)

        return total_size

    def _remove_path_and_metadata(self, file_list):
        """Removes path from filenames, removes .meta.json files from the list.
        """
        results = []
        for file_name in file_list:
            # remove self.path
            if file_name[:len(self.path)] == self.path:
                file_name = file_name[len(self.path):]
            # remove .meta.json files
            if file_name[-10:] == ".meta.json":
                continue
            else:
                results.append(file_name)
        return results


def auto_makedirs(destination):
    """Create directory tree if destination does not exist."""
    if not os.path.exists(os.path.dirname(destination)):
        os.makedirs(os.path.dirname(destination))
