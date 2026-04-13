# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import logging
import os
from functools import reduce
from io import BytesIO
from typing import BinaryIO, Optional, cast
from urllib.parse import unquote

import boto3
import botocore
import botocore.exceptions # ty wants this explicitly imported
from boto3.s3.transfer import TransferConfig
from botocore.utils import fix_s3_host
from dateutil.tz import tzlocal

from .backend import CCBackEnd, CloudStream

from .utils import (
    DEFAULT_ACL,
    ISBOTO_VERBOSE,
    MANDATORY_BUCKET_PREFIX,
    MPU_CHUNKSIZE,
    MPU_THRESHOLD,
    SEPARATOR,
    THREADS,
    bytes2human,
    clean_object_name,
    has_real_magic,
    mk_aws_path,
    pathjoin,
    remove_root,
    remove_trivial_magic,
    sanitize_metadata,
    string2bool,
    unquote_names,
)


class S3Client(CCBackEnd):
    """
    S3 client interface refactored out of cotton candy interfaces to allow for switching between
    S3 and Google Drive
    """

    @staticmethod
    def connect(ACCESS_KEY: str, SECRET_KEY: str, url: str, **kwargs):
        """Connect to S3 using boto

        Parameters
        ----------
        ACCESS_KEY
        SECRET_KEY
        url
        kwargs : dict
            Extra keyword arguments to `boto3.resource`

        Returns
        -------

        """
        s3 = boto3.resource('s3',
                            endpoint_url=url,
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY,
                            **kwargs)
        s3.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
        return s3

    def __init__(self, bucket: Optional[str], access_key: str, secret_key: str, s3url: str, force_bucket_creation: bool=False, **kwargs):
        """Constructor

        Parameters
        ----------
        bucket
        access_key
        secret_key
        s3url
        force_bucket_creation
        """
        super(S3Client, self).__init__()

        self.connection = S3Client.connect(access_key, secret_key, s3url, **kwargs)
        self.url = s3url
        self._bucket_name: Optional[str] = None

        if bucket:
            # bucket given
            if self.check_bucket_exists(bucket):
                self.set_current_bucket(bucket)
            elif force_bucket_creation:
                print('* Creating "%s" bucket...\n' % bucket)
                self.create_bucket(bucket)
            else:
                print('* BUCKET "%s" DOES NOT EXIST...\n' % bucket)

        if self.bucket_name is None:
            print('* cottoncandy instantiated without bucket...\n' \
                  '* Use with caution!\n' \
                  '* Many features will not work!!!\n')

        if string2bool(ISBOTO_VERBOSE) is False:
            logging.getLogger('boto3').setLevel(logging.WARNING)
            logging.getLogger('botocore').setLevel(logging.WARNING)

    def get_bucket_name(self, bucket_name: Optional[str] = None) -> Optional[str]:
        """

        Parameters
        ----------
        bucket_name

        Returns
        -------

        """
        bucket_name = self.bucket_name \
            if bucket_name is None \
            else bucket_name
        return bucket_name

    @property
    def bucket_name(self) -> Optional[str]:
        return self._bucket_name

    @clean_object_name
    def check_file_exists(self, cloud_name: str, bucket_name: Optional[str] = None) -> bool:
        """Check whether object exists in bucket

        Parameters
        ----------
        cloud_name : str
            The cloud name
        bucket_name : str
            The bucket name. If None, use the current bucket.
        """
        bucket_name = self.get_bucket_name(bucket_name)
        ob = self.connection.Object(key = cloud_name, bucket_name = bucket_name)

        try:
            ob.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise e
        else:
            exists = True
        return exists

    def check_bucket_exists(self, bucket_name: str):
        """Check whether the bucket exists

        Parameters
        ----------
        bucket_name

        Returns
        -------

        """
        try:
            self.connection.meta.client.head_bucket(Bucket = bucket_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            elif e.response['Error']['Code'] == "403":
                # 403 Forbidden means bucket exists but we lack HeadBucket permission
                exists = True
            else:
                raise e
        else:
            exists = True
        return exists

    def create_bucket(self, bucket_name: str, acl: str=DEFAULT_ACL):
        """Create a new bucket

        Parameters
        ----------
        bucket_name
        acl

        Returns
        -------

        """
        if MANDATORY_BUCKET_PREFIX:
            tt = len(MANDATORY_BUCKET_PREFIX)
            assert bucket_name[:tt] == MANDATORY_BUCKET_PREFIX

        self.connection.create_bucket(Bucket = bucket_name, ACL = acl)
        self.set_current_bucket(bucket_name)

    def set_current_bucket(self, bucket_name: str):
        """Sets which bucket to use

        Parameters
        ----------
        bucket_name

        Returns
        -------

        """
        if not self.check_bucket_exists(bucket_name):
            raise IOError('Bucket "%s" does not exist' % bucket_name)
        self._bucket_name = bucket_name

    def get_bucket(self):
        """Get bucket boto3 object

        Returns
        -------

        """
        s3_bucket = self.connection.Bucket(self.bucket_name)
        return s3_bucket

    def list_objects(self, **kwargs):
        """Get list of objects from the bucket
        This is a wrapper to ``self.get_bucket().bucket.objects``

        Parameters
        ----------
        limit : int, 1000
            Maximum number of items to return
        page_size : int, 1000
            The page size for pagination
        filter : dict
            A dictionary with key 'Prefix', specifying a prefix
            string.  Only return objects matching this string.
            Defaults to '/', all objects.
        kwargs : optional
            Dictionary of {method:value} for ``bucket.objects``

        Notes
        -----
        If you get a 'PaginationError', this means you have
        a lot of items on your bucket and should increase ``page_size``
        """
        defaults = dict(limit = 1000,
                        page_size = 1000,
                        filter = dict(Prefix = SEPARATOR),
                        )
        defaults.update(kwargs)
        bucket = self.get_bucket()
        prefix = cast(dict[str, str], defaults.pop('filter'))

        if prefix['Prefix'] == SEPARATOR:
            request = bucket.objects
        else:
            request = bucket.objects.filter(**prefix)

        for method_name, value in defaults.items():
            if value is None:
                continue
            method = getattr(request, method_name)
            if isinstance(value, dict):
                request = method(**value)
            else:
                request = method(value)

        response = request.all()
        return response

    @property
    def size(self) -> int:
        return self.get_current_bucket_size()

    def get_current_bucket_size(self, limit: int=10 ** 6, page_size: int=10 ** 6) -> int:
        """Counts the size of all objects in the current bucket.

        Parameters
        ----------
        limit : int, 1000
            Maximum number of items to return
        page_size : int, 1000
            The page size for pagination

        Returns
        -------
        total_bytes : int
            The byte count of all objects in the bucket.

        Notes
        -----
        Because paging does not work properly, if there are more than
        limit,page_size number of objects in the bucket, this function will
        underestimate the total size. Check the printed number of objects for
        suspicious round numbers.
        TODO(anunez): Remove this note when the bug is fixed.
        """
        assert self.bucket_name is not None, 'Must specify bucket to get size'
        assert self.check_bucket_exists(self.bucket_name)
        obs = self.list_objects(limit = limit, page_size = page_size)
        object_sizes = [t.size for t in obs]
        total_bytes = sum(object_sizes)
        num_objects = len(object_sizes)
        del object_sizes

        txt = "%i bytes (%s) over %i objects"
        print(txt % (total_bytes, bytes2human(total_bytes), num_objects))
        return total_bytes

    def show_all_buckets(self):
        """


        Returns
        -------

        """
        all_buckets = list(self.connection.buckets.all())
        timeformat = lambda x: x.creation_date.astimezone(tzlocal()).strftime
        info = ['{0: <40} {1}'.format(unquote(t.name),
                                      timeformat(t)('%Y/%m/%d (%H:%M:%S)')) \
                for t in all_buckets]
        print('\n'.join(info))

    @clean_object_name
    def get_s3_object(self, object_name: str, bucket_name: Optional[str] = None):
        """Get a boto3 object. Create it if it doesn't exist

        Parameters
        ----------
        object_name
        bucket_name

        Returns
        -------

        """
        bucket_name = self.get_bucket_name(bucket_name)
        return self.connection.Object(bucket_name = bucket_name, key = object_name)

    def upload_stream(self, stream: BinaryIO, cloud_name: str, metadata: dict[str, str], permissions: Optional[str], threads: int) -> None:
        """Uploads a stream

        Parameters
        ----------
        threads
        permissions
        metadata

        Returns
        -------

        """
        assert permissions is not None, 'Permissions must be specified for S3 uploads'

        obj = self.get_s3_object(cloud_name)
        config = TransferConfig(max_concurrency = threads,
                                multipart_chunksize = MPU_CHUNKSIZE,
                                multipart_threshold = MPU_THRESHOLD)
        return obj.upload_fileobj(stream, ExtraArgs = {'ACL': permissions, 'Metadata': metadata},
                                                Config = config)

    def download_stream(self, cloud_name: str, threads: int) -> CloudStream:
        """Download object raw data.
        This simply calls the object body ``read()`` method.

        Parameters
        ---------
        cloud_name : str

        Returns
        -------
        stream
            file-like stream of object data
        """
        if not self.check_file_exists(cloud_name):
            raise IOError('Object "%s" does not exist' % cloud_name)
        s3_object = self.get_s3_object(cloud_name)
        config = TransferConfig(max_concurrency = threads,
                                multipart_chunksize = MPU_CHUNKSIZE,
                                multipart_threshold = MPU_THRESHOLD)
        byteStream = BytesIO()
        s3_object.download_fileobj(byteStream, Config = config)
        byteStream.seek(0)
        return CloudStream(byteStream, sanitize_metadata(s3_object.metadata))

    def upload_file(self, file_name: str, cloud_name: Optional[str] = None, permissions: Optional[str] = None, threads: int = THREADS) -> None:
        """Upload a file to S3.

        Parameters
        ----------
        file_name : str
            Absolute path of file to upload
        cloud_name : str, None
            Name of uploaded object. If None, use
            the full file name as the object name.
        permissions: ???
            S3 permissions

        Returns
        -------
        response : boto3 response
        """
        assert os.path.exists(file_name)
        assert permissions is not None, 'Permissions must be specified for S3 uploads'
        if cloud_name is None:
            cloud_name = file_name
        s3_object = self.get_s3_object(cloud_name)
        config = TransferConfig(max_concurrency = threads,
                                multipart_chunksize = MPU_CHUNKSIZE,
                                multipart_threshold = MPU_THRESHOLD)
        return s3_object.upload_file(file_name, ExtraArgs={'ACL': permissions}, Config = config)

    def download_to_file(self, cloud_name: str, local_name: str, threads: int):
        """Download S3 object to a file

        Parameters
        ----------
        cloud_name : str
        local_name : str
            Absolute path where the data will be downloaded on disk
        """
        assert self.check_file_exists(cloud_name)  # make sure object exists
        s3_object = self.get_s3_object(cloud_name)
        config = TransferConfig(max_concurrency = threads,
                                multipart_chunksize = MPU_CHUNKSIZE,
                                multipart_threshold = MPU_THRESHOLD)
        return s3_object.download_file(local_name, Config = config)

    def copy(self, source: str, destination: str, source_bucket: Optional[str] = None, destination_bucket: Optional[str] = None, overwrite: bool = False):
        source_bucket = self.get_bucket_name(source_bucket)
        assert source_bucket is not None, 'Source bucket must be specified'
        dest_bucket = source_bucket if (destination_bucket is None) else destination_bucket
        dest_bucket = self.get_bucket_name(dest_bucket)

        assert self.check_file_exists(source, bucket_name = source_bucket)
        ob_new = self.get_s3_object(destination, bucket_name = dest_bucket)

        if self.check_file_exists(ob_new.key, bucket_name = dest_bucket):
            assert overwrite is True

        fpath = pathjoin(source_bucket, source)
        ob_new.copy_from(CopySource = fpath)
        return ob_new

    def move(self, source: str, destination: str, source_bucket: Optional[str] = None, destination_bucket: Optional[str] = None, overwrite: bool = False):
        new_ob = self.copy(source, destination, source_bucket, destination_bucket, overwrite)
        old_ob = self.get_s3_object(source, bucket_name = source_bucket)
        old_ob.delete()
        return new_ob

    def list_directory(self, path: str, limit: int) -> list[str]:
        """List the contents of a "directory"

        Parameters
        ----------
        path : str (default: "/")

        Returns
        -------
        matches : list
            The children of the path.
        """
        if has_real_magic(path):
            raise ValueError('Use ``ls()`` when using search patterns: "%s"' % path)

        if (path != '') and (path != '/'):
            path = remove_root(path)
        path = remove_trivial_magic(path)
        path = mk_aws_path(path)

        response = self.get_bucket().meta.client.list_objects(Bucket = self.bucket_name,
                                                              Delimiter = SEPARATOR,
                                                              Prefix = path,
                                                              MaxKeys = limit)
        object_names: list[str] = []
        if 'CommonPrefixes' in response:
            # we got common paths
            object_list: list[list[str]] = [list(t.values()) for t in response['CommonPrefixes']]
            object_names = object_names + list(reduce(lambda x, y: x + y, object_list)) # type: ignore[operator]
        if 'Contents' in response:
            # we got objects on the leaf nodes
            object_names += unquote_names([t['Key'] for t in response['Contents']])
        return [os.path.normpath(n) for n in object_names]

    def delete(self, cloud_name: str, recursive: bool=False, delete: bool=False):
        raise RuntimeError('Deleting on S3 backend is implemented by cottoncandy interface object')

    def get_object_metadata(self, object_name: str) -> dict[str, str]:
        """Get metadata associated with an object"""
        s3_object = self.get_s3_object(object_name)
        metadata = sanitize_metadata(s3_object.metadata)
        return metadata

    def get_object_size(self, object_name: str) -> int:
        """Get the size in bytes of an object"""
        s3_object = self.get_s3_object(object_name)
        size = s3_object.content_length
        return size
