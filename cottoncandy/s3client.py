from __future__ import print_function
from functools import reduce
import boto3
import botocore
import logging
from io import BytesIO

from botocore.utils import fix_s3_host
from cottoncandy.utils import *
from .backend import CCBackEnd, CloudStream

try:
    from urllib import unquote
except ImportError:
    from urllib.parse import unquote
try:
    import cPickle as pickle
except ImportError:
    import pickle


class S3Client(CCBackEnd):
    """
    S3 client interface refactored out of cotton candy interfaces to allow for switching between
    S3 and Google Drive
    """

    @staticmethod
    def connect(ACCESS_KEY, SECRET_KEY, url, **kwargs):
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
                            endpoint_url = url,
                            aws_access_key_id = ACCESS_KEY,
                            aws_secret_access_key = SECRET_KEY,
                            **kwargs)
        s3.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
        return s3

    def __init__(self, bucket, access_key, secret_key, s3url, force_bucket_creation=False, **kwargs):
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
        if self.check_bucket_exists(bucket):
            self.set_current_bucket(bucket)
        else:
            print('* Bucket "%s" does not exist' % bucket)
            if force_bucket_creation:
                print('* Creating "%s" bucket...\n' % bucket)
                self.create_bucket(bucket)
            else:
                print('* cottoncandy instantiated without bucket...\n' \
                      '* Use with caution!\n' \
                      '* Many features will not work!!!\n')
                self.bucket_name = None

        if string2bool(ISBOTO_VERBOSE) is False:
            logging.getLogger('boto3').setLevel(logging.WARNING)
            logging.getLogger('botocore').setLevel(logging.WARNING)

    def get_bucket_name(self, bucket_name):
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

    @clean_object_name
    def check_file_exists(self, file_name, bucket_name=None):
        """Check whether object exists in bucket

        Parameters
        ----------
        file_name : str
            The object name
        bucket_name
        """
        bucket_name = self.get_bucket_name(bucket_name)
        ob = self.connection.Object(key = file_name, bucket_name = bucket_name)

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

    def check_bucket_exists(self, bucket_name):
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
            else:
                raise e
        else:
            exists = True
        return exists

    def create_bucket(self, bucket_name, acl=DEFAULT_ACL):
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

    def set_current_bucket(self, bucket_name):
        """Sets which bucket to use

        Parameters
        ----------
        bucket_name

        Returns
        -------

        """
        if not self.check_bucket_exists(bucket_name):
            raise IOError('Bucket "%s" does not exist' % bucket_name)
        self.bucket_name = bucket_name

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
        prefix = defaults.pop('filter')

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
    def size(self):
        return self.get_current_bucket_size()

    def get_current_bucket_size(self, limit=10 ** 6, page_size=10 ** 6):
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
    def get_s3_object(self, file_name, bucket_name=None):
        """Get a boto3 object. Create it if it doesn't exist

        Parameters
        ----------
        file_name
        bucket_name

        Returns
        -------

        """
        bucket_name = self.get_bucket_name(bucket_name)
        return self.connection.Object(bucket_name = bucket_name, key = file_name)

    def upload_stream(self, body, file_name, metadata, permissions=DEFAULT_ACL):
        """Uploads a stream

        Parameters
        ----------
        file_name
        body : stream
        permissions
        metadata

        Returns
        -------

        """
        obj = self.get_s3_object(file_name)
        return obj.put(Body=body, ACL=permissions, Metadata=metadata)

    def download_stream(self, file_name):
        """Download object raw data.
        This simply calls the object body ``read()`` method.

        Parameters
        ---------
        file_name : str

        Returns
        -------
        stream
            file-like stream of object data
        """
        if not self.check_file_exists(file_name):
            raise IOError('Object "%s" does not exist' % file_name)
        s3_object = self.get_s3_object(file_name)
        return CloudStream(BytesIO(s3_object.get()['Body'].read()), s3_object.metadata)

    def upload_file(self, file_name, cloud_name=None, permissions=DEFAULT_ACL):
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
        if cloud_name is None:
            cloud_name = file_name
        s3_object = self.get_s3_object(cloud_name)
        return s3_object.upload_file(file_name, ExtraArgs = dict(ACL = permissions))

    def download_to_file(self, file_name, local_name):
        """Download S3 object to a file

        Parameters
        ----------
        file_name : str
        local_name : str
            Absolute path where the data will be downloaded on disk
        """
        assert self.check_file_exists(file_name)  # make sure object exists
        s3_object = self.get_s3_object(file_name)
        return s3_object.download_file(local_name)

    def upload_multipart(self, file_object, file_name, metadata, permissions=None,
                         buffersize=MPU_CHUNKSIZE, verbose=True, **kwargs):
        """Multi-part upload for a python file-object.

        This automatically creates a multipart upload of an object.
        Useful for large objects that are loaded in memory. This avoids
        having to write the file to disk and then using ``upload_from_file``.

        Parameters
        ----------
        file_name : str
        file_object :
            file-like python object (e.g. StringIO, file, etc)
        buffersize  : int
            Byte size of the individual parts to create.
            Defaults to 100MB
        verbose     : bool
            verbosity flag of whether to print mpu information to stdout
        metadata  : optional
            Metadata to store along with MPU object
        permissions : str?
            permissions for this file
        """
        client=self.connection.meta.client
        mpu = client.create_multipart_upload(Bucket = self.bucket_name,
                                             Key = file_name,
                                             Metadata = metadata)

        # get size
        nbytes_total = get_fileobject_size(file_object)
        file_object.seek(0)

        # check if our buffersize is sensible
        if nbytes_total < buffersize:
            npotential = nbytes_total / float(MIN_MPU_SIZE)
            if npotential > 10:
                # 10MB sensible minimum for 50MB < x < 100MB (default)
                buffersize = MIN_MPU_SIZE * 2
            else:
                # this file is smaller than chunksize by a little
                buffersize = MIN_MPU_SIZE

        # figure out parts split
        nparts = int(np.floor(nbytes_total / float(buffersize)))
        last_part_offset = int(nbytes_total - nparts * buffersize)

        # make sure we can upload
        assert nbytes_total < MAX_MPU_SIZE  # 5TB
        assert buffersize >= MIN_MPU_SIZE  # 5MB
        assert nparts < MAX_MPU_PARTS  # 10,000
        assert nbytes_total > buffersize

        mpu_info = dict(Parts = [])
        if verbose:
            print('MPU: %0.02fMB file in %i parts' % (nbytes_total / (2. ** 20), nparts))

        for chunk_idx in range(nparts):
            part_number = chunk_idx + 1
            if part_number == nparts:
                buffersize += last_part_offset
            txt = (part_number, nparts, buffersize / (2. ** 20), nbytes_total / (2. ** 20))
            if verbose:
                print('Uploading %i/%i: %0.02fMB of %0.02fMB' % txt)

            data_chunk = file_object.read(buffersize)
            response = client.upload_part(Bucket = self.bucket_name,
                                          Key = file_name,
                                          UploadId = mpu['UploadId'],
                                          PartNumber = part_number,
                                          Body = data_chunk)

            # store the part info
            part_info = dict(PartNumber = part_number,
                             ETag = response['ETag'])
            mpu_info['Parts'].append(part_info)

        # finalize
        mpu_response = client.complete_multipart_upload(Bucket = self.bucket_name,
                                                        Key = file_name,
                                                        UploadId = mpu['UploadId'],
                                                        MultipartUpload = mpu_info)
        return mpu_response

    def copy(self, source, destination, source_bucket, destination_bucket, overwrite):
        source_bucket = self.get_bucket_name(source_bucket)
        dest_bucket = source_bucket if (destination_bucket is None) else destination_bucket
        dest_bucket = self.get_bucket_name(dest_bucket)

        assert self.check_file_exists(source, bucket_name = source_bucket)
        ob_new = self.get_s3_object(destination, bucket_name = dest_bucket)

        if self.check_file_exists(ob_new.key, bucket_name = dest_bucket):
            assert overwrite is True

        fpath = pathjoin(source_bucket, source)
        ob_new.copy_from(CopySource = fpath)
        return ob_new

    def move(self, source, destination, source_bucket, destination_bucket, overwrite):
        new_ob = self.copy(source, destination, source_bucket, destination_bucket, overwrite)
        old_ob = self.get_s3_object(source, bucket_name = source_bucket)
        old_ob.delete()
        return new_ob

    def list_directory(self, path, limit):
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
        object_names = []
        if 'CommonPrefixes' in response:
            # we got common paths
            object_list = [t.values() for t in response['CommonPrefixes']]
            object_names += reduce(lambda x, y: x + y, object_list)
        if 'Contents' in response:
            # we got objects on the leaf nodes
            object_names += unquote_names([t['Key'] for t in response['Contents']])
        return map(os.path.normpath, object_names)

    def delete(self, file_name, recursive=False, delete=False):
        raise RuntimeError('Deleting on S3 backend is implemented by cottoncandy interface object')
