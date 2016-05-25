import os
import re
import json
import urllib
import fnmatch
from gzip import GzipFile
from dateutil.tz import tzlocal
from cStringIO import StringIO

import boto3
import botocore
from botocore.utils import fix_s3_host

import numpy as np

from utils import (clean_object_name,
                   has_magic,
                   has_real_magic,
                   remove_trivial_magic,
                   remove_root,
                   mk_aws_path,
                   objects2names,
                   unquote_names,
                   print_objects,
                   get_fileobject_size,
                   read_buffered,
                   GzipInputStream,
                   generate_ndarray_chunks,
                   )

import browser
# from . import browser


# S3 AWS
#---------
MB = 2**20
MIN_MPU_SIZE = 5*MB                # 5MB
MAX_PUT_SIZE = 5000*MB             # 5GB
MAX_MPU_SIZE = 5*MB*MB             # 5TB
MAX_MPU_PARTS = 10000              # 10,000
SEPARATOR = '/'



#------------------
# S3 Interfaces
#------------------

class InterfaceObject(object):
    pass


class BasicInterface(InterfaceObject):
    '''Basic cottoncandy interface to S3.
    '''
    def __init__(self, bucket_name,
                 ACCESS_KEY,SECRET_KEY,url,
                 verbose=True):
        '''
        Parameters
        ----------
        bucket_name : str
            Bucket to use
        ACCESS_KEY : str
            The S3 access key
        SECRET_KEY : str
            The S3 secret key
        url : str
            The URL for the S3 gateway

        Returns
        -------
        ccinterface  : ccio
            Cottoncandy interface object
        '''
        self.connection = self.connect(ACCESS_KEY=ACCESS_KEY,
                                       SECRET_KEY=SECRET_KEY,
                                       url=url)
        self.set_bucket(bucket_name)

        if verbose:
            print('Available buckets:')
            self.show_buckets()

    def __repr__(self):
        details = (__package__, self.bucket_name, self.url)
        return '%s.interface <bucket:%s on %s>' % details

    def connect(self, ACCESS_KEY=False, SECRET_KEY=False, url=None):
        '''Connect to S3 using boto'''
        self.url = url
        s3 = boto3.resource('s3',
                            endpoint_url=self.url,
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)
        s3.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
        return s3

    @clean_object_name
    def exists_object(self, object_name):
        '''Check whether object exists in bucket'''
        ob = self.connection.Object(self.bucket_name, object_name)
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

    def exists_bucket(self, bucket_name):
        '''Check whether the bucket exists'''
        try:
            self.connection.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise e
        else:
            exists = True
        return exists

    def create_bucket(self, bucket_name):
        '''Create a new bucket'''
        self.connection.create_bucket(Bucket=bucket_name)
        self.set_bucket(bucket_name)

    def set_bucket(self, bucket_name):
        '''Bucket to use'''
        if not self.exists_bucket(bucket_name):
            raise IOError('Bucket "%s" does not exist'%bucket_name)
        self.bucket_name = bucket_name

    def get_bucket(self):
        '''Get bucket boto3 object'''
        s3_bucket = self.connection.Bucket(self.bucket_name)
        return s3_bucket

    def get_bucket_objects(self, **kwargs):
        """Get list of objects from the bucket
        This is a wrapper to ``self.get_bucket().bucket.objects``

        Parameters
        ----------
        limit : int, 1000
            Maximum number of items to return
        page_size : int, 1000
            The page size for pagination
        filter : str
            Only return objects matching this string.
            Defaults to '/', all objects.
        kwargs : optional
            Dictionary of {method:value} for ``bucket.objects``

        Notes
        -----
        If you get a 'PaginationError', this means you have
        a lot of items on your bucket and should increase ``page_size``
        """
        defaults = dict(limit=1000,
                        page_size=1000,
                        filter=dict(Prefix=SEPARATOR),
                        )
        defaults.update(kwargs)
        bucket = self.get_bucket()
        prefix = defaults.pop('filter')

        if prefix['Prefix'] == SEPARATOR:
            request = bucket.objects
        else:
            request = bucket.objects.filter(**prefix)

        for method_name, value in defaults.iteritems():
            if value is None:
                continue
            method = getattr(request, method_name)
            if isinstance(value, dict):
                request = method(**value)
            else:
                request = method(value)

        response = request.all()
        return response

    def show_buckets(self):
        '''Show available buckets'''
        all_buckets = list(self.connection.buckets.all())
        timeformat = lambda x: x.creation_date.astimezone(tzlocal()).strftime
        info = ['{0: <40} {1}'.format(urllib.unquote(t.name),
                                      timeformat(t)('%Y/%m/%d (%H:%M:%S)'))\
                for t in all_buckets]
        print '\n'.join(info)

    @clean_object_name
    def get_object(self, object_name):
        """Get a boto3 object. Create it if it doesn't exist"""
        return self.connection.Object(self.bucket_name, object_name)

    def show_objects(self, limit=1000, page_size=1000):
        '''Print objects in the current bucket'''
        bucket = self.get_bucket()
        object_list = self.get_bucket_objects(limit=limit, page_size=page_size)
        try:
            print_objects(object_list)
        except botocore.exceptions.PaginationError:
            print('Loads of objects in "%s". Increasing page_size by 100x...'%self.bucket_name)
            object_list = self.get_bucket_objects(limit=limit, page_size=page_size*100)
            print_objects(object_list)

    @clean_object_name
    def download_object(self, object_name):
        '''Download object raw data.
        This simply calls the object body ``read()`` method.

        Parameters
        ---------
        object_name : str

        Returns
        -------
        byte_data : str
            Object byte contents
        '''
        if not self.exists_object(object_name):
            raise IOError('Object "%s" does not exist'%object_name)
        s3_object = self.get_object(object_name)
        return s3_object.get()['Body'].read()

    def upload_from_file(self, flname, object_name=None):
        '''Upload a file to S3.

        Parameters
        ----------
        flname : str
            Absolute path of file to upload
        object_name : str, None
            Name of uploaded object. If None, use
            the full file name as the object name.

        Returns
        -------
        response : boto3 response
        '''
        assert os.path.exists(flname)
        if object_name is None:
            object_name = os.path.abspath(flname)
        object_name = remove_root(object_name)
        s3_object = self.get_object(object_name)
        return s3_object.upload_file(flname)

    @clean_object_name
    def download_to_file(self, object_name, flname):
        '''Download S3 object to a file

        Parameters
        ----------
        object_name : str
        flname : str
            Absolute path where the data will be downloaded on disk
        '''
        assert self.exists_object(object_name) # make sure object exists
        s3_object = self.get_object(object_name)
        return s3_object.download_file(flname)

    @clean_object_name
    def mpu_fileobject(self, object_name, file_object,
                       buffersize=100*MB, **metadata):
        '''Multi-part upload for a python file-object.

        This automatically creates a multipart upload of an object.
        Useful for large objects that are loaded in memory. This avoids
        having to write the file to disk and then using ``upload_from_file``.

        Parameters
        ----------
        object_name : str
        file_object :
            file-like python object (e.g. StringIO, file, etc)
        buffersize  : int
            Byte size of the individual parts to create.
            Defaults to 100MB
        **metadata  : optional
            Metadata to store along with MPU object
        '''
        client = self.connection.meta.client
        mpu = client.create_multipart_upload(Bucket=self.bucket_name,
                                             Key=object_name,
                                             Metadata=metadata)

        # get size
        nbytes_total = get_fileobject_size(file_object)
        file_object.reset()

        # check if our buffersize is sensible
        if nbytes_total < buffersize:
            npotential = nbytes_total / float(MIN_MPU_SIZE)
            if npotential > 10:
                # 10MB sensible minimum for 50MB < x < 100MB (default)
                buffersize = MIN_MPU_SIZE*2
            else:
                # this file is smaller than chunksize by a little
                buffersize = MIN_MPU_SIZE

        # figure out parts split
        nparts = int(np.floor(nbytes_total / float(buffersize)))
        last_part_offset = int(nbytes_total - nparts*buffersize)

        # make sure we can upload
        assert nbytes_total < MAX_MPU_SIZE # 5TB
        assert buffersize >= MIN_MPU_SIZE  # 5MB
        assert nparts < MAX_MPU_PARTS      # 10,000
        assert nbytes_total > buffersize

        mpu_info = dict(Parts=[])
        print('MPU: %0.02fMB file in %i parts'%(nbytes_total/(2.**20), nparts))

        for chunk_idx in xrange(nparts):
            part_number = chunk_idx + 1
            if part_number == nparts:
                buffersize += last_part_offset
            txt = (part_number, nparts, buffersize/(2.**20), nbytes_total/(2.**20))
            print 'Uploading %i/%i: %0.02fMB of %0.02fMB'%txt

            data_chunk = file_object.read(buffersize)
            response = client.upload_part(Bucket=self.bucket_name,
                                          Key=object_name,
                                          UploadId=mpu['UploadId'],
                                          PartNumber=part_number,
                                          Body=data_chunk)

            # store the part info
            part_info = dict(PartNumber=part_number,
                             ETag=response['ETag'])
            mpu_info['Parts'].append(part_info)

        # finalize
        mpu_response = client.complete_multipart_upload(Bucket=self.bucket_name,
                                                        Key=object_name,
                                                        UploadId=mpu['UploadId'],
                                                        MultipartUpload=mpu_info)
        return mpu_response

    @clean_object_name
    def upload_json(self, object_name, ddict, **metadata):
        '''Upload a dict as a JSON using ``json.dumps``

        Parameters
        ----------
        object_name : str
        ddict : dict
        metadata : dict, optional
        '''
        json_data = json.dumps(ddict)
        obj = self.get_object(object_name)
        return obj.put(Body=json_data, Metadata=metadata)

    @clean_object_name
    def download_json(self, object_name):
        '''Download a JSON object

        Parameters
        ----------
        object_name : str

        Returns
        -------
        json_data : dict
            Dictionary representation of JSON file
        '''
        assert self.exists_object(object_name)
        obj = self.get_object(object_name)
        return json.loads(obj.get()['Body'].read())


class ArrayInterface(BasicInterface):
    '''Provides numpy.array concepts.
    '''
    def __init__(self, *args, **kwargs):
        '''
        Parameters
        ----------
        bucket_name : str
            Bucket to use
        ACCESS_KEY : str
            The S3 access key
        SECRET_KEY : str
            The S3 secret key
        url : str
            The URL for the S3 gateway

        Returns
        -------
        ccinterface : ccio
            Cottoncandy interface object
        '''
        super(ArrayInterface, self).__init__(*args, **kwargs)

    @clean_object_name
    def upload_npy_array(self, object_name, array, **metadata):
        '''Upload a np.ndarray using ``np.save``

        This method creates a copy of the array in memory
        before uploading since it relies on ``np.save`` to
        get a byte representation of the array.

        Parameters
        ----------
        object_name : str
        array : numpy.ndarray

        Returns
        -------
        reponse : boto3 upload response

        See Also
        --------
        The ``upload_raw_array`` method is more efficient
        '''
        # TODO: check array.dtype.hasobject
        arr_strio = StringIO()
        np.save(arr_strio, array)
        arr_strio.reset()
        try:
            response = self.get_object(object_name).put(Body=arr_strio.read(), Metadata=metadata)
        except OverflowError:
            response = self.mpu_fileobject(object_name, arr_strio, **metadata)
        return response

    @clean_object_name
    def download_npy_array(self, object_name):
        '''Download a np.ndarray uploaded using ``np.save`` with ``np.load``.

        Parameters
        ----------
        object_name : str

        Returns
        -------
        array : np.ndarray
        '''
        assert self.exists_object(object_name)
        array_object = self.get_object(object_name)
        array = np.load(StringIO(array_object.get()['Body'].read()))
        return array

    @clean_object_name
    def upload_raw_array(self, object_name, array, gzip=True, **metadata):
        '''Upload a a binary representation of a np.ndarray

        This method reads the array content from memory to upload.
        It does not have any overhead.

        Parameters
        ----------
        object_name : str
        array : np.ndarray
        gzip  : bool, optional
            Whether to gzip the array
        metadata : dict, optional

        Notes
        -----
        This method also uploads the array ``dtype``, ``shape``, and ``gzip``
        flag as metadata
        '''
        if array.nbytes >= 2**31:
            # avoid zlib issues
            gzip = False

        order = 'F' if array.flags.f_contiguous else 'C'
        meta = dict(dtype=array.dtype.str,
                    shape=','.join(map(str, array.shape)),
                    gzip=str(gzip),
                    order=order)

        # check for conflicts in metadata
        assert not any([key in meta for key in metadata.iterkeys()])
        meta.update(metadata)

        if gzip:
            zipdata = StringIO()
            gz = GzipFile(mode='wb', fileobj=zipdata)
            gz.write(array.data)
            gz.close()
            zipdata.seek(0)
            fl = zipdata
            data_nbytes = get_fileobject_size(fl)
        else:
            data_nbytes = array.nbytes
            fl = StringIO(array.data)

        if data_nbytes > 100*MB:
            response = self.mpu_fileobject(object_name, fl, **meta)
        else:
            response = self.get_object(object_name).put(Body=fl, Metadata=meta)

        return response

    @clean_object_name
    def download_raw_array(self, object_name, buffersize=2**16, **kwargs):
        '''Download a binary np.ndarray and return an np.ndarray object
        This method downloads an array without any disk or memory overhead.

        Parameters
        ----------
        object_name : str
        buffersize  : optional

        Returns
        -------
        array : np.ndarray

        Notes
        -----
        The object must have metadata containing: shape, dtype and a gzip
        boolean flag. This is all automatically handled by ``upload_raw_array``.
        '''
        assert self.exists_object(object_name)
        array_object = self.get_object(object_name)

        shape = array_object.metadata['shape']
        shape = map(int, shape.split(',')) if shape else ()
        dtype = np.dtype(array_object.metadata['dtype'])
        order = array_object.metadata.get('order','C')
        array = np.empty(shape, dtype=dtype, order=order)

        body = array_object.get()['Body']
        if 'gzip' in array_object.metadata and array_object.metadata['gzip'] == 'True':
            # gzipped!
            datastream = GzipInputStream(body)
        else:
            datastream = body

        read_buffered(datastream, array, buffersize=buffersize)
        return array

    @clean_object_name
    def dict2cloud(self, object_name, array_dict, **metadata):
        '''Upload an arbitrary depth dictionary containing arrays

        Parameters
        ----------
        object_name : str
        array_dict  : dict
            An arbitrary depth dictionary of arrays. This can be
            conceptualized as implementing an HDF-like group
        '''
        for k,v in array_dict.iteritems():
            name = SEPARATOR.join([object_name, k])

            if isinstance(v, dict):
                _ = self.dict2cloud(name, v, **metadata)
            elif isinstance(v, np.ndarray):
                _ = self.upload_raw_array(name, v, **metadata)
            else: # try converting to array
                _ = self.upload_raw_array(name, np.asarray(v))
        print 'Uploaded arrays in "%s"'%object_name

    @clean_object_name
    def upload_dask_array(self, object_name, arr, axis=-1, buffersize=100*MB):
        '''Upload an array in chunks and store the metadata to reconstruct
        the complete matrix with ``dask``.

        Parameters
        ----------
        object_name : str
        arr  : np.ndarray
        axis : int, None
            The axis along which to slice the array. If None is given,
            the array is chunked into ideal isotropic voxels.
            ``axis=None`` is WIP and atm works fine for near isotropic matrices
        buffersize : scalar
            Byte size of the desired array chunks

        Returns
        -------
        response : boto3 response

        Notes
        -----
        Each array chunk is uploaded as a raw np.array with the prefix "pt%04i".
        The metadata is stored as a json file ``metadata.json``. For example, if
        an array is uploaded with the name "my_array_name" and split into 2 parts,
        the following objects are created:

        * my_array_name/pt0000
        * my_array_name/pt0001
        * my_array_name/metadata.json
        '''
        metadata = dict(shape=arr.shape,
                        dtype=arr.dtype.str,
                        dask=[],
                        chunk_sizes=[],
                        )

        generator = generate_ndarray_chunks(arr, axis=axis, buffersize=buffersize)
        total_upload = 0.0
        for idx, (chunk_coord, chunk_arr) in enumerate(generator):
            chunk_arr = chunk_arr.copy()
            total_upload += chunk_arr.nbytes
            txt = (idx+1, total_upload/MB, arr.nbytes/np.float(MB))
            print 'uploading %i: %0.02fMB/%0.02fMB'%txt

            part_name = SEPARATOR.join([object_name, 'pt%04i'%idx])
            metadata['dask'].append((chunk_coord, part_name))
            metadata['chunk_sizes'].append(chunk_arr.shape)
            self.upload_raw_array(part_name, chunk_arr)

        # convert to dask convention (sorry)
        details = [t[0] for t in metadata['dask']]
        dimension_sizes = [dict() for idx in xrange(arr.ndim)]
        for dim, chunks in enumerate(zip(*details)):
            for sample_idx, chunk_idx in enumerate(chunks):
                if chunk_idx not in dimension_sizes[dim]:
                    dimension_sizes[dim][chunk_idx] = metadata['chunk_sizes'][sample_idx][dim]

        chunks = [[value for k,value in sorted(sizes.iteritems())] for sizes in dimension_sizes]
        metadata['chunks'] = chunks
        return self.upload_json(SEPARATOR.join([object_name, 'metadata.json']), metadata)

    @clean_object_name
    def download_dask_array(self, object_name, dask_name='array'):
        """Downloads a split matrix as a ``dask.array.Array`` object

        This uses the stored object metadata to reconstruct the full
        n-dimensional array uploaded using ``upload_dask_array``.

        Examples
        --------
        >>> s3_response = fog.upload_dask_array('test_dim', arr, axis=-1)
        >>> dask_object = fog.download_dask_array('test_dim')
        >>> dask_object
        dask.array<array, shape=(100, 600, 1000), dtype=float64, chunksize=(100, 600, 100)>
        >>> dask_slice = dask_object[..., :200]
        >>> dask_slice
        dask.array<getitem..., shape=(100, 600, 1000), dtype=float64, chunksize=(100, 600, 100)>
        >>> downloaded_data = np.asarray(dask_slice) # this downloads the array
        >>> downloaded_data.shape
        (100, 600, 200)
        """
        from dask import array as da

        metadata = self.download_json(SEPARATOR.join([object_name, 'metadata.json']))
        chunks = metadata['chunks']
        shape = metadata['shape']
        dtype = np.dtype(metadata['dtype'])

        dask = {(dask_name,)+tuple(shape): (self.download_raw_array, part_name) \
                for shape, part_name in metadata['dask']}

        return da.Array(dask, dask_name, chunks, shape=shape, dtype=dtype)


class FileSystemInterface(BasicInterface):
    '''Emulate some file system functionality.
    '''
    def __init__(self, *args, **kwargs):
        '''
        Parameters
        ----------
        bucket_name : str
            Bucket to use
        ACCESS_KEY : str
            The S3 access key
        SECRET_KEY : str
            The S3 secret key
        url : str
            The URL for the S3 gateway

        Returns
        -------
        ccinterface : ccio
            Cottoncandy interface object
        '''
        super(FileSystemInterface, self).__init__(*args, **kwargs)

    @clean_object_name
    def lsdir(self, prefix, limit=10**3):
        '''List the contents of a directory
        '''
        if has_real_magic(prefix):
            raise ValueError('Use ``ls()`` when using search patterns: "%s"'%prefix)

        prefix = remove_trivial_magic(prefix)
        prefix = mk_aws_path(prefix)

        response = self.get_bucket().meta.client.list_objects(Bucket=self.bucket_name,
                                                              Delimiter=SEPARATOR,
                                                              Prefix=prefix,
                                                              MaxKeys=limit)
        object_names = []
        if 'CommonPrefixes' in response:
            # we got common paths
            object_list = [t.values() for t in response['CommonPrefixes']]
            object_names += reduce(lambda x,y: x+y, object_list)
        if 'Contents' in response:
            # we got objects on the leaf nodes
            object_names += unquote_names([t['Key'] for t in response['Contents']])
        return map(os.path.normpath, object_names)

    @clean_object_name
    def ls(self, pattern, page_size=10**3, limit=10**3):
        '''File-system like search for S3 objects
        '''
        pattern = remove_trivial_magic(pattern)
        pattern = os.path.normpath(pattern)

        if has_real_magic(pattern):
            magic_check = re.compile('[*?[]')
            prefix = magic_check.split(pattern)[0]
        else:
            prefix = pattern

        # get objects that match common prefix
        if not has_real_magic(pattern):
            object_names = self.lsdir(prefix, limit=limit)
        else:
            object_list = self.get_bucket_objects(filter=dict(Prefix=prefix),
                                                  page_size=page_size,
                                                  limit=limit)
            object_names = objects2names(object_list)

        # remove trailing '/'
        object_names = map(os.path.normpath, object_names)

        if has_real_magic(pattern):
            # get the unique sub-directories
            depth = len(pattern.split(SEPARATOR))
            object_names = {SEPARATOR.join(t.split(SEPARATOR)[:depth]):1 for t in object_names}.keys()
            # filter the list with glob pattern
            object_names = fnmatch.filter(object_names, pattern)
        print '\n'.join(sorted(object_names))

    @clean_object_name
    def glob(self, pattern, **kwargs):
        """Return a list of object names in the bucket
        that match the glob pattern.

        Parameters
        ----------
        pattern : str,
            A glob pattern string
        verbose : bool, optional
            If True, also print object name and creation date
        limit : None, int, optional
        page_size: int, optional

        Returns
        -------
        object_names : list

        Example
        -------
        >>> myinterface.glob('/path/to/*/file01*.hdf/image_data')
        ['/path/to/my/file01a.hdf/image_data',
         '/path/to/my/file01b.hdf/image_data',
         '/path/to/your/file01a.hdf/image_data',
         '/path/to/your/file01b.hdf/image_data']
        >>> items = myinterface.glob('/path/to/my/file02*.hdf/*')
        ['/path/to/my/file02a.hdf/image_data',
         '/path/to/my/file02a.hdf/text_data',
         '/path/to/my/file02b.hdf/image_data',
         '/path/to/my/file02b.hdf/text_data',]

        Extended Summary
        ----------------
        Some gotchas

        limit: None, int, optional
            The maximum number of objects to return
        page_size: int, optional
            This is important for buckets with loads of objects.
            By default, ``glob`` will download a maximum of 10^6
            object names and perform the search. If more objects exist,
            the search might not find them and the page_size should
            be increased.

        Notes
        -----
        If more than 10^6 objects, provide ``page_size=10**7`` kwarg.
        """
        # determine if we're globbing
        magic_check = re.compile('[*?[]')
        def has_magic(s):
            return magic_check.search(s) is not None

        # find the common prefix
        prefix = magic_check.split(pattern)[0] \
                 if has_magic(pattern) else pattern

        page_size = kwargs.get('page_size', 1000000)
        limit = kwargs.get('limit', None)

        object_list = self.get_bucket_objects(filter=dict(Prefix=prefix),
                                              page_size=page_size,
                                              limit=limit)

        mapper = {urllib.unquote(obj.key):obj for obj in object_list}
        object_names = mapper.keys()

        matches = fnmatch.filter(object_names, pattern)\
                  if has_magic(pattern) else object_names
        matches = sorted(matches)

        if kwargs.get('verbose', False):
            # print objects found
            objects_found = [mapper[match] for match in matches]
            if len(objects_found):
                print_objects(objects_found)
            print('Found %i objects matching "%s"'%(len(objects_found), pattern))
        return matches

    @clean_object_name
    def search(self, pattern, **kwargs):
        '''Print the objects matching the glob pattern

        See ``glob`` documentation for details
        '''
        matches = self.glob(pattern, verbose=True, **kwargs)


    def get_browser(self):
        '''Return an object which can be tab-completed
        to browse the contents of the bucket as if it were a file-system

        See documentation for ``cottoncandy.get_browser``
        '''
        return browser.S3Directory('', interface=self)


class DefaultInterface(FileSystemInterface,
                       ArrayInterface,
                       BasicInterface):
    '''Default cottoncandy interface to S3

    This includes numpy.array and file-system-like
    concepts for easy data I/O and bucket/object exploration.
    '''
    def __init__(self, *args, **kwargs):
        '''
        Parameters
        ----------
        bucket_name : str
            Bucket to use
        ACCESS_KEY : str
            The S3 access key
        SECRET_KEY : str
            The S3 secret key
        url : str
            The URL for the S3 gateway
        '''
        super(DefaultInterface, self).__init__(*args, **kwargs)
