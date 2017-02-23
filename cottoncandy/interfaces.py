import json

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    from urllib import unquote
except ImportError:
    from urllib.parse import unquote

import fnmatch
from gzip import GzipFile

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

from base64 import b64decode, b64encode

import numpy as np
from scipy.sparse import (coo_matrix,
                          csr_matrix,
                          csc_matrix,
                          bsr_matrix,
                          dia_matrix)

import cottoncandy.browser

from S3Client import *
from GDriveClient import *
from utils import *
from warnings import warn
from Encryption import *


# ------------------
# Cloud Interfaces
# ------------------

class InterfaceObject(object):
    pass


class BasicInterface(InterfaceObject):
    """Basic cottoncandy interface to S3.
    """

    def __init__(self, bucket_name,
                 ACCESS_KEY, SECRET_KEY, url = None,
                 force_bucket_creation = False,
                 verbose = True, backend = 's3'):
        """
        Parameters
        ----------
        bucket_name : str
            Bucket to use
        ACCESS_KEY : str
            The S3 access key, or client secrets json file
        SECRET_KEY : str
            The S3 secret key, or client credentials file
        url : str
            The URL for the S3 gateway
        force_bucket_creation: bool
            if bucket does not exist, make it?
        verbose: bool
            print things?
        backend: 's3'|'gdrive'
            Access s3 or google drive?

        Returns
        -------
        cci  : ccio
            Cottoncandy interface object
        """

        if backend == 's3':
            self.interface = S3Client(bucket_name, ACCESS_KEY, SECRET_KEY, url, force_bucket_creation)
        elif backend == 'gdrive':
            self.interface = GDriveClient(ACCESS_KEY, SECRET_KEY)
        else:
            raise ValueError('Bad backend')

        if verbose:
            if backend == 's3':
                print('Available buckets:')
                self.show_buckets()
            else:
                print('Google drive backend instantiated.')

    def __repr__(self):
        if isinstance(self.interface, S3Client):
            details = (__package__, self.bucket_name, self.interface.url)
            return '%s.interface <bucket:%s on %s>' % details
        else:
            return '{}.interface on Google Drive'.format(__package__)

    def _get_bucket_name(self, bucket_name):
        return self.interface._get_bucket_name(bucket_name)

    @property
    def bucket_name(self):
        if isinstance(self.interface, S3Client):
            return self.interface.bucket_name
        else:
            print('Google drive has no concept of buckets')
            return None

    @clean_object_name
    def exists_object(self, object_name, bucket_name = None):
        """Check whether object exists in bucket

        Parameters
        ----------
        object_name : str
            The object name
        """
        return self.interface.check_file_exists(object_name, bucket_name)

    def exists_bucket(self, bucket_name):
        """Check whether the bucket exists"""
        return self.interface.check_bucket_exists(bucket_name)

    def create_bucket(self, bucket_name, acl = DEFAULT_ACL):
        """Create a new bucket"""
        self.interface.create_bucket(bucket_name, acl)

    def set_bucket(self, bucket_name):
        """Bucket to use"""
        self.interface.set_current_bucket(bucket_name)

    def get_bucket(self):
        """Get bucket boto3 object"""
        return self.interface.get_bucket()

    def get_bucket_objects(self, **kwargs):
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
        warn('Deprecated. Use get_objects() instead', DeprecationWarning)
        return self.interface.list_objects(**kwargs)

    def get_objects(self, **kwargs):
        """
        Like get_bucket_objects, but more aptly named to the generic interface
        Parameters
        ----------
        self
        kwargs

        Returns
        -------

        """
        return self.interface.list_objects(**kwargs)

    def get_bucket_size(self, limit = 10 ** 6, page_size = 10 ** 6):
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
        warn('Deprecated, use get_size() instead', DeprecationWarning)
        return self.interface.size

    def get_size(self):
        """
        Gets the total size of the current container of objects. Generic naming.
        Parameters
        ----------
        self

        Returns
        -------

        """
        return self.interface.size

    def show_buckets(self):
        """Show available buckets"""
        self.interface.show_all_buckets()

    @clean_object_name
    def get_object(self, object_name, bucket_name = None):
        """Get a boto3 object. Create it if it doesn't exist"""
        # TODO: should this be here? this is an s3-specific action
        # NOTE: keeping this in case outside code is using this.
        return self.interface.get_s3_object(object_name, bucket_name)

    def show_objects(self, limit = 1000, page_size = 1000):
        """Print objects in the current bucket"""
        if isinstance(self.interface, S3Client):
            object_list = self.interface.list_objects(limit, page_size)
            try:
                print_objects(object_list)
            except botocore.exceptions.PaginationError:
                print('Loads of objects in "%s". Increasing page_size by 100x...' % self.bucket_name)
                object_list = self.interface.list_objects(limit = limit, page_size = page_size * 100)
                print_objects(object_list)
        else:
            driveFiles = self.interface.drive.ListFile({'q': "trashed=false"}).GetList()
            object_list = [df['title'] for df in driveFiles]
            for obj in object_list:
                # TODO: also print last modified date and whatever else to match s3
                print(obj)

    # With the abstraction of the cloud interface, and the encrypting interface, all file I/O methods
    # should be calling upload_object() and download_stream() instead of directly interfacing with the
    # CCBackEnd object or the actual cloud APIs

    @clean_object_name
    def upload_object(self, object_name, body, acl = DEFAULT_ACL, **metadata):
        self.interface.upload_stream(body, object_name, metadata, acl)

    def download_stream(self, object_name):
        """
        Returns the CloudStream object for an object
        Parameters
        ----------
        self
        object_name

        Returns
        -------
        CloudStream object
        """
        return self.interface.download_stream(object_name)

    def upload_from_file(self, flname, object_name = None,
                         ExtraArgs = dict(ACL = DEFAULT_ACL)):
        """Upload a file to S3.

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
        """
        return self.interface.upload_file(flname, object_name, ExtraArgs['ACL'])

    @clean_object_name
    def download_to_file(self, object_name, fileName):
        """Download S3 object to a file

        Parameters
        ----------
        object_name : str
        fileName : str
            Absolute path where the data will be downloaded on disk
        """
        return self.interface.download_to_file(object_name, fileName)

    @clean_object_name
    def download_object(self, object_name):
        """Download object raw data.
        This simply calls the object body ``read()`` method.

        Parameters
        ---------
        object_name : str

        Returns
        -------
        byte_data : str
            Object byte contents
        """
        return self.download_stream(object_name).content.read()

    @clean_object_name
    def mpu_fileobject(self, object_name, file_object,
                       buffersize = MPU_CHUNKSIZE, verbose = True, **metadata):
        """Multi-part upload for a python file-object.

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
        verbose     : bool
            verbosity flag of whether to print mpu information to stdout
        **metadata  : optional
            Metadata to store along with MPU object
        """
        return self.interface.upload_multipart(file_object, object_name, metadata)

    @clean_object_name
    def upload_json(self, object_name, ddict, acl = DEFAULT_ACL, **metadata):
        """Upload a dict as a JSON using ``json.dumps``

        Parameters
        ----------
        object_name : str
        ddict : dict to upload
        metadata : dict, optional
        """
        json_data = json.dumps(ddict)
        return self.upload_object(object_name, StringIO(json_data), acl, **metadata)

    @clean_object_name
    def download_json(self, object_name):
        """Download a JSON object

        Parameters
        ----------
        object_name : str

        Returns
        -------
        json_data : dict
            Dictionary representation of JSON file
        """
        assert self.exists_object(object_name)
        obj = self.download_object(object_name)
        return json.loads(obj)

    @clean_object_name
    def upload_pickle(self, object_name, data_object, acl = DEFAULT_ACL):
        """Upload an object using pickle: ``pickle.dumps``

        Parameters
        ----------
        object_name : str
        data_object : object
        """
        return self.upload_object(object_name, StringIO(pickle.dumps(data_object)), acl)

    @clean_object_name
    def download_pickle(self, object_name):
        """Download a pickle object

        Parameters
        ----------
        object_name : str

        Returns
        -------
        data_object : object
        """
        assert self.exists_object(object_name)
        obj = self.download_object(object_name)
        return pickle.loads(obj)


class ArrayInterface(BasicInterface):
    """Provides numpy.array concepts.
    """

    def __init__(self, *args, **kwargs):
        """
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
        cci : ccio
            Cottoncandy interface object
        """
        super(ArrayInterface, self).__init__(*args, **kwargs)

    @clean_object_name
    def upload_npy_array(self, object_name, array, acl = DEFAULT_ACL, **metadata):
        """Upload a np.ndarray using ``np.save``

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
        :func:`upload_raw_array` which is more efficient
        """
        # TODO: check array.dtype.hasobject
        arr_strio = StringIO()
        np.save(arr_strio, array)
        arr_strio.reset()
        try:
            response = self.upload_object(object_name, arr_strio, acl, **metadata)
        except OverflowError:
            response = self.mpu_fileobject(object_name, arr_strio, **metadata)
        return response

    @clean_object_name
    def download_npy_array(self, object_name):
        """Download a np.ndarray uploaded using ``np.save`` with ``np.load``.

        Parameters
        ----------
        object_name : str

        Returns
        -------
        array : np.ndarray
        """
        assert self.exists_object(object_name)
        array = np.load(StringIO(self.download_object(object_name)))
        return array

    @clean_object_name
    def upload_raw_array(self, object_name, array, gzip = True, acl = DEFAULT_ACL, **metadata):
        """Upload a a binary representation of a np.ndarray

        This method reads the array content from memory to upload.
        It does not have any overhead.

        Parameters
        ----------
        object_name : str
        array : np.ndarray
        gzip  : bool, optional
            Whether to gzip the array
        acl : str
            "access control list", specifies permissions for s3 data.
            default is "authenticated-read" (authenticated users can read)
        metadata : dict, optional

        Notes
        -----
        This method also uploads the array ``dtype``, ``shape``, and ``gzip``
        flag as metadata
        """
        if array.nbytes >= 2 ** 31:
            # avoid zlib issues
            gzip = False

        order = 'F' if array.flags.f_contiguous else 'C'
        if not array.flags['%s_CONTIGUOUS' % order]:
            print ('array is a slice along a non-contiguous axis. copying the array '
                   'before saving (will use extra memory)')
            array = np.array(array, order = order)

        meta = dict(dtype = array.dtype.str,
                    shape = ','.join(map(str, array.shape)),
                    gzip = str(gzip),
                    order = order)

        # check for conflicts in metadata
        assert not any([key in meta for key in metadata.iterkeys()])
        meta.update(metadata)

        if gzip:
            zipdata = StringIO()
            gz = GzipFile(mode = 'wb', fileobj = zipdata)
            gz.write(array.data)
            gz.close()
            zipdata.seek(0)
            fileStream = zipdata
            data_nbytes = get_fileobject_size(fileStream)
        else:
            data_nbytes = array.nbytes
            fileStream = StringIO(array.data)

        if data_nbytes > MPU_THRESHOLD:
            response = self.mpu_fileobject(object_name, fileStream, metadata = meta)
        else:
            response = self.upload_object(object_name, fileStream, DEFAULT_ACL, **meta)

        return response

    @clean_object_name
    def download_raw_array(self, object_name, buffersize = 2 ** 16, **kwargs):
        """Download a binary np.ndarray and return an np.ndarray object
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
        """
        assert self.exists_object(object_name)
        arrayStream = self.download_stream(object_name)

        shape = arrayStream.metadata['shape']
        shape = map(int, shape.split(',')) if shape else ()
        dtype = np.dtype(arrayStream.metadata['dtype'])
        order = arrayStream.metadata.get('order', 'C')
        array = np.empty(shape, dtype = dtype, order = order)

        body = arrayStream.content
        if 'gzip' in arrayStream.metadata and arrayStream.metadata['gzip'] == 'True':
            # gzipped!
            datastream = GzipInputStream(body)
        else:
            datastream = body

        read_buffered(datastream, array, buffersize = buffersize)
        return array

    @clean_object_name
    def dict2cloud(self, object_name, array_dict, acl = DEFAULT_ACL,
                   verbose = True, **metadata):
        """Upload an arbitrary depth dictionary containing arrays

        Parameters
        ----------
        object_name : str
        array_dict  : dict
            An arbitrary depth dictionary of arrays. This can be
            conceptualized as implementing an HDF-like group
        verbose : bool
            Whether to print object_name after completion
        """
        for k, v in array_dict.iteritems():
            name = self.pathjoin(object_name, k)

            if isinstance(v, dict):
                _ = self.dict2cloud(name, v, acl = acl, **metadata)
            elif isinstance(v, np.ndarray):
                _ = self.upload_raw_array(name, v, acl = acl, **metadata)
            else:  # try converting to array
                _ = self.upload_raw_array(name, np.asarray(v), acl = acl)

        if verbose:
            print('uploaded arrays in "%s"' % object_name)

    @clean_object_name
    def cloud2dict(self, object_root, verbose = True, **metadata):
        """Download all the arrays of the object branch and return a dictionary.
        This is the complement to ``dict2cloud``

        Parameters
        ----------
        object_root : str
            The branch to create the dictionary from
        verbose : bool
            Whether to print object_root after completion

        Returns
        -------
        datadict  : dict
            An arbitrary depth dictionary.
        """

        # TODO: gdrive compatibility?
        from cottoncandy.browser import S3Directory
        ob = S3Directory(object_root, interface = self)

        datadict = {}
        subdirs = ob._ls()

        for subdir in subdirs:
            path = self.pathjoin(object_root, subdir)
            if self.exists_object(path):
                # TODO: allow non-array things
                try:
                    arr = self.download_raw_array(path)
                except KeyError as e:
                    print('could not download "%s: missing %s from metadata"' % (path, e))
                    arr = None
                datadict[subdir] = arr
            else:
                datadict[subdir] = self.cloud2dict(path)

        if verbose:
            print('downloaded arrays in "%s"' % object_root)

        return datadict

    @clean_object_name
    def cloud2dataset(self, object_root, **metadata):
        """Get a dataset representation of the object branch.

        Parameters
        ----------
        object_root : str
            The branch to create a dataset from

        Returns
        -------
        cc_dataset_object  : cottoncandy.BrowserObject
            This can be conceptualized as implementing an h5py/pytables
            object with ``load()`` and ``keys()`` methods.
        """
        from cottoncandy.browser import S3Directory
        return S3Directory(object_root, interface = self)

    @clean_object_name
    def upload_dask_array(self, object_name, arr, axis = -1, buffersize = DASK_CHUNKSIZE, **metakwargs):
        """Upload an array in chunks and store the metadata to reconstruct
        the complete matrix with ``dask``.

        Parameters
        ----------
        object_name : str
        arr  : np.ndarray
        axis : int or None (default: -1)
            The axis along which to slice the array. If None is given,
            the array is chunked into ideal isotropic voxels.
            ``axis=None`` is WIP and atm works fine for near isotropic matrices
        buffersize : scalar (default: 100MB)
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
        """
        metadata = dict(shape = arr.shape,
                        dtype = arr.dtype.str,
                        dask = [],
                        chunk_sizes = [],
                        )

        generator = generate_ndarray_chunks(arr, axis = axis, buffersize = buffersize)
        total_upload = 0.0
        for idx, (chunk_coord, chunk_arr) in enumerate(generator):
            chunk_arr = chunk_arr.copy()
            total_upload += chunk_arr.nbytes
            txt = (idx + 1, total_upload / MB, arr.nbytes / np.float(MB))
            print('uploading %i: %0.02fMB/%0.02fMB' % txt)

            part_name = self.pathjoin(object_name, 'pt%04i' % idx)
            metadata['dask'].append((chunk_coord, part_name))
            metadata['chunk_sizes'].append(chunk_arr.shape)
            self.upload_raw_array(part_name, chunk_arr)

        # convert to dask convention (sorry)
        details = [t[0] for t in metadata['dask']]
        dimension_sizes = [dict() for idx in range(arr.ndim)]
        for dim, chunks in enumerate(zip(*details)):
            for sample_idx, chunk_idx in enumerate(chunks):
                if chunk_idx not in dimension_sizes[dim]:
                    dimension_sizes[dim][chunk_idx] = metadata['chunk_sizes'][sample_idx][dim]

        chunks = [[value for k, value in sorted(sizes.iteritems())] for sizes in dimension_sizes]
        metadata['chunks'] = chunks
        return self.upload_json(self.pathjoin(object_name, 'metadata.json'), metadata, **metakwargs)

    @clean_object_name
    def download_dask_array(self, object_name, dask_name = 'array'):
        """Downloads a split matrix as a ``dask.array.Array`` object

        This uses the stored object metadata to reconstruct the full
        n-dimensional array uploaded using ``upload_dask_array``.

        Examples
        --------
        >>> s3_response = cci.upload_dask_array('test_dim', arr, axis=-1)
        >>> dask_object = cci.download_dask_array('test_dim')
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

        metadata = self.download_json(self.pathjoin(object_name, 'metadata.json'))
        chunks = metadata['chunks']
        shape = metadata['shape']
        dtype = np.dtype(metadata['dtype'])

        dask = {(dask_name,) + tuple(shape): (self.download_raw_array, part_name) \
                for shape, part_name in metadata['dask']}

        return da.Array(dask, dask_name, chunks, shape = shape, dtype = dtype)

    @clean_object_name
    def upload_sparse_array(self, object_name, arr):
        """Uploads a scipy.sparse array as a folder of array objects

        Parameters
        ----------
        object_name : str
            The name of the object to be stored.
        arr : scipy.sparse.spmatrix
            A scipy.sparse array to be saved. If type is DOK or LIL,
            it will be converted to csr before saving
        """
        if isinstance(arr, csr_matrix):
            attrs = ['data', 'indices', 'indptr']
            arrtype = 'csr'
        elif isinstance(arr, coo_matrix):
            attrs = ['row', 'col', 'data']
            arrtype = 'coo'
        elif isinstance(arr, csc_matrix):
            attrs = ['data', 'indices', 'indptr']
            arrtype = 'csc'
        elif isinstance(arr, bsr_matrix):
            attrs = ['data', 'indices', 'indptr']
            arrtype = 'bsr'
        elif isinstance(arr, dia_matrix):
            attrs = ['data', 'offsets']
            arrtype = 'dia'
        else:  # dok and lil: convert to csr and save
            # TODO: warn user here that matrix type will be changed?
            arr = arr.tocsr()
            attrs = ['data', 'indices', 'indptr']
            arrtype = 'csr'

        # Upload parts
        for attr in attrs:
            self.upload_raw_array(self.pathjoin(object_name, attr), getattr(arr, attr))

        # Upload metadata
        metadata = dict(type = arrtype, attrs = attrs, shape = arr.shape)
        return self.upload_json(self.pathjoin(object_name, 'metadata.json'), metadata)

    @clean_object_name
    def download_sparse_array(self, object_name):
        """Downloads a scipy.sparse array

        Parameters
        ----------
        object_name : str
            The object name for the sparse array to be retrieved.

        Returns
        -------
        arr : scipy.sparse.spmatrix
            The array stored at the location given by object_name
        """
        # Get metadata
        metadata = self.download_json(self.pathjoin(object_name, 'metadata.json'))
        # Get type, shape
        arrtype = metadata['type']
        shape = metadata['shape']
        # Get data
        d = dict()
        for attr in metadata['attrs']:
            d[attr] = self.download_raw_array(self.pathjoin(object_name, attr))

        if arrtype == 'csr':
            arr = csr_matrix((d['data'], d['indices'], d['indptr']),
                             shape = shape)
        elif arrtype == 'coo':
            arr = coo_matrix((d['data'], (d['row'], d['col'])),
                             shape = shape)
        elif arrtype == 'csc':
            arr = csc_matrix((d['data'], d['indices'], d['indptr']),
                             shape = shape)
        elif arrtype == 'bsr':
            arr = bsr_matrix((d['data'], d['indices'], d['indptr']),
                             shape = shape)
        elif arrtype == 'dia':
            arr = dia_matrix((d['data'], d['offsets']), shape = shape)

        return arr


class FileSystemInterface(BasicInterface):
    """Emulate some file system functionality.
    """

    def __init__(self, *args, **kwargs):
        """
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
        cci : ccio
            Cottoncandy interface object
        """
        super(FileSystemInterface, self).__init__(*args, **kwargs)

    def lsdir(self, path = '/', limit = 10 ** 3):
        return self.interface.list_directory(path, limit)

    @clean_object_name
    def ls(self, pattern, page_size = 10 ** 3, limit = 10 ** 3, verbose = False):
        """File-system like search for S3 objects

        Parameters
        ----------
        pattern : str
            A ls-style command line like query

        page_size : int (default: 1,000)
        limit : int (default: 1,000)

        Returns
        -------
        object_names : list
            Object names that match the search pattern

        Notes
        -----
        Increase ``page_size`` and ``limit`` if you have a lot of objects
        otherwise, the search might not return all matching objects in store.
        """
        pattern = remove_trivial_magic(pattern)
        pattern = os.path.normpath(pattern)

        if has_real_magic(pattern):
            magic_check = re.compile('[*?[]')
            prefix = magic_check.split(pattern)[0]
        else:
            prefix = pattern

        # get objects that match common prefix
        if not has_real_magic(pattern):
            object_names = self.lsdir(prefix, limit = limit)
        else:
            object_list = self.get_objects(filter = dict(Prefix = prefix),
                                                  page_size = page_size,
                                                  limit = limit)
            object_names = objects2names(object_list)

        # remove trailing '/'
        object_names = map(os.path.normpath, object_names)

        if has_real_magic(pattern):
            # get the unique sub-directories
            depth = len(pattern.split(SEPARATOR))
            object_names = {SEPARATOR.join(t.split(SEPARATOR)[:depth]): 1 for t in object_names}.keys()
            # filter the list with glob pattern
            object_names = fnmatch.filter(object_names, pattern)
        if verbose:
            print('\n'.join(sorted(object_names)))
        return object_names

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
        >>> cci.glob('/path/to/*/file01*.grp/image_data')
        ['/path/to/my/file01a.grp/image_data',
         '/path/to/my/file01b.grp/image_data',
         '/path/to/your/file01a.grp/image_data',
         '/path/to/your/file01b.grp/image_data']
        >>> cci.glob('/path/to/my/file02*.grp/*')
        ['/path/to/my/file02a.grp/image_data',
         '/path/to/my/file02a.grp/text_data',
         '/path/to/my/file02b.grp/image_data',
         '/path/to/my/file02b.grp/text_data',]

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

        if isinstance(self.interface, S3Client):
            return self.glob_s3(pattern, **kwargs)
        else:
            return self.glob_google_drive(pattern)

    def glob_google_drive(self, pattern):
        """
        Globbing on google drive
        Parameters
        ----------
        pattern

        Returns
        -------

        """
        matches = []
        if '/' not in pattern:	# end tree, list all objects
            return self.interface.list_objects(True)

        nextFolderExp = r'^/?[^/]*/'
        nextFolder = re.match(nextFolderExp, pattern).group(0)
        pattern = re.sub(nextFolderExp, '', pattern)

        if '*' not in nextFolder:	# no wildcard, simply cd into it
            self.interface.cd(nextFolder)
        else:						# find all wildcard matches and recursively glob them
            files = self.interface.list_objects()
            for f in files:
                matches.append(self.glob_google_drive())

        # TODO: finish this

        return


    def glob_s3(self, pattern, **kwargs):
        """
        Globbing on S3

        Parameters
        ----------
        pattern
        kwargs

        Returns
        -------

        """
        prefix = MAGIC_CHECK.split(pattern)[0] if has_magic(pattern) else pattern

        page_size = kwargs.get('page_size', 1000000)
        limit = kwargs.get('limit', None)

        object_list = self.get_objects(filter = dict(Prefix = prefix),
                                       page_size = page_size,
                                       limit = limit)

        mapper = {unquote(obj.key): obj for obj in object_list}
        object_names = mapper.keys()

        matches = fnmatch.filter(object_names, pattern) \
            if has_magic(pattern) else object_names
        matches = sorted(matches)

        if kwargs.get('verbose', False):
            # print objects found
            objects_found = [mapper[match] for match in matches]
            if len(objects_found):
                print_objects(objects_found)
            print('Found %i objects matching "%s"' % (len(objects_found), pattern))
        return matches

    @clean_object_name
    def download_directory(self, directory, diskName):
        """
        Download an entire directory

        Parameters
        ----------
        self
        directory : str
            directory on s3 to download
        diskName :
            name of directory on disk to download to

        Returns
        -------

        """
        if has_real_magic(directory):
            raise NotImplementedError('Wildcards not implemented')

        if (directory != '') and (directory != '/'):
            directory = remove_root(directory)
        directory = remove_trivial_magic(directory)
        directory = mk_aws_path(directory)

        if not os.path.exists(diskName):
            os.mkdir(diskName)

        files = self.glob(directory + '*')
        for f in files:
            if f[-1] == '/':
                continue
            subpath = re.sub(directory, '', f)
            path = os.path.join(diskName, subpath)
            subfolder = re.match('.*\/', path).group(0)
            if not os.path.exists(subfolder):
                os.makedirs(subfolder)
            self.download_to_file(f, path)

    @clean_object_name
    def search(self, pattern, **kwargs):
        """Print the objects matching the glob pattern

        See ``glob`` documentation for details
        """
        matches = self.glob(pattern, verbose = True, **kwargs)

    def get_browser(self):
        """Return an object which can be tab-completed
        to browse the contents of the bucket as if it were a file-system

        See documentation for ``cottoncandy.get_browser``
        """
        return cottoncandy.browser.S3Directory('', interface = self)

    def cp(self, source_name, dest_name,
           source_bucket = None, dest_bucket = None, overwrite = False):
        """Copy an object

        Parameters
        ----------
        source_name : str
            Name of object to be copied
        dest_name : str
            Copy name
        source_bucket : str
            If copying from a bucket different from the default.
            Defaults to ``self.bucket_name``
        dest_bucket : str
            If copying to a bucket different from the source bucket.
            Defaults to ``source_bucket``
        overwrite : bool (defaults to False)
            Whether to overwrite the `dest_name` object if it already exists
        """
        # TODO: support directories
        return self.interface.copy(source_name, dest_name, source_bucket, dest_bucket, overwrite)

    def mv(self, source_name, dest_name,
           source_bucket = None, dest_bucket = None, overwrite = False):
        """Move an object (make copy and delete old object)

        Parameters
        ----------
        source_name : str
            Name of object to be moved
        dest_name : str
            New object name
        source_bucket : str
            If moving object from a bucket different from the default.
            Defaults to ``self.bucket_name``
        dest_bucket : str (defaults to None)
            If moving to another bucket, provide the bucket name.
            Defaults to ``source_bucket``
        overwrite : bool (defaults to False)
            Whether to overwrite the `dest_name` object if it already exists.
        """
        # TODO: Support directories
        return self.interface.move(source_name, dest_name, source_bucket, dest_bucket, overwrite)

    def rm(self, object_name, recursive = False, delete = True):
        """Delete an object, or a subtree ('path/to/stuff').

        Parameters
        ----------
        object_name : str
            The name of the object to delete. It can also
            be a subtree
        recursive : bool
            When deleting a subtree, set ``recursive=True``. This is
            similar in behavior to 'rm -r /path/to/directory'.
        delete : bool
            When in google drive, actually delete the file or only trash it?

        Example
        -------
        >>> import cottoncandy as cc
        >>> cci = cc.get_interface('mybucket', verbose=False)
        >>> response = cci.rm('data/experiment/file01.txt')
        >>> cci.rm('data/experiment')
        cannot remove 'data/experiment': use `recursive` to remove branch
        >>> cci.rm('data/experiment', recursive=True)
        deleting 15 objects...
        """

        if isinstance(self.interface, GDriveClient):
            return self.interface.delete(object_name, recursive, delete)

        # not moving this to the basic S3Client because it depends on glob
        if self.exists_object(object_name):
            return self.get_object(object_name).delete()

        has_objects = len(self.ls(object_name)) > 0
        if has_objects:
            if recursive:
                all_objects = self.glob(object_name)
                print('deleting %i objects...' % len(all_objects))
                for obname in self.glob(object_name):
                    _ = self.get_object(obname).delete()
                return

        msg = "cannot remove '%s': use `recursive` to remove branch" \
            if has_objects else \
            "nothing found under '%s"
        print(msg % object_name)

    def get_object_owner(self, object_name):
        assert self.exists_object(object_name)
        ob = self.get_object(object_name)
        try:
            acl = ob.Acl()
            info = acl.owner
        except botocore.exceptions.ClientError as e:
            if 's3cmd-attrs' in ob.metadata:
                info = ob.metadata['s3cmd-attrs'].split('/')
                info = dict(map(lambda x: x.split(':'), info))
            else:
                raise e
        print(info)


class DefaultInterface(FileSystemInterface,
                       ArrayInterface,
                       BasicInterface):
    """Default cottoncandy interface to S3

    This includes numpy.array and file-system-like
    concepts for easy data I/O and bucket/object exploration.
    """

    def __init__(self, *args, **kwargs):
        """
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
        """
        super(DefaultInterface, self).__init__(*args, **kwargs)

class EncryptedInterface(DefaultInterface):
    """
    Interface that transparently encrypts everything uploaded to the cloud
    """
    def __init__(self, bucket, access, secret, url, encryption = 'AES', encryptionKey = None, *args, **kwargs):
        """

        Parameters
        ----------
        bucket
        access
        secret
        url
        encryption : 'AES' | 'RSA'
        encryptionKey : str
            if AES, key; if RSA, filename of .pem format key
        args
        kwargs
        """
        super(EncryptedInterface, self).__init__(bucket_name = bucket, ACCESS_KEY = access, SECRET_KEY = secret,
                                                 url = url, *args, **kwargs)

        if encryption not in ['AES', 'RSA']:
            raise ValueError('Encryption type {} not recognised. Currently AES and RSA are available'.format(encryption))
        self.encryption = encryption
        if encryption == 'AES':
            self.encryptor = AESEncryption(encryptionKey)
        else:
            self.encryptor = RSAAESEncryption(encryptionKey)

    # File I/O methods that encrypt object streams before passing them to the backend
    def upload_object(self, object_name, body, acl = DEFAULT_ACL, **metadata):

        if self.encryption == 'AES':
            encryptedStream = self.encryptor.encrypt_stream(body)
            return self.interface.upload_stream(encryptedStream, object_name, metadata, acl)
        else:
            encryptedStream, encryptedKey = self.encryptor.encrypt_stream(body)
            metadata['key'] = b64encode(encryptedKey)
            return self.interface.upload_stream(encryptedStream, object_name, metadata, acl)

    def download_stream(self, object_name):

        stream = self.interface.download_stream(object_name)
        if self.encryption == 'AES':
            stream.content = self.encryptor.decrypt_stream(stream.content)
        else:
            stream.content = self.encryptor.decrypt_stream(stream.content, b64decode(stream.metadata['key']))
        return stream

    def upload_from_file(self, local_file_name, object_name = None, ExtraArgs = dict(ACL = DEFAULT_ACL)):

        if not object_name:
            object_name = local_file_name
        with open(local_file_name) as f:
            return self.upload_object(object_name, f, ExtraArgs['ACL'])

    def download_to_file(self, object_name, fileName):

        with open(fileName, 'wb') as localFile:
            stream = self.download_stream(object_name)
            localFile.write(stream.content.read())