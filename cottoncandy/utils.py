'''Helper functions
'''
import os
import re
import six
import zlib
import string
import urllib
import itertools
from dateutil.tz import tzlocal
from functools import wraps


try:
    from urllib import unquote
except ImportError:
    from urllib.parse import unquote


import numpy as np

from cottoncandy import options

##############################
# Globals
##############################



# S3 AWS
#---------
MB = 2**20
MIN_MPU_SIZE = int(options.config.get('upload_settings', 'min_mpu_size'))*MB # 5MB
MAX_PUT_SIZE = int(options.config.get('upload_settings', 'max_put_size'))*MB # 5GB
MAX_MPU_SIZE = int(options.config.get('upload_settings', 'max_mpu_size_TB'))*MB*MB # 5TB
MAX_MPU_PARTS = int(options.config.get('upload_settings', 'max_mpu_parts')) # 10,000
MPU_THRESHOLD = int(options.config.get('upload_settings', 'mpu_use_threshold'))*MB
MPU_CHUNKSIZE = int(options.config.get('upload_settings', 'mpu_chunksize'))*MB
DASK_CHUNKSIZE = int(options.config.get('upload_settings', 'dask_chunksize'))*MB

SEPARATOR = options.config.get('basic', 'path_separator')

DEFAULT_ACL = options.config.get('basic', 'default_acl')
MANDATORY_BUCKET_PREFIX = options.config.get('basic', 'mandatory_bucket_prefix')

ISBOTO_VERBOSE = options.config.get('login', 'verbose_boto')

THREADS = int(options.config.get('basic', 'threads'))

##############################
# misc functions
##############################

def sanitize_metadata(metadict):
    outdict = {}
    for key,val in metadict.items():
        outdict[key.lower()] = val
    return outdict

def pathjoin(a, *p):
    """Join two or more pathname components, inserting SEPARATOR as needed.
    If any component is an absolute path, all previous path components
    will be discarded.  An empty last part will result in a path that
    ends with a separator."""
    path = a
    for b in p:
        if b.startswith(SEPARATOR):
            path = b
        elif path == '' or path.endswith(SEPARATOR):
            path += b
        else:
            path += SEPARATOR + b
    return path

def string2bool(mstring):
    '''
    '''
    truth_value = False
    if mstring in ['True','true', 'tru', 't',
                   'y','yes', '1']:
        truth_value = True
    elif mstring == 'None':
        truth_value = None
    return truth_value


def bytes2human(nbytes):
    '''Return string representation of bytes.

    Parameters
    ----------
    nbytes : int
        Number of bytes

    Returns
    -------
    human_bytes : str
        Human readable byte size (e.g. "10.00MB", "1.24GB", etc.).
    '''
    if nbytes == 0:
        return '0.00B'
    mapper = {0 : 'B',
              10 : 'KB',
              20 : 'MB',
              30 : 'GB',
              40 : 'TB',
              50 : 'PB',
              60 : 'EB',
              70 : 'ZB',
              }

    exps = sorted(mapper.keys())
    exp_coeff = np.log2(nbytes)
    exp_closest = int((exps[np.abs(exps - exp_coeff).argmin()]))
    if np.log10(nbytes/2.**exp_closest) < 0:
        exp_closest -= 10
    return '%0.02f%s'%(nbytes/2.**exp_closest, mapper[exp_closest])


def get_object_size(boto_s3_object):
    '''Return the size of the S3 object in MB

    Parameters
    ----------
    boto_s3_object : boto object

    Returns
    -------
    object_size : float (in MB)
    '''
    boto_s3_object.load()
    return boto_s3_object.meta.data['ContentLength']/2.**20


def get_fileobject_size(file_object):
    '''Return byte size of file-object

    Parameters
    ----------
    file_object : file object

    Returns
    -------
    nbytes : int
    '''
    file_object.seek(0,2)
    nbytes = file_object.tell()
    file_object.seek(0)
    return nbytes


def get_key_from_s3fs():
    '''Get AWS keys from default S3fs location if available.

    Returns
    -------
    ACCESS_KEY : str
    SECRET_KEY : str

    Notes
    -----
    Reads ~/.passwd-s3fs to get ACCESSKEY and SECRET KEY
    '''
    key_path = os.path.expanduser('~/.passwd-s3fs')
    if os.path.exists(key_path):
        with open(key_path, 'r') as kfl:
            content = kfl.readline()
            ACCESS_KEY, SECRET_KEY = content.strip().split(':')
            return ACCESS_KEY, SECRET_KEY


def get_key_from_environ():
    '''Get AWS keys from environmental variables if available

    Returns
    -------
    ACCESS_KEY : str
    SECRET_KEY : str

    Notes
    -----
    Reads AWS_ACCESS_KEY and AWS_SECRET_KEY
    '''
    try:
        return os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY']
    except:
        return


def get_keys():
    '''Read AWS keys from S3fs configuration or environmental variables.

    Returns
    -------
    ACCESS_KEY : str
    SECRET_KEY : str
    '''
    # try to outload keys
    resulta = get_key_from_s3fs()
    resultb = get_key_from_environ()
    result = resultb if (resulta is None) else resulta
    assert result is not None
    access_key, secret_key = result
    return access_key, secret_key


##############################
# object display
##############################

def objects2names(objects):
    '''Return the name of all objects in a list

    Parameters
    ----------
    objects : list (of boto3 objects)

    Returns
    -------
    object_names : list (of strings)
    '''
    return [unquote(t.key) for t in objects]


def unquote_names(object_names):
    '''Clean URL names from a list.

    Parameters
    ----------
    object_names : list (of strings)

    Returns
    -------
    clean_object_names : list (of strings)
    '''
    return [unquote(t) for t in object_names]


def print_objects(object_list):
    '''Print name, size, and creation date of objects in list.

    Parameters
    ----------
    object_list : list (of boto3 objects)
    '''
    object_names = objects2names(object_list)
    if len(object_names):
        maxlen = max(map(len, object_names))
        dates = [t.last_modified.astimezone(tzlocal()).strftime('%Y/%m/%d (%H:%M:%S)')\
                 for t in object_list]
        padding = '{0: <%i} {1} {2}M'%(min(maxlen+3, 70))
        sizes = [round(t.meta.data['Size']/2.**20,1) for t in object_list]
        info = [padding.format(name[-100:],date,size) for name,date,size in zip(object_names, dates, sizes)]
        print('\n'.join(info))


# object naming convention
##############################


def clean_object_name(input_function):
    '''Remove leading "/" from object_name

    This is important for compatibility with S3fs.
    S3fs does not list objects with a "/" prefix.
    '''
    @wraps(input_function)
    def iremove_root(self, object_name, *args, **kwargs):
        object_name = re.sub('//+', '/', object_name)

        if object_name == '':
            pass
        elif object_name[0] == SEPARATOR:
            object_name = object_name[1:]

        return input_function(self, object_name, *args, **kwargs)
    return iremove_root


def remove_root(string_):
    '''remove leading "/" from a string'''
    if string_[0] == SEPARATOR:
        string_ = string_[1:]
    return string_


##############################
# path handling
##############################

check_digits = re.compile('[0-9]')

def has_start_digit(s):
    return check_digits.match(s) is not None

MAGIC_CHECK = re.compile('[*?[]')

def has_magic(s):
    '''Check string to see if it has any glob magic
    '''
    return MAGIC_CHECK.search(s) is not None


def has_trivial_magic(s):
    '''Check string to see if it has trivial glob magic
    (e.g. "path/*")
    '''
    trivial_magic = re.compile('[^*]*/[\*]$') # "/*" at end only
    istrivial = trivial_magic.match(s) is not None
    magic_check = re.compile('[?[]')
    ismagic = magic_check.search(s) is not None
    if istrivial and (not ismagic):
        return True
    else:
        return False


def has_real_magic(s):
    '''Check if string has non-trivial glob pattern
    '''
    return has_magic(s) and (not has_trivial_magic(s))


def remove_trivial_magic(s):
    '''
    * xxx/*      -> xxx/
    * xxx/       -> xxx/
    * xxx/*/yyy/ -> xxx/*/yyy/
    '''
    if has_real_magic(s) or (not has_magic(s)):
        return s
    assert has_trivial_magic(s)
    return s[:-1] # remove '*' at end


def split_uri(uri, pattern='s3://', separator='/'):
    """Convert a URI to a bucket, object name tuple.

    's3://bucket/path/to/thing' -> ('bucket', 'path/to/thing')
    """
    assert pattern in uri
    parts = uri[len(pattern):].split(separator)
    bucket = parts[0]
    path = separator.join(parts[1:])
    return bucket, path


def mk_aws_path(path):
    """Make the `path` behave as expected when querying S3 with
    `list_objects`.

    * xxx/yyy -> xxx/yyy/
    * xxx/    -> xxx/
    * xxx     -> xxx/
    * /       -> ''
    * ''      -> ''
    """
    if (path == SEPARATOR) or (path == ''):
        return ''

    matcher = re.compile('/$') # at end only
    if matcher.search(path) is not None:
        return path
    else:
        return path + SEPARATOR


##############################
# low-level array handling
##############################


def generate_ndarray_chunks(arr, axis=None, buffersize=100*MB):
    '''A generator that splits an array into chunks of desired byte size

    Parameters
    ----------
    arr  : np.ndarray
    axis : int, None
        The axis along which to slice the array. If None is given,
        the array is chunked into ideal isotropic voxels.
    buffersize : scalar
        Byte size of the desired array chunks

    Returns
    -------
    iterator : generator object
        The object yields the tuple:
        (chunk_coordinates, chunk_data_slice)

        * chunk_coordinates:
          Indices of the current chunk along each dimension
        * chunk_data_slice:
          Data for this chunk
    Notes
    -----
    ``axis=None`` is WIP and only works well for near isotropic matrices.
    '''
    shape = arr.shape
    nbytes_total = arr.nbytes
    nparts = int(np.ceil(nbytes_total / float(buffersize)))

    # make sure we can upload
    assert nbytes_total < MAX_MPU_SIZE # 5TB
    assert buffersize >= MIN_MPU_SIZE  # 5MB
    assert nparts < MAX_MPU_PARTS      # 10,000

    if axis is None:
        # split array into ideal isotropic chunks
        logsum = 0
        factor = arr.ndim
    else:
        if axis == -1: axis = (arr.ndim - 1)
        dims = [arr.shape[t] for t in range(arr.ndim) if t != axis]
        logsum = np.sum([np.log(t) for t in  dims])
        factor = 1

    logii = ((np.log(buffersize) - np.log(arr.itemsize)) - logsum)/factor
    ii = int(np.ceil(np.exp(logii)))
    dim_nchunks = map(lambda x: int(np.ceil(x/ii)) + 1, shape)

    if axis is not None:
        # only slicing one dimension
        dim_nchunks = [(t if (i == axis) else 1) for i,t in enumerate(dim_nchunks)]

    chunk_shapes = [ii if (axis is None) else \
                    (arr.shape[dim] if (dim != axis) else ii) \
                    for dim in range(arr.ndim)]

    dim_ranges = map(lambda x: range(x), dim_nchunks)
    iterator = itertools.product(*dim_ranges)
    for chunk_idx, chunk_coords in enumerate(iterator):
        beg = [chunk_shapes[dim]*cc for dim, cc in enumerate(chunk_coords)]
        end = [min(chunk_shapes[dim]*(cc+1), shape[dim])for dim, cc in enumerate(chunk_coords)]
        slicers = tuple(map(lambda dim_lims: slice(dim_lims[0],dim_lims[1]), zip(beg,end)))
        yield chunk_coords, arr[slicers]


def read_buffered(frm, to, buffersize=64):
    '''Fill a numpy n-d array with file-like object contents

    Parameters
    ----------
    frm : buffer
        Object with a ``read`` method
    to : np.ndarray
        Array to which the contents will be put
    '''
    nbytes_total = to.size * to.dtype.itemsize
    if six.PY3:
        if to.flags['F_CONTIGUOUS']:
            vw = to.T.view()
        else:
            vw = to.view()
        vw.shape = (-1,)                # Must be a ravel-able object
        vw.dtype = np.dtype('uint8')    # 256 values in each byte

    for ci in range(int(np.ceil(nbytes_total / float(buffersize)))):
        start = ci * buffersize
        end = min(nbytes_total, (ci + 1) * buffersize)
        if six.PY2:
            to.data[start:end] = frm.read(end - start)
        elif six.PY3:
            vw.data[start:end] = frm.read(end - start)
        else:
            raise("Unknown python version") # not sure six will ever do anything here (6=2x3)

class GzipInputStream(object):
    """Simple class that allow streaming reads from GZip files
    (from https://gist.github.com/beaufour/4205533).

    Python 2.x gzip.GZipFile relies on .seek() and .tell(), so it
    doesn't support this (@see: http://bo4.me/YKWSsL).

    Adapted from: http://effbot.org/librarybook/zlib-example-4.py
    """

    def __init__(self, fileobj, block_size=16384):
        """
        Initialize with the given file-like object.

        @param fileobj: file-like object,
        """
        self.BLOCK_SIZE = block_size                       # Read block size
        # zlib window buffer size, set to gzip's format
        self.WINDOW_BUFFER_SIZE = 16 + zlib.MAX_WBITS

        self._file = fileobj
        self._zip = zlib.decompressobj(self.WINDOW_BUFFER_SIZE)
        self._offset = 0  # position in unzipped stream
        self._data = b''

    def __fill(self, num_bytes):
        """
        Fill the internal buffer with 'num_bytes' of data.

        @param num_bytes: int, number of bytes to read in (0 = everything)
        """

        if not self._zip:
            return

        while not num_bytes or len(self._data) < num_bytes:
            data = self._file.read(self.BLOCK_SIZE)
            if not data:
                self._data = self._data + self._zip.flush()
                self._zip = None  # no more data
                break

            self._data = self._data + self._zip.decompress(data)

    def __iter__(self):
        return self

    def seek(self, offset, whence=0):
        if whence == 0:
            position = offset
        elif whence == 1:
            position = self._offset + offset
        else:
            raise IOError("Illegal argument")
        if position < self._offset:
            raise IOError("Cannot seek backwards")

        # skip forward, in blocks
        while position > self._offset:
            if not self.read(min(position - self._offset, self.BLOCK_SIZE)):
                break

    def tell(self):
        return self._offset

    def read(self, size=0):
        self.__fill(size)
        if size:
            data = self._data[:size]
            self._data = self._data[size:]
        else:
            data = self._data
            self._data = b''
        self._offset = self._offset + len(data)
        return data

    def next(self):
        line = self.readline()
        if not line:
            raise StopIteration()
        return line

    def readline(self):
        # make sure we have an entire line
        while self._zip and "\n" not in self._data:
            self.__fill(len(self._data) + 512)

        pos = string.find(self._data, "\n") + 1
        if pos <= 0:
            return self.read()
        return self.read(pos)

    def readlines(self):
        lines = []
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
        return lines
