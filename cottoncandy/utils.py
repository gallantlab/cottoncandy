'''Helper functions
'''
import os
import re
import zlib
import string
import urllib
from dateutil.tz import tzlocal
from functools import wraps

import numpy as np

##############################
# Globals
##############################



# S3 AWS
#---------
MB = 2**20
MIN_MPU_SIZE = 5*MB                # 5MB
MAX_PUT_SIZE = 5000*MB             # 5GB
MAX_MPU_SIZE = 5*MB*MB             # 5TB
MAX_MPU_PARTS = 10000              # 10,000


SEPARATOR = '/'


##############################
# misc functions
##############################

def get_object_size(boto_s3_object):
    '''Return the size of the S3 object in MB
    '''
    boto_s3_object.load()
    return boto_s3_object.meta.data['ContentLength']/2.**20


def get_fileobject_size(file_object):
    '''Return byte size of file-object
    '''
    file_object.seek(0,2)
    nbytes = file_object.tell()
    file_object.reset()
    return nbytes


def get_key_from_s3fs():
    '''If user has s3fs-fuse keys,return them
    '''
    key_path = os.path.expanduser('~/.passwd-s3fs')
    if os.path.exists(key_path):
        with open(key_path, 'r') as kfl:
            content = kfl.readline()
            ACCESS_KEY, SECRET_KEY = content.strip().split(':')
            return ACCESS_KEY, SECRET_KEY


def get_key_from_environ():
    try:
        return os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY']
    except:
        return


def get_keys():
    '''try to find the user keys in the machine
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
    '''
    return [urllib.unquote(t.key) for t in objects]


def unquote_names(object_names):
    '''Clean the URL object name
    '''
    return [urllib.unquote(t) for t in object_names]


def print_objects(object_list):
    '''Print the name and creation date of a list of objects.
    '''
    object_names = objects2names(object_list)
    if len(object_names):
        maxlen = max(map(len, object_names))
        dates = [t.last_modified.astimezone(tzlocal()).strftime('%Y/%m/%d (%H:%M:%S)')\
                 for t in object_list]
        padding = '{0: <%i} {1} {2}M'%(min(maxlen+3, 70))
        sizes = [round(t.meta.data['Size']/2.**20,1) for t in object_list]
        info = [padding.format(name,date,size) for name,date,size in zip(object_names, dates, sizes)]
        print '\n'.join(info)


# object naming convention
##############################


def clean_object_name(input_function):
    '''remove leading "/" from object_name

    This is important for compatibility with s3fs.
    s3fs does not list objects with a "/" prefix.
    '''
    @wraps(input_function)
    def iremove_root(self, object_name, *args, **kwargs):
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


def has_magic(s):
    '''Check string to see if it has any glob magic
    '''
    magic_check = re.compile('[*?[]')
    return magic_check.search(s) is not None


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
        `axis=None` is WIP and atm works fine for near isotropic matrices
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
        dims = [arr.shape[t] for t in xrange(arr.ndim) if t != axis]
        logsum = np.sum(map(np.log, dims))
        factor = 1

    logii = ((np.log(buffersize) - np.log(arr.itemsize)) - logsum)/factor
    ii = int(np.ceil(np.exp(logii)))
    dim_nchunks = map(lambda x: int(np.ceil(x/ii)) + 1, shape)

    if axis is not None:
        # only slicing one dimension
        dim_nchunks = [(t if (i == axis) else 1) for i,t in enumerate(dim_nchunks)]

    chunk_shapes = [ii if (axis is None) else \
                    (arr.shape[dim] if (dim != axis) else ii) \
                    for dim in xrange(arr.ndim)]

    dim_ranges = map(lambda x: range(x), dim_nchunks)
    iterator = itertools.product(*dim_ranges)
    for chunk_idx, chunk_coords in enumerate(iterator):
        beg = [chunk_shapes[dim]*cc for dim, cc in enumerate(chunk_coords)]
        end = [min(chunk_shapes[dim]*(cc+1), shape[dim])for dim, cc in enumerate(chunk_coords)]
        slicers = map(lambda dim_lims: slice(dim_lims[0],dim_lims[1]), zip(beg,end))
        yield chunk_coords, arr[slicers]


def read_buffered(frm, to, buffersize=64):
    '''Fill an array with file-like object contents
    '''
    nbytes_total = to.size * to.dtype.itemsize
    for ci in range(int(np.ceil(nbytes_total / float(buffersize)))):
        start = ci * buffersize
        end = min(nbytes_total, (ci + 1) * buffersize)
        to.data[start:end] = frm.read(buffersize)


class GzipInputStream(object):
    """Simple class that allow streaming reads from GZip files
    (from https://gist.github.com/beaufour/4205533).

    Python 2.x gzip.GZipFile relies on .seek() and .tell(), so it
    doesn't support this (@see: http://bo4.me/YKWSsL).

    Adapted from: http://effbot.org/librarybook/zlib-example-4.py
    """

    def __init__(self, fileobj):
        """
        Initialize with the given file-like object.

        @param fileobj: file-like object,
        """
        self.BLOCK_SIZE = 16384                       # Read block size
        # zlib window buffer size, set to gzip's format
        self.WINDOW_BUFFER_SIZE = 16 + zlib.MAX_WBITS

        self._file = fileobj
        self._zip = zlib.decompressobj(self.WINDOW_BUFFER_SIZE)
        self._offset = 0  # position in unzipped stream
        self._data = ""

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
            self._data = ""
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
