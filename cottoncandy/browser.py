'''
'''
import os

from utils import (clean_object_name,
                   has_start_digit,
                   has_magic,
                   has_real_magic,
                   has_trivial_magic,
                   remove_trivial_magic,
                   remove_root,
                   mk_aws_path,
                   objects2names,
                   unquote_names,
                   print_objects,
                   get_fileobject_size,
                   get_object_size,
                   read_buffered,
                   GzipInputStream,
                   generate_ndarray_chunks,
                   )




# globals
##############################

HDFEXT = ['.hdf', '.h5', '.hf5', '.hdf5', '.h5py', '.grp', '.arr', '.dar']
NPYEXT = ['.npy']
JSONEXT = ['.json']


#------------------
# Browsing objects
#------------------

class BrowserObject(object):
    pass

class S3FSLike(BrowserObject):
    '''Base class for file-system-like interface to S3
    '''
    @clean_object_name
    def __init__(self, path, interface):
        '''
        path: str
            The path to start naviation from
        interface:
            A `cottoncandy.InterfaceObject` instance
        '''

        self.interface = interface
        parent, curdir = os.path.split(path)
        self._curdir = curdir
        self._parent = parent

    @property
    def _fullpath(self):
        return os.path.join(self._parent, self._curdir)

    def __call__(self):
        assert self.interface.exists_object(self._fullpath)
        return self.interface.get_object(self._fullpath)


class S3Directory(S3FSLike):
    """Get an object that allows you to tab-complete your
    way through your objects.
    One also has access to ``_ls()`` and ``_glob()`` methods.
    """
    @clean_object_name
    def __init__(self, path, interface):
        """
        Parameters
        ----------
        path: str
            The path to start naviation from
        interface:
            A `cottoncandy.InterfaceObject` instance

        Returns
        -------
        ccbrowser : cottoncandy.BrowserObject
            A cottoncandy browser object

        Examples
        --------
        >>> browser = cloud.get_browser('anunez_raid')
        >>> browser.auto.k<TAB-COMPLETION>
        browser.auto.k1
        browser.auto.k8

        >>> browser.auto.k8
        <directory-like-object @anunez_raid bucket: auto/k8>

        >>> browser.auto.k8.anunez.proj.deepnet.caffenet_<TAB-COMPLETION>
        browser.auto.k8.anunez.proj.deepnet.caffenet_ANfs_performance_HDF
        browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF

        >>> browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF
        <h5py-like-group @anunez_raid bucket: caffenet_BGfs_performance.hdf (6 keys)>

        >>> # We can also explore the contents of the 'HDF' file
        >>> browser.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.<TAB-COMPLETION>
        browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc6
        browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc7
        browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc8

        >>> # let's look at one
        >>> browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc8
        <h5py-like-dataset @anunez_raid bucket: caffenet_conv3 [shape=(73221)]>

        >>> # we can download the data as an array
        >>> arr = browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc8.load()
        >>> arr.shape
        (73221,)

        >>> # we can also download the object. useful when no class for object exists
        >>> browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc8()
        s3.Object(bucket_name='anunez_raid', key='auto/k8/anunez/proj/deepnet/caffenet_BGfs_performance.hdf/caffenet_fc8')
        """
        super(S3Directory, self).__init__(path, interface=interface)

        subdirs = self.interface.lsdir(self._fullpath)
        subdirs = [os.path.split(t)[-1] for t in subdirs]
        self._subdirs = {}

        for sdir in subdirs:
            if sdir:
                # modify name for tab-completion purposes
                sdir_copy = sdir[:]
                # clean numbers
                sdir_copy = 'NUM_%s'%sdir_copy if has_start_digit(sdir) else sdir_copy
                # clean extension
                fl, ext = os.path.splitext(sdir_copy)
                kk = fl+'_DOT_'+ext[1:] if ext else sdir_copy
                self._subdirs[kk] = sdir

    def _ls(self):
        '''Show the directories and files contained at this level
        '''
        return sorted(self._subdirs.keys())

    def _glob(self):
        '''Show all the objects contained under this branch
        '''
        return sorted(self.interface.glob(self._fullpath))

    def __dir__(self):
        return []

    def __repr__(self):
        if len(self._subdirs):
            details = (__package__, self.interface.bucket_name, self._curdir)
            return "%s-path <bucket:%s> %s"%details
        else:
            # no children, it's gotta be an object b/c we're in S3
            obj = self.interface.get_object(self._fullpath)
            size = get_object_size(obj)
            details = (__package__, self.interface.bucket_name, size)
            return "%s-file <bucket:%s> [%0.01fMB]"%details

    def __len__(self):
        return len(self._subdirs)

    def __dir__(self):
        return list(self._subdirs.keys())

    def __getattr__(self, attr):
        if attr in self._subdirs:
            child_path = os.path.join(self._fullpath, self._subdirs[attr])
        else:
            # allow user-provided paths. useful when >1000 objects
            child_path = os.path.join(self._fullpath, attr)
            assert (self.interface.exists_object(child_path) or \
                    self.interface.lsdir(child_path))

        try:
            has_ext = '.' in child_path # shitty check...
            ishdf = any([t in child_path for t in HDFEXT])

            if ishdf:
                return S3HDF5(child_path, interface=self.interface)
            else:
                return S3Directory(child_path, interface=self.interface)

        except:
            raise AttributeError


class S3HDF5(S3Directory):
    '''
    '''
    @clean_object_name
    def __init__(self, path, interface):
        '''
        '''
        super(S3HDF5, self).__init__(path, interface=interface)

    def __repr__(self):
        if len(self._subdirs) == 0:
            # bottom object, probably array
            obj = self.interface.get_object(self._fullpath)
            if 'shape' in obj.metadata:
                shape =  obj.metadata['shape']
                size = get_object_size(obj)
                details = (__package__, self.interface.bucket_name, size, shape)
                return "%s-dataset <bucket:%s [%0.01fMB:shape=(%s)]>"%details
        # otherwise it's the file itself
        details = (__package__, self.interface.bucket_name,self._curdir, len(self._subdirs))
        return "<%s-group <bucket:%s> (%s: %i keys)>"%details

    def __dir__(self):
        return ['load', 'keys'] + list(self._subdirs.keys())

    def keys(self):
        '''Show the contents of
        '''
        return sorted(self._ls())

    def load(self, key=None):
        '''
        '''
        dataset_path = self._fullpath
        if key is not None:
            if key in self._subdirs:
                dataset_path = os.path.join(self._fullpath, self._subdirs[key])
            else:
                raise ValueError('"%s" not in %s'%(key, dataset_path))

        if self.interface.exists_object(dataset_path):
            return self.interface.download_raw_array(dataset_path)
        print('Specify key to download:\n%s'%','.join(sorted(self._subdirs.keys())))
