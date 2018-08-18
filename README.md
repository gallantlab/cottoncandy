<p align="center">
<img src="https://user-images.githubusercontent.com/17423004/43179059-9f1ca3b0-8f85-11e8-8f75-587062e75339.png"
     alt="cottoncandy logo"
     width=50%
/>
</p>  

# Welcome to cottoncandy!
[![Build Status](https://travis-ci.org/gallantlab/cottoncandy.svg?branch=master)](https://travis-ci.org/gallantlab/cottoncandy)
[![DOI](https://zenodo.org/badge/58677370.svg)](https://zenodo.org/badge/latestdoi/58677370)

*sugar for s3*

https://gallantlab.github.io/cottoncandy


## What is cottoncandy?
A python scientific library for storing and accessing numpy array data on S3. This is achieved by reading arrays from memory and downloading arrays directly into memory. This means that you don't have to download your array to disk, and then load it from disk into your python session.

This library relies heavily on [boto3](https://github.com/boto/boto3)

## Installation

### via pip

```
$ pip install cottcandy
```

### From the repo
Clone the repo from GitHub and do the usual python install from the command line

```
$ git clone https://github.com/gallantlab/cottoncandy.git
$ cd cottoncandy
$ sudo python setup.py install
```

### Configuration file

After installation, the cottoncandy configuration file will be saved under:
* Linux: `~/.config/cottoncandy/options.cfg` 
* MAC OS: `~/Library/Application Support/cottoncandy/options.cfg`
* Windows (sorry not tested, nor supported): `C:\Users\<username>\AppData\Local\<AppAuthor>\cottoncandy\options.cfg`

Upon installation cottoncandy will try to find your AWS keys and store them in this file. See the [default file](https://github.com/gallantlab/cottoncandy/blob/master/cottoncandy/defaults.cfg) for more configuration options.

Object and bucket permissions are set to ``authenticated-read`` by default. If you wish to keep all your objects private, modify the configuration file and set ``default_acl = private``. See [AWS ACL overview](http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html) for more information on S3 permissions.


## Getting started
Setup the connection (endpoint, access and secret keys can be specified in the configuration file instead)::

```python
>>> import cottoncandy as cc
>>> cci = cc.get_interface('my_bucket',
                           ACCESS_KEY='FAKEACCESSKEYTEXT',
                           SECRET_KEY='FAKESECRETKEYTEXT',
                           endpoint_url='https://s3.amazonaws.com')
```

### Storing numpy arrays

```python
>>> import numpy as np
>>> arr = np.random.randn(100)
>>> s3_response = cci.upload_raw_array('myarray', arr)
>>> arr_down = cci.download_raw_array('myarray')
>>> assert np.allclose(arr, arr_down)
```

### Storing dask arrays

```python
>>> arr = np.random.randn(100,600,1000)
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
```


### Command-line search

```python
>>> cci.glob('/path/to/*/file01*.grp/image_data')
['/path/to/my/file01a.grp/image_data',
 '/path/to/my/file01b.grp/image_data',
 '/path/to/your/file01a.grp/image_data',
 '/path/to/your/file01b.grp/image_data']
>>> cci.glob('/path/to/my/file02*.grp/*')
['/path/to/my/file02a.grp/image_data',
 '/path/to/my/file02a.grp/text_data',
 '/path/to/my/file02b.grp/image_data',
 '/path/to/my/file02b.grp/text_data']
```

### File system-like object browsing


```python
>>> import cottoncandy as cc
>>> browser = cc.get_browser('my_bucket_name',
                             ACCESS_KEY='FAKEACCESSKEYTEXT',
                             SECRET_KEY='FAKESECRETKEYTEXT',
                             endpoint_url='https://s3.amazonaws.com')
>>> browser.sweet_project.sub<TAB>
browser.sweet_project.sub01_awesome_analysis_DOT_grp
browser.sweet_project.sub02_awesome_analysis_DOT_grp
>>> browser.sweet_project.sub01_awesome_analysis_DOT_grp
<cottoncandy-group <bucket:my_bucket_name> (sub01_awesome_analysis.grp: 3 keys)>
>>> browser.sweet_project.sub01_awesome_analysis_DOT_grp.result_model01
<cottoncandy-dataset <bucket:my_bucket_name [1.00MB:shape=(10000)]>
```

### Google Drive backend

`cottoncandy` can also use Google Drive as a back-end. This equires a `client_secrets.json` file in your `~/.config/cottoncandy` folder and the [pydrive](https://github.com/googledrive/PyDrive) package. 

See the [Google Drive setup instructions](https://github.com/gallantlab/cottoncandy/blob/master/google_drive_setup_instructions.md) for more details.

```python
>>> import cottoncandy as cc
>>> cci = cc.get_interface(backend='gdrive')
```

### Encryption

`cottoncandy`provides a transparent encryption interface for AWS S3 and Google Drive. This requires the `pycrypto` package. 

**WARNING**: Encryption is an advance feature. Make sure to create a backup of the encryption keys  (stored in `~/.config/cottoncandy/options.cfg`). If you lose your encryption keys you will not be able to recover your data!


```python
>>> import cottoncandy as cc
>>> cci = cc.get_encrypted_interface('my_bucket_name',
                                      ACCESS_KEY='FAKEACCESSKEYTEXT',
                                      SECRET_KEY='FAKESECRETKEYTEXT',
                                      endpoint_url='https://s3.amazonaws.com')                               
```


### Contributing
* If you find any issues with `cottoncandy`, please report it by submitting an issue on GitHub.
* If you wish to contribute, please submit a pull request. Include information as to how you ran the tests and the full output log if possible. Running tests on AWS can incur costs. 

## Cite as

Nunez-Elizalde AO, Zhang T, Huth AG, Gao JS, Slivkoff, Lescroart MD, Deniz F, McNeil C, Gibboni R, Popham SF, Rokem A, Oliver MD and Gallant JL. cottoncandy: scientific python package for easy cloud storage. Zenodo. 2017. http://doi.org/10.5281/zenodo.1034342



