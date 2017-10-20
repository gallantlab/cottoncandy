# cottoncandy
[![Build Status](https://travis-ci.org/gallantlab/cottoncandy.svg?branch=master)](https://travis-ci.org/gallantlab/cottoncandy)
[![DOI](https://zenodo.org/badge/58677370.svg)](https://zenodo.org/badge/latestdoi/58677370)

sugar for s3
http://gallantlab.github.io/cottoncandy/


This is a python scientific library for storing and accessing numpy array data on S3. This is achieved by reading arrays from memory and downloading arrays directly into memory. This means that you don't have to download your array directly to disk, and then load it from disk into your python session.

This library relies heavily on boto3 (https://github.com/boto/boto3) 

```
import numpy as np
import cottoncandy as cc
cci = cc.get_interface('my_bucket', ACCESS_KEY='FAKEACCESSKEYTEXT', SECRET_KEY='FAKESECRETKEYTEXT', endpoint_url='https://s3.amazonaws.com')

arr = np.random.randn(100)
cci.upload_raw_array('myarray', arr)
arr_down = cci.download_raw_array('myarray')
assert np.allclose(arr, arr_down)
```

now with (limited) google drive support. requires a `client_secrets.json` file in your cottoncandy config folder and the pydrive package.
```
cci = cc.get_interface(backend = 'gdrive')
```

and also transparent encryption of cloud files. requires the pycrypto package. HIGHLY EXPERIMENTAL
```
cci = cc.get_encrypted_interface()
```

## cite as
Anwar O Nunez-Elizalde, Tianjiao Zhang, Alexander G Huth, James Gao, ..., Jack L Gallant. (2017, October 20). cottoncandy: scientific python package for easy cloud storage. Zenodo. http://doi.org/10.5281/zenodo.1034342

