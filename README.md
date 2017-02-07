# cottoncandy
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

now with (limited) google drive support. requires a `client_secrets.json` file in your cottoncandy config folder.
```
cci = cc.get_interface(backend = 'gdrive')
```