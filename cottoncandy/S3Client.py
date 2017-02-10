from __future__ import print_function
import boto3
import botocore
from io import BytesIO

from botocore.utils import fix_s3_host
from cottoncandy.utils import *
from CCBackEnd import *

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
	def connect(ACCESS_KEY, SECRET_KEY, url):
		"""
		Connect to S3 using boto

		Parameters
		----------
		ACCESS_KEY
		SECRET_KEY
		url

		Returns
		-------

		"""
		s3 = boto3.resource('s3',
							endpoint_url = url,
							aws_access_key_id = ACCESS_KEY,
							aws_secret_access_key = SECRET_KEY)
		s3.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
		return s3

	def __init__(self, bucket, accessKey, secretKey, s3url, forceBucketCreation = False):
		"""
		Constructor

		Parameters
		----------
		bucket
		accessKey
		secretKey
		s3url
		forceBucketCreation
		"""
		self.connection = S3Client.connect(accessKey, secretKey, s3url)
		self.url = s3url
		if self.CheckBucketExists(bucket):
			self.SetBucket(bucket)
		else:
			print('* Bucket "%s" does not exist' % bucket)
			if forceBucketCreation:
				print('* Creating "%s" bucket...\n' % bucket)
				self.CreateBucket(bucket)
			else:
				print('* cottoncandy instantiated without bucket...\n' \
					  '* Use with caution!\n' \
					  '* Many features will not work!!!\n')
				self.bucket = None

	def GetBucketName(self, bucket_name):
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
	def CheckFileExists(self, fileName, bucket_name = None):
		"""Check whether object exists in bucket

		Parameters
		----------
		fileName : str
			The object name
		bucket_name
		"""
		bucket_name = self.GetBucketName(bucket_name)
		ob = self.connection.Object(key = fileName, bucket_name = bucket_name)

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

	def CheckBucketExists(self, bucket_name):
		"""
		Check whether the bucket exists
		
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

	def CreateBucket(self, bucket_name, acl = DEFAULT_ACL):
		"""
		Create a new bucket

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

		self.connection.CreateBucket(Bucket = bucket_name, ACL = acl)
		self.SetBucket(bucket_name)

	def SetBucket(self, bucket_name):
		"""
		Sets which bucket to use

		Parameters
		----------
		bucket_name

		Returns
		-------

		"""
		if not self.CheckBucketExists(bucket_name):
			raise IOError('Bucket "%s" does not exist' % bucket_name)
		self.bucket_name = bucket_name

	def GetBucket(self):
		"""
		Get bucket boto3 object

		Returns
		-------

		"""
		s3_bucket = self.connection.Bucket(self.bucket_name)
		return s3_bucket

	def ListObjects(self, **kwargs):
		"""
		Get list of objects from the bucket
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
		bucket = self.GetBucket()
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

	@property
	def size(self):
		return self.GetBucketSize()

	def GetBucketSize(self, limit = 10 ** 6, page_size = 10 ** 6):
		"""
		Counts the size of all objects in the current bucket.

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
		assert self.CheckBucketExists(self.bucket_name)
		obs = self.ListObjects(limit = limit, page_size = page_size)
		object_sizes = [t.size for t in obs]
		total_bytes = sum(object_sizes)
		num_objects = len(object_sizes)
		del object_sizes

		txt = "%i bytes (%s) over %i objects"
		print(txt % (total_bytes, bytes2human(total_bytes), num_objects))
		return total_bytes

	def ShowBuckets(self):
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
	def GetS3Object(self, fileName, bucket_name = None):
		"""
		Get a boto3 object. Create it if it doesn't exist

		Parameters
		----------
		fileName
		bucket_name

		Returns
		-------

		"""
		bucket_name = self.GetBucketName(bucket_name)
		return self.connection.Object(bucket_name = bucket_name, key = fileName)

	def UploadStream(self, body, fileName, metadata, acl = DEFAULT_ACL):
		"""
		Uploads a stream

		Parameters
		----------
		fileName
		body : stream
		acl
		metadata

		Returns
		-------

		"""
		obj = self.GetS3Object(fileName)
		return obj.put(Body = body, ACL = acl, Metadata = metadata)

	def DownloadStream(self, fileName):
		"""
		Download object raw data.
		This simply calls the object body ``read()`` method.

		Parameters
		---------
		fileName : str

		Returns
		-------
		stream
			file-like stream of object data
		"""
		if not self.CheckFileExists(fileName):
			raise IOError('Object "%s" does not exist' % fileName)
		s3_object = self.GetS3Object(fileName)
		return CloudStream(BytesIO(s3_object.get()['Body'].read()), s3_object.metadata)

	def UploadFile(self, fileName, cloudFileName = None, acl = DEFAULT_ACL):
		"""Upload a file to S3.

		Parameters
		----------
		fileName : str
			Absolute path of file to upload
		cloudFileName : str, None
			Name of uploaded object. If None, use
			the full file name as the object name.
		acl: ???
			S3 permissions

		Returns
		-------
		response : boto3 response
		"""
		assert os.path.exists(fileName)
		if fileName is None:
			fileName = os.path.abspath(fileName)
		fileName = remove_root(fileName)
		s3_object = self.GetS3Object(cloudFileName)
		return s3_object.upload_file(fileName, ExtraArgs = dict(ACL = acl))

	def DownloadFile(self, fileName, localFileName):
		"""Download S3 object to a file

		Parameters
		----------
		fileName : str
		localFileName : str
			Absolute path where the data will be downloaded on disk
		"""
		assert self.CheckFileExists(fileName)  # make sure object exists
		s3_object = self.GetS3Object(fileName)
		return s3_object.download_file(localFileName)

	def UploadMultiPart(self, fileName, file_object,
					   buffersize = MPU_CHUNKSIZE, verbose = True, **metadata):
		"""Multi-part upload for a python file-object.

		This automatically creates a multipart upload of an object.
		Useful for large objects that are loaded in memory. This avoids
		having to write the file to disk and then using ``upload_from_file``.

		Parameters
		----------
		fileName : str
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
		client = self.connection.meta.client
		mpu = client.create_multipart_upload(Bucket = self.bucket_name,
											 Key = fileName,
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
										  Key = fileName,
										  UploadId = mpu['UploadId'],
										  PartNumber = part_number,
										  Body = data_chunk)

			# store the part info
			part_info = dict(PartNumber = part_number,
							 ETag = response['ETag'])
			mpu_info['Parts'].append(part_info)

		# finalize
		mpu_response = client.complete_multipart_upload(Bucket = self.bucket_name,
														Key = fileName,
														UploadId = mpu['UploadId'],
														MultipartUpload = mpu_info)
		return mpu_response

	def Copy(self, source, destination, sourceBucket, destinationBucket, overwrite):
		source_bucket = self.GetBucketName(sourceBucket)
		dest_bucket = source_bucket if (destinationBucket is None) else destinationBucket
		dest_bucket = self.GetBucketName(dest_bucket)

		assert self.CheckFileExists(source, bucket_name = source_bucket)
		ob_new = self.GetS3Object(destination, bucket_name = dest_bucket)

		if self.CheckFileExists(ob_new.key, bucket_name = dest_bucket):
			assert overwrite is True

		fpath = PathJoin(source_bucket, source)
		ob_new.copy_from(CopySource = fpath)
		return ob_new

	def Move(self, source, destination, sourceBucket, destinationBucket, overwrite):
		new_ob = self.Copy(source, destination, sourceBucket, destinationBucket, overwrite)
		old_ob = self.GetS3Object(source, bucket_name = sourceBucket)
		old_ob.delete()
		return new_ob

	def ListDirectory(self, path, limit):
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

		response = self.GetBucket().meta.client.list_objects(Bucket = self.bucket_name,
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