
class FileNotFoundError(RuntimeError):
	"""File not found error"""

class AbstractMethod(RuntimeError):
	"""
	This method is abstract
	"""

class CCBackEnd(object):
	"""
	Interface for cottoncandy backends
	"""
	def __init__(self):
		pass

	## Basic File IO

	def CheckFileExists(self, fileName, bucketName):
		"""
		Checks whether a file exists on the cloud

		Parameters
		----------
		fileName : str
			file on cloud
		bucketName : str
			(s3) bucket to check in

		Returns
		-------
		bool
		"""
		raise AbstractMethod

	def UploadStream(self, stream, cloudName, metadata, permissions):
		"""
		Uploads a stream object with a .read() function

		Parameters
		----------
		stream : stream
			streaming object
		cloudName : str
			name to use on cloud
		metadata : dict
			custom metadata for this file
		permissions : str?
			permissions for this file

		Returns
		-------
		bool, upload success
		"""
		raise AbstractMethod

	def UploadFile(self, fileName, cloudName, permissions):
		"""
		Uploads a file from disk

		Parameters
		----------
		fileName : str
			name of file to upload
		cloudName : str
			name to use on the cloud
		permissions : str?
			permissions for this file

		Returns
		-------
		bool, upload success
		"""
		raise AbstractMethod

	def UploadMultiPart(self, stream, cloudName, metadata, permissions):
		"""
		Multi-part upload for large stream objects

		Parameters
		----------
		stream : stream
			streaming object
		cloudName : str
			name to use on cloud
		metadata : dict
			custom metadata
		permissions : str?
			permissions for this file

		Returns
		-------
		bool, upload success
		"""
		raise AbstractMethod

	def DownloadStream(self, cloudName):
		"""
		Downloads a object to an in-memory stream

		Parameters
		----------
		cloudName : str
			name of object to download

		Returns
		-------
		CloudStream object
		"""
		raise AbstractMethod

	def DownloadFile(self, cloudName, fileName):
		"""
		Downloads an object directly to disk

		Parameters
		----------
		cloudName : str
			name of object to download
		fileName : str
			name on disk to use

		Returns
		-------
		bool, download success
		"""
		raise AbstractMethod

	## Basic File management

	def ListDirectory(self, path, limit):
		"""
		Lists the content of a directory

		Parameters
		----------
		path : str
			path on the cloud to get contents for

		Returns
		-------

		"""
		raise AbstractMethod

	def Copy(self, source, destination, sourceBucket, destinationBucket, overwrite):
		"""
		Copys an object

		Parameters
		----------
		source : str
			origin path
		destination : str
			destination path
		sourceBucket : str
			(s3) origin bucket
		destinationBucket : str
			(s3) destination bucket
		overwrite : bool
			overwrite if destination exists?

		Returns
		-------
		bool, copy success
		"""
		raise AbstractMethod

	def Move(self, source, destination, sourceBucket, destinationBucket, overwrite):
		"""
		Moves an object

		Parameters
		----------
		source
		destination
		sourceBucket
		destinationBucket
		overwrite

		Returns
		-------

		"""
		raise AbstractMethod

	def Delete(self, fileName, recursive = False, delete = False):
		"""
		Deletes an object

		Parameters
		----------
		fileName : str
			name of cloud object to delete
		recursive : bool
			recursively delete directory?
		delete : bool
			(gdrive) hard delete the file(s)?

		Returns
		-------

		"""
		raise AbstractMethod

	@property
	def size(self):
		"""
		Size of stored cloud items in bytes
		Returns
		-------
		int
		"""
		raise AbstractMethod

class CloudStream(object):
	"""
	A simple unified representation of an object downloaded from the cloud.
	 .content is a streaming object with a .read() function
	 .metadata is a dictionary of the custom metadata of this object

	TODO: unified metadata
	"""
	def __init__(self, stream, metadata):
		self.content = stream
		self.metadata = metadata