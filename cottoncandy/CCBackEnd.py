
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

	def CheckFileExists(self, fileName, bucketName):
		"""

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
		bool
		"""
		raise AbstractMethod

	def UploadFile(self, fileName, cloudName, permissions):
		"""

		Parameters
		----------
		fileName
		cloudName
		permissions

		Returns
		-------

		"""
		raise AbstractMethod

	def UploadMultiPart(self, stream, cloudName, metadata, permissions):
		"""

		Parameters
		----------
		stream
		cloudName
		metadata
		permissions

		Returns
		-------

		"""
		raise AbstractMethod

	def DownloadStream(self, cloudName):
		"""

		Parameters
		----------
		cloudName

		Returns
		-------
		CloudStream object
		"""
		raise AbstractMethod

	def DownloadFile(self, cloudName, fileName):
		"""

		Parameters
		----------
		cloudName
		fileName

		Returns
		-------

		"""
		raise AbstractMethod

	def Copy(self, source, destination, sourceBucket, destinationBucket, overwrite):
		"""

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

	def Move(self, source, destination, sourceBucket, destinationBucket, overwrite):
		"""

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

	def Delete(self, fileName, recursive = False, trash = False):
		"""

		Parameters
		----------
		fileName
		recursive
		trash

		Returns
		-------

		"""
		raise AbstractMethod

	@property
	def size(self):
		raise AbstractMethod

class CloudStream(object):
	"""
	Stream + metadata
	"""
	def __init__(self, stream, metadata):
		self.content = stream
		self.metadata = metadata