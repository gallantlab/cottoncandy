from __future__ import print_function
import os
import struct

from Crypto import Random
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.PublicKey import RSA
from io import BytesIO

CIPHER_BLOCK_CHAIN = AES.MODE_CBC
INIT_VECT_SIZE = 16
DEFAULT_CHUNKSIZE = 64 * 1024
FILE_LENGTH_FIELD_SIZE = struct.calcsize('Q')
WHENCE_EOF = 2


class AbstractMethod(NotImplementedError):
	"""
	A more meaningful error name.
	"""


class Encryption(object):
	"""
	Abstract base class for a file encrypt/decrypt object
	"""

	def __init__(self, key = None):
		"""
		Interface constructor
		@param key:		stored encryption/decryption key, if None, generates a new key
		"""

		if key is not None:
			self.ReadKey(key)
		else:
			self.GenerateKey()

	def ReadKey(self, fileName):
		"""
		Reads a stored key
		@param fileName:
		@return:
		"""
		with open(fileName) as file:
			self.key = file.read()

	def StoreKey(self, fileName = 'key.key'):
		"""
		Saves the encryption key to a file
		@param fileName:
		@return:
		"""
		with open(fileName) as keyFile:
			keyFile.write(self.key)

	def GenerateKey(self):
		"""
		Generates a key for encrypting/decrypting files for this object
		@return:
		"""
		raise AbstractMethod

	def EncryptFile(self, fileName, encryptedFileName = None):
		"""
		Encrypts a file on the disk to disk
		@param fileName: 			str, name of file to encrypt
		@param encryptedFileName: 	str, name of output, if None, is fileName + 'enc'
		@return:
		"""
		raise AbstractMethod

	def EncryptStream(self, inputStream):
		"""
		Encrypts a stream object to a stream object
		@param inputStream: 	stream, object to be encrypted
		@return: stream, encrypted stream
		"""
		raise AbstractMethod

	def DecryptFile(self, fileName, key = None, decryptedFileName = None):
		"""
		Decrypts an encrypted file on disk to disk
		@param fileName: 			str, name of file to decrypt
		@param key:					str, key for decryption
		@param decryptedFileName: 	str, name of output, if none, is fileName - 'enc'
		@return:
		"""
		raise AbstractMethod

	def DecryptStream(self, inputStream, key = None):
		"""
		Decrypts a stream object to stream
		@param inputStream:	stream, object to be decrypted
		@param key: 		str, key for decyption
		@return: stream, decrypted stream
		"""
		raise AbstractMethod


class AESEncryption(Encryption):
	"""
	Encrypts files using an AES cipher. All files passing through the same object will be encrypted with the same key
	Encrypted files have the following structure:

	[  16 bytes default   ][		file size 		     || file size % 16 - 8 bytes ][	 	8 bytes		 ]
	[initialization vector][ binary ciphertext for file  ||   padding with spaces	 ][file size in bytes]
	"""

	def __init__(self, key = None, mode = CIPHER_BLOCK_CHAIN, chunkSize = DEFAULT_CHUNKSIZE, initVectSize = INIT_VECT_SIZE):
		"""

		@param key:				str, file for stored key
		@param mode: 			AES mode enum
		@param chunkSize:		int, size of each chunk to encrypt
		@param initVectSize:	int, size of initialization vector
		"""
		super(AESEncryption, self).__init__(key)
		self.mode = mode
		self.chunkSize = chunkSize
		self.initVectSize = initVectSize

	def GenerateKey(self, keySize = 32):
		"""
		Generates AES key used by this object
		@param keySize: int, AES key size in bytes
		@return:
		"""
		if keySize not in [16, 24, 32]:
			raise RuntimeError('Bad key length')
		self.key = Random.get_random_bytes(keySize)

	def EncryptFile(self, fileName, encryptedFileName = None, key = None, chunksize = None, initVectSize = None):
		"""
		Overridden
		@param key:				str, key used for encryption
		@param chunksize: 		int, AES encrypt chunk size
		@param initVectSize:	int, initialization vector size
		@return:
		"""
		if chunksize is not None and chunksize % 16 != 0:
			raise RuntimeError('Bad chunksize')
		if encryptedFileName is None:
			encryptedFileName = fileName + 'enc'
		if chunksize is None:
			chunksize = self.chunkSize
		if initVectSize is None:
			initVectSize = self.initVectSize

		# Initialize AES encryption object
		initVect = Random.new().read(initVectSize)
		encryptor = AES.new(self.key if key is None else key, self.mode, initVect)

		# record file size since the encrypted file might be padded
		inputSize = os.path.getsize(fileName)
		fileLengthField = struct.pack('<Q', inputSize)

		with open(fileName, 'rb') as inputFile:
			with open(encryptedFileName, 'wb') as outputFile:
				assert len(initVect) == initVectSize
				outputFile.write(initVect)  # record the initialization vector

				# encrypt the file in chunks
				thisChunk = None
				isFinalChunk = False

				while not isFinalChunk:
					thisChunk = inputFile.read(chunksize)

					if len(thisChunk) == 0 or len(thisChunk) % 16 != 0:  # end of file
						# pad the end chunk if necessary - AES needs things to be in 16-byte blocks
						padSize = 16 - (len(thisChunk) + FILE_LENGTH_FIELD_SIZE) % 16
						padding = ' ' * padSize
						thisChunk += padding
						thisChunk += fileLengthField  # record the actual file length so we can get ride of the padding on decryption
						isFinalChunk = True

					outputFile.write(encryptor.encrypt(thisChunk))
		del encryptor

	def EncryptStream(self, inputStream, key = None, chunkSize = None, initVectSize = None):
		"""
		Overridden
		@param key:				str, key used for encryption
		@param chunkSize: 		int, AES encrypt chunk size
		@param initVectSize: 	int, initialization vector size
		@return: 	encrypted stream
		"""
		if chunkSize is None:
			chunkSize = self.chunkSize
		if chunkSize is not None and chunkSize % 16 != 0:
			raise RuntimeError('Bad chunksize')
		if initVectSize is None:
			initVectSize = self.initVectSize

		initVect = Random.new().read(initVectSize)
		encryptor = AES.new(self.key if key is None else key, self.mode, initVect)

		inputStream.seek(0, os.SEEK_END)
		inputSize = inputStream.tell()
		inputStream.seek(0)
		fileLengthField = struct.pack('<Q', inputSize)

		outputStream = BytesIO()
		outputStream.write(initVect)

		thisChunk = None
		isFinalChunk = False

		while not isFinalChunk:
			thisChunk = inputStream.read(chunkSize)

			if len(thisChunk) == 0 or len(thisChunk) % 16 != 0:  # end of file
				# pad the end chunk if necessary
				padSize = 16 - (len(thisChunk) + FILE_LENGTH_FIELD_SIZE) % 16
				padding = ' ' * padSize
				thisChunk += padding
				thisChunk += fileLengthField
				isFinalChunk = True

			outputStream.write(encryptor.encrypt(thisChunk))

		del encryptor
		outputStream.seek(0)
		return outputStream

	def DecryptFile(self, fileName, key = None, decryptedFileName = None, chunkSize = None, initVectSize = None):
		"""
		Decrypt a file
		@param chunkSize: 		int, read-in chunk size
		@param key:				str, key used for encryption
		@return:
		"""
		if chunkSize is not None and chunkSize % 16 != 0:
			raise RuntimeError('Bad chunksize')
		if decryptedFileName is None:
			decryptedFileName = fileName[:-3]  # strip off the 'enc' in the file extension
		if chunkSize is None:
			chunkSize = self.chunkSize
		if initVectSize is None:
			initVectSize = self.initVectSize

		with open(fileName, 'rb') as inputFile:
			initVect = inputFile.read(initVectSize)
			decryptor = AES.new(self.key if key is None else key, self.mode, initVect)
			with open(decryptedFileName, 'wb+') as outputFile:

				thisChunk = None
				isFinalChunk = False

				while not isFinalChunk:
					thisChunk = inputFile.read(chunkSize)

					if len(thisChunk) == 0:  # EOF
						# read out actual size of file
						outputFile.seek(-FILE_LENGTH_FIELD_SIZE, WHENCE_EOF)
						fileLengthField = outputFile.read(FILE_LENGTH_FIELD_SIZE)
						originalSize = struct.unpack('<Q', fileLengthField)[0]
						isFinalChunk = True

					outputFile.write(decryptor.decrypt(thisChunk))
				# truncate to original size
				outputFile.truncate(originalSize)
		del decryptor

	def DecryptStream(self, inputStream, key = None, chunkSize = None, initVectSize = None):
		"""
		Decrypt a stream
		@param inputStream:		stream object
		@param chunkSize:		int, decryption chunk size
		@param initVectSize:	int, initialization vector size for this file
		@param key:				str, key used for encryption
		@return:	BytesIO, decrypted stream
		"""
		if chunkSize is not None and chunkSize % 16 != 0:
			raise RuntimeError('Bad chunksize')
		if chunkSize is None:
			chunkSize = self.chunkSize
		if initVectSize is None:
			initVectSize = self.initVectSize

		inputStream.seek(0)
		initVect = inputStream.read(initVectSize)
		decryptor = AES.new(self.key if key is None else key, self.mode, initVect)
		outputStream = BytesIO()

		thisChunk = None
		isFinalChunk = False

		while not isFinalChunk:
			thisChunk = inputStream.read(chunkSize)

			if len(thisChunk) == 0:
				outputStream.seek(-FILE_LENGTH_FIELD_SIZE, WHENCE_EOF)
				fileLengthField = outputStream.read(FILE_LENGTH_FIELD_SIZE)
				originalSize = struct.unpack('<Q', fileLengthField)[0]
				isFinalChunk = True

			outputStream.write(decryptor.decrypt(thisChunk))

		del decryptor
		outputStream.truncate(originalSize)
		outputStream.seek(0)
		return outputStream


class RSAAESEncryption(AESEncryption):
	"""
	Encrypts each file using a unique AES key. Keys are then encrypted with RSA and returned along with the objects,
	the RSA private key is needed to decrypt the AES key.
	"""

	def __init__(self, key = None, mode = CIPHER_BLOCK_CHAIN, chunkSize = DEFAULT_CHUNKSIZE, initVectSize = INIT_VECT_SIZE, AESkeySize = 32):
		"""

		@param key:				str, key file for the RSA key
		@param mode:			enum, AES mode
		@param chunkSize:		int, chunk size
		@param initVectSize:	int, ini
		@param AESkeySize: 		int, size of AES keys used for file encryption
		"""
		super(RSAAESEncryption, self).__init__(key, mode, chunkSize, initVectSize)
		if AESkeySize not in [16, 24, 32]:
			raise ValueError('Bad AES key size')
		self.AESkeySize = AESkeySize

	@property
	def canDecrypt(self):
		"""
		Can this object decrypt? i.e. does it have a private RSA key?
		@return:
		"""
		if not self.RSAcipher:
			return False
		return self.RSAcipher.can_decrypt()

	@property
	def canEncrypt(self):
		"""
		Can this object encrypt?, i.e. does it have a public key?
		@return:
		"""
		if not self.RSAcipher:
			return False
		return self.RSAcipher.can_encrypt()

	def GenerateKey(self, keySize = 2048):
		"""
		Generates the RSA key used by this object
		@param keySize:		int, bits desired in RSA key
		@return:
		@raise ValueError if keySize is not a multiple of 256
		"""
		if keySize % 256 != 0:
			raise ValueError('RSA key size must be divisible by 256')
		self.key = RSA.generate(keySize)  # RSA key
		self.RSAcipher = PKCS1_OAEP.new(self.key)  # salts/pads things to be encrypted

	def ReadKey(self, fileName):
		with open(fileName) as keyFile:
			self.key = RSA.importKey(keyFile.read())
		self.RSAcipher = PKCS1_OAEP.new(self.key)

	def StoreKey(self, fileName = 'key.key', public = True):
		"""
		Stores the RSA keys
		@param fileName:		str, name of key file
		@param public: 			bool, if true, saves only the public half of the key
		@return:
		"""
		with open(fileName, 'w') as keyFile:
			if not public:
				keyFile.write(self.key.exportKey())
			else:
				keyFile.write(self.key.publickey().exportKey())

	def GenerateAESKey(self, keySize = None):
		"""
		Generates a new AES key
		@param keySize:		int, AES key size in bytes
		@return:	str, AES key
		"""
		if keySize is None:
			keySize = self.AESkeySize
		if keySize not in [16, 24, 32]:
			raise ValueError('Bad AES key size')
		return Random.get_random_bytes(keySize)

	def EncryptFile(self, fileName, encryptedFileName = None, AESkey = None, chunksize = None, initVectSize = None):
		"""
		Encrypts a file
		@param fileName:			str, name of file to encrypt
		@param encryptedFileName:	str, name of encrypted file
		@param AESkey:				str, AES key to use
		@param chunksize:			int, encryption chunk size
		@param initVectSize:		int, initialization vector size
		@return:	str, encrypted AES key for this particular file
		"""
		if AESkey is None:
			AESkey = self.GenerateAESKey()

		super(RSAAESEncryption, self).EncryptFile(fileName, encryptedFileName, AESkey, chunksize, initVectSize)
		return self.RSAcipher.encrypt(AESkey)

	def EncryptStream(self, inputStream, AESkey = None, chunkSize = None, initVectSize = None):
		"""
		Encrypts a stream
		@param inputStream:		stream, stream to be encrypted
		@param AESkey:			str, AES key to use
		@param chunkSize:		int, encryption chunk size
		@param initVectSize:	int, initialization vector size
		@return: 	stream, encrypted stream, and str, encrypted AES key for this stream
		"""
		if AESkey is None:
			AESkey = self.GenerateAESKey()

		outstream = super(RSAAESEncryption, self).EncryptStream(inputStream, AESkey, chunkSize, initVectSize)
		return outstream, self.RSAcipher.encrypt(AESkey)

	def EncryptString(self, plaintext):
		"""
		Encrypts a plaintext string using RSA
		@param plaintext: 	str, plaintext string
		@return:	str, encrypted string
		"""
		return self.RSAcipher.encrypt(plaintext)

	def DecryptString(self, ciphertext):
		"""
		Decrypts an RSA-encrypted string
		@param ciphertext: 	str, ciphertext string
		@return: 	str, plaintext
		"""
		return self.RSAcipher.decrypt(ciphertext)

	def DecryptFile(self, fileName, EncryptedAESkey = None, decryptedFileName = None, chunkSize = None, initVectSize = None):
		"""
		Decrypts a file on disk
		@param fileName:			str, name of file to be decrypted
		@param EncryptedAESkey:		str, encrypted AES key for this file
		@param decryptedFileName:	str, name of decrypted file
		@param chunkSize:			int, chunk size to use
		@param initVectSize:		int, initialization vector size
		@return:
		@raise RuntimeError if no AES key is given
		"""
		if EncryptedAESkey is None:
			raise RuntimeError('You need a key!')
		AESkey = self.RSAcipher.decrypt(EncryptedAESkey)
		super(RSAAESEncryption, self).DecryptFile(fileName, AESkey, decryptedFileName, chunkSize, initVectSize)

	def DecryptStream(self, inputStream, EncryptedAESkey = None, chunkSize = None, initVectSize = None):
		"""
		Decrypts a stream
		@param inputStream:			stream, stream to be decrypted
		@param EncryptedAESkey: 	str, encrypted AES key for this file
		@param chunkSize:			int, chunk size to use
		@param initVectSize:		int, initialization vector size
		@return:	stream, decrypted object
		@raise RuntimeError if no AES key is given
		"""
		if EncryptedAESkey is None:
			raise RuntimeError('You need a key!')
		AESkey = self.RSAcipher.decrypt(EncryptedAESkey)
		return super(RSAAESEncryption, self).DecryptStream(inputStream, AESkey, chunkSize, initVectSize)
