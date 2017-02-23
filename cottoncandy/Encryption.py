from __future__ import print_function
import os
import struct

from Crypto import Random
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.PublicKey import RSA
from io import BytesIO

CIPHER_BLOCK_CHAIN = AES.MODE_CBC
INIT_VECT_SIZE = 16
DEFAULT_CHUNK_SIZE = 64 * 1024
FILE_LENGTH_FIELD_SIZE = struct.calcsize('Q')
WHENCE_EOF = 2


class Encryption(object):
    """
    Abstract base class for a file encrypt/decrypt object
    """

    def __init__(self, key = None, keyfile = None):
        """

        Parameters
        ----------
        key : str
            key to use
        keyfile : str
            key from which to read stored key
        """

        if key is not None:
            self.key = key
        elif keyfile is not None:
            self.read_key(keyfile)
        else:
            self.generate_key()

    def read_key(self, file_name):
        """
        Reads a stored key

        Parameters
        ----------
        file_name : str
            name of file to read from

        Returns
        -------

        """
        with open(file_name) as file:
            self.key = file.read()

    def store_key(self, file_name = 'key.key'):
        """
        Stores key to file

        Parameters
        ----------
        file_name : str
            path of tile to store

        Returns
        -------

        """
        with open(file_name) as keyFile:
            keyFile.write(self.key)

    def generate_key(self):
        """
        Generates a key for encrypting/decrypting files for this object
        """
        raise NotImplementedError

    def encrypt_file(self, file_name, encrypted_file_name = None):
        """
        Encrypts a file on disk

        Parameters
        ----------
        file_name : str
            path to file to be encrypted
        encrypted_file_name : str
            path for encrypted file

        Returns
        -------

        """
        raise NotImplementedError

    def encrypt_stream(self, instream):
        """
        Encrypts a stream object in memory

        Parameters
        ----------
        instream : stream
            object in memory with a .read() function

        Returns
        -------
        : stream
            encrypted object
        """
        raise NotImplementedError

    def decrypt_file(self, file_name, key = None, decrypted_file_name = None):
        """
        Decrypts a file on disk

        Parameters
        ----------
        file_name : str
            path to encrypted file
        key : str
            key to use for decryption
        decrypted_file_name : str
            name for decrypted file

        Returns
        -------

        """
        raise NotImplementedError

    def decrypt_stream(self, instream, key = None):
        """
        Decrypts a stream in memory

        Parameters
        ----------
        instream : stream
            object with .read() function
        key : str
            key to use for decryption

        Returns
        -------
        : stream
            decrypted stream
        """
        raise NotImplementedError


class AESEncryption(Encryption):
    """
    Encrypts files using an AES cipher. All files passing through the same object will be encrypted with the same key
    Encrypted files have the following structure:

    [  16 bytes default   ][		file size 		     || file size % 16 - 8 bytes ][	 	8 bytes		 ]
    [initialization vector][ binary ciphertext for file  ||   padding with spaces	 ][file size in bytes]
    """

    def __init__(self, key = None, keyfile = None, mode = CIPHER_BLOCK_CHAIN, chunk_size = DEFAULT_CHUNK_SIZE, initialisation_vector_size = INIT_VECT_SIZE):
        """

        Parameters
        ----------
        key : str
            key to use
        keyfile : str
            path to stored key, overrides key value
        mode : int
            encrytion mode
        chunk_size : int
            size in bytes of each chunk during encryption
        initialisation_vector_size : int
            bytes of initialisation vector
        """
        super(AESEncryption, self).__init__(key, keyfile)
        self.mode = mode
        self.chunk_size = chunk_size
        self.initialisation_vector_size = initialisation_vector_size

    def generate_key(self, key_size = 32):
        """
        Generates a new AES key

        Parameters
        ----------
        key_size : int
            bits in key

        Returns
        -------

        """
        if key_size not in [16, 24, 32]:
            raise RuntimeError('Bad key length')
        self.key = Random.get_random_bytes(key_size)

    def encrypt_file(self, file_name, encrypted_file_name = None, key = None, chunk_size = None, initialisation_vector_size = None):
        """

        Parameters
        ----------
        file_name
        encrypted_file_name
        key : str
            key to use
        chunk_size : int
            bytes in each chunk
        initialisation_vector_size : int
            bytes in initialisation vector

        Returns
        -------

        """
        if chunk_size is not None and chunk_size % 16 != 0:
            raise RuntimeError('Bad chunk_size')
        if encrypted_file_name is None:
            encrypted_file_name = file_name + 'enc'
        if chunk_size is None:
            chunk_size = self.chunk_size
        if initialisation_vector_size is None:
            initialisation_vector_size = self.initialisation_vector_size

        # Initialize AES encryption object
        initialisation_vector = Random.new().read(initialisation_vector_size)
        encryptor = AES.new(self.key if key is None else key, self.mode, initialisation_vector)

        # record file size since the encrypted file might be padded
        input_size = os.path.getsize(file_name)
        file_length_field = struct.pack('<Q', input_size)

        with open(file_name, 'rb') as input_file:
            with open(encrypted_file_name, 'wb') as output_file:
                assert len(initialisation_vector) == initialisation_vector_size
                output_file.write(initialisation_vector)  # record the initialization vector

                # encrypt the file in chunks
                this_chunk = None
                is_final_chunk = False

                while not is_final_chunk:
                    this_chunk = input_file.read(chunk_size)

                    if len(this_chunk) == 0 or len(this_chunk) % 16 != 0:  # end of file
                        # pad the end chunk if necessary - AES needs things to be in 16-byte blocks
                        padSize = 16 - (len(this_chunk) + FILE_LENGTH_FIELD_SIZE) % 16
                        padding = ' ' * padSize
                        this_chunk += padding
                        this_chunk += file_length_field  # record the actual file length so we can get ride of the padding on decryption
                        is_final_chunk = True

                    output_file.write(encryptor.encrypt(this_chunk))
        del encryptor

    def encrypt_stream(self, instream, key = None, chunk_size = None, initialisation_vector_size = None):
        """

        Parameters
        ----------
        instream : stream
            object in memory with a .read() function
        key : str
            key to use
        chunk_size : int
            bytes in each chunk
        initialisation_vector_size : int
            bytes in initialisation vector

        Returns
        -------
        output_stream : stream
            encrypted stream
        """
        if chunk_size is None:
            chunk_size = self.chunk_size
        if chunk_size is not None and chunk_size % 16 != 0:
            raise RuntimeError('Bad chunk_size')
        if initialisation_vector_size is None:
            initialisation_vector_size = self.initialisation_vector_size

        initialisation_vector = Random.new().read(initialisation_vector_size)
        encryptor = AES.new(self.key if key is None else key, self.mode, initialisation_vector)

        instream.seek(0, os.SEEK_END)
        input_size = instream.tell()
        instream.seek(0)
        file_length_field = struct.pack('<Q', input_size)

        output_stream = BytesIO()
        output_stream.write(initialisation_vector)

        this_chunk = None
        is_final_chunk = False

        while not is_final_chunk:
            this_chunk = instream.read(chunk_size)

            if len(this_chunk) == 0 or len(this_chunk) % 16 != 0:  # end of file
                # pad the end chunk if necessary
                padSize = 16 - (len(this_chunk) + FILE_LENGTH_FIELD_SIZE) % 16
                padding = ' ' * padSize
                this_chunk += padding
                this_chunk += file_length_field
                is_final_chunk = True

            output_stream.write(encryptor.encrypt(this_chunk))

        del encryptor
        output_stream.seek(0)
        return output_stream

    def decrypt_file(self, file_name, key = None, decrypted_file_name = None, chunk_size = None, initialisation_vector_size = None):
        """

        Parameters
        ----------
        file_name : str
            path to encrypted file
        key : str
            key to use
        decrypted_file_name
        chunk_size : int
            bytes in each chunk
        initialisation_vector_size : int
            bytes in initialisation vector

        Returns
        -------

        """
        if chunk_size is not None and chunk_size % 16 != 0:
            raise RuntimeError('Bad chunk_size')
        if decrypted_file_name is None:
            decrypted_file_name = file_name[:-3]  # strip off the 'enc' in the file extension
        if chunk_size is None:
            chunk_size = self.chunk_size
        if initialisation_vector_size is None:
            initialisation_vector_size = self.initialisation_vector_size

        with open(file_name, 'rb') as input_file:
            initialisation_vector = input_file.read(initialisation_vector_size)
            decryptor = AES.new(self.key if key is None else key, self.mode, initialisation_vector)
            with open(decrypted_file_name, 'wb+') as output_file:

                this_chunk = None
                is_final_chunk = False

                while not is_final_chunk:
                    this_chunk = input_file.read(chunk_size)

                    if len(this_chunk) == 0:  # EOF
                        # read out actual size of file
                        output_file.seek(-FILE_LENGTH_FIELD_SIZE, WHENCE_EOF)
                        file_length_field = output_file.read(FILE_LENGTH_FIELD_SIZE)
                        original_size = struct.unpack('<Q', file_length_field)[0]
                        is_final_chunk = True

                    output_file.write(decryptor.decrypt(this_chunk))
                # truncate to original size
                output_file.truncate(original_size)
        del decryptor

    def decrypt_stream(self, instream, key = None, chunk_size = None, initialisation_vector_size = None):
        """

        Parameters
        ----------
        instream : stream
            object in memory with a .read() function
        key : str
            key to use
        chunk_size : int
            bytes in each chunk
        initialisation_vector_size : int
            bytes in initialisation vector

        Returns
        -------
        output_stream : stream
            decrypted stream
        """
        if chunk_size is not None and chunk_size % 16 != 0:
            raise RuntimeError('Bad chunk_size')
        if chunk_size is None:
            chunk_size = self.chunk_size
        if initialisation_vector_size is None:
            initialisation_vector_size = self.initialisation_vector_size

        instream.seek(0)
        initialisation_vector = instream.read(initialisation_vector_size)
        decryptor = AES.new(self.key if key is None else key, self.mode, initialisation_vector)
        output_stream = BytesIO()

        this_chunk = None
        is_final_chunk = False

        while not is_final_chunk:
            this_chunk = instream.read(chunk_size)

            if len(this_chunk) == 0:
                output_stream.seek(-FILE_LENGTH_FIELD_SIZE, WHENCE_EOF)
                file_length_field = output_stream.read(FILE_LENGTH_FIELD_SIZE)
                original_size = struct.unpack('<Q', file_length_field)[0]
                is_final_chunk = True

            output_stream.write(decryptor.decrypt(this_chunk))

        del decryptor
        output_stream.truncate(original_size)
        output_stream.seek(0)
        return output_stream


class RSAAESEncryption(AESEncryption):
    """
    Encrypts each file using a unique AES key. Keys are then encrypted with RSA and returned along with the objects,
    the RSA private key is needed to decrypt the AES key.
    """

    def __init__(self, key = None, keyfile = None, mode = CIPHER_BLOCK_CHAIN, chunk_size = DEFAULT_CHUNK_SIZE, initialisation_vector_size = INIT_VECT_SIZE, AES_key_length = 32):
        """

        Parameters
        ----------
        key : str
            key to use
        keyfile
        mode
        chunk_size : int
            bytes in each chunk
        initialisation_vector_size : int
            bytes in initialisation vector
        AES_key_length : int
            bits in AES keys
        """
        super(RSAAESEncryption, self).__init__(key, keyfile, mode, chunk_size, initialisation_vector_size)
        if AES_key_length not in [16, 24, 32]:
            raise ValueError('Bad AES key size')
        self.AES_key_length = AES_key_length

    @property
    def can_decrypt(self):
        """
        Can this object decrypt? i.e. does it have a private RSA key?
        """
        if not self.RSAcipher:
            return False
        return self.RSAcipher.can_decrypt()

    @property
    def can_encrypt(self):
        """
        Can this object encrypt?, i.e. does it have a public key?
        """
        if not self.RSAcipher:
            return False
        return self.RSAcipher.can_encrypt()

    def generate_key(self, key_size = 2048):
        """

        Parameters
        ----------
        key_size : int
            bits in RSA key

        Returns
        -------

        """
        if key_size % 256 != 0:
            raise ValueError('RSA key size must be divisible by 256')
        self.key = RSA.generate(key_size)  # RSA key
        self.RSAcipher = PKCS1_OAEP.new(self.key)  # salts/pads things to be encrypted

    def read_key(self, file_name):
        with open(file_name) as keyFile:
            self.key = RSA.importKey(keyFile.read())
        self.RSAcipher = PKCS1_OAEP.new(self.key)

    def store_key(self, file_name = 'key.key', public = True):
        """

        Parameters
        ----------
        file_name
        public : bool
            store public key only?

        Returns
        -------

        """
        with open(file_name, 'w') as keyfile:
            if not public:
                keyfile.write(self.key.exportKey())
            else:
                keyfile.write(self.key.publickey().exportKey())

    def generate_AES_key(self, key_size = None):
        """
        Generates a new AES key

        Parameters
        ----------
        key_size : int
            bits in key

        Returns
        -------
        key : str
            AES key
        """
        if key_size is None:
            key_size = self.AES_key_length
        if key_size not in [16, 24, 32]:
            raise ValueError('Bad AES key size')
        return Random.get_random_bytes(key_size)

    def encrypt_file(self, file_name, encrypted_file_name = None, AESkey = None, chunk_size = None, initialisation_vector_size = None):
        """

        Parameters
        ----------
        file_name
        encrypted_file_name
        AESkey : str
            AES key to use for this particular file
        chunk_size
        initialisation_vector_size

        Returns
        -------

        """
        if AESkey is None:
            AESkey = self.generate_AES_key()

        super(RSAAESEncryption, self).encrypt_file(file_name, encrypted_file_name, AESkey, chunk_size, initialisation_vector_size)
        return self.RSAcipher.encrypt(AESkey)

    def encrypt_stream(self, instream, AESkey = None, chunk_size = None, initialisation_vector_size = None):
        """

        Parameters
        ----------
        instream : stream
            object in memory with a .read() function
        AESkey : str
            AES key to use for this particular stream
        chunk_size
        initialisation_vector_size

        Returns
        -------

        """
        if AESkey is None:
            AESkey = self.generate_AES_key()

        outstream = super(RSAAESEncryption, self).encrypt_stream(instream, AESkey, chunk_size, initialisation_vector_size)
        return outstream, self.RSAcipher.encrypt(AESkey)

    def encrypt_string(self, plaintext):
        """
        Encrypts a string

        Parameters
        ----------
        plaintext : str
            plaintext

        Returns
        -------
        ciphertext : str
            ciphertext
        """
        return self.RSAcipher.encrypt(plaintext)

    def decrypt_string(self, ciphertext):
        """
        Decrypts a string

        Parameters
        ----------
        ciphertext : str
            ciphertext

        Returns
        -------
        plaintext : str
            plaintext
        """
        return self.RSAcipher.decrypt(ciphertext)

    def decrypt_file(self, file_name, encrypted_AES_key = None, decrypted_file_name = None, chunk_size = None, initialisation_vector_size = None):
        """

        Parameters
        ----------
        file_name
        encrypted_AES_key : str
            The encrypted AES key associated with this file
        decrypted_file_name
        chunk_size : int
            bytes in each chunk
        initialisation_vector_size : int
            bytes in initialisation vector

        Returns
        -------

        """
        if encrypted_AES_key is None:
            raise RuntimeError('You need a key!')
        AESkey = self.RSAcipher.decrypt(encrypted_AES_key)
        super(RSAAESEncryption, self).decrypt_file(file_name, AESkey, decrypted_file_name, chunk_size, initialisation_vector_size)

    def decrypt_stream(self, instream, encrypted_AES_Key = None, chunk_size = None, initialisation_vector_size = None):
        """

        Parameters
        ----------
        instream : stream
            object in memory with a .read() function
        encrypted_AES_Key : str
            The encrypted AES key associated with this file
        chunk_size : int
            bytes in each chunk
        initialisation_vector_size : int
            bytes in initialisation vector

        Returns
        -------

        """
        if encrypted_AES_Key is None:
            raise RuntimeError('You need a key!')
        AESkey = self.RSAcipher.decrypt(encrypted_AES_Key)
        return super(RSAAESEncryption, self).decrypt_stream(instream, AESkey, chunk_size, initialisation_vector_size)
