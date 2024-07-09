"""
This is an abstract base class for authentication. It provides methods to authenticate and validate tokens.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import base64
import os

import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from abc import ABC, abstractmethod


class Auth(ABC):
    # set the secret key during instantiation
    def __init__(self, secret_key):
        # secret_key must be set
        assert secret_key, 'Secret key must be set'
        self._secret_key = secret_key

    def authenticate(self, database, username, password):
        # validate submitted credentials and obtain user ID to store in token
        uid = self._validate_submitted_credentials(database, username, password)

        # encrypt password
        encrypted_password, salt = self.encrypt(self._secret_key, password)

        payload = {"database": database, "uid": uid, "password": encrypted_password, "salt": salt}
        token = jwt.encode(payload, self._secret_key, algorithm='HS256')
        return token

    def validate_token(self, token):
        # decode token
        payload = jwt.decode(token, self._secret_key, algorithms=['HS256'])

        # get connection parameters from payload
        database, uid, encrypted_password, salt = payload['database'], payload['uid'], payload['password'], payload['salt']

        # decrypt password
        password = self.decrypt(self._secret_key, encrypted_password, salt)

        # validate token credentials and return resulting objects
        result = self._validate_token_credentials(database, uid, password)

        # return validation result
        return result

    @abstractmethod
    def _validate_submitted_credentials(self, database, username, password):
        pass

    @abstractmethod
    def _validate_token_credentials(self, database, username, password):
        pass

    def encrypt(self, secret, plaintext):
        salt = os.urandom(16)  # Generate a random salt
        dek = self._derive_key_from_secret(secret, salt)

        encrypted_secret = self._encrypt_with_derived_key(plaintext, dek)
        return base64.urlsafe_b64encode(encrypted_secret).decode(), base64.urlsafe_b64encode(salt).decode()

    def _encrypt_with_derived_key(self, plaintext, dek):
        cipher = Cipher(algorithms.AES(dek), modes.ECB(), backend=default_backend())
        encryptor = cipher.encryptor()

        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(plaintext.encode()) + padder.finalize()

        ciphertext = encryptor.update(padded_data) + encryptor.finalize()
        return ciphertext

    def decrypt(self, secret, ciphertext, salt):
        dek = self._derive_key_from_secret(secret, base64.urlsafe_b64decode(salt))
        encrypted_data = base64.urlsafe_b64decode(ciphertext)

        decrypted_data = self._decrypt_with_derived_key(encrypted_data, dek)

        return decrypted_data

    def _decrypt_with_derived_key(self, ciphertext, dek):
        cipher = Cipher(algorithms.AES(dek), modes.ECB(), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_data = decryptor.update(ciphertext) + decryptor.finalize()

        # Use PKCS7 unpadding
        unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
        plaintext = unpadder.update(decrypted_data) + unpadder.finalize()

        return plaintext.decode()

    def _derive_key_from_secret(self, passphrase, salt):
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # Length of the derived key
            salt=salt,
            iterations=100000,  # Adjust as needed
            backend=default_backend()
        )
        # return base64.urlsafe_b64encode(kdf.derive(passphrase.encode())).decode()
        return kdf.derive(passphrase.encode())
