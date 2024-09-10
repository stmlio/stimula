import jwt
import pytest
from jwt import InvalidSignatureError

from tests.conftest import TestAuth


def test_authenticate(auth, db_params):
    token = auth.authenticate(db_params['database'], db_params['user'], db_params['password'])
    assert token
    # decode the jwt token
    payload = jwt.decode(token, 'secret', algorithms=['HS256'])
    # validate the payload contains the url
    assert payload['database'] == db_params['database']


def test_validate_token(auth, url, db_params):
    cipher, salt = auth.encrypt('secret', db_params['password'])

    # create token
    token = jwt.encode({"database": url, "uid": db_params['user'], "password": cipher, "salt": salt}, 'secret', algorithm='HS256')
    # validate token
    auth.validate_token(token)


def test_authenticate_then_validate(auth, db_params):
    # create token
    token = auth.authenticate(db_params['database'], db_params['user'], db_params['password'])
    # validate token
    auth.validate_token(token)


def test_validate_token_wrong_secret(auth, url):
    # create token
    token = jwt.encode({"url": url}, 'not a secret', algorithm='HS256')
    # expects a ValueError
    with pytest.raises(InvalidSignatureError):
        # validate token
        auth.validate_token(token)


def test_validate_token_not_connected(auth, url):
    # create token
    token = jwt.encode({"url": url + 'xxx'}, 'secret', algorithm='HS256')
    # expects a ValueError
    with pytest.raises(Exception):
        # validate token
        auth.validate_token(token)


def test_encrypt_and_decrypt(auth):
    # verify we can encrypt and decrypt
    secret, plaintext = 'secret', 'plaintext spanning multiple blocks'
    cipher, salt = auth.encrypt(secret, plaintext)
    result = auth.decrypt(secret, cipher, salt)
    assert result == plaintext


def test_cipher_is_different_each_time(auth):
    # encrypt twice
    secret, plaintext = 'secret', 'plaintext'
    cipher1, salt1 = auth.encrypt(secret, plaintext)
    cipher2, salt2 = auth.encrypt(secret, plaintext)

    assert cipher1 != cipher2
    assert salt1 != salt2


def test_cipher_length_always_the_same(auth):
    # encrypt twice, different lengths, not longer than block size of 128 bits
    secret, plaintext1, plaintext2 = 'secret', 'short', 'a longer string'
    cipher1, salt1 = auth.encrypt(secret, plaintext1)
    cipher2, salt2 = auth.encrypt(secret, plaintext2)
    assert len(cipher1) == len(cipher2)


def test_authenticate_expired_token(db_params):
    auth = TestAuth('secret', lifetime=0)
    # create token
    token = auth.authenticate(db_params['database'], db_params['user'], db_params['password'])
    # expect ExpiredSignatureError
    with pytest.raises(jwt.ExpiredSignatureError):
        # validate
        auth.validate_token(token)
