import paramiko
import pytest

from sftp.client import make_key

TEST_OPENSSH_ED25519_KEY = """-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACACPiOUMQDbTJPBZSHjzeju07UqKN7MfRWo1RKxJtcQXwAAAJixNzTgsTc0
4AAAAAtzc2gtZWQyNTUxOQAAACACPiOUMQDbTJPBZSHjzeju07UqKN7MfRWo1RKxJtcQXw
AAAECzwXtxlplZvJaKyO/KGCtC8LJs82/CdBN6DjS23+q4WwI+I5QxANtMk8FlIePN6O7T
tSoo3sx9FajVErEm1xBfAAAAEHRlc3RAZXhhbXBsZS5jb20BAgMEBQ==
-----END OPENSSH PRIVATE KEY-----"""


def test_make_key_openssh_ed25519():
    """make_key parses a valid OpenSSH Ed25519 private key."""
    result = make_key(TEST_OPENSSH_ED25519_KEY)
    assert isinstance(result, paramiko.Ed25519Key)


def test_make_key_ssh_ed25519_public_key():
    """make_key parses an ssh-ed25519 public key string."""
    # Derive the public key from the test private key
    private_key = make_key(TEST_OPENSSH_ED25519_KEY)
    pub_blob = private_key.asbytes()
    pub_b64 = __import__('base64').b64encode(pub_blob).decode()
    pub_keystr = f"ssh-ed25519 {pub_b64} test@example.com"

    result = make_key(pub_keystr)
    assert isinstance(result, paramiko.Ed25519Key)


def test_make_key_openssh_invalid_payload():
    """make_key raises when the OpenSSH key payload is garbage."""
    bad_key = "-----BEGIN OPENSSH PRIVATE KEY-----\nnotavalidkey\n-----END OPENSSH PRIVATE KEY-----"
    with pytest.raises(Exception):
        make_key(bad_key)
