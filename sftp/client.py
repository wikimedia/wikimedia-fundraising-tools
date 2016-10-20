import os
import os.path
import base64
import paramiko
import StringIO

from process.logging import Logger as log
import process.globals


class Client(object):
    def __init__(self):
        self.client = None
        self.connect()
        self.config = process.globals.get_config()

    def __del__(self):
        if self.client:
            self.client.close()

    def connect(self):
        log.info("Connecting to {host}".format(host=self.config.sftp.host))
        transport = paramiko.Transport((self.config.sftp.host, 22))
        params = {
            'username': self.config.sftp.username,
        }
        if hasattr(self.config.sftp, 'host_key'):
            params['hostkey'] = make_key(self.config.sftp.host_key)
        if hasattr(self.config.sftp, 'password'):
            params['password'] = self.config.sftp.password
        if hasattr(self.config.sftp, 'private_key'):
            params['pkey'] = make_key(self.config.sftp.private_key)
        transport.connect(**params)
        self.client = paramiko.SFTPClient.from_transport(transport)

    def ls(self, path):
        return self.client.listdir(path)

    def get(self, filename, dest_path):
        try:
            self.client.get(filename, dest_path)
        except:
            if os.path.exists(dest_path):
                log.info("Removing corrupted download: {path}".format(path=dest_path))
                os.unlink(dest_path)
            raise

    def put(self, localpath, remotepath):
        self.client.put(localpath, os.path.join(self.config.sftp.remote_root, remotepath))


class Crawler(object):
    @staticmethod
    def pull():
        '''Pull down new remote files'''

        config = process.globals.get_config()

        # Check against both unprocessed and processed files to find new remote files
        local_paths = [
            config.incoming_path,
            config.archive_path,
        ]
        if hasattr(config, 'extra_paths'):
            local_paths.extend(config.extra_paths)
        local_files = walk_files(local_paths)

        remote = Client()
        remote_files = remote.ls(config.sftp.remote_root)
        empty_failures = []

        for filename in remote_files:
            if filename in local_files:
                log.info("Skipping already downloaded file {filename}".format(filename=filename))
                continue

            log.info("Downloading file {filename}".format(filename=filename))
            dest_path = os.path.join(config.incoming_path, filename)
            remote.get(os.path.join(config.sftp.remote_root, filename), dest_path)

            # Assert that the file is not empty
            if os.path.getsize(dest_path) == 0:
                os.unlink(dest_path)
                empty_failures.append(filename)
                log.warn("Stupid file was empty, removing locally: {path}".format(path=dest_path))

        if empty_failures:
            log.error("The following files were empty, please contact your provider: {failures}".format(failures=", ".join(empty_failures)))

            if hasattr(config, 'panic_on_empty') and config.panic_on_empty:
                raise RuntimeError("Stupid files did not download correctly.")


def walk_files(paths):
    '''List all files under these path(s)

    Parameters
    ==========
    * paths - single or list of paths

    Return value
    ============
    A list of all files found under the root directory(ies)
    '''

    result = []

    if not hasattr(paths, 'extend'):
        paths = [paths]

    for root in paths:
        for dirpath, dirnames, filenames in os.walk(root):
            result += filenames

    return result


def make_key(keystr=None):
    '''Cheesily detect a key string's type and create a Key object from it

    FIXME: janky pseudo-standard format in use: 'ssh-rsa <KEYMATERIAL>'
    '''

    if 'BEGIN RSA PRIVATE KEY' in keystr:
        fileish = StringIO.StringIO(keystr)
        return paramiko.RSAKey.from_private_key(fileish)
    elif 'ssh-rsa' in keystr:
        return paramiko.RSAKey(data=base64.decodestring(keystr.split(' ')[1]))
    elif 'BEGIN DSS PRIVATE KEY' in keystr:
        fileish = StringIO.StringIO(keystr)
        return paramiko.DSSKey.from_private_key(fileish)
    elif 'ssh-dss' in keystr:
        return paramiko.DSSKey(data=base64.decodestring(keystr.split(' ')[1]))
    elif 'ecdsa-' in keystr:
        return paramiko.ECDSAKey(data=base64.decodestring(keystr.split(' ')[1]))

    raise Exception('Unknown key provided')
