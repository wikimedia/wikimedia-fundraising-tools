import os, os.path
import base64
import paramiko

from process.logging import Logger as log
from process.globals import config

class Client(object):
    def __init__(self, host_rsa_key=None, host_dss_key=None):
        if host_dss_key:
            self.host_public_key = paramiko.DSSKey(data=base64.decodestring(host_dss_key))
        elif host_rsa_key:
            self.host_public_key = paramiko.RSAKey(data=base64.decodestring(host_rsa_key))

        self.connect()

    def __del__(self):
        self.client.close()

    def connect(self):
        log.info("Connecting to {host}".format(host=config.sftp.host))
        transport = paramiko.Transport((config.sftp.host, 22))
        transport.connect(username=config.sftp.username, password=config.sftp.password, hostkey=self.host_public_key)
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
