#!/usr/bin/env python

import glob
import os

import process
from process.logging import Logger as log
from sftp.client import Client as SftpClient
import process.lock as lock


def upload_most_recent():
    """
    Send recently exported CSVs to Silverpop.
    """
    config = process.globals.get_config()
    updatesglob = os.path.join(config.working_path, "DatabaseUpdate-*.csv")
    unsubglob = os.path.join(config.working_path, "Unsubscribes-*.csv")
    # Find most recently created export files.
    updatefile = max(glob.iglob(updatesglob), key=os.path.getctime)
    unsubfile = max(glob.iglob(unsubglob), key=os.path.getctime)

    upload([updatefile, unsubfile])


def upload(files=None):
    log.info("Uploading to silverpop")
    sftpc = SftpClient()
    for path in files:
        log.info("Putting file %s" % path)
        sftpc.put(path, os.path.basename(path))


if __name__ == '__main__':
    process.globals.load_config('silverpop_export')

    lock.begin()

    upload_most_recent()

    lock.end()
