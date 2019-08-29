#!/usr/bin/env python3

import glob
import logging
import os
import sys

import process
from sftp.client import Client as SftpClient
import process.lock as lock

log = logging.getLogger(__name__)


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

    for i in range(3):
        try:
            upload_most_recent()
            break
        except Exception:
            log.error("Ran into trouble: " + str(sys.exc_info()))

            if i == 2:
                raise

    lock.end()
