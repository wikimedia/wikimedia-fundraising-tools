'''
Lockfile using a temporary file and the process id.

Self-corrects stale locks unless "failopen" is True.
'''
import os
import os.path
import sys

from log import Logger as log

lockfile = None


def begin(filename=None, failopen=False):
    if not filename:
        unique = os.environ['LOGNAME']
        cmd = os.path.basename(sys.argv[0])
        filename = "/tmp/%s-%s.lock" % (unique, cmd)

    if os.path.exists(filename):
        log.warn("Lockfile found!")
        f = open(filename, "r")
        pid = None
        try:
            pid = int(f.read())
        except ValueError:
            pass
        f.close()
        if not pid:
            log.error("Invalid lockfile contents.")
        else:
            try:
                os.getpgid(pid)
                log.error("Aborting! Previous process ({pid}) is still alive. Remove lockfile manually if in error: {path}".format(pid=pid, path=filename))
                sys.exit(1)
            except OSError:
                if failopen:
                    log.fatal("Aborting until stale lockfile is investigated: {path}".format(path=filename))
                    sys.exit(1)
                log.error("Lockfile is stale.")
        log.info("Removing old lockfile.")
        os.unlink(filename)

    f = open(filename, "w")
    f.write(str(os.getpid()))
    f.close()

    global lockfile
    lockfile = filename


def end():
    global lockfile
    if lockfile:
        os.unlink(lockfile)
    else:
        raise RuntimeError("Already unlocked!")
