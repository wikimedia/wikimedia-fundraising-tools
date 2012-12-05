'''
Lockfile using a temporary file and the process id.

Self-corrects stale locks unless "failopen" is True.
'''
import os, os.path
import sys

lockfile = None

def begin(filename=None, failopen=False):
    if os.path.exists(filename):
        print "Lockfile found!"
        f = open(filename, "r")
        pid = None
        try:
            pid = int(f.read())
        except ValueError:
            pass
        f.close()
        if not pid:
            print "Invalid lockfile contents."
        else:
            try:
                os.getpgid(pid)
                print "Aborting! Previous process (%d) is still alive. Remove lockfile manually if in error: %s" % (pid, filename, )
                sys.exit(1)
            except OSError:
                if failopen:
                    print "Aborting until stale lockfile is investigated: %s" % filename
                    sys.exit(1)
                print "Lockfile is stale."
        print "Removing old lockfile."
        os.unlink(filename)

    f = open(filename, "w")
    f.write(str(os.getpid()))
    f.close()

    global lockfile
    lockfile = filename

def end():
    global lockfile
    os.unlink(lockfile)
