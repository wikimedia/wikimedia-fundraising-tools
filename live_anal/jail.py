'''
Phony "jail" provides inline "virtualenv" execution
'''
import os
import subprocess

jail_path = os.path.dirname(os.path.abspath(__file__)) + "/jailenv"

def run(source):
    env = os.environ
    env['PATH'] = jail_path + "/bin:" + env['PATH']
    ret = subprocess.call( [ "python", "-c", source ], env=env )
    if ret is not 0:
        raise Exception("Failed to run jail command, result=%d" % ret)
