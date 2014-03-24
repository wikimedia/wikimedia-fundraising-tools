import sys
import syslog
import os.path

class Logger(object):
    
    @staticmethod
    def debug(message):
        Logger.log(message, syslog.LOG_DEBUG)

    @staticmethod
    def info(message):
        Logger.log(message, syslog.LOG_INFO)

    @staticmethod
    def warn(message):
        Logger.log(message, syslog.LOG_WARNING)

    @staticmethod
    def error(message):
        Logger.log(message, syslog.LOG_ERR)

    @staticmethod
    def fatal(message):
        Logger.log(message, syslog.LOG_CRIT)
        print >>sys.stderr, message

    @staticmethod
    def log(message, severity):
        app_name = os.path.basename(sys.argv[0])
        syslog.openlog(app_name)
        syslog.syslog(severity, message)
        syslog.closelog()

        # FIXME:
        # if sys.stdout.isatty():
        # or not config.quiet
        print(message)
