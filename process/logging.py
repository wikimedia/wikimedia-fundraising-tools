import sys
import syslog

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
        syslog.openlog()
        syslog.syslog(severity, message)
        syslog.closelog()

        if sys.stdout.isatty():
            print(message)
