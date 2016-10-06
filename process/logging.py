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
        print >>sys.stderr, message

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

        # TODO: Echo to stdout here since we can't log yet?
        config = process.globals.get_config()
        if sys.stdout.isatty() or (not hasattr(config, 'quiet') or not config.quiet):
            print(message)
            sys.stdout.flush()


# FIXME: This late import is to cheat a circular dependency.
import process.globals
