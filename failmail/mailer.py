from email.mime.text import MIMEText
import logging
import smtplib
import sys
import traceback
import yaml

from process.globals import get_config

log = logging.getLogger(__name__)


class FailMailer(object):
    @staticmethod
    def mail(errorcode, data=None, print_exception=False):
        body = ""
        if print_exception:
            exception_info = "".join(traceback.format_exception(*sys.exc_info()))
            body = body + exception_info
        if data:
            if not isinstance(data, str):
                data = yaml.safe_dump([data], default_flow_style=False, allow_unicode=True)
            body = body + "\n\nWhile processing:\n{data}".format(data=data)

        log.error("sending failmail: " + body)

        msg = MIMEText(body)

        config = get_config()
        from_address = config.failmail_sender
        to_address = config.failmail_recipients
        if hasattr(to_address, 'split'):
            to_address = to_address.split(",")

        msg['Subject'] = "Fail Mail: {code} ({process})".format(code=errorcode, process=config.app_name)
        msg['From'] = from_address
        msg['To'] = to_address[0]

        mailer = smtplib.SMTP('localhost')
        mailer.sendmail(from_address, to_address, msg.as_string())
        mailer.quit()
