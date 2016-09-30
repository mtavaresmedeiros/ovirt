#!/usr/bin/env python
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Author: Marcelo Medeiros <marceloltm@hotmail.com>
# Author: Amador Pahim <apahim@redhat.com>


"""
Utility module to send e-mails.
"""


import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class ConnError(Exception):
    """
    Error class to represent smtp connection errors.
    """
    pass


class SendError(Exception):
    """
    Error class to represent sendmail errors.
    """
    pass


class Email(object):
    """
    Class to configure and send e-mails.
    """

    def __init__(self,
                 server,
                 username,
                 password):
        self.server = server
        self.username = username
        self.password = password

    def send(self, fromaddr, toaddr, subject, body):
        """
        Send the e-mail.
        """
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        try:
            conn = self._connect()
        except Exception as exception:
            raise ConnError(exception.message)

        try:
            conn.sendmail(fromaddr, toaddr, msg.as_string())
        except Exception as exception:
            raise SendError(exception.message)

        conn.quit()

    def _connect(self):
        """
        Connect to mail server.
        """
        conn = smtplib.SMTP(self.server)
        conn.starttls()
        conn.login(self.username, self.password)
        return conn
