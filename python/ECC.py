#!/usr/bin/env python3
#
# Utility functions and helpers for all ECC code.
#

import os
import sys
import base64
import smtplib
import pytz
import Google
import logging
import platform
import logging.handlers

from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleAuthRequest

from email.message import EmailMessage

local_tz_name = 'America/Louisville'
local_tz = pytz.timezone(local_tz_name)

#-------------------------------------------------------------------

def diediedie(msg):
    print(msg)
    print("Aborting")

    exit(1)

#-------------------------------------------------------------------

class ECCSlackLogHandler(logging.StreamHandler):
    def __init__(self, token_filename, channel="#bot-errors"):
        logging.StreamHandler.__init__(self)
        self.channel = channel

        if not os.path.exists(token_filename):
            print(f"ERROR: Slack token filename {token_filename} does not exist")
            exit(1)

        with open(token_filename, "r") as fp:
            self.token = fp.read().strip()

        # We'll initialize this the first time it is actually used. No need to
        # login to Slack unless we actually intend to send a message.  Note that
        # we set the level for this logger to be "CRITIAL", so we won't actually
        # login to Slack unless log.critical() is invoked.
        self.client  = None

    def emit(self, record):
        # If this is the first time we're emitting a message, then initialize
        # the Slack client object.  This allows apps who don't use the Slack
        # handler to not have the slack_sdk module installed/available.
        import slack_sdk
        if not self.client:
            self.client = slack_sdk.WebClient(token=self.token)

        msg      = self.format(record)
        response = self.client.chat_postMessage(channel=self.channel,
                                                text=msg)

#-------------------------------------------------------------------

def setup_logging(name=sys.argv[0], info=True, debug=False, logfile=None,
                  log_millisecond=True, rotate=False,
                  slack_token_filename=None, slack_channel="#bot-errors"):
    level=logging.ERROR

    if debug:
        level="DEBUG"
    elif info:
        level="INFO"

    log = logging.getLogger('ECC')
    log.setLevel(level)

    # Make sure to include the timestamp in each message
    extra = "%Y-%m-%d %H:%M:%S" if not log_millisecond else ""
    f = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s', extra)

    # Default log output to stdout
    s = logging.StreamHandler()
    s.setFormatter(f)
    log.addHandler(s)

    if slack_token_filename:
        s = ECCSlackLogHandler(slack_token_filename, slack_channel)
        s.setLevel('CRITICAL')
        log.addHandler(s)

    # Optionally save to a logfile
    if logfile:
        if rotate:
            s = logging.handlers.RotatingFileHandler(filename=logfile,
                                                     maxBytes=(pow(2,20) * 10),
                                                     backupCount=50)
        else:
            if platform.system() != "Windows":
                # According to
                # https://docs.python.org/3/library/logging.handlers.html#watchedfilehandler,
                # the WatchedFile handler is not appropriate for MS
                # Windows.  The WatchedFile handler is friendly to
                # services like the Linux logrotater (i.e.,
                # WatchedFile will check the file before it writes
                # anything, and will re-open the file if it needs to).
                s = logging.handlers.WatchedFileHandler(filename=logfile)
            else:
                s = logging.FileHandler(filename=logfile)
        s.setFormatter(f)
        log.addHandler(s)

    # If on a Linux system with journald running, also emit to syslog
    # (which will end up at the journald).  Note: the journald may not
    # be running in a WSL environment.
    dev_log = '/dev/log'
    if platform.system() == "Linux" and os.path.exists(dev_log):
        syslog = logging.handlers.SysLogHandler(address=dev_log)

        # For the syslog, we need to get the basename of the
        # python script we are running (otherwise, it'll default to
        # "python" or "python3" or the like).
        b = os.path.basename(name)
        f = logging.Formatter(f'{b}: %(message)s')
        syslog.setFormatter(f)

        log.addHandler(syslog)

    log.debug('Starting')

    return log

#===================================================================
# Email functions (Gmail SMTP with XOAUTH2 via service account)
#
# Outbound mail is sent via Gmail's SMTP endpoint using the XOAUTH2
# authentication mechanism.  Authentication is performed using a Google
# Cloud service account that has been granted domain-wide delegation (DWD)
# in the Google Workspace Admin Console.  This matches the approach used
# by the ps-contribution-letters script.
#
# Why SMTP + XOAUTH2 instead of the Gmail REST API?
# -------------------------------------------------
# The Gmail REST API (users.messages.send) requires the scope:
#   https://www.googleapis.com/auth/gmail.send
# SMTP + XOAUTH2 requires the scope:
#   https://mail.google.com/
# Domain-wide delegation is granted per scope in the Workspace Admin
# Console.  If only 'https://mail.google.com/' has been authorised for
# the service account (which is the typical setup), using the Gmail API
# scope produces an 'unauthorized_client' error even though the key file
# is otherwise valid.
#
# Prerequisites (one-time setup by a Google Workspace administrator):
#
#   1. Create a service account in the Google Cloud Console and download
#      its JSON key file (IAM & Admin → Service Accounts → Keys).
#   2. Copy the service account's numeric Client ID from the GCP Console.
#   3. In the Google Workspace Admin Console go to:
#        Security → Access and data control → API controls
#        → Manage Domain Wide Delegation → Add new
#      Enter the Client ID and the scope:
#        https://mail.google.com/
#
# Typical usage:
#
#   ECC.setup_email(
#       service_account_json='service-account-key.json',
#       impersonated_user='no-reply@epiphanycatholicchurch.org',
#       log=log)
#   ECC.send_email(to_addr='someone@example.com',
#                  subject='Hello',
#                  body='World',
#                  log=log)
#
# Alternatively, if the calling script has already built a delegated
# google.oauth2.service_account.Credentials object, pass it directly:
#
#   ECC.setup_email(delegated_credentials=creds,
#                   impersonated_user='no-reply@…',
#                   log=log)
#===================================================================

# Delegated service-account credentials, initialised by setup_email().
# These are refreshed automatically before each send so tokens never
# expire mid-run for long-running scripts.
_smtp_credentials     = None

# The Google Workspace user being impersonated (used as the XOAUTH2 login).
_smtp_impersonated_user = None

# Default "From" address; can be overridden in setup_email() or per-call.
_gmail_from_addr      = 'no-reply@epiphanycatholicchurch.org'

# SMTP connection parameters.  smtp.gmail.com supports both SSL (port 465)
# and STARTTLS (port 587) with XOAUTH2.
_smtp_server          = 'smtp.gmail.com'
_smtp_port            = 465
_smtp_debug           = False

#-------------------------------------------------------------------

def setup_email(service_account_json=None, impersonated_user=None,
                delegated_credentials=None,
                from_addr='no-reply@epiphanycatholicchurch.org',
                smtp_server=None, smtp_port=None, smtp_debug=False,
                log=None):
    """Configure Gmail SMTP credentials for sending authenticated email via
    a Google Cloud service account with domain-wide delegation (DWD).

    Email is delivered over Gmail's SMTP endpoint using the XOAUTH2
    authentication mechanism.  The required DWD scope is::

        https://mail.google.com/

    This is **different** from the Gmail REST API scope
    (``https://www.googleapis.com/auth/gmail.send``).  Using the wrong scope
    produces an ``unauthorized_client`` error even with a valid key file.

    Two usage patterns are supported:

    **Pattern 1 – service account key file (most common)**::

        ECC.setup_email(
            service_account_json='service-account-key.json',
            impersonated_user='no-reply@epiphanycatholicchurch.org',
            log=log)

    The JSON key file is downloaded from the Google Cloud Console
    (IAM & Admin → Service Accounts → Keys → Add Key → JSON).
    The service account must have domain-wide delegation granted for the
    ``https://mail.google.com/`` scope in the Google Workspace Admin Console
    (Security → API controls → Manage Domain Wide Delegation).

    ``impersonated_user`` is the Google Workspace address the service account
    will act as.  It is used as the XOAUTH2 login identity and must be a real
    user in the Workspace domain (service accounts have no Gmail mailbox of
    their own).  The ``from_addr`` should match this address (or be one of its
    configured "Send As" aliases).

    **Pattern 2 – pass pre-built delegated credentials**::

        ECC.setup_email(delegated_credentials=creds,
                        impersonated_user='no-reply@…',
                        log=log)

    Useful when the calling script has already constructed a
    ``google.oauth2.service_account.Credentials`` object with the correct
    subject and scope.  ``impersonated_user`` must still be provided so that
    :func:`send_email` can supply the correct XOAUTH2 login string.

    Parameters
    ----------
    service_account_json : str or None
        Path to the service account JSON key file.  Required when
        ``delegated_credentials`` is ``None``.
    impersonated_user : str or None
        Google Workspace email address to impersonate.  Required in both
        usage patterns (used as the XOAUTH2 login identity).
    delegated_credentials : google.oauth2.service_account.Credentials or None
        Pre-built delegated credentials.  When provided,
        ``service_account_json`` is ignored.
    from_addr : str
        Default "From" address used by :func:`send_email` when no explicit
        ``from_addr`` argument is passed.  Should match ``impersonated_user``
        (or one of its "Send As" aliases).
        Default: ``'no-reply@epiphanycatholicchurch.org'``.
    smtp_server : str or None
        SMTP server hostname.  Default: ``'smtp.gmail.com'``.
    smtp_port : int or None
        SMTP port.  Default: ``465`` (SSL).  Use ``587`` for STARTTLS.
    smtp_debug : bool
        When ``True``, enable SMTP protocol-level debug output to stdout.
        Default: ``False``.
    log : logging.Logger or None
        Optional logger for debug/info messages.
    """
    global _smtp_credentials, _smtp_impersonated_user, _gmail_from_addr
    global _smtp_server, _smtp_port, _smtp_debug

    _gmail_from_addr = from_addr

    if smtp_server is not None:
        _smtp_server = smtp_server
    if smtp_port is not None:
        _smtp_port = smtp_port
    _smtp_debug = smtp_debug

    if not impersonated_user:
        diediedie("setup_email: 'impersonated_user' is required so the service "
                  "account knows which Google Workspace mailbox to send from.")
    _smtp_impersonated_user = impersonated_user

    if delegated_credentials is not None:
        # Caller already built the credentials – just store them.
        _smtp_credentials = delegated_credentials
        if log:
            log.debug("setup_email: using caller-supplied delegated credentials")
        return

    # Validate that a key file was supplied when no credentials object was given.
    if not service_account_json:
        diediedie("setup_email: 'service_account_json' is required when "
                  "'delegated_credentials' is not provided.")

    # Load the service account key and scope it to the full Gmail SMTP scope.
    # NOTE: use 'https://mail.google.com/' here, NOT the Gmail REST API scope
    # 'https://www.googleapis.com/auth/gmail.send'.  DWD in the Workspace Admin
    # Console must be granted specifically for the scope listed here.
    credentials = service_account.Credentials.from_service_account_file(
        service_account_json,
        scopes=[Google.scopes['gmail']],   # resolves to https://mail.google.com/
    )

    # Apply domain-wide delegation so every Gmail API / SMTP call is made on
    # behalf of ``impersonated_user`` rather than the service account itself.
    _smtp_credentials = credentials.with_subject(impersonated_user)

    if log:
        log.debug(f"setup_email: credentials loaded from '{service_account_json}' "
                  f"impersonating '{impersonated_user}'")

#-------------------------------------------------------------------

def _get_xoauth2_string(credentials, user_email):
    """Return a base64-encoded XOAUTH2 initial client response string.

    Refreshes ``credentials`` if the access token is absent or expired so that
    long-running scripts never fail because a token has aged out.

    The XOAUTH2 initial response format is defined in RFC 7628::

        base64("user=" + user_email + "\x01" +
               "auth=Bearer " + access_token + "\x01\x01")

    Parameters
    ----------
    credentials : google.oauth2.service_account.Credentials
        Delegated credentials scoped to ``https://mail.google.com/``.
    user_email : str
        The Google Workspace address being impersonated (used in the
        XOAUTH2 ``user=`` field).

    Returns
    -------
    str
        ASCII base64url string ready to pass to the SMTP ``AUTH XOAUTH2``
        command.
    """
    # Refresh the token if it is missing or has expired.
    # google-auth handles the JWT signing and HTTP round-trip internally.
    if not credentials.token or not credentials.valid:
        credentials.refresh(GoogleAuthRequest())

    auth_string = (f"user={user_email}\x01"
                   f"auth=Bearer {credentials.token}\x01\x01")
    return base64.b64encode(auth_string.encode('utf-8')).decode('ascii')


def _build_mime_message(message_body, content_type, to_addr, subject,
                        from_addr, log, attachments=None):
    """Build and return an :class:`email.message.EmailMessage` (RFC 2822).

    This is an internal helper consumed by
    :func:`send_email_via_smtp_xoauth2` and :func:`send_email`.  It
    assembles headers, sets the body, and adds any binary attachments.

    Parameters
    ----------
    message_body : str
        Body text (plain or HTML).
    content_type : str
        MIME type of the body, e.g. ``'text/plain'`` or ``'text/html'``.
        The subtype is extracted automatically (``'text/html'`` → ``'html'``).
    to_addr : str
        Recipient email address.
    subject : str
        Subject line.
    from_addr : str
        Sender email address.
    log : logging.Logger
        Logger for debug output.
    attachments : dict or None
        Optional dictionary of file attachments.  Each key is an arbitrary
        sort key and each value is a dict with:

        * ``'filename'`` – absolute path to the file to attach.
        * ``'type'``     – a key into :data:`Google.mime_types`
          (e.g. ``'pdf'``, ``'xlsx'``).

    Returns
    -------
    email.message.EmailMessage
    """
    msg = EmailMessage()

    # Standard RFC 2822 headers.
    msg['Subject'] = subject
    msg['From']    = from_addr
    msg['To']      = to_addr

    # Extract the MIME subtype from the content_type string.
    # Examples: 'text/html' → 'html',  'text/plain' → 'plain',
    #           'plain' → 'plain'  (bare subtype also accepted).
    subtype = content_type.split('/')[-1] if '/' in content_type else content_type
    msg.set_content(message_body, subtype=subtype)

    # Attach binary files, sorted by key for deterministic ordering.
    if attachments:
        for key in sorted(attachments.keys()):
            attachment = attachments[key]
            fname      = attachment['filename']
            ftype      = attachment['type']
            mime_full  = Google.mime_types[ftype]   # e.g. 'application/pdf'
            log.debug(f"Attaching: {fname}  ({mime_full})")
            mime_type, mime_subtype = mime_full.split('/', 1)

            with open(fname, 'rb') as ap:
                msg.add_attachment(ap.read(),
                                   maintype=mime_type,
                                   subtype=mime_subtype,
                                   filename=os.path.basename(fname))

    return msg

#-------------------------------------------------------------------

def send_email_via_smtp_xoauth2(message_body, content_type, smtp_to,
                                smtp_subject, smtp_from, credentials,
                                impersonated_user, log, attachments=None,
                                smtp_server='smtp.gmail.com', smtp_port=465,
                                smtp_debug=False):
    """Send a single email over Gmail SMTP authenticated with XOAUTH2.

    This is the low-level counterpart to the high-level :func:`send_email`
    helper.  Call this directly when the calling script manages its own
    credentials and wants to send a message without the module-level
    singleton set up by :func:`setup_email`.

    The XOAUTH2 mechanism works as follows:

    1. The service-account credentials are refreshed (if expired) to obtain
       a short-lived OAuth2 access token.
    2. The token is embedded in an XOAUTH2 initial-response string and sent
       to the SMTP server via ``AUTH XOAUTH2``.
    3. The assembled RFC 2822 message is delivered over the authenticated
       connection and the connection is cleanly closed.

    Parameters
    ----------
    message_body : str
        Body text (plain or HTML).
    content_type : str
        MIME type of the body, e.g. ``'text/plain'`` or ``'text/html'``.
    smtp_to : str
        Recipient email address.
    smtp_subject : str
        Subject line.
    smtp_from : str
        Sender address.  Must be an address the impersonated user is
        permitted to "Send As" in their Google Workspace account.
    credentials : google.oauth2.service_account.Credentials
        Delegated service-account credentials scoped to
        ``https://mail.google.com/``.
    impersonated_user : str
        Google Workspace email address being impersonated.  Used as the
        XOAUTH2 ``user=`` identity.
    log : logging.Logger
        Logger for debug/info output.
    attachments : dict or None
        Optional attachments dict (see :func:`_build_mime_message` for format).
    smtp_server : str
        SMTP server hostname.  Default: ``'smtp.gmail.com'``.
    smtp_port : int
        SMTP port.  Default: ``465`` (SSL).  Use ``587`` for STARTTLS.
    smtp_debug : bool
        Enable SMTP protocol debug output.  Default: ``False``.
    """
    # Build the RFC 2822 message.
    msg = _build_mime_message(message_body, content_type,
                              smtp_to, smtp_subject, smtp_from,
                              log, attachments)

    # Obtain (or refresh) the access token and construct the XOAUTH2 auth
    # string.  This is done immediately before opening the SMTP connection
    # so the token is as fresh as possible.
    xoauth2_b64 = _get_xoauth2_string(credentials, impersonated_user)

    # Open an SSL connection to Gmail's SMTP endpoint.
    # smtp.gmail.com:465 requires SSL from the start (SMTP_SSL).
    # For STARTTLS on port 587 use smtplib.SMTP + smtp.ehlo() / smtp.starttls().
    with smtplib.SMTP_SSL(host=smtp_server, port=smtp_port) as smtp:
        if smtp_debug:
            smtp.set_debuglevel(2)

        smtp.ehlo()

        # Authenticate using the XOAUTH2 SASL mechanism.  The server returns
        # 235 on success; any other code is an authentication failure.
        code, response = smtp.docmd('AUTH', f'XOAUTH2 {xoauth2_b64}')
        if code != 235:
            raise RuntimeError(
                f'XOAUTH2 authentication failed: {code} {response!r}')

        smtp.send_message(msg)

    log.debug(f'Mail sent to {smtp_to}, subject "{smtp_subject}"')

#-------------------------------------------------------------------

def send_email(to_addr, subject, body, log, content_type='text/plain',
               from_addr=None, attachments=None):
    """High-level helper that sends an email via Gmail SMTP + XOAUTH2.

    :func:`setup_email` must be called before this function to initialise
    the module-level service-account credentials.

    Parameters
    ----------
    to_addr : str
        Recipient email address.
    subject : str
        Subject line.
    body : str
        Message body (plain text or HTML; see ``content_type``).
    log : logging.Logger
        Logger used for informational and debug output.
    content_type : str
        MIME type of ``body``.  Common values: ``'text/plain'`` (default),
        ``'text/html'``.
    from_addr : str or None
        Sender address.  When ``None`` (the default), the address supplied
        to :func:`setup_email` via its ``from_addr`` parameter is used.
    attachments : dict or None
        Optional attachments (see :func:`_build_mime_message` for format).
    """
    global _smtp_credentials, _smtp_impersonated_user, _gmail_from_addr
    global _smtp_server, _smtp_port, _smtp_debug

    # Fall back to the module-level default sender if none supplied here.
    effective_from = from_addr if from_addr is not None else _gmail_from_addr

    log.info(f'Sending email to {to_addr}, subject "{subject}"')

    if _smtp_credentials is None:
        import traceback
        lines = ''.join(traceback.format_stack()[:-1])
        msg = (f"ECC.send_email() called before ECC.setup_email().\n"
               f"Call stack:\n{lines}\nCannot continue.  Aborting.")
        log.critical(msg)
        exit(1)

    send_email_via_smtp_xoauth2(body, content_type,
                                smtp_to=to_addr,
                                smtp_subject=subject,
                                smtp_from=effective_from,
                                credentials=_smtp_credentials,
                                impersonated_user=_smtp_impersonated_user,
                                log=log,
                                attachments=attachments,
                                smtp_server=_smtp_server,
                                smtp_port=_smtp_port,
                                smtp_debug=_smtp_debug)
