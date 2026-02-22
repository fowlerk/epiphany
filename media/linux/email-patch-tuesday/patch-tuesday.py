#!/usr/bin/env python3

#
# Send a simple automated email.
#
# This script presumes that smtp-relay.gmail.com has been setup to
# accept emails from this public IP address without authentication.
# To set this up:
#
# 1. Login to admin.google.com as an administrator.
# 2. Apps
# 3. G Suite
# 4. Gmail
# 5. Advanced settings (at the bottom of the list)
# 6. Scroll down to Routing and find SMTP relay service
# 7. Add the desired IP address:
#    - require TLS encryption: yes
#    - require SMTP authentication: no
#    - allowed senders: Only addresses in my domains
#
# And then make sure that the sender is actually a valid email address
# in the ECC Google domain.
#

import os
import sys
import argparse

# We assume that there is a "ecc-python-modules" sym link in this
# directory that points to the directory with ECC.py and friends.
moddir = os.path.join(os.getcwd(), 'ecc-python-modules')
if not os.path.exists(moddir):
    print("ERROR: Could not find the ecc-python-modules directory.")
    print("ERROR: Please make a ecc-python-modules sym link and run again.")
    exit(1)
# On MS Windows, git checks out sym links as a file with a single-line
# string containing the name of the file that the sym link points to.
if os.path.isfile(moddir):
    with open(moddir) as fp:
        dir = fp.readlines()
    moddir = os.path.join(os.getcwd(), dir[0])

sys.path.insert(0, moddir)

import ECC

smtp_from   = 'Epiphany reminder <no-reply@epiphanycatholicchurch.org>'
smtp_to     = 'staff@epiphanycatholicchurch.org,itadmin@epiphanycatholicchurch.org'
# JMS DEBUG OVERRIDE
smtp_to     = 'jeff@squyres.com'
subject     = 'Epiphany patch Tuesday reminder'

parser = argparse.ArgumentParser(description='Patch Tuesday email sender')
parser.add_argument('--service-account-json',
                    default='ecc-emailer-service-account.json',
                    help='File containing the Google service account JSON key')
parser.add_argument('--impersonated-user',
                    default='no-reply@epiphanycatholicchurch.org',
                    help='Google Workspace user to impersonate via DWD')
args = parser.parse_args()

log = ECC.setup_logging(info=True, debug=False)

ECC.setup_email(service_account_json=args.service_account_json,
                impersonated_user=args.impersonated_user,
                from_addr=smtp_from,
                log=log)

body        = '''<h1>REMINDER!</h1>

<p>The Tech committee needs to run updates on your computer this evening.
Please:</p>

<ol>
<li> Leave your computer powered on tonight.</li>
<li> Ensure that the computer is connected to AC power.</li>
<li> Ensure that the computer is connected to the internet.</li>
</ol>

<p>You can still logout of your computer when you are finished; the Tech
Committee just needs the machine powered on, connected to AC power, and
connected to the internet.</p>

<p>If you cannot leave your computer on tonight, please let the Tech
Committee know.  Thanks.</p>

<p>Your friendly server,<br />
Myrador</p>'''

#------------------------------------------------------------------

ECC.send_email(to_addr=smtp_to,
               subject=subject,
               body=body,
               log=log,
               content_type='text/html',
               from_addr=smtp_from)
