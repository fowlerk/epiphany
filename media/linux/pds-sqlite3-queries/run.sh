#!/bin/zsh

set -x

base=/home/itadmin/git/epiphany/media/linux
prog_dir=$base/pds-sqlite3-queries
logfile=$base/logfile.txt
sqlite_dir=$base/pds-data

cd $prog_dir

# Generate the list of email addresses from PDS data
./pds-sqlite3-queries.py \
    --sqlite3-db=$sqlite_dir/pdschurch.sqlite3 \
    --logfile=$logfile \
    --verbose

# This generated mailman-parishioner.txt.
# Copy this file up to the mailman server.
file=mailman-parishioner.txt
scp $file jeff@lists.epiphanycatholicchurch.org:ecc
# Now update the list
ssh jeff@lists.epiphanycatholicchurch.org ecc/replace-parishioners.sh ecc/$file