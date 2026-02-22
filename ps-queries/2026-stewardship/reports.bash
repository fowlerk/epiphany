#!/bin/bash

set -euo pipefail

# This is a convenience script to invoke the "run the reports"
# script.

start=`date`
echo "========================================================="
echo "Starting at: $start"
echo "========================================================="

additional="--all"

./nightly-reports.py \
    --debug \
    $additional \
    --ps-cache-dir ps-data \
    --service-account-json ecc-emailer-service-account.json \
    --impersonated-user no-reply@epiphanycatholicchurch.org \
    2>&1 | tee out.txt

echo "========================================================="
echo "Started at:  $start"
echo "Finished at: `date`"
echo "========================================================="
