#!/bin/bash
export AWS_ACCESS_KEY_ID='your_access_key'
export AWS_SECRET_ACCESS_KEY='your_secret_key'

PDIP='10.0.42.31'
BIN_PATH='/usr/bin'
S3_ENDPOINT='http://s3.test.com'
RESTORE_PATH='s3://{$your_bucket}/{$prefix}/'
LOG_PATH='/var/log/br-restore.log'

${BIN_PATH}/br restore txn \
    --pd "${PDIP}:2379" \
    -s "${RESTORE_PATH}" \
    --no-schema \
    --s3.endpoint "${S3_ENDPOINT}" \
    --send-credentials-to-tikv=true \
    --log-file ${LOG_PATH}
