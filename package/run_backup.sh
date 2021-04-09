#!/bin/bash
export AWS_ACCESS_KEY_ID='your_access_key'
export AWS_SECRET_ACCESS_KEY='your_secret_key'

PDIP='10.0.42.31'
BIN_PATH='/usr/bin'
S3_ENDPOINT='http://s3.test.com'
S3_BUCKET='backup_bucket'
DATE_PREFIX=`date '+%Y%m%d%H%M%S'`
LOG_PATH='/var/log/br-backup.log'
STORAGE_PATH="s3://${S3_BUCKET}/${DATE_PREFIX}"
BACKUP_CRON="0 0 * * * *"

echo ${STORAGE_PATH}

${BIN_PATH}/br backup txn \
  --pd "${PDIP}:2379" \
  -s "${STORAGE_PATH}" \
  --s3.endpoint "${S3_ENDPOINT}" \
  --send-credentials-to-tikv=true \
  --log-file ${LOG_PATH} \
  --cron "$BACKUP_CRON"