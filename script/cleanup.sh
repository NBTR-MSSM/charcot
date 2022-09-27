#!/bin/bash

#
# Helper script to remove S3 buckets and DynamoDB tables, so that  SST does not faile
# with "resource already exists" message when we go to redeploy
#
stage=$1
paid_account_profile=$2
odp_account_profile=$3

if [ $stage == 'prod' ]
  then
    exit 1, "You specified the 'prod' environment, are you crazy?!??!!??"
fi

for cmd in "env AWS_PROFILE=$odp_account_profile aws s3 rb s3://nbtr-production-$stage" "env AWS_PROFILE=$odp_account_profile aws s3 rm --recursive s3://cerebrum-image-zip-$stage" "env AWS_PROFILE=$odp_account_profile aws s3 rb s3://cerebrum-image-zip-$stage" "env AWS_PROFILE=$paid_account_profile aws s3 rb s3://nbtr-odp-staging-$stage" "env AWS_PROFILE=$paid_account_profile aws dynamodb delete-table --table-name $stage-charcot-cerebrum-image-metadata" "env AWS_PROFILE=$paid_account_profile aws dynamodb delete-table --table-name $stage-charcot-cerebrum-image-order"
do
  echo "Executing $cmd"
  $cmd
done
