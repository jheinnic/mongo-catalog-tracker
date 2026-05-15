#!/bin/bash

set -x
set -e

script_dir="$( /usr/bin/dirname "${0}" )"
bucket_name="${1}"
path_prefix="${2}"

epoch_seconds="$( /bin/date +%s )"
day_num="$(( epoch_seconds / 86400 ))"
day_second="$(( epoch_seconds % day_num ))"
tos3="$( /bin/mktemp )"
/bin/rm "${tos3}"
/usr/bin/mkfifo "${tos3}"

AWS_DEFAULT_REGION=us-east-1
export AWS_DEFAULT_REGION

instance_id="$(cloud-init query instance-id)"
instance_name="$(aws ec2 describe-instances --instance-id "${instance_id}" | jq -r '.Reservations[0].Instances[0].Tags[] | select(.Key == "Name") | .Value')"

/bin/cat "${tos3}" | aws s3 cp - "s3://${bucket_name}/${path_prefix}/${instance_name}/${day_num}_${day_second}/idx.log" &
echo "s3://${bucket_name}/${path_prefix}/${instance_name}/${day_num}_${day_second}/idx.log"
"${script_dir}/keyhole" -index mongodb://127.0.0.1:27017/Collections 2>&1 | tee "${tos3}"
echo "Fin"
