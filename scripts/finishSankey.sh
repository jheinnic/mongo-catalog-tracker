#!/bin/bash

set -e

scriptDir="$(dirname "$(realpath "${0}")")"
fileFormat="${1}"

if [[ "x${fileFormat}" != 'xparquet' ]] && [[ "x${fileFormat}" != 'xorc' ]]
then
        echo "First argument must be either `parquet` or `orc`"
        exit -2
fi

. "${scriptDir}/../.venv/bin/activate"
python3 "${scriptDir}/../${fileFormat}/queryActivitySankey.py" | grep '^|' | sed 's/^|//' | sed 's/\s\+|$//' | grep -v Not | grep -v samkey_row | sort
