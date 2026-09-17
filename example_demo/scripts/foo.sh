#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"
fileFormat="${1}"

if [[ "x${fileFormat}" != 'xparquet' ]] && [[ "x${fileFormat}" != 'xorc' ]]
then
        echo "First argument must be either `parquet` or `orc`"
        exit -2
fi

sourceBasePath="/home/ionadmin/Git/mongo-catalog-tracker/mongo_worksim/my_outdir"

