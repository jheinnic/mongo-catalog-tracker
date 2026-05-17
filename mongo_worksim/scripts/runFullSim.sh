#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"
mongoDBUri="${1}"
simRoot="${2}"
dataLakeFormat="${3}"
dataLakeRoot="${4}"

if ! which keyhole >/dev/null 2>/dev/null
then
	echo "keyhole executable must be on your path"
	exit -2
fi

if [[ ! -d "${simRoot}" ]]
then
	echo "simRoot, ${simRoot}, must be a directory" 
	exit -3
fi
if [[ -e "${simRoot}/generatedPlan" ]]
then
	echo "${simRoot}/generatedPlan already exists"
	exit -7
fi
mkdir "${simRoot}/generatedPlan"

if [[ ! "x${dataLakeFormat}" != 'xorc' ]] && [[ "x${dataLakeFormat}" != 'xparquet' ]]
then
	echo "dataLakeFormat, ${dataLakeFormat}, must be orc or parquet"
	exit -4
fi
if [[ ! -d "${dataLakeRoot}" ]]
then
	mkdir -p "${dataLakeRoot}/${dataLakeFormat}"
	if [[ ! -d "${dataLakeRoot}" ]]
	then
		echo "dataLakeRoot, ${dataLakeRoot}, must be a directory or permit creation as a directory"
		exit -5
	fi
fi
if [[ ! -d "${dataLakeRoot}/${dataLakeFormat}" ]]
then
	mkdir "${dataLakeRoot}/${dataLakeFormat}"
	if [[ ! -d "${dataLakeRoot}/${dataLakeFormat}" ]]
	then
		echo "dataLakeFormat, ${dataLakeFormat}, must be a directory under dataLakeRoot, ${dataLakeRoot}, or permit creation as a subdirectory"
		exit -5
	fi
fi

# . "${scriptDir}/../.venv/bin/activate"
"${scriptDir}/../main.py" \
    --mongo-uri   "${mongoDBUri}" \
    --keyhole-url "${mongoDBUri}" \
    --names-file  "${simRoot}/names-file.txt" \
    --config      "${simRoot}/sim_params.yaml" \
    "${simRoot}/generatedPlan" \
    "${dataLakeRoot}/${dataLakeFormat}"
