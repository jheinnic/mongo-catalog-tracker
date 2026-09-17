#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"
mongoDBUri="${1}"
simRoot="${2}"
dataLakeRoot="${3}"

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
# if [[ -e "${simRoot}/generatedPlan" ]]
# then
# 	echo "${simRoot}/generatedPlan already exists"
# 	exit -7
# fi
mkdir "${simRoot}/generatedPlan"

if [[ ! -d "${dataLakeRoot}" ]]
then
	mkdir -p "${dataLakeRoot}"
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

mongoWorkSim \
    --mongo-uri   "${mongoDBUri}" \
    --keyhole-url "${mongoDBUri}" \
    --names-file  "${simRoot}/names-file.txt" \
    --config      "${simRoot}/sim_params.yaml" \
    --skip-bootstrap --seed 12345 \
    --start-interval 36 \
    "${simRoot}/generatedPlan" \
    "${dataLakeRoot}/${dataLakeFormat}"
