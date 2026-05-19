#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"
. "${scriptDir}/scriptArgs.sh"

dataLakeRoot=$(getDataLakeRoot)

echo "## Transforming keyhole index reports under <${dataLakeRoot}> to CSV for ingest"
echo ""
for nextSample in `/usr/bin/ls -1 "${dataLakeRoot}"`
do
	cd "${dataLakeRoot}/${nextSample}"
	"${scriptDir}/basicExtract.sh"
	echo "-- Extracted CSV from <${dataLakeRoot}/${nextSample}>"
	cd -
done