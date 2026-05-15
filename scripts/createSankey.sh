#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"
fileFormat="${1}"

if [[ "x${fileFormat}" != 'xparquet' ]] && [[ "x${fileFormat}" != 'xorc' ]]
then
	echo "First argument must be either `parquet` or `orc`"
	exit -2
fi

sourceBasePath="/home/ionadmin/Git/mongo-catalog-tracker/mongo_worksim/my_outdir"

for nextSample in `/usr/bin/ls -d "${sourceBasePath}"/*`
do
	cd "${sourceBasePath}/${nextSample}"
	"${scriptDir}/basicExtract.sh"
done
cd "${scriptDir}"

. "${scriptDir}/../.venv/bin/activate"
python3 "${scriptDir}/../${fileFormat}/loadData.py" \
&& python3 "${scriptDir}/../${fileFormat}/redoAgg.py" \
&& python3 "${scriptDir}/../${fileFormat}/deriveRanges.py" \
&& python3 "${scriptDir}/../${fileFormat}/labelByActivity.py" \
&& python3 "${scriptDir}/../${fileFormat}/labelInactivePolicy.py" \
&& "${scriptDir}/finishSankey.sh" "${fileFormat}"
