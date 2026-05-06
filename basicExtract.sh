#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"

if [[ -d "${1}" ]]
then
	cd "${1}"
fi
if [[ ! -f idx.log ]]
then
	echo "No idx.log file in $(pwd), exitting"
	exit -2
fi

if [[ -r trimmedCleanedSifted.dat ]]
then
   rm trimmedCleanedSifted.dat
fi
mkfifo trimmedCleanedSifted.dat
cat idx.log | sed 's/^[^{]\+{/{/' | sed 's/}[^}]\+$/}/' | grep -v '^$' | grep -v 'R simagix/keyhole' | grep -v 'is larger than the max int32' | grep -v 'I GetIndexes ends' > trimmedCleanedSifted.dat &

# rm -rf records
# mkdir -p records

"${scriptDir}/rawToCsv.pl" trimmedCleanedSifted.dat allCollectionIndices.csv

