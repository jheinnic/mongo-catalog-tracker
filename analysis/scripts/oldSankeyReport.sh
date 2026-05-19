#!/bin/bash

set -e

scriptDir="$(dirname "$(realpath "${0}")")"
. "${scriptDir}/scriptArgs.sh"

warehouseRoot=$(getWarehouseRoot)
warehouseFormat=$(getWarehouseFormat)

tmpFile="$(mktemp)"

queryActivitySankey --warehouseRoot "${warehouseRoot}" --warehouseFormat "${warehouseFormat}" \
 | grep '^|' \
 | sed 's/^|//' \
 | sed 's/\s\+|$//' \
 | grep -v sankey_row > "${tmpFile}"

mkdir parts

for pre in Active Idle Restorable
do
        for post in Deleted Active Idle Restorable Inactive
        do
                egrep "^${pre}" "${tmpFile}" | egrep "] ${post}" > "parts/${pre}_${post}"
        done
done

for pre in Deleted Inactive
do
        egrep "^${pre}" "${tmpFile}" | egrep "] ${pre}" > "parts/${pre}_${pre}"
done

egrep "^Not" "${tmpFile}" > parts/Not

cd parts
# cat Deleted_Deleted Active_Deleted Active_Active Restorable_Active Restorable_Restorable Active_Idle Idle_Restorable Idle_Active Idle_Idle Idle_Inactive Inactive_Inactive > ../fixed_${file}
cat Not Active_Deleted Active_Active Restorable_Active Restorable_Restorable Active_Idle Idle_Restorable Idle_Active Idle_Idle Idle_Inactive > "${tmpFile}_fixed"
cd ..

cat "${tmpFile}_fixed"
echo
echo "${tmpFile}"
