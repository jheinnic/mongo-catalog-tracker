#!/bin/bash

set -e

scriptDir="$(dirname "$(realpath "${0}")")"
. "${scriptDir}/scriptArgs.sh"

warehouseRoot=$(getWarehouseRoot)
warehouseFormat=$(getWarehouseFormat)

tmpFile="$(mktemp)"
tmpFile2="$(mktemp)"

queryActivitySankey --warehouseRoot "${warehouseRoot}" --warehouseFormat "${warehouseFormat}" \
 | grep '^|' \
 | sed 's/^|//' \
 | sed 's/\s\+|$//' \
 | grep -v sankey_row > "${tmpFile}"

set -x

cat "${tmpFile}" \
 | egrep -v '] Not' \
 | egrep -v '^Delete' \
 | egrep -v '^Inacti' \
 | sed 's/NotLoaded/NL/' \
 | sed 's/NotCreated/NC/' \
 | sed 's/Active/A/g' \
 | sed 's/Idle/I/g' \
 | sed 's/Restorable/R/g' \
 | sed 's/\(^.*\(Deleted\|Inactive\)\).*/\1/' > "${tmpFile2}"

echo "${tmpFile}" "${tmpFile2}"
