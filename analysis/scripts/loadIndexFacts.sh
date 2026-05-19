#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"
. "${scriptDir}/scriptArgs.sh"

dataLakeRoot=$(getDataLakeRoot)
warehouseRoot=$(getWarehouseRoot)
warehouseFormat=$(getWarehouseFormat)

loadData --dataLakeRoot "${dataLakeRoot}" --warehouseRoot "${warehouseRoot}" --warehouseFormat "${warehouseFormat}"  \
&& deriveRanges --warehouseRoot "${warehouseRoot}" --warehouseFormat "${warehouseFormat}"  \
