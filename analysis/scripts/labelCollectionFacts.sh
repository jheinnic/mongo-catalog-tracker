#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"
. "${scriptDir}/scriptArgs.sh"

warehouseRoot=$(getWarehouseRoot)
warehouseFormat=$(getWarehouseFormat)

labelByActivity --warehouseRoot "${warehouseRoot}" --warehouseFormat "${warehouseFormat}" 
