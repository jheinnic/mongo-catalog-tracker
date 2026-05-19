#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"
. "${scriptDir}/scriptArgs.sh"

warehouseRoot=$(getWarehouseRoot)
warehouseFormat=$(getWarehouseFormat)
daysBeforeInactive=$(getDaysBeforeInactive)

labelInactivePolicy --warehouseRoot "${warehouseRoot}" --warehouseFormat "${warehouseFormat}" --nInactivity "${daysBeforeInactive}"
