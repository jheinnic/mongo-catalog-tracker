#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"


"${scriptDir}/parseDailyReports.sh"
&& "${scriptDir}/loadIndexFacts.sh"
&& "${scriptDir}/aggregateCollectionFacts.sh"
&& "${scriptDir}/labelCollectionFacts.sh"
&& "${scriptDir}/applyInactivityPolicy.sh"
&& "${scriptDir}/altSankeyReport.sh"
