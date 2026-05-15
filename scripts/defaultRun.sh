#!/bin/bash

scriptDir="$(dirname "$(realpath "${0}")")"

. "${scriptDir}/../mongo_worksim/.venv/bin/activate"
python "${scriptDir}/../mongo_worksim/main.py" \
    --mongo-uri   "mongodb://localhost:27017" \
    --keyhole-url "mongodb://localhost:27017" \
    --names-file  "${scriptDir}/../mongo_worksim/names-file.txt" \
    --db-name     loadtest \
    --config      "${scriptDir}/../mongo_worksim/sim_params.yaml" \
    "${scriptDir}/../mongo_worksim/my_plan" \
    "${scriptDir}/../mongo_worksim/my_output"
