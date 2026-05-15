#!/bin/sh

python main.py \
    --mongo-uri   "mongodb://localhost:27017" \
    --keyhole-url "mongodb://localhost:27017" \
    --names-file  names-file.txt \
    --db-name     loadtest \
    --config      ./sim_params.yaml \
    ./my_plan \
    ./my_output \
