#!/bin/bash

VENV=.venv
PYTHON=${VENV}/bin/python
PIP=${VENV}/bin/pip

if [ -z "$1" ]; then
    echo "---------------HELP-----------------"
    echo "To setup the project: sudo bash bootstrap.sh --setup"
    echo "To test the project type make test: sudo bash bootstrap.sh --test"
    echo "To run the project: sudo bash bootstrap.sh --run"
    echo "To run just the post-processing project: sudo bash bootstrap.sh --run-post-processing"
    echo "------------------------------------"
fi

if [ "$1" == "--setup" ]; then
    echo "Checking if project files are generated..."
    [ -d data ] || (
        echo "No data directory found, creating ..."
        mkdir data && cd data && mkdir enterprise_ids && mkdir processed_enterprise && mkdir raw_enterprise
    )
    [ -d assets ] || (echo "No assets directory found, creating ..." && mkdir assets)
    [ -d .venv ] || (echo "No virtual environment found, creating ..." && python3.9 -m venv ${VENV})
    source ${VENV}/bin/activate && ${PYTHON} -m pip install --upgrade pip && ${PIP} install -r requirements.txt
    source ${VENV}/bin/activate && playwright install
    chmod 777 -R data && chmod 777 -R assets
fi

if [ "$1" == "--test" ]; then
    echo "Tests not implemented"
fi

if [ "$1" == "--run" ]; then
    ${PYTHON} -m fgscraper --run
fi

if [ "$1" == '--run-post-processing' ]; then
    ${PYTHON} -m fgscraper --post-processing
fi
