#!/bin/bash

VENV=.venv
PYTHON=${VENV}/bin/python
PIP=${VENV}/bin/pip

if [ -z "$1" ]; then
    echo "---------------HELP-----------------"
    echo "To setup the project: bash bootstrap.sh --setup"
    echo "To test the project type make test: bash bootstrap.sh --test"
    echo "To run the project type make run: bash bootstrap.sh --run"
    echo "------------------------------------"
fi

if [ "$1" == "--setup" ]; then
    echo "Checking if project files are generated..."
    [ -d data ] || (
        echo "No data directory found, creating ..."
        mkdir data && cd data && mkdir enterprise_ids && mkdir processed_enterprise && raw_enterprise
    )
    [ -d assets ] || (echo "No assets directory found, creating ..." && mkdir assets)
    [ -d .venv ] || (echo "No virtual environment found, creating ..." && python3.9 -m venv ${VENV})
    source ${VENV}/bin/activate && ${PYTHON} -m pip install --upgrade pip && ${PIP} install -r requirements.txt
    playwright install
fi

if [ "$1" == "--test" ]; then
    echo "Tests not implemented"
fi

if [ "$1" == "--run" ]; then
    ${PYTHON} -m fgscraper
fi
