#!/bin/bash

# print command
set -ex

# colours
export TERM=xterm-256color

DATE=`/bin/date +%Y-%m-%d-%H:%M:%S`

if [ "$1" == "pygyver-tests" ]; then
    echo "Starting pygyver-tests entrypoint"
    pytest --junitxml='test_output/test-out-pygyver-tests.xml' -sv || [[ $? -eq 1 ]]
    echo "Finished pygyver-tests entrypoint."
    exit $?
fi

if [ "$1" == "local" ]; then
    echo "Starting pygyver-tests-local entrypoint"
    pytest tests/test_gooddata.py --junitxml='test_output/test-out-pygyver-tests.xml' -sv || [[ $? -eq 1 ]]
    echo "Finished pygyver-tests-local entrypoint."
    exit $?
fi
