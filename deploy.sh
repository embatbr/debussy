#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $PROJECT_ROOT_PATH


# TARGET_PROJECT="$1"
TARGET_PROJECT="dotzpay"


python setup.py bdist_egg
cp dist/debussy-0.1.0-py3.6.egg ../${TARGET_PROJECT}/composer/dags/debussy.egg
