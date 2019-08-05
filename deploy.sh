#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}


rm -Rf ${PROJECT_ROOT_PATH}/../dotzpay/composer/dags/debussy
cp -R debussy ${PROJECT_ROOT_PATH}/../dotzpay/composer/dags


# python setup.py bdist_egg
# cp dist/debussy-1.0.0-py3.6.egg ../dotzpay/composer/dags/debussy.egg
