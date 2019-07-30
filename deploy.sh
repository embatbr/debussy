#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}


cp -R debussy ${PROJECT_ROOT_PATH}/../dotzpay/composer/dags


# python setup.py bdist_egg
# cp dist/debussy-0.2.0-py3.6.egg ../dotzpay/composer/dags/debussy.egg
