#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}


TARGET_PROJECT="$1"
if [ "${TARGET_PROJECT}" == "dotzpay" ]
then
    TARGET_PATH="${TARGET_PROJECT}/composer"
elif [ "${TARGET_PROJECT}" == "dotz-workflows" ]
then
    TARGET_PATH="${TARGET_PROJECT}"
fi


rm -Rf ${PROJECT_ROOT_PATH}/../${TARGET_PATH}/dags/debussy
cp -R debussy ${PROJECT_ROOT_PATH}/../${TARGET_PATH}/dags


# python setup.py bdist_egg
# cp dist/debussy-1.0.0-py3.6.egg ../${TARGET_PATH}/dags/debussy.egg
