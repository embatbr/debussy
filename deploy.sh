#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}


VERSION="$(cat version)"
echo version ${VERSION}


TARGET_PROJECT="$1"


rm -Rf ${PROJECT_ROOT_PATH}/../${TARGET_PROJECT}/composer/dags/debussy
cp -R debussy ${PROJECT_ROOT_PATH}/../${TARGET_PROJECT}/composer/dags


# python setup.py bdist_egg
# cp dist/debussy-${VERSION}-py3.6.egg ../${TARGET_PROJECT}/composer/dags/debussy.egg
