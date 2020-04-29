#!/bin/bash -ex

pushd .
pip install virtualenv
rm -rf target
rm -rf temp
mkdir target
virtualenv temp --python=python3.8
source temp/bin/activate
cd temp
pip install gremlinpython
pip install requests
pip install backoff
cd lib/python3.8/site-packages
rm -rf certifi-*
rm -rf easy_install.py
rm -rf six.py
cp -r ../../../../*.py .
cp -r ../../../../neptune_python_utils .
zip -r neptune_python_utils.zip neptune_python_utils gremlin_python aenum isodate tornado backoff
mv neptune_python_utils.zip ../../../../target/neptune_python_utils.zip
deactivate
popd
rm -rf temp