#!/bin/bash -ex

pip install virtualenv
rm -rf target
rm -rf temp
mkdir target
virtualenv temp --python=python3.9
source temp/bin/activate
pushd temp
cd lib/python3.9/site-packages
rm -rf certifi-*
rm -rf easy_install.py
cp -r ../../../../*.py .
zip -r dynamic_custom_endpoints.zip *.py -x "_virtualenv.py"
mv dynamic_custom_endpoints.zip ../../../../../target/dynamic_custom_endpoints.zip
deactivate
popd
rm -rf temp



