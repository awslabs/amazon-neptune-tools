#!/bin/bash -ex

pushd .
pip install virtualenv
rm -rf target
rm -rf temp
mkdir target
virtualenv temp
source temp/bin/activate
cd temp
pip install requests
cd lib/python3.8/site-packages
aws s3 cp s3://aws-neptune-customer-samples-us-east-1/neptune-sagemaker/bin/neptune-python-utils/neptune_python_utils.zip .
unzip neptune_python_utils.zip
rm -rf certifi-*
rm -rf easy_install.py
rm -rf six.py
cp -r ../../../../*.py .
zip -r stream_handler.zip *.py neptune_python_utils gremlin_python aenum isodate tornado
mv stream_handler.zip ../../../../target/stream_handler.zip
deactivate
popd
rm -rf temp