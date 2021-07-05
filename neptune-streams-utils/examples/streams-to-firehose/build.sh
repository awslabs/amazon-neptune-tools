#!/bin/bash -ex

pushd .
pip install virtualenv
rm -rf target
rm -rf temp
mkdir target
virtualenv temp
source temp/bin/activate
cd temp
#pip install requests
cd lib/python3.8/site-packages
aws s3 cp s3://aws-neptune-customer-samples-us-east-1/neptune-sagemaker/bin/neptune-python-utils/neptune_python_utils.zip .
unzip neptune_python_utils.zip
rm -rf certifi-*
rm -rf easy_install.py
rm -rf six.py
cp -r ../../../../*.py .
zip -r neptune_firehose_handler.zip ./* -x "*pycache*" -x "*.so" -x "*dist-info*" -x "*.virtualenv" -x "pip*" -x "pkg_resources*" -x "setuptools*" -x "wheel*" -x "certifi*"
mv neptune_firehose_handler.zip ../../../../target/neptune_firehose_handler.zip
deactivate
popd
rm -rf temp