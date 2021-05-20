#!/bin/bash -ex

pushd .
sudo pip install virtualenv
rm -rf target
rm -rf temp
mkdir target
virtualenv temp --python=python3.8
source temp/bin/activate
cd temp
pip install gremlinpython
pip install requests
pip install backoff
pip install cchardet
pip install aiodns
pip install idna-ssl
cd lib/python3.8/site-packages
rm -rf certifi-*
rm -rf easy_install.py
rm -rf six.py
cp -r ../../../../neptune_python_utils .
zip -r neptune_python_utils.zip ./* -x "*pycache*" -x "*.so" -x "*dist-info*" -x "*.virtualenv" -x "pip*" -x "pkg_resources*" -x "setuptools*" -x "wheel*" -x "certifi*"
mv neptune_python_utils.zip ../../../../target/neptune_python_utils.zip
deactivate
popd
rm -rf temp