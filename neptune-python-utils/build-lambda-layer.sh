#!/bin/bash -ex

pushd .
pip install virtualenv
rm -rf target
rm -rf temp
mkdir target
virtualenv temp --python=python3.8
source temp/bin/activate
cd temp
pip install gremlinpython==3.5.1
pip install requests
pip install backoff
pip install cchardet
pip install aiodns
pip install idna-ssl
pushd lib/python3.8/site-packages
#rm -rf certifi-*
rm -rf easy_install.py
rm -rf six.py
cp -r ../../../../neptune_python_utils .
popd
mkdir python
mv lib python/lib
zip -r neptune_python_utils_lambda_layer.zip python \
	-x "*pycache*" \
	-x "*.so" \
	-x "*dist-info*" \
	-x "*.virtualenv" \
	-x "*/pip*" \
	-x "*/pkg_resources*" \
	-x "*/setuptools*" \
	-x "*/wheel*" \
	-x "*distutils*" \
	-x "*/_virtualenv.*" \
	#-x "*/certifi*"
deactivate
popd
mv temp/neptune_python_utils_lambda_layer.zip target/neptune_python_utils_lambda_layer.zip
rm -rf temp