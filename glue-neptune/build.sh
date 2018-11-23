#!/bin/bash -ex

pushd .
pip install virtualenv
rm -rf target
rm -rf temp
mkdir target
virtualenv temp --python=python2.7
source temp/bin/activate
cd temp
pip install gremlinpython
cd lib/python2.7/site-packages
rm -rf certifi
rm -rf certifi-*
cp -r ../../../../glue_neptune .
zip -r glue_neptune.zip *
mv glue_neptune.zip ../../../../target/glue_neptune.zip
deactivate
popd
rm -rf temp