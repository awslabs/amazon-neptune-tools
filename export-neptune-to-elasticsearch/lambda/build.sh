#!/bin/sh

pip install virtualenv
rm -rf target
rm -rf temp
mkdir target
virtualenv temp --python=python3.8
source temp/bin/activate
pip install -r requirements.txt
cd temp/lib/python3.8/site-packages
cp -r ../../../../*.py .
zip -r ../../../../target/export-neptune-to-elasticsearch.zip ./*
deactivate
cd ../../../../
rm -rf temp