#!/bin/bash -ex

rm -rf target
mkdir target

pushd dynamic-custom-endpoints
sh build.sh
popd




