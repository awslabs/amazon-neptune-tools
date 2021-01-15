#!/bin/bash -ex

pushd .
rm -rf neptune-streams-layer.zip
rm -rf lib
aws s3 cp s3://aws-neptune-customer-samples/neptune-stream/lambda/java8/neptune-streams-layer.zip .
unzip neptune-streams-layer.zip
mv java/lib/ .
rm -rf java
rm -rf neptune-streams-layer.zip
mvn install:install-file -Dfile=lib/amazon-neptune-streams-replicator-core-1.0.0.jar \
  -DgroupId=com.amazonaws \
  -DartifactId=amazon-neptune-streams-replicator-core \
  -Dversion=1.0.0 \
  -Dpackaging=jar
mvn install:install-file -Dfile=lib/amazon-neptune-streams-replicator-lambda-1.0.0.jar \
  -DgroupId=com.amazonaws \
  -DartifactId=amazon-neptune-streams-replicator-lambda \
  -Dversion=1.0.0 \
  -Dpackaging=jar
popd
