#!/bin/bash

# Copyright 2020 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
VERSION_FILE="VERSION"
GIT_CMD=`which git`

GIT_BRANCH=`${GIT_CMD} branch | grep \* | cut -d' ' -f2`

if [ ! -f "$VERSION_FILE" ] ; then
	echo "Version file not present. Please create and retry."
	exit 1
fi

if [ "$GIT_BRANCH" != "master" ] ; then
	echo "WARNING: Starting a release from a non-master branch."
fi

let NEW_VERSION=`cat $VERSION_FILE`+1
VERSION_STRING="1.${NEW_VERSION}"
echo $NEW_VERSION > $VERSION_FILE


echo "Starting release for $VERSION_STRING"

RELEASE_BRANCH="amazon-neptune-tools-$VERSION_STRING"


echo "Creating new release branch:  $RELEASE_BRANCH"

$GIT_CMD checkout -b $RELEASE_BRANCH

#Utility script to build the jars to make a release.
ARTIFACT_DIR=`pwd`/artifacts
rm -rf $ARTIFACT_DIR
mkdir -p $ARTIFACT_DIR
MAVEN_ARTIFACTS="neo4j-to-neptune neptune-export neptune-gremlin-client"
for artifact in $MAVEN_ARTIFACTS; do
	pushd $artifact >& /dev/null
	mvn versions:set -DnewVersion=${VERSION_STRING} versions:update-child-modules
	mvn clean
	mvn install
	#All of the jars are shaded. Only take the shaded, bundled jars.
	for jar in `find . -name "*.jar" -print | grep -vE "SNAPSHOT|original|\-$VERSION_STRING"`; do
		cp $jar $ARTIFACT_DIR
	done
	popd >& /dev/null
done

#Also get the non-shaded gremlin-client jar
cp "neptune-gremlin-client/gremlin-client/target/gremlin-client-$VERSION_STRING.jar" $ARTIFACT_DIR

#Build the neptune-python-utils artifact
pushd neptune-python-utils >& /dev/null
./build.sh
cp target/neptune_python_utils.zip $ARTIFACT_DIR
popd >& /dev/null

cp ./graphml2csv/graphml2csv.py $ARTIFACT_DIR

#drop-graph needs to be installed as a module
#cp ./drop-graph/drop-graph.py $ARTIFACT_DIR

${GIT_CMD} commit -a -m "POM version updates for $RELEASE_BRANCH"

echo "Creating Release Tag"

${GIT_CMD} tag -a $RELEASE_BRANCH  -m "amazon-neptune-tools Release ${VERSION_STRING}"

repo=origin

echo "Pushing the release branch to $repo."
${GIT_CMD} push "${repo}" refs/heads/${RELEASE_BRANCH}

echo "Pushing the release tags to $repo."
${GIT_CMD} push "${repo}" refs/tags/${RELEASE_BRANCH}

#Update the VERSION on master

${GIT_CMD} checkout master
echo $NEW_VERSION > $VERSION_FILE
${GIT_CMD} pull $repo master
${GIT_CMD} commit -am "Incremented release version to `cat $VERSION_FILE`"
${GIT_CMD} push $repo master

#Return to the initial branch
${GIT_CMD} checkout ${GIT_BRANCH}

echo "To complete the release, upload the contents of artifacts/ to the ${RELEASE_BRANCH} tag on github: https://github.com/awslabs/amazon-neptune-tools/releases."
