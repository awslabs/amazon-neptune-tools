#!/bin/bash -e

jar=$(find . -name neo4j-to-neptune.jar -print -quit)
java -jar ${jar} "$@"
