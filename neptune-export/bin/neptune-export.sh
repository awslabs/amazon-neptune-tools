#!/bin/bash -e

jar=$(find . -name neptune-export.jar)
java -jar ${jar} "$@"