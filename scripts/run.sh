#!/bin/bash

mvn install -DskipTests
export MAVEN_OPTS="-Xmx14G"
mvn exec:java -Dexec.mainClass="org.rcsb.geneprot.common.dataframes.CreateHumanHomologuesParquetFile"


