#!/bin/bash

mvn install -DskipTests
mvn exec:java -Dexec.mainClass="org.rcsb.common.dataframes.CreateHumanHomologuesParquetFile"

