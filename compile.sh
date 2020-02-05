#!/bin/bash

mvn package
cd jmetalsp-externalsource/
docker build --no-cache -f docker/Dockerfile -t traffic-ingestion:v1 .
