#!/bin/bash

stage='debug'
if [ "$1" != "" ]
  then
    stage=$1
fi

echo 'Stopping...'
docker container stop charcot-fulfillment
echo 'Removing...'
docker container rm charcot-fulfillment
cd ~/git/charcot/fulfillment
echo 'Maven build...'
mvn -U clean install
echo "Building docker image with stage '$stage'..."
$(echo "docker build --tag charcot-fulfillment --build-arg STAGE=$stage .")
echo 'Running...'
docker run --rm -dit -v ~/.aws:/root/.aws:ro -e IS_LOCAL='true' -p 80:80 --name charcot-fulfillment charcot-fulfillment:latest
#docker run --rm -dit -v ~/.aws:/root/.aws:ro --name charcot-fulfillment 045387143127.dkr.ecr.us-east-1.amazonaws.com/cdk-hnb659fds-container-assets-045387143127-us-east-1:a68cb6ca8301b855d7a15311336d1f0726ff0c6ee5f1289d2b5563a471f88d40
echo 'Enjoy, have a nice day!'
