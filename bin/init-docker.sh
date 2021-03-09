#!/bin/bash
# Config git for LF endlines
IMAGE_NAME="spark-centos-land-analyzer"
CONTAINER_NAME="land-analyzer"

git config --global core.eol lf
git config --global core.autocrlf input

echo "Checking if ${IMAGE_NAME} image exists locally..."
if [[ "$(docker images -q ${IMAGE_NAME} 2> /dev/null)" == "" ]]; then
  echo "No ${IMAGE_NAME} image found, building from Dockerfile..."
  docker build -t ${IMAGE_NAME} .
fi

echo "Starting docker ${IMAGE_NAME}..."
docker run --rm -dit -v `pwd -W`:/home/developer/land_analyzer -p 14040:4040 --name ${CONTAINER_NAME} ${IMAGE_NAME}
echo "Container '${CONTAINER_NAME}' is now running"
echo "In VSCode use F1 and the option Remote-Containers: Attach to running container, and select ${CONTAINER_NAME}"
echo "When you are finnished run 'docker stop ${CONTAINER_NAME}' to stop the container"