#!/bin/bash

# Build and push nextmini dataplane to Docker Hub
# Usage: ./build-and-push.sh <dockerhub-username> [tag]
# Example: ./build-and-push.sh myusername v1.0.0

set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <dockerhub-username> [tag]"
    echo "Example: $0 myusername v1.0.0"
    exit 1
fi

DOCKERHUB_USERNAME=$1
TAG=${2:-latest}
IMAGE_NAME="nextmini-dataplane"

echo "Building Docker image..."
cd ..
docker build -f dataplane/Dockerfile -t ${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG} .

if [ "$TAG" != "latest" ]; then
    docker tag ${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG} ${DOCKERHUB_USERNAME}/${IMAGE_NAME}:latest
fi

echo ""
echo "✓ Image built successfully"
docker images | grep ${IMAGE_NAME}
echo ""

read -p "Push to Docker Hub? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Pushing to Docker Hub..."
    docker push ${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG}

    if [ "$TAG" != "latest" ]; then
        docker push ${DOCKERHUB_USERNAME}/${IMAGE_NAME}:latest
    fi

    echo ""
    echo "✓ Successfully pushed to Docker Hub!"
    echo ""
    echo "Run with:"
    echo "  docker run -d --privileged --cap-add=NET_ADMIN -p 8080:8080 \\"
    echo "    -v \$(pwd)/config.toml:/var/nextmini/config.toml \\"
    echo "    ${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG} \\"
    echo "    ./nextmini --config-path config.toml ws://<controller-ip>:3000"
    echo ""
    echo "See docs/pages/examples/docker-run.mdx for detailed guide."
else
    echo "Skipping push."
fi
