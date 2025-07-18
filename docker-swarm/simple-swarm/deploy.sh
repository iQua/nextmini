#!/bin/bash

set -e

STACK_NAME="nextmini"

echo "Building Docker images..."
docker build -t nextmini_controller -f ../../controller/Dockerfile ../../
docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../

echo "Deploying stack to Docker Swarm..."
docker stack deploy -c docker-compose.swarm.yml $STACK_NAME

echo "Waiting for services to start..."
sleep 10

echo "Stack deployment completed. Checking status..."
docker stack services $STACK_NAME

echo ""
echo "To check logs:"
echo "  docker service logs ${STACK_NAME}_controller"
echo "  docker service logs ${STACK_NAME}_dataplane"
echo "  docker service logs ${STACK_NAME}_postgres"
echo ""
echo "To remove stack:"
echo "  docker stack rm $STACK_NAME"