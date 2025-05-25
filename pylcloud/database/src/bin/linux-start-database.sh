#!/bin/bash

echo "Starting the RAGbot database..."
cd ..
docker-compose -f docker-compose-database.yml -p database up

echo "Program completed. You may close this terminal. If an error arose, see the exception above."

read -p "Press any key to continue... " -n1 -s