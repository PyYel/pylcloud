@echo off


echo Starting the RAGbot database...
cd ..
docker-compose -f docker-compose-database.yml -p database up -d

echo Program completed. You may close this terminal. If an error araised, see the exception above.

pause