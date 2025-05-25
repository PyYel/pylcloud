@echo off

echo Starting the MiniIO storage server...
@REM cd ..
docker-compose -f "docker-compose-mini-io.yml" -p "storage" up -d

echo Program completed. You may close this terminal. If an error arose, see the exception above.

pause