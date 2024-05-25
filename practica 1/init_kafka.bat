@echo off
cd /d C:\kafka
start cmd /k ".\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"
timeout /t 5 /nobreak > NUL
start cmd /k ".\bin\windows\kafka-server-start.bat .\config\server.properties"


cd /d C:\Users\paula\Desktop\SD\Prácticas\Drones