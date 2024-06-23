@echo off

cd /d C:\Users\paula\Desktop\SD\Prácticas\Drones
start cmd /k python AD_Weather.py 0.0.0.0:8888 localhost:5555
start cmd /k python AD_Engine.py localhost:5555 localhost:8888 localhost:7777 localhost:9092
start cmd /k python AD_Registry.py localhost:6666 localhost:7777
start cmd /k python AD_Drone.py localhost:7777 localhost:6666 localhost:5555 localhost:9092
start cmd /k python AD_Drone.py localhost:7777 localhost:6666 localhost:5555 localhost:9092
