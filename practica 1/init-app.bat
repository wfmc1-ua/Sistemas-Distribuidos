@echo off
: cd /d C:\Users\Rayane\Desktop\universidad5\Sistemas distribuidos\git2 prac2\PRACTICA-SD
start cmd /k python AD_Engine.py localhost:6060 4 localhost:9092 localhost:3000 localhost localhost:5000
start cmd /k python AD_Registry.py localhost:5050
start cmd /k python AD_Drone.py localhost:6060 localhost:9092 localhost:5050
start cmd /k python AD_Drone.py localhost:6060 localhost:9092 localhost:5050
start cmd /k python AD_Drone.py localhost:6060 localhost:9092 localhost:5050
start cmd /k python AD_Drone.py localhost:6060 localhost:9092 localhost:5050
start http://localhost:3000/tablero.html
: start cmd /k C:\Users\Rayane\Desktop\universidad5\REST_SD\start npm
: start cmd /k 