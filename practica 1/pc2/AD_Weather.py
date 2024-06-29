
import os
import random
import socket
import sys
from flask import Flask, request, jsonify
import requests

#### CONSTANTES #####
HOST = ""
PORT = 0
Host_ENGINE = ""
PORT_ENGINE = ""

app = Flask(__name__)

# API Key de OpenWeather
API_KEY = None
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

#Funciones

@app.route('/api/clima', methods=['GET'])
def get_weather():
    ciudad = request.args.get('ciudad')
    if not ciudad:
        return jsonify({'error': 'Debe proporcionar el nombre de una ciudad'}), 400

    # Construir la URL para la solicitud a la API
    url = f'{BASE_URL}?q={ciudad}&appid={API_KEY}&units=metric'
    print(f"Consultando URL: {url}")
    response = requests.get(url)
    print(f"Estado de respuesta de OpenWeather: {response.status_code}")
    print(f"Contenido de la respuesta: {response.text}")
    
    if response.status_code == 200:
        data = response.json()
        temperatura = data.get('main', {}).get('temp')
        return jsonify({'ciudad': ciudad, 'temperatura': temperatura})
    else:
        print(f"Error al obtener el clima: {response.text}")
        return jsonify({'error': 'No se pudo obtener el clima'}), response.status_code

def consultar():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    print("Servidor escuchando en el puerto 2222...")
    server_socket.listen(5)
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")
        
        data = client_socket.recv(1024).decode('utf-8')
        print(f"Se solicita la temperatura de la siguiente ciudad: {data}")

        url = f'{BASE_URL}?q={data}&appid={API_KEY}&units=metric'
        response = requests.get(url)
        
        if response.status_code == 200:
            weather_data = response.json()
            temperatura = weather_data.get('main', {}).get('temp')
            if temperatura is not None:
                enviar = str(temperatura)
            else:
                enviar = 'Error: No se encontró la temperatura'
        else:
            enviar = 'Error: No se pudo obtener el clima'

        client_socket.send(enviar.encode('utf-8'))
        client_socket.close()


def readArgs():
    
    global HOST
    global PORT
    global Host_ENGINE
    global PORT_ENGINE

    while True:
            try:
                # Obtener los argumentos de la línea de comandos
                argumentos = sys.argv
                # Verificar si se proporcionaron suficientes argumentos
                if len(argumentos) == 2:  # El primer argumento es el nombre del script
                    # Asignar los valores de los puertos
                    mi_data = str(argumentos[1])
                    #data_E = str(argumentos[2])
                    
                    W = mi_data.split(":")
                    #E = data_E.split(":")
                    
                    HOST = W[0]
                    PORT = int(W[1])
                    
                    #Host_ENGINE = E[0]
                    #PORT_ENGINE = int(E[1])
                    
                    # Mostrar los valores asignados
                    print(f"El valor de server_host es: {HOST}")
                    print(f"El valor de server_port es: {PORT}")
                    break  # Romper el bucle si los valores son válidos
                else:
                    print("Por favor, proporcione los valores para HOST y PORT.")
                    sys.exit(1)  # Salir del programa si los argumentos no son suficientes

            except (ValueError, IndexError) as e:
                print("Error: Asegúrate de proporcionar valores enteros para HOST y PORT")
    
def main():
    global Host_ENGINE, PORT_ENGINE
    global HOST, PORT
    global API_KEY

    readArgs()
    API_KEY = input("Indique la API_Key para acceder a OpenWeather: ")
    print(HOST)
    cert_path = os.path.join(os.path.dirname(__file__), 'certs', 'cert.pem')
    key_path = os.path.join(os.path.dirname(__file__), 'certs', 'key.pem')
    app.run(host=HOST, port=PORT, ssl_context=(cert_path, key_path))

    #consultar()


if __name__ == "__main__":
    main()