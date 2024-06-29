import os
import uuid
import socket
import threading
import sys
import json
import time
from flask import Flask, request, jsonify
#client = MongoClient("mongodb://localhost:27017/")
##### CONSTANTES ########
HOST = "localhost"
PORT = 6666
HOST_DRON = ""
PORT_DRON = 0
##### VARIABLES #########
#db = client['SD']
#collection = db['Drones']
ID= 1
IDs_lock = threading.Lock() # para evitar que la comunicacion entre hilos altere de forma
                            # no deseada los ids
DB_FILE = 'drones.json'

app = Flask(__name__)

def load_database():
    try:
        with open(DB_FILE, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {"drones": []}

def save_database(database):
    with open(DB_FILE, 'w') as file:
        json.dump(database, file, indent=4)

def emitir_token():
    token = str(uuid.uuid4())
    expiration_time = time.time() + 20  # Expira en 20 segundos
    return token, expiration_time


@app.route('/register', methods=['POST'])
def register():
    global ID

    data = request.json  # Obtiene los datos de la solicitud JSON enviada por el dron
    alias = data.get('alias')  # Extrae el alias del dron de los datos recibidos

    if not alias:
        return jsonify({'error': 'Missing alias'}), 400

    database = load_database()

    # Generar un nuevo drone_id único
    drone_id = ID
    ID += 1
    token, expiration_time = emitir_token()
        
    new_drone = {
        'Id': drone_id,
        'alias': alias,
        'token': {
            'value': token,
            'expires_at': expiration_time
        }
    }
    database['drones'].append(new_drone)
    save_database(database)
    return jsonify({'Id': drone_id, 'token': token, 'expires_in': 20})

@app.route('/request-token', methods=['POST'])
def request_token():
    data = request.json
    drone_id = data.get('Id')

    if not drone_id:
        return jsonify({'error': 'Missing drone_id'}), 400

    database = load_database()
    for drone in database['drones']:
        if drone['Id'] == drone_id:
            token, expiration_time = emitir_token()
            drone['token'] = {
                'value': token,
                'expires_at': expiration_time
            }
            save_database(database)
            return jsonify({'token': token, 'expires_in': 20})

    return jsonify({'error': 'Drone not registered'}), 404

def readArgs():
    
    global HOST, PORT
    global HOST_DRON, PORT_DRON

    while True:
            try:
                # Obtener los argumentos de la línea de comandos
                argumentos = sys.argv

                # Verificar si se proporcionaron suficientes argumentos
                if len(argumentos) == 2:  # El primer argumento es el nombre del script
                    # Asignar los valores de los puertos
                    mi_data = str(argumentos[1])
                    #data_Dron = str(argumentos[2])
                    R= mi_data.split(":")
                    #D=data_Dron.split(":")
                    HOST = R[0]
                    PORT = int(R[1])
                    # HOST_DRON = D[0]
                    # PORT_DRON = int(D[1])

                    # Mostrar los valores asignados
                    print(f"El valor de server_host es: {HOST}")
                    print(f"El valor de server_port es: {PORT_DRON}")
                    break  # Romper el bucle si los valores son válidos

                else:
                    print("Por favor, proporcione los valores para HOST y PORT.")
                    sys.exit(1)  # Salir del programa si los argumentos no son suficientes

            except (ValueError, IndexError) as e:
                print(f"estos es lo que valen los argumentos  {len(argumentos)}")
                print("Error: Asegúrate de proporcionar valores enteros para HOST y PORT")
        
def main():
    global HOST
    global PORT
    readArgs()
    cert_path = os.path.join(os.path.dirname(__file__), 'certs', 'cert.pem')
    key_path = os.path.join(os.path.dirname(__file__), 'certs', 'key.pem')
    app.run(host=HOST, port=PORT, ssl_context=(cert_path, key_path))
    #handle_Cliente()

if __name__ == "__main__":
    main()