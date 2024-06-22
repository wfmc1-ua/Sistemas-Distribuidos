import uuid
from pymongo import MongoClient
import socket
import threading
import sys
import json
import time
from flask import Flask, request, jsonify
#client = MongoClient("mongodb://localhost:27017/")
##### CONSTANTES ########
HOST = ""
PORT = 0
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
        with open('DB_FILE', 'r') as file:
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
def register(client_socket, client_database):
    global ID

    data = request.json  # Obtiene los datos de la solicitud JSON enviada por el dron
    drone_id = data.get('drone_id')  # Extrae el ID del dron de los datos recibidos
    alias = data.get('alias')  # Extrae el alias del dron de los datos recibidos

    if not drone_id or not alias:
        return jsonify({'error': 'Missing drone_id or alias'}), 400

    database = load_database()
    for drone in database['drones']:
        if drone['Id'] == drone_id:
            return jsonify({'error': 'Drone already registered'}), 409

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
    return jsonify({'token': token, 'expires_in': 20})

<<<<<<< HEAD
@app.route('/request-token', methods=['POST'])
def request_token():
    data = request.json
    drone_id = data.get('drone_id')
=======
        client_database["drones"].append(datos)
        #collection.insert_one(datos)
        
        with open('drones.json', 'w') as file:
            json.dump(client_database, file, indent=2)
        
        enviar = f"{ID}|{'d' + str(ID)}|{token}"
        ID += 1
>>>>>>> 2ed22f5557a485a3f865e8b782e792bdb04057e9

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

@app.route('/validate-token', methods=['POST'])
def validate_token():
    token = request.json.get('token')
    if not token:
        return jsonify({'error': 'Missing token'}), 400

    database = load_database()
    for drone in database['drones']:
        if 'token' in drone and drone['token']['value'] == token:
            if drone['token']['expires_at'] > time.time():
                return jsonify({'valid': True, 'drone_id': drone['Id']})
            else:
                del drone['token']  # Elimina token expirado
                save_database(database)
                return jsonify({'valid': False, 'reason': 'Token expired'}), 401

    return jsonify({'valid': False, 'reason': 'Token not found'}), 401

def readArgs():
    
    global HOST
    global PORT
    global HOST_DRON
    global PORT_DRON

    while True:
            try:
                # Obtener los argumentos de la línea de comandos
                argumentos = sys.argv

                # Verificar si se proporcionaron suficientes argumentos
                if len(argumentos) == 3:  # El primer argumento es el nombre del script
                    # Asignar los valores de los puertos
                    mi_data = str(argumentos[1])
                    data_Dron = str(argumentos[2])
                    R= mi_data.split(":")
                    D=data_Dron.split(":")
                    HOST = R[0]
                    PORT = int(R[1])
                    HOST_DRON = D[0]
                    PORT_DRON = int(D[1])

                    # Mostrar los valores asignados
                    print(f"El valor de server_host es: {HOST}")
                    print(f"El valor de server_port es: {PORT_DRON}")
                    break  # Romper el bucle si los valores son válidos

                else:
                    print("Por favor, proporcione los valores para HOST y PORT_Dron.")
                    sys.exit(1)  # Salir del programa si los argumentos no son suficientes

            except (ValueError, IndexError) as e:
                print("Error: Asegúrate de proporcionar valores enteros para HOST y PORT_Dron")
        
def main():
    global HOST
    global PORT
    readArgs()
    app.run(host=HOST, port=PORT)
    #handle_Cliente()

if __name__ == "__main__":
    main()