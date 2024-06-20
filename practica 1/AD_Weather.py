
from pymongo import MongoClient
import random
import socket

from flask import Flask, request, jsonify
import requests

# Variables globales
client = MongoClient('mongodb://localhost:27017/')
db = client['SD']
collection = db['Weather']

##################################################################3 Paula
app = Flask(__name__)

# API Key de OpenWeather
API_KEY = 'b8480b4264c16a8e8ac372939983013c'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
################################################################## Paula

#Funciones

@app.route('/weather', methods=['GET'])
def get_weather():
    city = request.args.get('city')
    if not city:
        return jsonify({'error': 'Debe proporcionar el nombre de una ciudad'}), 400

    # Construir la URL para la solicitud a la API
    url = f'{BASE_URL}?q={city}&appid={API_KEY}&units=metric'
    
    response = requests.get(url)
    if response.status_code != 200:
        return jsonify({'error': 'No se pudo obtener el clima'}), 500
    
    data = response.json()
    temperature = data.get('main', {}).get('temp')
    
    if temperature is None:
        return jsonify({'error': 'No se encontró información de temperatura para la ciudad proporcionada'}), 404

    return jsonify({'city': city, 'temperature': temperature})


def crearDatos():

    # Lista de ciudades
    ciudades = [
        "Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza",
        "Málaga", "Murcia", "Palma", "Las Palmas", "Bilbao",
        "Alicante", "Córdoba", "Valladolid", "Vigo", "Gijón",
        "Hospitalet", "A Coruña", "Vitoria", "Granada", "Elche"
    ]

    # Generar datos de temperaturas y crear documentos
    documentos = []
    for ciudad in ciudades:
        temperatura = random.randint(-5, 20)
        documento = {
            'ciudad': ciudad,
            'temperatura': temperatura
        }
        documentos.append(documento)

    # Insertar los documentos en la colección
    collection.insert_many(documentos)

    # Verificar la inserción
    for doc in collection.find():
        print(doc)
        
def consultar():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 2222))
    print("Servidor escuchando en el puerto 2222...")
    server_socket.listen(5)
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")
        
        data = client_socket.recv(1024).decode('utf-8')
        print(f"Se solicita la temperatura de la siguiente ciudad: {data}")

        # Llamar a la API de OpenWeather en lugar de la base de datos local @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Paula
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
        ##@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Paula
        #filtro = {"ciudad" : data}
        #result = collection.find(filtro)
        #for doc in result:
        #    enviar = doc["temperatura"]
        #    break
        
        #enviar =str(enviar)
        client_socket.send(enviar.encode('utf-8'))
        client_socket.close()



def main():
    consultar()


if __name__ == "__main__":
    main()