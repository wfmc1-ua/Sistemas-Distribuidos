
from pymongo import MongoClient
import random
import socket
import sys

#### CONSTANTES #####
HOST = ""
PORT = 0
Host_ENGINE = ""
PORT_ENGINE = ""

#### Variables globales ######
client = MongoClient('mongodb://localhost:27017/')
db = client['SD']
collection = db['Weather']


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
    server_socket.bind((HOST, PORT))
    print("Servidor escuchando en el puerto 2222...")
    server_socket.listen(5)
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")
        
        data = client_socket.recv(1024).decode('utf-8')
        print(f"Se solicita la temperatura de la siguiente ciudad: {data}")
        filtro = {"ciudad" : data}
        result = collection.find(filtro)
        for doc in result:
            enviar = doc["temperatura"]
            break
        
        enviar =str(enviar)
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
                if len(argumentos) == 3:  # El primer argumento es el nombre del script
                    # Asignar los valores de los puertos
                    mi_data = str(argumentos[1])
                    data_E = str(argumentos[2])
                    
                    W = mi_data.split(":")
                    E = data_E.split(":")
                    
                    HOST = W[0]
                    PORT = int(W[1])
                    
                    Host_ENGINE = E[0]
                    PORT_ENGINE = int(E[1])
                    

                    # Mostrar los valores asignados
                    print(f"El valor de server_host es: {Host_ENGINE}")
                    print(f"El valor de server_port es: {PORT_ENGINE}")
                    break  # Romper el bucle si los valores son válidos

                else:
                    print("Por favor, proporcione los valores para HOST y PORT_E.")
                    sys.exit(1)  # Salir del programa si los argumentos no son suficientes

            except (ValueError, IndexError) as e:
                print("Error: Asegúrate de proporcionar valores enteros para HOST y PORT_E")
    
def main():
    
    readArgs()
    consultar()


if __name__ == "__main__":
    main()