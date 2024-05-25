
from pymongo import MongoClient
import random
import socket
# Variables globales
client = MongoClient('mongodb://localhost:27017/')
db = client['SD']
collection = db['Weather']
#Funciones

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
        filtro = {"ciudad" : data}
        result = collection.find(filtro)
        for doc in result:
            enviar = doc["temperatura"]
            break
        
        enviar =str(enviar)
        client_socket.send(enviar.encode('utf-8'))
        client_socket.close()



def main():
    consultar()


if __name__ == "__main__":
    main()