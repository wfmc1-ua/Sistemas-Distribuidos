import socket
import threading
import requests
import json
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client['SD']
collection = db['Figuras']


def send_positions():
    # URL de la vista en Django que maneja la actualización de posiciones
    url = 'http://localhost:8000/update_positions/'
    
    # Nuevas posiciones que deseas enviar
    new_positions = [
        (6, 1, 1), (8, 2, 2), (5, 3, 3), (6, 4, 4), (7, 5, 5),
        (8, 6, 6), (14, 7, 7), (15, 8, 8), (16, 9, 9), (17, 10, 10),
        (13, 11, 11), (18, 12, 12), (13, 13, 13), (12, 14, 14), (13, 15, 15),
        (11, 16, 16), (10, 17, 17), (11, 18, 18), (10, 19, 19), (9, 20, 20),
        (9, 21, 21), (8, 22, 22), (5, 23, 23), (5, 24, 24), (6, 25, 25),
        (7, 26, 26), (8, 27, 27), (9, 28, 28), (9, 29, 29), (8, 30, 30),
        (8, 31, 31), (9, 32, 32), (10, 33, 33)
    ]

    # Convertir las posiciones a una lista de strings
    positions_str = [f"{pos[0]},{pos[1]},{pos[2]}" for pos in new_positions]

    # Datos que se enviarán en la solicitud POST
    data = {
        'positions': positions_str,
        'csrfmiddlewaretoken': '9yV2HZ9XuXdKsHuzTWvyKKu1FDplXmf1RoEYkv5qagHe4h0HEWBppkRcDvWxwmpt'  # Reemplaza con tu token CSRF
    }

    # Hacer la solicitud POST
    response = requests.post(url, data=data)

    # Imprimir la respuesta del servidor
    print(response.json())


def consultar():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 2222)) # Establece conexion
    ciudad=input("Indique la ciudad donde se realiza el espectaculo: ")
    client_socket.send(ciudad.encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024).decode('utf-8')
    
    response = int(response)
    return response
def SendCoord(pos):
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'coordenadas'
    coordinates_json = json.dumps(pos).encode('utf-8')
    
    # Enviar el mensaje
    producer.send(topic, value=coordinates_json)
    producer.flush()
    producer.close()

def autentificar(client_socket,drones):
    data = client_socket.recv(1024).decode('utf-8')
    texto,data = data.split(':')
    print(f"Recibido: {data}")



    # Iterar sobre los resultados y mostrarlos
    for documento in drones:
        if documento['ID'] == int(data):
            pos = documento['POS']
            SendCoord(pos)
            print(f"Enviando coordenada {pos} ")
            client_socket.send("Envio correcto de coordenadas".encode('utf-8'))
            client_socket.close()
            break


        
    
def handle_Cliente(drones):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 3333))
    print("Servidor escuchando en el puerto 12345...")
    server_socket.listen(5)
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")
        client_handler = threading.Thread(target=autentificar, args=(client_socket,drones))
        client_handler.start()
        
    
    
        
    
def main():
    for figura in collection.find():
        temperatura = consultar()
        if temperatura >= 0:
            
            handle_Cliente(figura["Drones"])
        else:
            print("No se puede iniciar el espectaculo")

            


if __name__ == "__main__":
    main()