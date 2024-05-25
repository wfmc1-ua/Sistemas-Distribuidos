import socket
from kafka import KafkaConsumer, KafkaProducer
import json
#### Variables ####
Id = 0
Alias = ""
Token = ""
Coord = (1,1)
CoordF = (0,0)
def registrar():
    global Id
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 12345)) # Establece conexion

    client_socket.send("Solicitud de registro".encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024).decode('utf-8')
    ID, Alias,Token = response.split('|')
    Id = int(ID)
    print(f"Soy el dron: {Id} con el alias {Alias} y token {Token}")
def reciveCoord():
    consumer = KafkaConsumer(
    'coordenadas',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='dron' + str(Id))
    
    #for message in consumer:
    message = next(consumer)
    coordinates = json.loads(message.value.decode('utf-8'))
    print(f"Mi id es {Id}")
    print(f"Coordenadas recibidas: {coordinates}")

    consumer.close()
def espectaculo():
    global Id
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 3333)) # Establece conexion
    solicitud = "Solicitud de registro del dron:" + str(Id) 
    client_socket.send(solicitud.encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024).decode('utf-8')
    print(f"{response}")
    reciveCoord()

    

def main():
    opcion = 0
    while opcion != 3:
        
        print("Selecciona una opcion:")
        print("1- Registrar Dron")
        print("2- Unirse al espectaculo")
        print("3- Salir")

        opcion = int(input("Opcion:"))
        if opcion == 1:
            registrar()
        elif opcion == 2:
            espectaculo()
        elif opcion == 3:
            print("Gracias por utilizar esta opcion")
        else:
            print("ERROR: No es una opcion valida")

    


if __name__ == "__main__":
    main()