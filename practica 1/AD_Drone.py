import socket
from kafka import KafkaConsumer, KafkaProducer
import json
import time
#### Variables ####
Id = 0
Alias = ""
Token = ""
Coord = (1,1)
CoordsF = []
def registrar():
    global Id
    global Token
    global Alias
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 12345)) # Establece conexion

    client_socket.send("Solicitud de registro".encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024).decode('utf-8')
    ID, Alias,Token = response.split('|')
    Id = int(ID)
    print(f"Soy el dron: {Id} con el alias {Alias} y token {Token}")
    
def reciveCoord():
    
    global CoordsF
    global Id
    consumer = KafkaConsumer(
    'coordenadas',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='dron' + str(Id))
    try:
        for message in consumer:
        #message = next(consumer)
            datos = json.loads(message.value.decode('utf-8'))
            coordinates , nDrones = datos.split(":")
            if coordinates not in CoordsF:
                print(f"Coordenadas a guardar: {coordinates}")
                CoordsF.append(coordinates)
                
            if int(nDrones) == len(CoordsF):
                break
        print(f"el dron {Id} tiene como coordenada final :{CoordsF[Id-1]}")
    except KeyboardInterrupt:
        print("Interrupcion del usuario")
    finally:
            
        consumer.close()
        
def SendMovement(move):
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'movimiento'
    x, y = Coord
     
    coord = str(x) + "," + str(y)
    datos=str(Id) + ":" + coord + ":" + move
    coordinates_json = json.dumps(datos).encode('utf-8')
    
    try:
        # Enviar el mensaje
        producer.send(topic, value=coordinates_json)
        producer.flush()
    except Exception as e:
        print(f"Error al enviar las coordenadas: {e}")
    finally:
        producer.close()
        
def selectMove():
    global Coord
    
    x1,y1 = Coord
    x2,y2 = CoordsF[Id-1].split(',')
    x2=int(x2)
    y2 = int(y2)
    if x1 < x2:
        return "SUR"
    elif x1 > x2:
        return "NORTE"
    elif y1 < y2:
        return "ESTE"
    elif y1 > y2:
        return "OESTE"
    else:
        return "DESTINO ALCANZADO"
    
def espectaculo():
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 3333)) # Establece conexion
    solicitud = "Solicitud de registro del dron:" + Token 
    client_socket.send(solicitud.encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024)
########################Confirmacion de que todos estan autentificados##################


    # solicitud = "Puede comenzar el espectaculo?"
    # client_socket.send(solicitud.encode('utf-8')) # Envio de solicitud
    # response = client_socket.recv(1024)

    # if response.strip() == "True":
    reciveCoord()
    movimiento=selectMove()
    SendMovement(movimiento)
    while movimiento != "DESTINO ALCANZADO":
        reciveCoord()
        movimiento=selectMove()
        SendMovement(movimiento)
    client_socket.close() ## confirmacion que tu estas autentificado
    

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