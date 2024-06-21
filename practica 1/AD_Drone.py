import socket
from kafka import KafkaConsumer, KafkaProducer
from confluent_kafka import Producer,Consumer,TopicPartition, KafkaException, KafkaError

from colorama import init, Fore, Style
import json
import time
import sys

#### CONSTANTES #####
HOST = ""
PORT = 0
HOST_REGISTRY = ""
PORT_REGISTRY = 0
HOST_ENGINE = ""
PORT_ENGINE = 0
KAFKA_ADDR = ""
#### Variables ####
Id = 0
Alias = ""
Token = ""
CoordsF = []
CoordsI = []
TABLERO = []
esperar = True

def registrar(alias):
    
    global Id
    global Token
    global Alias
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((HOST_REGISTRY, PORT_REGISTRY)) # Establece conexion
    mensaje = "Solicitud de registro y va tener el  alias:" + alias
    client_socket.send(mensaje.encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024).decode('utf-8')
    ID, Alias,Token = response.split('|')
    Id = int(ID)
    print(f"Soy el dron: {Id} con el alias {Alias} y token {Token}")
            
def reciveCoord():
    
    global CoordsF
    global CoordsI
    global Id
    
    consumer = KafkaConsumer(
    'coordenadas',
    bootstrap_servers=KAFKA_ADDR,
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
                CoordsI.append((1,1))
                
            if int(nDrones) == len(CoordsF):
                break
        print(f"el dron {Id} tiene como coordenada final :{CoordsF[Id-1]}")
    except KeyboardInterrupt:
        print("Interrupcion del usuario")
    finally:
            
        consumer.close()

def ReciveMap():
    global TABLERO
    global CoordsI
    global Id
    consumer = KafkaConsumer(
    'mapa',
    bootstrap_servers=KAFKA_ADDR,
    auto_offset_reset='earliest',
    group_id='dron' + str(Id))
    try:
        message = next(consumer)
        datos = json.loads(message.value.decode('utf-8'))
        TABLERO = datos['mapa']
        
    except KeyboardInterrupt:
        print("Interrupcion del usuario")
    finally:
        consumer.close()
                    
def SendMovement(move,destino):
    
    global CoordsI
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR)
    topic = 'movimiento'
    x, y = move
     
    coord = str(x) + "," + str(y)
    datos=str(Id) + ":" + coord + ":" + destino
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
    
    global CoordsI
    global CoordsF
    global Id

    # Definir los movimientos posibles
    movimientos = [(-1, 0), (0, 1), (1, 0), (0, -1), (1, 1), (1, -1), (-1, -1), (-1, 1)]
    posiciones = []
    camino_min = float('inf')  # Utilizar infinito para comparación inicial

    # Coordenadas iniciales y finales
    x1, y1 = CoordsI[Id - 1]
    x2, y2 = map(int, CoordsF[Id - 1].split(','))

    # Calcular las posiciones posibles y sus correspondientes distancias
    for movimiento in movimientos:
        posicion_x = (x1 + movimiento[0]) % 20  # Envolvimiento de 0 a 19
        posicion_y = (y1 + movimiento[1]) % 20  # Envolvimiento de 0 a 19

        if posicion_x == 0:
            posicion_x = 19
        if posicion_y == 0:
            posicion_y = 19

        posiciones.append((posicion_x, posicion_y))

    # Encontrar el movimiento con el camino mínimo
    for posicion in posiciones:
        camino = (x2 - posicion[0], y2 - posicion[1])
        maximo = max(abs(camino[0]), abs(camino[1]))

        if maximo < camino_min:
            camino_min = maximo
            bestmove = posicion

    return bestmove

def imprimir_tablero(fin):
    
    global Id
    global TABLERO
    print()
    print()

    for fila in TABLERO:
        print("[",end="")
        for i,x in enumerate(fila):
            if x == "" + str(Id) + "" and fin == False:
                print(Fore.RED + " " + x + " " + Style.RESET_ALL,end="")
            elif x == "" + str(Id) + "" and fin:
                print(Fore.GREEN + " " + x + " "  + Style.RESET_ALL,end="")
            elif x != " x ":
                print(Fore.RED + " " + x + " " + Style.RESET_ALL,end="")
            else:
                print(" x ",end="")
            
            if  i == len(fila)-1:
                print("]")
                    
def espectaculo():
    global esperar
    global CoordsI
    global CoordsF
    global Id
    global TABLERO
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((HOST_ENGINE, PORT_ENGINE)) # Establece conexion
    solicitud = "Solicitud de registro del dron:" + Token 
    client_socket.send(solicitud.encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024).decode('utf-8')
    print(response)
    
    while response != "All": 
        response = client_socket.recv(1024).decode('utf-8')

    reciveCoord()
    
    x,y = CoordsF[Id-1].split(',')
    x = int(x)
    y = int(y)
    
    movimiento=selectMove()
    CoordsI[Id - 1] = movimiento
    
    if movimiento == (x,y):
        destino = "True"
    else:
        destino = "False"
        
    SendMovement(movimiento,destino)
    ReciveMap()
    imprimir_tablero(False)
    
    while destino != "True":
        movimiento=selectMove()
        
        if movimiento == (x,y):
            destino = "True"
        else:
            destino = "False"
        
        CoordsI[Id - 1] = movimiento
        SendMovement(movimiento,destino)
        
        ReciveMap()
        
        if CoordsI[Id-1] == CoordsF[Id-1]:
            fin = True
        else:
            fin = False
        imprimir_tablero(fin)
        
    while esperar == True:
        ReciveMap()
        imprimir_tablero(fin)
        
        print("TERMINA")
        
        espera = client_socket.recv(1024).decode('utf-8')
        print("PASA DEL ESPERA")
        print(espera)
        if espera == "Termina":
            esperar = False
        

    client_socket.close() ## confirmacion que tu estas autentificado
    
def readArgs():
    
    global HOST
    global PORT
    global HOST_ENGINE
    global PORT_ENGINE
    global HOST_REGISTRY
    global PORT_REGISTRY
    global KAFKA_ADDR
    
    while True:
            try:
                # Obtener los argumentos de la línea de comandos
                argumentos = sys.argv

                # Verificar si se proporcionaron suficientes argumentos
                if len(argumentos) == 5:  # El primer argumento es el nombre del script
                    # Asignar los valores de los puertos
                    HOST_Local = str(argumentos[1])
                    Conx_Registry = str(argumentos[2])
                    Conx_Engine = str(argumentos[3])
                    KAFKA_ADDR = str(argumentos[4])

                    H= HOST_Local.split(":")
                    R = Conx_Registry.split(":")
                    E=  Conx_Engine.split(":")
                    
                    HOST = H[0]
                    PORT= H[1]
                    
                    HOST_ENGINE = E[0]
                    PORT_ENGINE = int(E[1])

                    HOST_REGISTRY = R[0]
                    PORT_REGISTRY = int(R[1])
                    # Mostrar los valores asignados
                    print(f"El valor de HOST es: {HOST}")
                    print(f"El valor de PORT_Registry es: {PORT_REGISTRY}")
                    print(f"El valor de PORT_Engine es: {PORT_ENGINE}")
                    print(f"El valor de la ip de kafka es: {KAFKA_ADDR}")
                    break  # Romper el bucle si los valores son válidos

                else:
                    print("Por favor, proporcione los valores para HOST, PORT_Registry, PORT_Engine y KAFKA_ADDR")
                    sys.exit(1)  # Salir del programa si los argumentos no son suficientes

            except (ValueError, IndexError) as e:
                print("Error: Asegúrate de proporcionar valores enteros para HOST, PORT_Registry, PORT_Engine y KAFKA_ADDR")

    
def main():
    
    global Id
    opcion = 0
    
    readArgs()
    
    while opcion != 3:
        
        print("Selecciona una opcion:")
        print("1- Registrar Dron")
        print("2- Unirse al espectaculo")
        print("3- Salir")

        opcion = int(input("Opcion:"))
        if opcion == 1:
            if  Id == 0:
                alias = input("Inserte un alias para el dron: ")
                registrar(alias)
            else:
                print(f"Ya esta registrado con el id {Id}")
                
        elif opcion == 2:
            espectaculo()
        elif opcion == 3:
            print("Gracias por utilizar esta opcion")
        else:
            print("ERROR: No es una opcion valida")

    


if __name__ == "__main__":
    main()