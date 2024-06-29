import datetime
import os
import socket
from kafka import KafkaConsumer, KafkaProducer
from colorama import init, Fore, Style
import json
import time
import sys
import requests
import ssl

# CIFRADO SIMÉTRICO
# Crear cifradores para cada clave
# map_cipher = None
# movement_cipher = None
# coord_cipher = None


#### CONSTANTES #####
HOST = ""
PORT = 0
HOST_REGISTRY = ""
PORT_REGISTRY = 0
HOST_ENGINE = ""
PORT_ENGINE = 0
KAFKA_ADDR = ""

REGISTER_URL= ""
REQUEST_TOKEN_URL = ""
#### Variables ####
Id = 0
Alias = ""
Token = ""
CoordsF = []
CoordsI = []
TABLERO = []
esperar = True
PARARTODO = False
ESTADO = None


# ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
# ssl_context.options |= ssl.OP_NO_SSLv2
# ssl_context.options |= ssl.OP_NO_SSLv3
# ssl_context.options |= ssl.OP_NO_TLSv1
# ssl_context.options |= ssl.OP_NO_TLSv1_1
# ssl_context.load_cert_chain(
#     certfile='/app/certs/kafka/kafka.server.keystore.pem', 
#     keyfile='/app/certs/kafka/kafka.server.key.pem',  
#     password='passwordsupersegura'         # Contraseña si el archivo está protegido por contraseña
# )

# ssl_context.verify_mode = ssl.CERT_REQUIRED
# ssl_context.load_verify_locations('/app/certs/kafka/ca-cert.pem')  # Archivo PEM con el certificado de la CA



def registrar():
    global REGISTER_URL
    global Id, Token, Alias

    intentos = 5  # Número de intentos para intentar conectar
    while intentos > 0:
        try:
            response = requests.post(REGISTER_URL, json={'alias': Alias}, verify=False)
            if response.status_code == 200:
                result = response.json()
                Id = result.get('Id')
                Token = result.get('token')
                print(f"Dron registrado con ID: {Id} y Token: {Token}")
                return True
            else:
                print(f"Error al registrar dron: {response.text}")
                return False
        except requests.RequestException:
            print("Error al conectar con AD_Registry.")
            intentos -= 1
            print(f"Quedan {intentos} intentos")
            time.sleep(2)  # Espera 2 segundos antes de intentar nuevamente

    print("Se ha caído el AD_Registry. No se pudo registrar el dron.")
    return False

def solicitar_token():
    global REQUEST_TOKEN_URL
    global Id, Token

    intentos = 5  # Número de intentos para intentar conectar
    while intentos > 0:
        try:
            response = requests.post(REQUEST_TOKEN_URL, json={'Id': Id}, verify=False)
            if response.status_code == 200:
                Token = response.json().get('token')
                print(f"Nuevo token recibido: {Token}")
                return True
            else:
                print(f"Error al solicitar token: {response.text}")
                return False
        except requests.RequestException:
            print("Error al conectar con AD_Registry.")
            intentos -= 1
            print(f"Quedan {intentos} intentos")
            time.sleep(2)  # Espera 2 segundos antes de intentar nuevamente

    print("Se ha caído el AD_Registry. No se pudo solicitar el token.")
    return False



# # Incluir la función para cargar o generar claves
# def load_or_generate_keys(map_key_file='map_key.txt', movement_key_file='movement_key.txt', coord_key_file = 'coord_key.txt'):
#     global map_cipher, movement_cipher, coord_cipher

#     try:
#         if os.path.exists(map_key_file):
#             with open(map_key_file, 'rb') as file:
#                 map_key = file.read()
#         else:
#             map_key = Fernet.generate_key()
#             with open(map_key_file, 'wb') as file:
#                 file.write(map_key)

#         if os.path.exists(movement_key_file):
#             with open(movement_key_file, 'rb') as file:
#                 movement_key = file.read()
#         else:
#             movement_key = Fernet.generate_key()
#             with open(movement_key_file, 'wb') as file:
#                 file.write(movement_key)
        
#         if os.path.exists(coord_key_file):
#             with open(coord_key_file, 'rb') as file:
#                 coord_key = file.read()
#         else:
#             coord_key = Fernet.generate_key()
#             with open(coord_key_file, 'wb') as file:
#                 file.write(coord_key)

#         map_cipher = Fernet(map_key)
#         movement_cipher = Fernet(movement_key)
#         coord_cipher = Fernet(coord_key)
#         print(f"Map_cipher: {map_cipher}")
#         print(f"Movement_cipher: {movement_cipher}")
#         print(f"Coord_cipher: {coord_cipher}")
#     except Exception as e:
#         print(f"Error al cargar o generar claves: {e}")

#####################################33 FUNCIONES KAFKA ##########################################3
def reciveCoord():
    global PARARTODO
    global CoordsF, CoordsI
    global Id
    # global ssl_context
    
    intentos = 5  # Número de intentos para intentar conectar

    while intentos > 0:
        print("ENTRA EN EL WHILE")
        try:
            consumer = KafkaConsumer(
                'coordenadas',
                bootstrap_servers=KAFKA_ADDR,
                auto_offset_reset='earliest',
                group_id='dron' + str(Id)
                # security_protocol='SSL',
                # ssl_context=ssl_context,
            )
            print("CREA EL CONSUMIDOR")
            for message in consumer:
                
                print("ENTRA EN EL FOR")
                data = message.value
                # Descifrar los datos
                # print(f"LO QUE RECIBE ENCRIPTADO ES -> {encrypted_data}")
                # print("QUE PASA??????????????")
                # decrypted_data = coord_cipher.decrypt(encrypted_data)
                # print(f"DESENCRIPTA EL  VALOR COMO -> {decrypted_data}")
    
                datos = json.loads(data.decode('utf-8'))
                coordinates, nDrones = datos.split(":")

                print(f"Coordenadas a guardar: {coordinates}")
                CoordsF.append(coordinates)
                CoordsI.append((1, 1))

                if int(nDrones) == len(CoordsF):
                    break
            print(f"El dron {Id} tiene como coordenada final: {CoordsF[Id - 1]}")
            break  # Salir del bucle si se completa con éxito
        except Exception as e:
            print()
            print(f"Error al intentar consumir la coordenada de AD_Engine: {e}")
            print()
            print("Posible caida del AD_Engine")
            intentos -= 1
            print(f"Quedan {intentos} ")
            time.sleep(2)  # Espera 2 segundos antes de intentar nuevamente
        finally:
            if 'consumer' in locals():
                consumer.close()

    if intentos == 0:
        print("Se ha caído AD_Engine o Kafka. No se pudo recibir coordenadas.")
        PARARTODO = True



def ReciveMap():
    global TABLERO, PARARTODO
    global CoordsI
    global Id
    # global ssl_context
    
    intentos = 5  # Número de intentos para intentar conectar

    while intentos > 0:
        try:
            consumer = KafkaConsumer(
                'mapa',
                bootstrap_servers=KAFKA_ADDR,
                auto_offset_reset='earliest',
                group_id='dron' + str(Id)
                # security_protocol='SSL',
                # ssl_context=ssl_context,
            )

            message = next(consumer)
            data = message.value
            # Descifrar los datos
            # decrypted_data = map_cipher.decrypt(encrypted_data)
            datos = json.loads(data.decode('utf-8'))
            TABLERO = datos['mapa']
            break  # Salir del bucle si se completa con éxito
        except Exception:
            print(f"Error al intentar consumir el mapa de AD_Engine:")
            print("Posible caida del AD_Engine")
            intentos -= 1
            print(f"Quedan {intentos} ")
            time.sleep(2)  # Espera 2 segundos antes de intentar nuevamente
        finally:
            if 'consumer' in locals():
                consumer.close()

    if intentos == 0:
        print("Se ha caído AD_Engine o Kafka. No se pudo recibir el mapa.")
        PARARTODO = True



                    
def SendMovement(move,destino):
    
    global CoordsI, ESTADO
    # global ssl_context
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR 
        # security_protocol='SSL',
        # ssl_context=ssl_context,
        # key_serializer=str.encode,
        # value_serializer=str.encode
    )
    topic = 'movimiento'
    x, y = move
    
    coord = str(x) + "," + str(y)
    datos=str(Id) + ":" + coord + ":" + destino + ":" + ESTADO
    coordinates_json = json.dumps(datos).encode('utf-8')

    # Cifrar los datos
    #encrypted_data = movement_cipher.encrypt(coordinates_json)
    
    try:
        # Enviar el mensaje
        producer.send(topic, value=coordinates_json)
        producer.flush()
    except Exception as e:
        print(f"Error al enviar las coordenadas: {e}")
    finally:
        producer.close()

#####################################33 FUNCIONES KAFKA ##########################################3

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
                    
def autentificar():
    global esperar, PARARTODO
    global CoordsI, CoordsF
    global Id, Token, ESTADO
    global TABLERO

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((HOST_ENGINE, PORT_ENGINE))  # Establece conexion
        solicitud = "Solicitud de registro del dron:" + Token 
        client_socket.send(solicitud.encode('utf-8'))  # Envio de solicitud
        response = client_socket.recv(1024).decode('utf-8')
        print(f"Respuesta del Engine: {response}")

        if response == "No te puedes  autentificar":
            print("Solicitando nuevo token...")
            while not solicitar_token():
                print("Solicitando nuevo token...")
                time.sleep(8)
            autentificar()  # Reintenta la autenticación con el nuevo token
            return
        print(" ESTOY AUTENTIFICADO ")

        #load_or_generate_keys()

        while response != "All": 
            print("Estoy esperando para comenzar el espectaculo")
            try:
                response = client_socket.recv(1024).decode('utf-8')
            except ConnectionResetError:
                print("El AD_Engine se ha caído. No es posible continuar el espectáculo.")
                PARARTODO = True
                break
        espectaculo(client_socket)
    except ConnectionRefusedError:
        print("No se pudo conectar al AD_Engine. Verifique si el servidor está en funcionamiento.")
        sys.exit("El AD_Engine se ha caído. No es posible realizar ningún espectáculo.")
       
def espectaculo(client_socket):
    global esperar, PARARTODO
    global CoordsI, CoordsF
    global Id, Token, ESTADO
    global TABLERO
    
    while not PARARTODO:
        ESTADO = "RUN"
        print("PREPARADO PARA RECIBIR MI COORENADA")
        reciveCoord()
        print("Tengo mi coordenada")
        
        x, y = CoordsF[Id-1].split(',')
        x = int(x)
        y = int(y)
        
        movimiento = selectMove()
        CoordsI[Id - 1] = movimiento
        
        if movimiento == (x, y):
            destino = "True"
        else:
            destino = "False"
            
        SendMovement(movimiento, destino)
        ReciveMap()
        imprimir_tablero(False)
        
        while destino != "True":
            movimiento = selectMove()
            
            if movimiento == (x, y):
                destino = "True"
            else:
                destino = "False"
            
            CoordsI[Id - 1] = movimiento
            SendMovement(movimiento, destino)
            
            ReciveMap()
            
            fin = False
            if CoordsI[Id-1] == CoordsF[Id-1]:
                print()
                fin = True
            imprimir_tablero(fin)

        ESTADO = "END"    
        while esperar:
            ReciveMap()
            imprimir_tablero(fin)
            
            print("TERMINA")
            
            try:
                espera = client_socket.recv(1024).decode('utf-8')
                if espera == "Termina":
                    CoordsF = []
                    if CoordsI[Id - 1] != (1,1):
                        espectaculo(client_socket)
                    CoordsI = []
            
                    esperar = False
                    print(f"al terminar la coordI es igual a {CoordsI}")
            except ConnectionResetError:
                print("El AD_Engine se ha caído. No es posible continuar el espectáculo.")
                PARARTODO = True
                break

    client_socket.close()  # confirmacion que tu estas autentificado
    if PARARTODO:
        sys.exit("El AD_Engine se ha caído. No es posible continuar el espectáculo.")

def readArgs():
    
    global HOST
    global PORT
    global HOST_ENGINE
    global PORT_ENGINE
    global HOST_REGISTRY
    global PORT_REGISTRY
    global KAFKA_ADDR
    global REGISTER_URL
    global REQUEST_TOKEN_URL
    
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

                    REGISTER_URL = f'https://{HOST_REGISTRY}:{PORT_REGISTRY}/register'
                    REQUEST_TOKEN_URL = f'https://{HOST_REGISTRY}:{PORT_REGISTRY}/request-token'
                    
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
    global Alias
    global KAFKA_ADDR   
    global certs_dir
    opcion = 0
    
    readArgs()
    # ssl_context = ssl.create_default_context()
    # ssl_context.load_cert_chain(
    #     certfile='/app/certs/kafka/kafka.server.keystore.pem', 
    #     keyfile='/app/certs/kafka/kafka.ca.keystore.pem',  
    #     password='passwordsupersegura'         # Contraseña si el archivo está protegido por contraseña
    # )
    
    # ssl_context.verify_mode = ssl.CERT_REQUIRED
    # ssl_context.load_verify_locations('/app/certs/kafka/ca-cert.pem')  # Archivo PEM con el certificado de la CA
    
    # consumer = KafkaConsumer(
    #     'coordenadas',
    #     bootstrap_servers=KAFKA_ADDR,
    #     auto_offset_reset='earliest',
    #     group_id='dron' + str(Id),
    #     security_protocol='SSL',
    #     ssl_context=ssl_context,
    # )

    while opcion != 3:
        
        print("Selecciona una opcion:")
        print("1- Registrar Dron")
        print("2- Unirse al espectaculo")
        print("3- Salir")

        opcion = int(input("Opcion:"))
        if opcion == 1:
            if  Id == 0:
                print(f"SE CONECTA A {HOST_REGISTRY} Y AL PUERTO {PORT_REGISTRY}")
                Alias = input("Inserte un alias para el dron: ")
                registrar()
            else:
                print(f"Ya esta registrado con el id {Id}")
                
        elif opcion == 2:
            autentificar()
            print(CoordsI)
        elif opcion == 3:
            print("Gracias por utilizar esta opcion")
        else:
            print("ERROR: No es una opcion valida")

    


if __name__ == "__main__":
    main()