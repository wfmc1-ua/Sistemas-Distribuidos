import atexit
from datetime import datetime
import os
import socket
import ssl
import threading
import requests
import json
import time
from colorama import init, Fore, Style
from kafka import KafkaConsumer, KafkaProducer
import sys

# CIFRADO SIMÉTRICO
# Crear cifradores para cada clave
# map_cipher = None
# movement_cipher = None
# coord_cipher = None

##### CONSTANTES ######
FILAS = 20
COLUMNAS = 20
TABLERO =[]
HOST = ""
PORT = 0
HOST_WEATHER = ""
PORT_WEATHER= 0
HOST_DRON = ""
PORT_DRON = 0
KAFKA_ADDR = ""
##### VARIABLES ######
coordDrones = []
autentify = False
parar=0
lock = threading.Lock()
nmove = 1
authenticated_clients = []
DB_FILE = 'drones.json'
TABLERO_FILE = 'tablero.json'
activos = 0

numero_Figura = 0


# Diccionario para rastrear las posiciones actuales de los drones
posiciones_drones = {}

#WEATHER_API_URL = 'http://localhost:5000/api/clima'  # URL de la API REST de AD_Weather
WEATHER_API_URL = ""

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

def readArgs():
    
    global HOST, PORT
    global HOST_WEATHER, PORT_WEATHER
    global HOST_DRON, PORT_DRON
    global KAFKA_ADDR
    global WEATHER_API_URL
    
    while True:
            try:
                # Obtener los argumentos de la línea de comandos
                argumentos = sys.argv

                # Verificar si se proporcionaron suficientes argumentos
                if len(argumentos) == 5:  # El primer argumento es el nombre del script
                    # Asignar los valores de los puertos
                    mi_data = str(argumentos[1])
                    data_Weather = str(argumentos[2])
                    data_Dron = str(argumentos[3])
                    KAFKA_ADDR = str(argumentos[4])
                    
                    E= mi_data.split(":")
                    W = data_Weather.split(":")
                    D = data_Dron.split(":")
                    
                    HOST=E[0]
                    PORT = int(E[1])
                    
                    HOST_WEATHER = W[0]
                    PORT_WEATHER = int(W[1])
                    WEATHER_API_URL = f'https://{HOST_WEATHER}:{PORT_WEATHER}/api/clima'
                    
                    HOST_DRON= D[0]
                    PORT_DRON = int(D[1])

                    # Mostrar los valores asignados
                    print(f"El valor de server_host es: {HOST}")
                    print(f"El valor de server_port para el Weather es: {PORT_WEATHER}")
                    print(f"El valor de server_port para los drones es: {PORT_DRON}")
                    print(f"El valor de la ip de kafka es: {KAFKA_ADDR}")
                    break  # Romper el bucle si los valores son válidos

                else:
                    print("Por favor, proporcione los valores para HOST, PORT_WeatheR Y kafka_addr.")
                    sys.exit(1)  # Salir del programa si los argumentos no son suficientes

            except (ValueError, IndexError) as e:
                print("Error: Asegúrate de proporcionar valores enteros para HOST, PORT_Weather y kafka_addr")#-------------------------------------------------------------------------------------------------------------------------

def load_database_drones():
    try:
        with open(DB_FILE, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {"drones": [], "espectaculo": {"estado": "INICIAL", "figuraNumero": 1}}

def save_database_drones(drones):
    with open(DB_FILE, 'w') as file:
        json.dump(drones, file, indent=4)

#-------------------------------------------------------------------------------------------------------------------------

# Modifica la función actualizar_estado para actualizar el estado en drones.json
def actualizar_estado_espectaculo(nuevo_estado):
    database = load_database_drones()
    database["espectaculo"]["estado"] = nuevo_estado
    save_database_drones(database)

def actualizar_estado_dron(dron_id, nuevo_estado):
    drones = load_database_drones()
    dron_id_str = str(dron_id)
    if dron_id_str in drones:
        drones[dron_id_str]['estado'] = nuevo_estado
        print(f"Estado del dron {dron_id} actualizado a '{nuevo_estado}'.")
    else:
        drones[dron_id_str] = {'estado': nuevo_estado}
        print(f"Añadido dron {dron_id} con estado '{nuevo_estado}'.")
    save_database_drones(drones)

#-------------------------------------------------------------------------------------------------------------------------

# Función para registrar eventos de auditoría
def registrar_evento(tipo, evento, descripcion, detalles, ip):
    global HOST, PORT
    registro = {
        'TIPO DE EVENTO' : tipo,
        'Evento': evento,
        'Descripcion': descripcion,
        'Detalles': detalles, 
        'Fecha_Hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'IP_ENGINE': {'HOST': HOST, 'PORT' : PORT},
        'IP_ORIGEN': ip
    }
    
    log_file = 'auditoria_log.json'
    try:
        # Verifica si el archivo ya existe y agrega el nuevo evento
        if os.path.exists(log_file):
            with open(log_file, 'r') as file:
                registros = json.load(file)
            registros.append(registro)
        else:
            registros = [registro]

        # Escribe los registros en el archivo
        with open(log_file, 'w') as file:
            json.dump(registros, file, indent=4)

        print(f"Evento registrado: {registro}")
    except Exception as e:
        print(f"Error al registrar evento: {e}")

############################### FUNCIONES KAFKA #####################################

def SendCoord(pos,nDrones):
    global HOST_DRON, PORT_DRON
    global KAFKA_ADDR
    # global ssl_context

    print(f"KAFKA ADDRESS { KAFKA_ADDR}")
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR 
        # security_protocol='SSL',
        # ssl_context=ssl_context,
        # key_serializer=str.encode,
        # value_serializer=str.encode
    )
    
    topic = 'coordenadas'
    datos=pos + ":" + str(nDrones)
    coordinates_json = json.dumps(datos).encode('utf-8')
    
    registrar_evento(
        tipo='INFORMATIVA',
        evento='Enviar coordenada a Dron',
        descripcion='Envio por Kafka encriptado con la clave',
        detalles={'Coordenada': pos, 'nDrones': nDrones},
        ip={'HOST_DRONE': HOST_DRON, 'PORT_DRON': PORT_DRON}
    )
    
    try:
        
        # Enviar el mensaje
        producer.send(topic, value=coordinates_json)
        producer.flush()
        print(f"ENVIO COORDENADAS")
    except Exception as e:
        registrar_evento(
            tipo='ERROR',
            evento='Enviar coordenada a Dron - ERROR',
            descripcion=f'Fallo en el envio por Kafka: {e}',
            detalles={'Coordenada': pos, 'nDrones': nDrones},
            ip={'HOST_DRONE': HOST_DRON, 'PORT_DRON': PORT_DRON}
        )
        print(f"Error al enviar las coordenadas: {e}")
    finally:
        producer.close()
    
def SendMap():
    
    global coordDrones
    global TABLERO
    global KAFKA_ADDR
    # global ssl_context
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR 
        # security_protocol='SSL',
        # ssl_context=ssl_context,
        # key_serializer=str.encode,
        # value_serializer=str.encode
    )
    topic = 'mapa'
    
    datos = { 
        "coordenadas": coordDrones,
        "mapa":TABLERO
    }
    
    map_json = json.dumps(datos).encode('utf-8')

    
    try:
        # Enviar el mensaje
        producer.send(topic, value=map_json)
        producer.flush()
        print(f"ENVIE EL MAPA")
    except Exception as e:
        registrar_evento(
            tipo='ERROR',
            evento='Enviar el mapa a Dron - ERROR',
            descripcion=f'Fallo en el envio por Kafka: {e}',
            detalles={},
            ip={'HOST_DRONE': HOST_DRON, 'PORT_DRON': PORT_DRON}
        )

        print(f"Error al enviar el mapa: {e}")
    finally:
        producer.close()  
        
# def ReciveMovement(drones):
    
#     global parar,d
#     global coordDrones
#     global HOST_DRON, PORT_DRON
#     global KAFKA_ADDR
#     # global ssl_context
#     intentos = 5
#     consumer = KafkaConsumer(
#         'movimiento',
#         bootstrap_servers=KAFKA_ADDR,
#         auto_offset_reset='earliest',
#         group_id='engine'
#         # security_protocol='SSL',
#         # ssl_context=ssl_context,
#     )
#     print("DESPUES DE CREAR CONSUMIDOR DE RECIVIR MOVIMIENTO")
#     id = 0
#     movimiento = 0
#     destino = 0
#     while intentos > 0:
#         try:   
#             message = next(consumer)
#             print("DESPUES DE COGER UN MENSAJE")
#             data = message.value
#             print("DESPUES DE QUE COGIERA EL VALOR DEL MENSAJE")
#             #decrypted_data = movement_cipher.decrypt(encrypted_data) # Desencriptar los datos
            
#             # Convertir los datos desencriptados de nuevo a JSON
#             datos = json.loads(data.decode('utf-8'))
#             print("EN LA DESCODIFICACION DEL MENSAJE")
#             id ,movimiento,destino, estado = datos.split(":")
#             x, y = movimiento.split(',')
#             x = int(x)
#             y = int(y)

#             registrar_evento(
#                 tipo='INFORMATIVA',
#                 evento='Recepcion de movimiento de Dron',
#                 descripcion='Movimiento recibido',
#                 detalles={'id': id, 'movimiento': movimiento, 'destino': destino},
#                 ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
#             )

#             if destino == "True":
#                 estado = "END"  # Actualizamos el estado a "END" cuando el dron llega a su destino
#             else:
#                 estado = "RUN"  # Mantenemos el estado "RUN" mientras se está moviendo

#             actualizar_estado_dron(id, estado)
#             actualizar_tablero(x, y, id, estado)
#             # actualizar_tablero(coordDrones[int(id) -1][0],coordDrones[int(id) -1][1],id,False)
#             # coordDrones[int(id) -1] = (x,y)
#             # actualizar_tablero(coordDrones[int(id) -1][0],coordDrones[int(id) -1][1],id,True)

#             time.sleep(0.5)
#             if destino == "True":
#                 parar +=1

#         except Exception as e:
#             registrar_evento(
#                 tipo='ERROR',
#                 evento='Recepcion de movimiento de Dron - ERROR',
#                 descripcion=f'Fallo en Movimiento recibido: {e}',
#                 detalles={'id': id, 'movimiento': movimiento, 'destino': destino},
#                 ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
#             )
#         finally:
#             consumer.close()

#     if intentos == 0:
#         print("Se cayo un dron")
#         parar += 1
#         d -= 1

def ReciveMovement():
    global parar, activos
    global coordDrones
    global HOST_DRON, PORT_DRON

    intentos = 5
    consumer = KafkaConsumer(
        'movimiento',
        bootstrap_servers=KAFKA_ADDR,
        auto_offset_reset='earliest',
        group_id='engine'
    )
    
    while intentos > 0:
        try:   
            message = next(consumer)
            data = message.value
            
            # Convertir los datos de JSON
            datos = json.loads(data.decode('utf-8'))
            
            id, movimiento, destino, estado = datos.split(":")
            x, y = movimiento.split(',')
            x = int(x)
            y = int(y)
            print(f"El movimiento recibido es {(x,y)}")
            registrar_evento(
                tipo='INFORMATIVA',
                evento='Recepcion de movimiento de Dron',
                descripcion='Movimiento recibido',
                detalles={'id': id, 'movimiento': movimiento, 'destino': destino},
                ip={'HOST_DRON': HOST_DRON, 'PORT_DRON': PORT_DRON}
            )

            if destino == "True":
                estado = "END"  # Actualizamos el estado a "END" cuando el dron llega a su destino
            else:
                estado = "RUN"  # Mantenemos el estado "RUN" mientras se está moviendo

            actualizar_estado_dron(id, estado)
            actualizar_tablero(x, y, id, estado)

            #time.sleep(0.5)
            if destino == "True":
                parar += 1
            break  # Si el mensaje se procesa correctamente, salir del bucle

        except Exception as e:
            print(f"Error al intentar consumir el movimiento de AD_Drone: {e}")
            print("Posible caida de uno de los Drones")
            intentos -= 1
            print(f"Quedan {intentos} intentos por si se reincorpora")
            time.sleep(2)  # Espera 2 segundos antes de intentar nuevamente

            registrar_evento(
                tipo='ERROR',
                evento='Recepcion de movimiento de Dron - ERROR',
                descripcion=f'Fallo en Movimiento recibido: {e}',
                detalles={'id': 'Desconocido', 'movimiento': 'Desconocido', 'destino': 'Desconocido'},
                ip={'HOST_DRON': HOST_DRON, 'PORT_DRON': PORT_DRON}
            )
        finally:
            consumer.close()

    if intentos == 0:
        print("Se ha caído un Dron. El espectáculo se hará con un dron menos")
        parar += 1
        activos -= 1
##################################### TABLERO ##############################################

# Cargar el tablero desde el archivo
def load_database_tablero():
    global TABLERO
    try:
        with open(TABLERO_FILE, 'r') as f:
            TABLERO = json.load(f)
    except FileNotFoundError:
        TABLERO = [[" " for _ in range(20)] for _ in range(20)]
        save_database_tablero()

# Guardar el tablero en el archivo
def save_database_tablero():
    global TABLERO
    with open(TABLERO_FILE, 'w') as f:
        json.dump(TABLERO, f, indent=4)

# ------------------------------------------------------------------------------------

# Actualizar la posición y el estado del dron en el tablero
def eliminar_dron_de_posicion_anterior(dron_id):
    global TABLERO, posiciones_drones
    
    dron_id_str = str(dron_id)
    
    # Limpiar la posición anterior del dron, si existe
    if dron_id_str in posiciones_drones:
        prev_x, prev_y = posiciones_drones[dron_id_str]
        contenido_celda = TABLERO[prev_x][prev_y]
        
        # Separar los drones que están en la misma casilla
        drones_en_celda = contenido_celda.split(', ')
        
        # Eliminar solo el dron que se está moviendo
        drones_en_celda = [dron for dron in drones_en_celda if not dron.startswith(dron_id_str)]
        
        # Actualizar la casilla con los drones restantes o dejarla vacía
        if drones_en_celda:
            TABLERO[prev_x][prev_y] = ', '.join(drones_en_celda)
        else:
            TABLERO[prev_x][prev_y] = ' x '

# Actualizar la posición y el estado del dron en el tablero
def actualizar_tablero(x, y, dron_id, estado):
    global TABLERO, posiciones_drones
    
    # Ajustar las coordenadas para que empiecen desde 1
    x -= 1
    y -= 1

    dron_id_str = str(dron_id)
    
    # Eliminar el dron de su posición anterior
    eliminar_dron_de_posicion_anterior(dron_id_str)
    
    # Actualizar la nueva posición y estado del dron
    if 0 <= x < 20 and 0 <= y < 20:
        if TABLERO[x][y] == ' x ':
            TABLERO[x][y] = f"{dron_id} ({estado})"
        else:
            TABLERO[x][y] += f", {dron_id} ({estado})"
    
    # Guardar la nueva posición del dron
    posiciones_drones[dron_id_str] = (x, y)
    
    save_database_tablero()

def createTablero(filas, columnas):
    global TABLERO
    for i in range(filas):
        fila = []
        for j in range(columnas):
            fila.append(' x ')
        TABLERO.append(fila) 

def imprimir_tablero(fin=False):   
    global TABLERO
    
    print()
    print()
    for fila in TABLERO:
        print("[",end="")   
        for i,x in enumerate(fila):
            if x != " x " and fin == False:
                print(Fore.RED +" " + x + " " + Style.RESET_ALL,end="")
            elif x != " x " and fin:
                print(Fore.GREEN + " "+ x + " " + Style.RESET_ALL,end="")
            
            else:
                print(" x ",end="")
            
            if  i == len(fila)-1:
                print("]")

#################################################################################################################################33

def validar_token(token):
    database = load_database_drones()
    for drone in database['drones']:
        # Verifica que 'token' esté en el dron y que sea un diccionario
        if 'token' in drone and isinstance(drone['token'], dict):
            # Verifica si el token coincide
            if drone['token']['value'] == token:
                # Verifica si el token ha expirado
                if drone['token']['expires_at'] > time.time():
                    return drone['Id']  # Retorna el ID del dron
                else:
                    print(f"Token {token} ha expirado.")
                    return None  # Token ha expirado
    print(f"Token {token} no encontrado.")
    return None  # Token no encontrado

def autentificar(client_socket, figuras, stop_event):
    global activos
    global autentify, coordDrones, authenticated_clients
    global HOST_DRON, PORT_DRON

    data = client_socket.recv(1024).decode('utf-8') # Recibe del dron su texto, token e id
    print(f"data del drone para autentificar:{data}")
    texto,token = data.split(':')
    drone_id = 0
# while not drone_id:
    drone_id = validar_token(token)
    if drone_id:
        actualizar_estado_dron(drone_id, "-")
        actualizar_tablero(1, 1, drone_id, "-")
        actualizar_estado_espectaculo('AUTENTIFICANDO')

        autentify = True
        print(f"Dron {drone_id} autentificado con éxito")

        registrar_evento(
            tipo='INFORMATIVA',
            evento='Autenticacion exitosa',
            descripcion='Autenticacion de dron',
            detalles={'drone_id': drone_id, 'token': token},
            ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
        )

        if len(coordDrones) != len(figuras):
            for _ in range(len(figuras)):
                coordDrones.append((1, 1))

        client_socket.send("Te has autentificado".encode('utf-8'))
        authenticated_clients.append(client_socket)

        activos+=1
        print(len(authenticated_clients))    
        if len(authenticated_clients) >= len(coordDrones):
            for client in authenticated_clients:
                client.send("All".encode('utf-8'))
            espectaculo(client_socket, figuras, stop_event, drone_id)
    else:
        registrar_evento(
            tipo= 'INFORMATIVA',
            evento='Autenticacion INVALIDA',
            descripcion='Fallo en la Autenticacion de dron, Token expirado o Inválido',
            detalles={'drone_id': drone_id, 'token': token},
            ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
        )
        print("Token inválido o expirado.")
        client_socket.send("No te puedes  autentificar".encode('utf-8'))
        activos-=1 ########################################################################################3
        client_socket.close()
# ___________________________________________________________________________________________________

def espectaculo(client_socket,drones,stop_event, drone_id):
    
    global parar
    global authenticated_clients
    global HOST_DRON, PORT_DRON

    #load_or_generate_keys()
    actualizar_estado_dron(drone_id, "RUN")
    actualizar_estado_espectaculo('EN_CURSO')

    for documento in drones:
        pos = documento['POS']
        SendCoord(pos,len(drones))
        print(f"Enviando coordenada {pos} ")
        
    fin = False

    while fin != True:
        print("ANTES DE RECIVIR MOVIMIENTO")
        ReciveMovement()
        imprimir_tablero(False)
        SendMap()
        
        if parar == len(drones):
            fin = True

    #     #client_socket.send("Sigue".encode('utf-8'))
    if stop_event.is_set():
        print("Espectáculo detenido debido a baja temperatura.")

    if len(authenticated_clients) >= len(coordDrones):
        actualizar_estado_espectaculo('COMPLETADO')
        time.sleep(5)
        for client in authenticated_clients:
            client.send("Termina".encode('utf-8'))

    # ------------------------------- VOLVER A LA (1,1) ---------------------------------------
    parar = 0
    actualizar_estado_dron(drone_id, "RUN")
    actualizar_estado_espectaculo('EN_CURSO')
    
    for i in range(len(drones)):
        SendCoord("1,1",len(drones))
    fin = False

    while fin != True:
        ReciveMovement()
        imprimir_tablero(False)
        SendMap()
        
        if parar == len(drones):
            fin = True
            
    if len(authenticated_clients) >= len(coordDrones):
        for client in authenticated_clients:
            client.send("Termina".encode('utf-8'))
    actualizar_estado_dron(drone_id,'-') 
    actualizar_estado_espectaculo('INICIAL')                
    registrar_evento(
        tipo='INFORMATIVA',
        evento='FINALIZACION DEL ESPECTACULO',
        descripcion='Todos los drones han finalizado',
        detalles="",
        ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
    )
    client_socket.close()
    authenticated_clients =[]
    
    print(f"PARO EL ESPECTACULO {parar}")
    parar = 0


# def handle_client(figuras, stop_event):
#     global authenticated_clients, activos
#     server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_socket.bind((HOST, PORT))
#     print()
#     print(f"Servidor escuchando en el puerto {PORT}")
#     print()
#     server_socket.listen(5)
#     threads=[]
    
#     while activos != len(figuras):
#         client_socket, addr = server_socket.accept()
#         print(f"Conexión aceptada de {addr}")
#         client_handler = threading.Thread(target=autentificar, args=(client_socket, figuras, stop_event))
#         client_handler.start()
#         threads.append(client_handler)
#         if activos == len(figuras):
#             print(f"PARAR TIENE {parar}")
#             for thread in threads:
                
#                 thread.join()

                
def handle_client(figuras, stop_event ,client_stop_event):
    global authenticated_clients, activos
    
    server_socket = None

    while not client_stop_event.is_set():
        threads = []
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((HOST, PORT))
            print(f"Servidor escuchando en el puerto {PORT}")
            server_socket.listen(5)

            while activos != len(figuras) and not client_stop_event.is_set():
                client_socket, addr = server_socket.accept()
                print(f"Conexión aceptada de {addr}")
                client_handler = threading.Thread(target=autentificar, args=(client_socket, figuras, stop_event))
                client_handler.start()
                threads.append(client_handler)

            # Esperar a que todos los hilos de autentificación terminen
            for thread in threads:
                thread.join()
            print("Todos los hilos de autentificación han terminado")

        except Exception as e:
            print(f"Error en handle_client: {e}")
        finally:
            client_stop_event.set()  # Detenemos todos los hilos de clientes
            for thread in threads:
                if thread.is_alive():
                    thread.join()  # Asegurarse de que todos los hilos se cierren
            if server_socket:
                server_socket.shutdown(socket.SHUT_RDWR)
                server_socket.close()  # Cerramos el socket del servidor para detener nuevas conexiones
                print("Servidor cerrado")

        break  # Salir del bucle principal después de cerrar el servidor

    print("Ciclo de manejo de clientes terminado")

######################################## Funciones para el consultar el clima ###########################################################

def detener_programa():
    print("El servidor del clima se ha caído. No es posible realizar ningún espectáculo.")
    os._exit(1)

def consultar():
    global HOST_WEATHER, PORT_WEATHER
    ciudad = input("Indique la ciudad donde se realiza el espectaculo: ")
    intentos = 5
    while intentos > 0:
        try:
            response = requests.get(f"{WEATHER_API_URL}?ciudad={ciudad}", timeout=5, verify=False)
            if response.status_code == 200:
                data = response.json()
                temperatura = int(data['temperatura'])
                return temperatura, ciudad
            else:
                print(f"Error al obtener el clima: {response.text}")
                return False, ciudad
        except requests.RequestException as e:
            print(f"Error al conectar con AD_Weather. ")
            print(f"Quedan {intentos-1} intentos")
            detalle = 'Posible caida del servidor del clima o no esta disponible en el intento ' + str(intentos)
            registrar_evento(
                tipo='ERROR',
                evento='Conexion con Weather',
                descripcion=f'Error al conectar con AD_Weather servidor del clima: {e}',
                detalles= detalle,
                ip={'HOST_WEATHER': HOST_WEATHER, 'PORT_WEATHER' : PORT_WEATHER}
            )
            intentos -= 1
            time.sleep(5)  # Espera 2 segundos antes de intentar nuevamente
    if intentos == 0:
        detalle = 'Posible caida del servidor del clima o no esta disponible. Quedan ' + str(intentos) + ' INTENTOS para la ' +  'Ciudad ' + ciudad
        registrar_evento(
            tipo='ERROR',
            evento='AD_Weather NO DISPONIBLE',
            descripcion='Error al conectar con AD_Weather servidor del clima TRAS 5 INTENTOS',
            detalles=detalle,
            ip={'HOST_WEATHER': HOST_WEATHER, 'PORT_WEATHER' : PORT_WEATHER}
        )
        print("Se ha caído el AD_Weather. No se pudo obtener la temperatura.")
        detener_programa()

def monitorear_temperatura(ciudad, stop_event):
    global HOST_WEATHER, PORT_WEATHER
    while not stop_event.is_set():
        intentos = 5
        while intentos > 0:
            try:
                response = requests.get(f"{WEATHER_API_URL}?ciudad={ciudad}", timeout=5, verify=False)
                if response.status_code == 200:
                    data = response.json()
                    temperatura = int(data['temperatura'])
                    print(f"Temperatura actual en {ciudad}: {temperatura}°C")

                    registrar_evento(
                        tipo='TEMPERATURA',
                        evento='Comprobacion de temperatura',
                        descripcion='Temperatura valida',
                        detalles={'Ciudad': ciudad, 'Temperatura': temperatura},
                        ip={'HOST_WEATHER': HOST_WEATHER, 'PORT_WEATHER': PORT_WEATHER}
                    )

                    if temperatura <= 0:
                        registrar_evento(
                            tipo='TEMPERATURA',
                            evento='Comprobacion de temperatura',
                            descripcion='Temperatura INvalida',
                            detalles={'Ciudad': ciudad, 'Temperatura': temperatura},
                            ip={'HOST_WEATHER': HOST_WEATHER, 'PORT_WEATHER': PORT_WEATHER}
                        )
                        print("Temperatura demasiado baja. Finalizando espectáculo.")
                        stop_event.set()  # Detiene el espectáculo
                    break
                else:
                    print(f"Error al obtener el clima: {response.text}")
                    break
            except requests.RequestException as e:
                print(f"Error al conectar con AD_Weather")
                detalle = 'Posible caida del servidor del clima o no esta disponible. Quedan ' + str(intentos) + ' INTENTOS para la ' +  'Ciudad' + ciudad
                registrar_evento(
                    tipo='ERROR',
                    evento='AD_Weather NO DISPONIBLE',
                    descripcion=f'Error al conectar con AD_Weather servidor del clima: {e}',
                    detalles=detalle,
                    ip={'HOST_WEATHER': HOST_WEATHER, 'PORT_WEATHER' : PORT_WEATHER}
                )
                intentos -= 1
                time.sleep(2)  # Espera 2 segundos antes de intentar nuevamente

        if intentos == 0:
            print("Se ha caído el AD_Weather. No se pudo obtener la temperatura.")
            detalle = 'Posible caida del servidor del clima o no esta disponible. Quedan ' + str(intentos) + ' INTENTOS para la ' +  'Ciudad' + ciudad
            registrar_evento(
                tipo='ERROR',
                evento='AD_Weather NO DISPONIBLE',
                descripcion='Error al conectar con AD_Weather servidor del clima: TRAS 5 INTENTOS',
                detalles= detalle,
                ip={'HOST_WEATHER': HOST_WEATHER, 'PORT_WEATHER' : PORT_WEATHER}
            )
            detener_programa()

        time.sleep(10)  # Espera 10 segundos antes de la siguiente verificación


######################################################################################################################################

def main():
    global activos, numero_Figura

    readArgs()
    createTablero(FILAS,COLUMNAS)

    temperatura, ciudad = consultar()
    print(f"TEMPERATURA: {temperatura}")
    if temperatura != False:
        stop_event = threading.Event()
        monitor_thread = threading.Thread(target=monitorear_temperatura, args=(ciudad, stop_event))
        monitor_thread.start()
        
        with open('AwD_figuras.json', 'r') as file:# file es como le voy a llamar al archivo cuando se mete en la variable
            datos = json.load(file) # El archivo de json esta en la variable datos

        figuras = datos.get("figuras", [])  # Obtiene la lista de figuras
        if not figuras:
            print("No quedan figuras en el archivo JSON.")
        else:
            if temperatura is not None and temperatura > 0:
                print(" ******** ART WITH DRONES ******** ")
                for figura in figuras:
                    numero_Figura += 1
                    actualizar_estado_espectaculo('INICIAL')
                    print()
                    print()
                    print(f"VAMOS A HACER ESTA FIGURA {figura}")
                    client_stop_event = threading.Event()
                    handle_client(figura["Drones"],stop_event,client_stop_event)
                    print("SIGUIENTE FIGURA")
                    activos=0
                print(" ######## FIN DEL ESPECTACULO ######## ")
                print(" ******** ART WITH DRONES ******** ")
                    
            else:
                print("No se puede iniciar el espectáculo.  Temperatura no adecuada.")
            
            stop_event.set()
            monitor_thread.join()
        
        # Registrar la función de eliminación para que se ejecute al finalizar
        #atexit.register(delete_key_files)
    else:
        print("NO SE HA PODIDO OBTENER LA TEMPERATURA")

if __name__ == "__main__":
    main()

############################################### KEYS ###########################################################################

# # Incluir la función para cargar o generar claves
# def load_or_generate_keys(map_key_file='map_key.txt', movement_key_file='movement_key.txt', coord_key_file='coord_key.txt'):
#     global map_cipher, movement_cipher, coord_cipher
#     global HOST, PORT
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
#                 file.write(map_key)

#         map_cipher = Fernet(map_key)
#         movement_cipher = Fernet(movement_key)
#         coord_cipher = Fernet(coord_key)
#         print(f"Map_cipher: {map_cipher}")
#         print(f"Movement_cipher: {movement_cipher}")
#         print(f"Coord_cipher: {coord_cipher}")

#         # Creación de detalles para el registro de auditoría
#         detalles = {
#             'map_cipher_key': map_key.decode('utf-8'),
#             'movement_cipher_key': movement_key.decode('utf-8'),
#             'coord_cipher_key': coord_key.decode('utf-8')
#         }

#         registrar_evento(
#             tipo='INFORMATIVA',
#             evento='Generacion de Keys de cifrado Simetrico',
#             descripcion='Keys generadas',
#             detalles=detalles,
#             ip={'HOST_DRON': HOST_DRON, 'PORT_DRON' : PORT_DRON}
#         )

#     except Exception as e:
#         registrar_evento(
#             tipo='ERROR',
#             evento='Generacion de Keys de cifrado Simetrico - ERROR',
#             descripcion='Error al cargar o generar claves',
#             detalles={'error': str(e)},
#             ip={'HOST': HOST, 'PORT': PORT}
#         )
#         print(f"Error al cargar o generar claves: {e}")

# def delete_key_files():
#     for key_file in ['map_key.txt', 'movement_key.txt', 'coord_key.txt']:
#         try:
#             if os.path.exists(key_file):
#                 os.remove(key_file)
#                 print(f"Archivo '{key_file}' eliminado.")
#             else:
#                 print(f"Archivo '{key_file}' no existe o ya fue eliminado.")
#         except Exception as e:
#             print(f"Error al eliminar el archivo '{key_file}': {e}")