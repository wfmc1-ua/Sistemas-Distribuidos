import atexit
from datetime import datetime
import os
import socket
import threading
import requests
import json
import time
from colorama import init, Fore, Style
from confluent_kafka import Consumer,Producer, KafkaException, KafkaError
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
import sys

from cryptography.fernet import Fernet

# CIFRADO SIMÉTRICO
# Crear cifradores para cada clave
map_cipher = None
movement_cipher = None
coord_cipher = None

# client = MongoClient("mongodb://localhost:27017/")
# db = client['SD']
# collection = db['Figuras']
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
d =0
#WEATHER_API_URL = 'http://localhost:5000/api/clima'  # URL de la API REST de AD_Weather
WEATHER_API_URL = ""

def consultar():
    ciudad = input("Indique la ciudad donde se realiza el espectaculo: ")
    try:
        response = requests.get(f"{WEATHER_API_URL}?ciudad={ciudad}")
        if response.status_code == 200:
            data = response.json()
            temperatura = int(data['temperatura'])
            return temperatura, ciudad
        else:
            print(f"Error al obtener el clima: {response.text}")
            return None, ciudad
    except requests.RequestException as e:
        print(f"Error al conectar con AD_Weather: {e}")
        
        return None, ciudad
    
# Función para registrar eventos de auditoría
def registrar_evento(evento, descripcion, detalles, ip):
    global HOST, PORT
    global map_cipher, movement_cipher, coord_cipher
    registro = {
        'fecha_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'ip_ENGINE': {'HOST': HOST, 'PORT' : PORT},
        'ip_ORIGEN': ip,
        'evento': evento,
        'descripcion': descripcion,
        'detalles': detalles
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


# Incluir la función para cargar o generar claves
def load_or_generate_keys(map_key_file='map_key.txt', movement_key_file='movement_key.txt', coord_key_file='coord_key.txt'):
    global map_cipher, movement_cipher, coord_cipher
    global HOST, PORT
    try:
        if os.path.exists(map_key_file):
            with open(map_key_file, 'rb') as file:
                map_key = file.read()
        else:
            map_key = Fernet.generate_key()
            with open(map_key_file, 'wb') as file:
                file.write(map_key)

        if os.path.exists(movement_key_file):
            with open(movement_key_file, 'rb') as file:
                movement_key = file.read()
        else:
            movement_key = Fernet.generate_key()
            with open(movement_key_file, 'wb') as file:
                file.write(movement_key)
        
        if os.path.exists(coord_key_file):
            with open(coord_key_file, 'rb') as file:
                coord_key = file.read()
        else:
            coord_key = Fernet.generate_key()
            with open(coord_key_file, 'wb') as file:
                file.write(map_key)

        map_cipher = Fernet(map_key)
        movement_cipher = Fernet(movement_key)
        coord_cipher = Fernet(coord_key)
        print(f"Map_cipher: {map_cipher}")
        print(f"Movement_cipher: {movement_cipher}")
        print(f"Coord_cipher: {coord_cipher}")

        # Creación de detalles para el registro de auditoría
        detalles = {
            'map_cipher_key': map_key.decode('utf-8'),
            'movement_cipher_key': movement_key.decode('utf-8'),
            'coord_cipher_key': coord_key.decode('utf-8')
        }

        registrar_evento(
            evento='Generacion de Keys de cifrado Simetrico',
            descripcion='Keys generadas',
            detalles=detalles,
            ip={'HOST_DRON': HOST_DRON, 'PORT_DRON' : PORT_DRON}
        )

    except Exception as e:
        registrar_evento(
            evento='Generacion de Keys de cifrado Simetrico - ERROR',
            descripcion='Error al cargar o generar claves',
            detalles={'error': str(e)},
            ip={'HOST': HOST, 'PORT': PORT}
        )
        print(f"Error al cargar o generar claves: {e}")

def delete_key_files():
    for key_file in ['map_key.txt', 'movement_key.txt', 'coord_key.txt']:
        try:
            if os.path.exists(key_file):
                os.remove(key_file)
                print(f"Archivo '{key_file}' eliminado.")
            else:
                print(f"Archivo '{key_file}' no existe o ya fue eliminado.")
        except Exception as e:
            print(f"Error al eliminar el archivo '{key_file}': {e}")


############################### FUNCIONES KAFKA #####################################

def SendCoord(pos,nDrones):
    global HOST_DRON, PORT_DRON
    global coord_cipher

    registrar_evento(
        evento='Enviar coordenada a Dron',
        descripcion='Envio por Kafka encriptado con la clave',
        detalles={'coord_cipher': str(coord_cipher), 'Coordenada': pos, 'nDrones': nDrones},
        ip={'HOST_DRONE': HOST_DRON, 'PORT_DRON': PORT_DRON}
    )

    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR)
    topic = 'coordenadas'
    datos=pos + ":" + str(nDrones)
    coordinates_json = json.dumps(datos).encode('utf-8')
    # Cifrar los datos
    encrypted_data = coord_cipher.encrypt(coordinates_json)
    try:
        print(nDrones)
        # Enviar el mensaje
        producer.send(topic, value=encrypted_data)
        producer.flush()
    except Exception as e:
        registrar_evento(
            evento='Enviar coordenada a Dron - ERROR',
            descripcion='Fallo en el envio por Kafka encriptado con la clave',
            detalles={'coord_cipher': str(coord_cipher), 'Coordenada': pos, 'nDrones': nDrones},
            ip={'HOST_DRONE': HOST_DRON, 'PORT_DRON': PORT_DRON}
        )
        print(f"Error al enviar las coordenadas: {e}")
    finally:
        producer.close()
    
def SendMap():
    
    global coordDrones
    global TABLERO
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR)
    topic = 'mapa'
    
    datos = { 
        "coordenadas": coordDrones,
        "mapa":TABLERO
    }
    
    map_json = json.dumps(datos).encode('utf-8')

    # Cifrar los datos
    encrypted_data = map_cipher.encrypt(map_json)
    
    try:
        # Enviar el mensaje
        producer.send(topic, value=encrypted_data)
        producer.flush()
    except Exception as e:
        print(f"Error al enviar el mapa: {e}")
    finally:
        producer.close()  
        
def ReciveMovement(drones):
    
    global parar
    global coordDrones
    global HOST_DRON, PORT_DRON
    
    consumer = KafkaConsumer(
        'movimiento',
        bootstrap_servers=KAFKA_ADDR,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        max_poll_interval_ms = 10000,
        group_id='engine'
    )
    
    try:
    
        message = next(consumer)

        # Obtener el mensaje cifrado
        encrypted_data = message.value
        
        # Desencriptar los datos
        decrypted_data = movement_cipher.decrypt(encrypted_data)
        
        # Convertir los datos desencriptados de nuevo a JSON

        datos = json.loads(decrypted_data.decode('utf-8'))
        
        id ,movimiento,destino, estado = datos.split(":")
        x, y = movimiento.split(',')
        x = int(x)
        y = int(y)

        registrar_evento(
            evento='Recepcion de movimiento de Dron',
            descripcion='Movimiento recibido',
            detalles={'id': id, 'movimiento': movimiento, 'destino': destino},
            ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
        )

        if destino == "True":
            estado = "END"  # Actualizamos el estado a "END" cuando el dron llega a su destino
        else:
            estado = "RUN"  # Mantenemos el estado "RUN" mientras se está moviendo

        actualizar_estado_dron(id, estado)
        actualizar_tablero(x, y, id, estado)
        # actualizar_tablero(coordDrones[int(id) -1][0],coordDrones[int(id) -1][1],id,False)
        # coordDrones[int(id) -1] = (x,y)
        # actualizar_tablero(coordDrones[int(id) -1][0],coordDrones[int(id) -1][1],id,True)
        time.sleep(1)
        if destino == "True":
            parar +=1

    except KeyboardInterrupt:
        registrar_evento(
            evento='Recepcion de movimiento de Dron - ERROR',
            descripcion='Fallo en Movimiento recibido',
            detalles={'id': id, 'movimiento': movimiento, 'destino': destino},
            ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
        )
        print("Interrupcion del usuario")
    finally:
            
        consumer.close()

############################### FUNCIONES KAFKA #####################################

def load_database_drones():
    try:
        with open(DB_FILE, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {"drones": []}

# Guardar la base de datos de drones
def save_database_drones(drones):
    with open(DB_FILE, 'w') as file:
        json.dump(drones, file, indent=4)

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
    global autentify
    global d
    global coordDrones
    global authenticated_clients
    global HOST_DRON, PORT_DRON
    
    data = client_socket.recv(1024).decode('utf-8') # Recibe del dron su texto, token e id
    print(f"data del drone para autentificar:{data}")
    texto,token = data.split(':')
    drone_id = 0
# while not drone_id:
    drone_id = validar_token(token)
    if drone_id:
        autentify = True
        print(f"Dron {drone_id} autentificado con éxito")

        registrar_evento(
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

        d+=1
        if len(authenticated_clients) == len(coordDrones):
            for client in authenticated_clients:
                client.send("All".encode('utf-8'))
            espectaculo(client_socket, figuras, stop_event, drone_id)
    else:
        registrar_evento(
            evento='Autenticacion INVALIDA',
            descripcion='Fallo en la Autenticacion de dron',
            detalles={'drone_id': drone_id, 'token': token},
            ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
        )
        print("Token inválido o expirado.")
        client_socket.send("No te puedes  autentificar".encode('utf-8'))
        d-=1
        client_socket.close()

    # with open(DB_FILE, 'r') as file:
    #     drones = json.load(file)
    # drones = drones.get("drones", [])

    # with lock: 
    #     print(drones)
    #     for dron in drones:

    #         if dron['token'] == data:
    #             autentify = True

        
def espectaculo(client_socket,drones,stop_event, drone_id):
    
    global parar
    global authenticated_clients
    global map_cipher, movement_cipher, coord_cipher
    global HOST_DRON, PORT_DRON

    load_or_generate_keys()
    actualizar_estado_dron(drone_id, "RUN")

    for documento in drones:
        pos = documento['POS']
        SendCoord(pos,len(drones))
        print(f"Enviando coordenada {pos} ")
        
    fin = False

    while fin != True:
        ReciveMovement(drones)
        imprimir_tablero(False)
        SendMap()
        
        if parar == len(drones):
            fin = True

    

    #     #client_socket.send("Sigue".encode('utf-8'))
    
    if stop_event.is_set():
        print("Espectáculo detenido debido a baja temperatura.")

    if len(authenticated_clients) == len(coordDrones):

        for client in authenticated_clients:
            client.send("Termina".encode('utf-8'))
    registrar_evento(
        evento='FINALIZACION DEL ESPECTACULO',
        descripcion='Todos los drones han finalizado',
        detalles="",
        ip={'HOST_DRON' : HOST_DRON, 'PORT_DRON' : PORT_DRON}
    )
    authenticated_clients =[]
    
    print(f"PARO EL ESPECTACULO {parar}")
    parar = 0
    client_socket.close()

    
def handle_Cliente(figuras, stop_event):
    global authenticated_clients
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    print("Servidor escuchando en el puerto 12345...")
    server_socket.listen(5)
    threads=[]
    global d
    while d != len(figuras):
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")
        client_handler = threading.Thread(target=autentificar, args=(client_socket, figuras, stop_event))
        client_handler.start()
        threads.append(client_handler)

        # if parar == len(figuras):
        #     print(f"PARAR TIENE {parar}")
        #     for thread in threads:
        #         thread.join()
        #     print("VA A SALIR")
        #     break
    

###################################################################################

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

# Actualizar la posición y el estado del dron en el tablero
# Diccionario para rastrear las posiciones actuales de los drones
# Diccionario para rastrear las posiciones actuales de los drones
posiciones_drones = {}

# Actualizar la posición y el estado del dron en el tablero
def actualizar_tablero(x, y, dron_id, estado):
    global TABLERO, posiciones_drones
    
    dron_id_str = str(dron_id)
    
    # Limpiar la posición anterior del dron, si existe
    if dron_id_str in posiciones_drones:
        prev_x, prev_y = posiciones_drones[dron_id_str]
        # Solo limpia la posición anterior si la nueva no es la misma
        if (prev_x, prev_y) != (x, y):
            TABLERO[prev_x][prev_y] = ' x '
    
    # Actualizar la nueva posición y estado del dron
    if 0 <= x < 20 and 0 <= y < 20:
        TABLERO[x][y] = f"{dron_id} ({estado})"
    
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
        
# def actualizar_tablero(x,y,id,avanza=False):
    
#     global TABLERO
    
#     if 0 <= x <len(TABLERO) and 0 <= y <len(TABLERO):
#         if avanza == False:
#             TABLERO[x][y]=' x '
#         else:
#             TABLERO[x][y] = "" + str(id) + ""

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
                
def readArgs():
    
    global HOST
    global PORT
    global HOST_WEATHER
    global PORT_WEATHER
    global HOST_DRON
    global PORT_DRON
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
                    WEATHER_API_URL = f'http://{HOST_WEATHER}:{PORT_WEATHER}/api/clima'
                    
                    HOST_DRON= D[0]
                    PORT_DRON = int(D[1])
                    

                    # Mostrar los valores asignados
                    print(f"El valor de server_host es: {HOST}")
                    print(f"El valor de server_port para el Weather es: {PORT_WEATHER}")
                    print(f"El valor de server_port para los drones es: {PORT_DRON}")
                    print(f"El valor de la ip de kafka es: {KAFKA_ADDR}")
                    break  # Romper el bucle si los valores son válidos

                else:
                    print("Por favor, proporcione los valores para HOST, PORT_WeatheR, PORT_Dron Y kafka_addr.")
                    sys.exit(1)  # Salir del programa si los argumentos no son suficientes

            except (ValueError, IndexError) as e:
                print("Error: Asegúrate de proporcionar valores enteros para HOST, PORT_Weather, PORT_Dron y kafka_addr")
    

def monitorear_temperatura(ciudad, stop_event):
    global HOST_WEATHER, PORT_WEATHER

    while not stop_event.is_set():
        try:
            response = requests.get(f"{WEATHER_API_URL}?ciudad={ciudad}")
            if response.status_code == 200:
                data = response.json()
                temperatura = int(data['temperatura'])
                print(f"Temperatura actual en {ciudad}: {temperatura}°C")

                registrar_evento(
                    evento='Comprobacion de temperatura',
                    descripcion='Temperatura valida',
                    detalles={'Ciudad': ciudad, 'Temperatura': temperatura},
                    ip={'HOST_WEATHER' : HOST_WEATHER, 'PORT_WEATHER' : PORT_WEATHER}
                )

                if temperatura <= 0:
                    registrar_evento(
                        evento='Comprobacion de temperatura',
                        descripcion='Temperatura INvalida',
                        detalles={'Ciudad': ciudad, 'Temperatura': temperatura},
                        ip={'HOST_WEATHER': HOST_WEATHER, 'PORT_WEATHER': PORT_WEATHER}
                    )
                    print("Temperatura demasiado baja. Finalizando espectáculo.")
                    stop_event.set()  # Detiene el espectáculo
            else:
                registrar_evento(
                    evento='Obtencion de la temperatura - ERROR',
                    descripcion='Temperatura INvalida',
                    detalles={'Ciudad': ciudad, 'Temperatura': temperatura},
                    ip={'HOST_WEATHER': HOST_WEATHER, 'PORT_WEATHER': PORT_WEATHER}
                )
                print(f"Error al obtener el clima: {response.text}")
        except requests.RequestException as e:
            print(f"Error al conectar con AD_Weather: {e}")

        time.sleep(10)  # Espera 10 segundos antes de la siguiente verificación

def main():
    global d
    readArgs()
    createTablero(FILAS,COLUMNAS)
    temperatura, ciudad = consultar()
    print(f"TEMPERATURA: {temperatura}")

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
            for figura in figuras:
                print(f"VAMOS A HACER ESTA FIGURA {figura}")
                handle_Cliente(figura["Drones"],stop_event)
                d=0
                print("SIGUIENTE FIGURA")
        else:
            print("No se puede iniciar el espectáculo.  Temperatura no adecuada.")
        
        stop_event.set()  # Asegúrate de detener el hilo de monitoreo al finalizar
        monitor_thread.join()
    
    # Registrar la función de eliminación para que se ejecute al finalizar
    atexit.register(delete_key_files)
    

if __name__ == "__main__":
    main()