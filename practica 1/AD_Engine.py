import atexit
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

# Incluir la función para cargar o generar claves
def load_or_generate_keys(map_key_file='map_key.txt', movement_key_file='movement_key.txt', coord_key_file='coord_key.txt'):
    global map_cipher, movement_cipher, coord_cipher

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

# Función para eliminar archivos de claves si existen
def delete_key_files():
    if os.path.exists('map_key.txt'):
        os.remove('map_key.txt')
        print("Archivo 'map_key.txt' eliminado.")
    else:
        print("Archivo 'map_key.txt' no existe o ya fue eliminado.")

    if os.path.exists('movement_key.txt'):
        os.remove('movement_key.txt')
        print("Archivo 'movement_key.txt' eliminado.")
    else:
        print("Archivo 'movement_key.txt' no existe o ya fue eliminado.")
    
    if os.path.exists('coord_key.txt'):
        os.remove('coord_key.txt')
        print("Archivo 'coord_key.txt' eliminado.")
    else:
        print("Archivo 'coord_key.txt' no existe o ya fue eliminado.")

############################### FUNCIONES KAFKA #####################################

def SendCoord(pos,nDrones):
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDR)
    topic = 'coordenadas'
    datos=pos + ":" + str(nDrones)
    coordinates_json = json.dumps(datos).encode('utf-8')
    # Cifrar los datos
    encrypted_data = map_cipher.encrypt(coordinates_json)
    
    print(nDrones)
    # Enviar el mensaje
    producer.send(topic, value=encrypted_data)
    producer.flush()
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
        print(f"Error al enviar las coordenadas: {e}")
    finally:
        producer.close()  
        
def ReciveMovement(drones):
    
    global parar
    global coordDrones
    
    consumer = KafkaConsumer(
    'movimiento',
    bootstrap_servers=KAFKA_ADDR,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    max_poll_interval_ms = 10000,
    group_id='engine')
    
    try:
    
        message = next(consumer)

        # Obtener el mensaje cifrado
        encrypted_data = message.value
        
        # Desencriptar los datos
        decrypted_data = movement_cipher.decrypt(encrypted_data)
        
        # Convertir los datos desencriptados de nuevo a JSON

        datos = json.loads(decrypted_data.value.decode('utf-8'))
        
        id ,movimiento,destino = datos.split(":")
        x, y = movimiento.split(',')
        x = int(x)
        y = int(y)

        actualizar_tablero(coordDrones[int(id) -1][0],coordDrones[int(id) -1][1],id,False)
        coordDrones[int(id) -1] = (x,y)
        actualizar_tablero(coordDrones[int(id) -1][0],coordDrones[int(id) -1][1],id,True)

        if destino == "True":
            parar +=1

    except KeyboardInterrupt:
        print("Interrupcion del usuario")
    finally:
            
        consumer.close()

############################### FUNCIONES KAFKA #####################################

def load_database():
    try:
        with open(DB_FILE, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {"drones": []}

def validar_token(token):
    database = load_database()
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
    global coordDrones
    global authenticated_clients
    
    data = client_socket.recv(1024).decode('utf-8') # Recibe del dron su texto, token e id
    print(f"data del drone para autentificar:{data}")
    texto,token = data.split(':')

    drone_id = validar_token(token)
    if drone_id:
        autentify = True
        print(f"Dron {drone_id} autentificado con éxito")
        if len(coordDrones) != len(figuras):
            for _ in range(len(figuras)):
                coordDrones.append((1, 1))
        client_socket.send("Te has autentificado".encode('utf-8'))
        authenticated_clients.append(client_socket)
        if len(authenticated_clients) == len(coordDrones):
            for client in authenticated_clients:
                client.send("All".encode('utf-8'))
            espectaculo(client_socket, figuras, stop_event)
    else:
        print("Token inválido o expirado.")
        client_socket.send("No te puedes  autentificar".encode('utf-8'))
        client_socket.close()
    
    # with open(DB_FILE, 'r') as file:
    #     drones = json.load(file)
    # drones = drones.get("drones", [])

    # with lock: 
    #     print(drones)
    #     for dron in drones:

    #         if dron['token'] == data:
    #             autentify = True

        
def espectaculo(client_socket,drones,stop_event):
    
    global parar
    global authenticated_clients
    load_or_generate_keys()

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
    authenticated_clients =[]
    client_socket.close()

    
def handle_Cliente(figuras, stop_event):
    global authenticated_clients
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    print("Servidor escuchando en el puerto 12345...")
    server_socket.listen(5)
    threads=[]
    
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")
        client_handler = threading.Thread(target=autentificar, args=(client_socket, figuras, stop_event))
        client_handler.start()
        threads.append(client_handler)

        if len(threads) == len(figuras):
            
            for thread in threads:
                thread.join()
        
            break
    



def createTablero(filas, columnas):

    global TABLERO
    for i in range(filas):
        fila = []
        for j in range(columnas):
            fila.append(' x ')
        TABLERO.append(fila) 
        
def actualizar_tablero(x,y,id,avanza=False):
    
    global TABLERO
    
    if 0 <= x <len(TABLERO) and 0 <= y <len(TABLERO):
        if avanza == False:
            TABLERO[x][y]=' x '
        else:
            TABLERO[x][y] = "" + str(id) + ""

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
    while not stop_event.is_set():
        try:
            response = requests.get(f"{WEATHER_API_URL}?ciudad={ciudad}")
            if response.status_code == 200:
                data = response.json()
                temperatura = int(data['temperatura'])
                print(f"Temperatura actual en {ciudad}: {temperatura}°C")
                if temperatura <= 0:
                    print("Temperatura demasiado baja. Finalizando espectáculo.")
                    stop_event.set()  # Detiene el espectáculo
            else:
                print(f"Error al obtener el clima: {response.text}")
        except requests.RequestException as e:
            print(f"Error al conectar con AD_Weather: {e}")

        time.sleep(10)  # Espera 10 segundos antes de la siguiente verificación

def main():
    
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
                
                handle_Cliente(figura["Drones"],stop_event)
        else:
            print("No se puede iniciar el espectáculo.  Temperatura no adecuada.")
        
        stop_event.set()  # Asegúrate de detener el hilo de monitoreo al finalizar
        monitor_thread.join()
    
    # Registrar la función de eliminación para que se ejecute al finalizar
    atexit.register(delete_key_files)
    

if __name__ == "__main__":
    main()