import socket
import threading
import requests
import json
from colorama import init, Fore, Style
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client['SD']
collection = db['Figuras']

autentify = 0
FILAS = 20
COLUMNAS = 20
TABLERO =[]
coordDrones = []
parar=0
lock = threading.Lock()

def consultar():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 2222)) # Establece conexion
    ciudad=input("Indique la ciudad donde se realiza el espectaculo: ")
    client_socket.send(ciudad.encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024).decode('utf-8')
    
    response = int(response)
    return response

def SendCoord(pos,nDrones):
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'coordenadas'
    datos=pos + ":" + str(nDrones)
    coordinates_json = json.dumps(datos).encode('utf-8')
    
    print(nDrones)
    # Enviar el mensaje
    producer.send(topic, value=coordinates_json)
    producer.flush()
    producer.close()
def SendMap():
    
    global coordDrones
    global TABLERO
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
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
    except Exception as e:
        print(f"Error al enviar las coordenadas: {e}")
    finally:
        producer.close()    
def ReciveMovement(drones):
    
    global parar
    global coordDrones
    
    consumer = KafkaConsumer(
    'movimiento',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='engine')
    
    try:
        nmove=0 
        for message in consumer:
        #message = next(consumer)
            datos = json.loads(message.value.decode('utf-8'))
            id , coord ,movimiento = datos.split(":")
            
            nmove+=1
            print(f"El dron { id } esta en la posicion {coord} y se mueve a { movimiento}")
            MoveDron(int(id),movimiento,coord)
            print(f"El dron {id} se ha movido y ahora esta en {coordDrones[int(id) -1]}")
            actualizar_tablero(coordDrones[int(id) -1][0],coordDrones[int(id) -1][1],id,True)
            
            if movimiento == "DESTINO ALCANZADO":
                parar +=1
                
            
            break
    except KeyboardInterrupt:
        print("Interrupcion del usuario")
    finally:
            
        consumer.close()
        
def MoveDron(id,move,coord):
    
    global coordDrones
    
    x, y = coord.split(',')
    x = int(x)
    y = int(y)
           
    # Define las reglas para los movimientos
    if move == "NORTE":
        if x < 0:
            x = 20  # Si estamos en el límite superior, apareceremos en el inferior
        else:
            x -= 1
    elif move == "SUR":
        if x > 20:
            x = 1  # Si estamos en el límite inferior, apareceremos en el superior
        else:
            x += 1
    elif move == "ESTE":
        y = (y + 1) % 20  # Si estamos en el límite derecho, apareceremos en el izquierdo
    elif move == "OESTE":
        y = (y - 1) % 20  # Si estamos en el límite izquierdo, apareceremos en el derecho
    
    if id > 0:
        print(f"el id es {id}")
        coordDrones[id-1] = (x,y) # Actualiza la posicion en el array de posiciones actuales de cada dron
    
def autentificar(client_socket,drones):
    global autentify
    global coordDrones
    data = client_socket.recv(1024).decode('utf-8')
    print(f" {data}")
    texto,data = data.split(':')
    texto,ids=data.split('.')
    
    
    print(len(drones))
    with lock: #me sobra pero son para hacer un debug
        for documento in drones:
            print(f" doc: {documento['ID']} / ids: {ids}")
            if documento['ID'] == int(ids):
                autentify+=1
                
    if len(coordDrones) != len(drones):
        for i in range(len(drones)):
            coordDrones.append((1,1))
                

    print(f" autentify : {autentify}")
    
    client_socket.send("Te has autentificado".encode('utf-8'))
    espectaculo(client_socket,drones)
    
def espectaculo(client_socket,drones):
    
    global parar
    
    for documento in drones:
    
        pos = documento['POS']
        SendCoord(pos,len(drones))
        print(f"Enviando coordenada {pos} ")
        
    fin = False
    while fin != True:
        
        ReciveMovement(drones)
        print('PERO ENTRAS?')
        imprimir_tablero(fin)
        print("PERO LLEGAS?")
        SendMap()
        #client_socket.send("Sigue".encode('utf-8'))
        
        if parar == len(drones):
            print("paro")
            fin = True
    client_socket.send("Termina".encode('utf-8'))

    client_socket.close()


        
def handle_Cliente(drones):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 3333))
    print("Servidor escuchando en el puerto 12345...")
    server_socket.listen(5)
    threads=[]
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")
        client_handler = threading.Thread(target=autentificar, args=(client_socket,drones))
        client_handler.start()
        threads.append(client_handler)
        
        if len(threads) == len(drones):
            for thread in threads:
                thread.join()
        
            break
    
    print("Hora de enviar coordenadas")
    # while True:
    #     client_socket, addr = server_socket.accept()
    #     print(f"Conexión aceptada de {addr}")
    #     client_handler = threading.Thread(target=espectaculo, args=(client_socket,drones))
    #     client_handler.start()            


def createTablero(filas, columnas):
    #tablero = []
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
            TABLERO[x][y] = "" + str(id) + ""#cambiar por id

def imprimir_tablero(fin=False):
    global TABLERO
    print()
    print()
    for fila in TABLERO:
        print("[",end="")
        for i,x in enumerate(fila):
            if x != " x "and fin == False:
                print(Fore.RED +" " + x + " " + Style.RESET_ALL,end="")
            elif x != " x " and fin:
                print(Fore.GREEN + " "+ x + " " + Style.RESET_ALL,end="")
            
            else:
                print(" x ",end="")
            
            if  i == len(fila)-1:
                print("]")
def main():
    
    createTablero(FILAS,COLUMNAS)
    for figura in collection.find():
        temperatura = consultar()
        if temperatura >= 0:
            
            handle_Cliente(figura["Drones"])
        else:
            print("No se puede iniciar el espectaculo")

            


if __name__ == "__main__":
    main()