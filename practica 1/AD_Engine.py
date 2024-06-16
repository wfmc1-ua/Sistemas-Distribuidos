import socket
import threading
import requests
import json
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client['SD']
collection = db['Figuras']
autentify = 0
noautentify = 0
lock = threading.Lock()
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
    
def ReciveMovement(drones):


    consumer = KafkaConsumer(
    'movimiento',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='dron')
    
    try:
        nmove=0 
        for message in consumer:
        #message = next(consumer)
            datos = json.loads(message.value.decode('utf-8'))
            id , coord ,movimiento = datos.split(":")
            
            nmove+=1
            print(f"El dron { id } esta en la posicion {coord} y se mueve a { movimiento}")
            coord = MoveDron(int(id),movimiento,coord)
            print(f"El dron {id} se ha movido y ahora esta en {coord}")
            if nmove == len(drones):
                break
    except KeyboardInterrupt:
        print("Interrupcion del usuario")
    finally:
            
        consumer.close()
def MoveDron(id,move,coord):
    
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

    return (x,y)
    
def autentificar(client_socket,drones):
    global autentify
    global noautentify
    data = client_socket.recv(1024).decode('utf-8')
    print(f" {data}")
    texto,data = data.split(':')
    texto,ids=data.split('.')
    
    
    print(len(drones))
    with lock:
        for documento in drones:
            print(f" doc: {documento['ID']} / ids: {ids}")
            if documento['ID'] == int(ids):
                autentify+=1
            else:
                noautentify+=1
    print(f" autentify : {autentify}")
    
    client_socket.send("Te has autentificado".encode('utf-8'))
    
    for documento in drones:
    
        pos = documento['POS']
        SendCoord(pos,len(drones))
        print(f"Enviando coordenada {pos} ")
    client_socket.close()
# def espectaculo(client_socket,drones):
#     data = client_socket.recv(1024).decode('utf-8')
#     print(f"recibido: {data}")
#     client_socket.send("True".encode('utf-8'))
#     client_socket.close()


        
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


        
    
def main():
    for figura in collection.find():
        temperatura = consultar()
        if temperatura >= 0:
            
            handle_Cliente(figura["Drones"])
        else:
            print("No se puede iniciar el espectaculo")

            


if __name__ == "__main__":
    main()