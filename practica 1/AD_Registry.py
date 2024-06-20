from pymongo import MongoClient
import socket
import threading
client = MongoClient("mongodb://localhost:27017/")

##### VARIABLES #########
db = client['SD']
collection = db['Drones']
ID= 1
IDs_lock = threading.Lock() # para evitar que la comunicacion entre hilos altere de forma
                            # no deseada los ids
def registrar(client_socket):
    global ID

    data = client_socket.recv(1024).decode('utf-8')
    
    data, alias = data.split(':')
    with IDs_lock:
        token = "token." + str(ID)
        datos = {
            'Id' : ID,
            'alias' : alias,
            "token" : token
            
        }
        
        collection.insert_one(datos)
        enviar = f"{ID}|{'d' + str(ID)}| {token}"
        ID += 1
    client_socket.send(enviar.encode('utf-8'))
    client_socket.close()
        
def handle_Cliente():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 12345))
    print("Servidor escuchando en el puerto 12345...")
    server_socket.listen(5)
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")

        client_handler = threading.Thread(target=registrar, args=(client_socket,))
        client_handler.start()
def main():
    handle_Cliente()

    


if __name__ == "__main__":
    main()