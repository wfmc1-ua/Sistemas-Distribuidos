from pymongo import MongoClient
import socket
import threading
import sys
client = MongoClient("mongodb://localhost:27017/")
##### CONSTANTES ########
HOST = ""
PORT = 0
HOST_DRON = ""
PORT_DRON = 0
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
    global HOST
    global PORT
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    print(f"Servidor escuchando en el puerto {PORT}...")
    server_socket.listen(5)
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión aceptada de {addr}")

        client_handler = threading.Thread(target=registrar, args=(client_socket,))
        client_handler.start()

def readArgs():
    
    global HOST
    global PORT
    global HOST_DRON
    global PORT_DRON

    while True:
            try:
                # Obtener los argumentos de la línea de comandos
                argumentos = sys.argv

                # Verificar si se proporcionaron suficientes argumentos
                if len(argumentos) == 3:  # El primer argumento es el nombre del script
                    # Asignar los valores de los puertos
                    mi_data = str(argumentos[1])
                    data_Dron = str(argumentos[2])
                    R= mi_data.split(":")
                    D=data_Dron.split(":")
                    HOST = R[0]
                    PORT = int(R[1])
                    HOST_DRON = D[0]
                    PORT_DRON = int(D[1])

                    # Mostrar los valores asignados
                    print(f"El valor de server_host es: {HOST}")
                    print(f"El valor de server_port es: {PORT_DRON}")
                    break  # Romper el bucle si los valores son válidos

                else:
                    print("Por favor, proporcione los valores para HOST y PORT_Dron.")
                    sys.exit(1)  # Salir del programa si los argumentos no son suficientes

            except (ValueError, IndexError) as e:
                print("Error: Asegúrate de proporcionar valores enteros para HOST y PORT_Dron")
        
def main():
    
    readArgs()
    handle_Cliente()

    


if __name__ == "__main__":
    main()