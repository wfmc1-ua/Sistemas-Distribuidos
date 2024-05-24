import socket
#### Variables ####
Id = 0
Alias = ""
Token = ""
def registrar():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 12345)) # Establece conexion

    client_socket.send("Solicitud de registro".encode('utf-8')) # Envio de solicitud
    response = client_socket.recv(1024).decode('utf-8')
    ID, Alias,Token = response.split('|')
    ID = int(ID)
    print(f"Soy el dron: {ID} con el alias {Alias} y token {Token}")


def main():
    print("Selecciona una opcion:")
    print("1- Registrar Dron")
    print("2- Unirse al espectaculo")
    print("3- Salir")
    registrar()
    #opcion =input("Opcion:")
    # while opcion == 3:
    #     if opcion == 1:
    #         registrar()
    #     elif opcion == 2:
    #         print("en proceso")
    #     elif opcion == 3:
    #         print("Gracias por utilizar esta opcion")
    #     else:
    #         print("ERROR: No es una opcion valida")

    


if __name__ == "__main__":
    main()