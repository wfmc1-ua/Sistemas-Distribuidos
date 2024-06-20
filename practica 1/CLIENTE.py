import socket

def solicitar_temperatura(ciudad):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 5000))
    client_socket.send(ciudad.encode('utf-8'))
    
    temperatura = client_socket.recv(1024).decode('utf-8')
    client_socket.close()
    
    return temperatura

ciudad = 'Alicante'
print(f'Temperatura en {ciudad}: {solicitar_temperatura(ciudad)}°C')
