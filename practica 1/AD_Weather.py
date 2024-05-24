
from pymongo import MongoClient
import random
# Variables globales
client = MongoClient('mongodb://localhost:27017/')

#Funciones

def crearDatos():
    # Conectar al servidor MongoDB

    # Crear o seleccionar la base de datos
    db = client['SD']

    # Crear o seleccionar la colección
    collection = db['Weather']

    # Lista de ciudades
    ciudades = [
        "Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza",
        "Málaga", "Murcia", "Palma", "Las Palmas", "Bilbao",
        "Alicante", "Córdoba", "Valladolid", "Vigo", "Gijón",
        "Hospitalet", "A Coruña", "Vitoria", "Granada", "Elche"
    ]

    # Generar datos de temperaturas y crear documentos
    documentos = []
    for ciudad in ciudades:
        temperatura = random.randint(-5, 20)
        documento = {
            'ciudad': ciudad,
            'temperatura': temperatura
        }
        documentos.append(documento)

    # Insertar los documentos en la colección
    collection.insert_many(documentos)

    # Verificar la inserción
    for doc in collection.find():
        print(doc)
def consultar():
    print("en proceso")
def main():
    consultar()

    


if __name__ == "__main__":
    main()