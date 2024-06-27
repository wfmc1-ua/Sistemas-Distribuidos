from flask import Flask, jsonify, send_from_directory
import json
import os

app = Flask(__name__)

# Ruta al archivo del tablero
TABLERO_FILE = 'tablero.json'

# Endpoint para servir la página HTML
@app.route('/')
def serve_map():
    return send_from_directory('static', 'mapa.html')

# Endpoint para obtener el tablero en formato JSON
@app.route('/get-tablero', methods=['GET'])
def get_tablero():
    try:
        with open(TABLERO_FILE, 'r') as file:
            tablero = json.load(file)
            return jsonify({'status': 'success', 'data': tablero})
    except FileNotFoundError:
        return jsonify({'status': 'error', 'message': 'Tablero no encontrado'}), 404

# Endpoint para servir archivos estáticos como CSS y JS
@app.route('/<path:filename>', methods=['GET'])
def serve_static(filename):
    return send_from_directory('static', filename)
    
@app.route('/get-estado', methods=['GET'])
def get_estado():
    with open('drones.json', 'r') as file:
        data = json.load(file)
    estado = data.get("espectaculo", {}).get("estado", "INICIAL")
    figura_numero = data.get("espectaculo", {}).get("figuraNumero", 1)
    return jsonify({'status': 'success', 'estado': estado, 'figuraNumero': figura_numero})

@app.route('/get-auditorias', methods=['GET'])
def get_auditorias():
    try:
        with open('auditoria_log.json', 'r') as file:
            auditorias = json.load(file)
        return jsonify({'status': 'success', 'data': auditorias})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


if __name__ == '__main__':
    cert_path = os.path.join(os.path.dirname(__file__), 'certs', 'cert.pem')
    key_path = os.path.join(os.path.dirname(__file__), 'certs', 'key.pem')
    app.run(host='0.0.0.0', port=443, debug=True, ssl_context=(cert_path, key_path))
