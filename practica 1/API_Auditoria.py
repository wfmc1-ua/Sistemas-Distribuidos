import os
from flask import Flask, jsonify
import json

app = Flask(__name__)



if __name__ == '__main__':
    cert_path = os.path.join(os.path.dirname(__file__), 'certs', 'cert.pem')
    key_path = os.path.join(os.path.dirname(__file__), 'certs', 'key.pem')
    app.run(host='0.0.0.0', port=443, debug=True, ssl_context=(cert_path, key_path))
