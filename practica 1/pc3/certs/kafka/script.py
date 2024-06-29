import subprocess
if __name__ == "__main__":
    # Ruta del archivo original y archivo de salida
    input_file = '/mnt/data/ca-cert'
    output_file = '/mnt/data/ca-cert.pem'

    # Verificar si el archivo es DER y convertirlo a PEM
    try:
        # Intentar leer el archivo como DER
        result = subprocess.run(['openssl', 'x509', '-inform', 'DER', '-in', input_file, '-out', output_file], capture_output=True)
        
        if result.returncode == 0:
            print("El archivo ha sido convertido a formato PEM con éxito.")
        else:
            # Si falló, puede que ya esté en formato PEM o haya otro problema
            print("Error al convertir el archivo a PEM. Detalles:")
            print(result.stderr.decode('utf-8'))
    except Exception as e:
        print(f"Se produjo un error: {e}")