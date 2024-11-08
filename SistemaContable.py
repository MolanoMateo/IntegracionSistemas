import pandas as pd
import os
import random
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
from datetime import datetime

def generar_nombre_archivo():
    # Obtenemos la fecha y hora actuales para crear un nombre único
    carpeta = 'FTP'
    
    # Verificar si la carpeta 'FTP' existe, si no, crearla
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)
    
    # Obtenemos la fecha y hora actuales para crear un nombre único
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear el nombre completo del archivo dentro de la carpeta 'FTP'
    return os.path.join(carpeta, f"registros_{timestamp}.csv")
def escribir_en_csv(archivo_csv,fecha, mes_reporte, anio_reporte, tipo_registro, monto):
    # Verifica si el archivo existe
    if os.path.exists(archivo_csv):
        # Leer el archivo CSV existente
        df = pd.read_csv(archivo_csv)
    else:
        # Si no existe, creamos un DataFrame vacío con las columnas necesarias
        df = pd.DataFrame(columns=['Fecha', 'MesReporte', 'AñoReporte', 'TipoRegistro', 'Monto'])

    # Crear un nuevo registro como DataFrame
    nuevo_registro = pd.DataFrame([[fecha, mes_reporte, anio_reporte, tipo_registro, monto]], 
                                  columns=['Fecha', 'MesReporte', 'AñoReporte', 'TipoRegistro', 'Monto'])
    
    # Concatenar el nuevo registro al DataFrame existente
    df = pd.concat([df, nuevo_registro], ignore_index=True)

    # Guardar el DataFrame actualizado de nuevo en el archivo CSV
    df.to_csv(archivo_csv, index=False)
def generar_registro():
    while True:
        # Obtener la fecha actual
        archivo_csv = generar_nombre_archivo()
        now = datetime.now()
        fecha = now.strftime("%d-%m-%Y")  # Día-Mes-Año
        mes_reporte = now.month
        anio_reporte = now.year

        # Elegir aleatoriamente el tipo de registro y el monto
        tipo_registro = random.choice(["Ingreso", "Egreso"])
        monto = random.randint(100, 50000)  # Generar monto aleatorio entre 1000 y 5000

        # Escribir en el archivo CSV
        escribir_en_csv(archivo_csv, fecha, mes_reporte, anio_reporte, tipo_registro, monto)

        print(f"Registro generado: {fecha}, {mes_reporte}, {anio_reporte}, {tipo_registro}, {monto}")
        print(f"El archivo generado es: {archivo_csv}")

        # Esperar 2 minutos antes de generar el siguiente registro
        time.sleep(120)
if __name__ == "__main__":
    generar_registro()