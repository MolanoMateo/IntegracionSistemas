import time
import os
import pandas as pd
import numpy as np
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pyodbc
from datetime import datetime

server = 'UPOAULA10705'  # Ejemplo: 'localhost' o '192.168.1.1'
database = 'FTP'

class FTPHandlerB(FileSystemEventHandler):
    def on_created(self, event):
        # Detectar si es un archivo CSV
        if not event.is_directory and event.src_path.endswith(".csv"):
            print(f"Nuevo archivo detectado: {event.src_path}")
            df = leer_ultimo_csv()
            if not df.empty:
                try:
                    # Establece la conexión con autenticación de Windows
                    conexion = pyodbc.connect(
                        f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
                    )
                    print("Conexión exitosa")
                
                    # Ejemplo de consulta
                    cursor = conexion.cursor()
                    fecha_actual = datetime.now()
                    errores = []
                    # Validar e insertar cada fila del DataFrame
                    for index, row in df.iterrows():
                        error = False  # Bandera para verificar si hay error en la fila actual

                        # Convertir Fecha a datetime y validar
                        try:
                            fecha = datetime.strptime(row['Fecha'], '%d-%m-%Y')
                            if fecha > fecha_actual:
                                print(f"Fila {index}: La fecha {row['Fecha']} es mayor a la fecha actual.")
                                error = True
                        except ValueError:
                            print(f"Fila {index}: Error de formato en la fecha {row['Fecha']}. Debe estar en formato YYYY-MM-DD.")
                            error = True

                        # Validar MesReporte y AñoReporte
                        if row['MesReporte'] > fecha_actual.month and row['AñoReporte'] >= fecha_actual.year:
                            print(f"Fila {index}: El MesReporte {row['MesReporte']} y AñoReporte {row['AñoReporte']} son inválidos.")
                            error = True

                        # Validar TipoRegistro
                        if row['TipoRegistro'] not in ["Ingreso", "Egreso"]:
                            print(f"Fila {index}: El TipoRegistro '{row['TipoRegistro']}' no es válido. Debe ser 'Ingreso' o 'Egreso'.")
                            error = True

                        # Validar que Monto es numérico
                        try:
                            monto = int(row['Monto'])
                        except ValueError:
                            print(f"Fila {index}: El monto '{row['Monto']}' no es numérico.")
                            error = True

                        # Si alguna validación falla, agregar la fila a la lista de errores
                        if error:
                            errores.append(row)
                            continue  # Saltar a la siguiente fila sin hacer la inserción
                        else:
                            # Insertar cada fila del DataFrame en la tabla
                            for index, row in df.iterrows():
                                cursor.execute(
                                    "INSERT INTO dbo.Registros (Fecha, MesReporte, AñoReporte, TipoRegistro, Monto) VALUES (?, ?, ?, ?, ?)",
                                    row['Fecha'], row['MesReporte'], row['AñoReporte'], row['TipoRegistro'], row['Monto']
                                )
                        
                            # Confirmar los cambios en la base de datos
                            conexion.commit()
                            print("Datos insertados con éxito.")
                
                    # Cierra la conexión
                    cursor.close()
                    conexion.close()
                except Exception as e:
                    print("Error al conectar:", e)
                if errores:
                    errores_df = pd.DataFrame(errores)
                    errores_df.to_csv("errores.csv", index=False)
                    print("Algunas filas no pasaron las validaciones y se han guardado en 'errores.csv'.")
                else:
                    print("Todas las filas se insertaron correctamente.")


def monitorizar_carpeta_ftp():
    # Usar os.getcwd() para obtener el directorio de trabajo actual
    carpeta_ftp = os.path.join(os.getcwd(), "FTP")
    
    event_handler = FTPHandlerB()
    observer = Observer()
    observer.schedule(event_handler, carpeta_ftp, recursive=False)
    
    observer.start()
    print(f"Monitorizando la carpeta: {carpeta_ftp}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

# Ruta de la carpeta 'FTP' donde se encuentran los archivos CSV
carpeta = 'FTP'

# Función para leer el archivo CSV más reciente de la carpeta 'FTP'
def leer_ultimo_csv():
    # Listar todos los archivos en la carpeta 'FTP'
    archivos = os.listdir(carpeta)
    
    # Filtrar solo los archivos .csv
    archivos_csv = [archivo for archivo in archivos if archivo.endswith('.csv')]
    
    # Si no hay archivos CSV, retornamos un DataFrame vacío
    if not archivos_csv:
        print("No se encontraron archivos CSV en la carpeta 'FTP'.")
        return pd.DataFrame()

    # Ordenar los archivos por la fecha de creación (más reciente primero)
    archivos_csv.sort(key=lambda x: os.path.getmtime(os.path.join(carpeta, x)), reverse=True)

    # Tomamos el archivo más reciente
    archivo_csv_reciente = archivos_csv[0]

    # Construir la ruta completa del archivo CSV
    ruta_csv = os.path.join(carpeta, archivo_csv_reciente)

    # Leer el archivo CSV en un DataFrame
    df = pd.read_csv(ruta_csv)

    # Mostrar información sobre el archivo leído
    print(f"Se ha cargado el archivo CSV: {archivo_csv_reciente}")
    
    return df

if __name__ == "__main__":
    # Iniciar monitorización de la carpeta FTP
    monitorizar_carpeta_ftp()