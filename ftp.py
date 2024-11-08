from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import os

def iniciar_servidor_ftp():
    authorizer = DummyAuthorizer()
    
    carpeta_ftp = os.path.join(os.getcwd(), "FTP")
    if not os.path.exists(carpeta_ftp):
        os.makedirs(carpeta_ftp)
    
    authorizer.add_user("user", "12345", carpeta_ftp, perm="elradfmw")
    
    # Crear un manejador para el servidor FTP
    handler = FTPHandler
    handler.authorizer = authorizer

    servidor = FTPServer(("0.0.0.0", 2121), handler)
    
    # Iniciar el servidor
    print(f"Servidor FTP iniciado en el puerto 2121. Carpeta FTP: {carpeta_ftp}")
    servidor.serve_forever()

# Iniciar el servidor
if __name__ == "__main__":
    iniciar_servidor_ftp()