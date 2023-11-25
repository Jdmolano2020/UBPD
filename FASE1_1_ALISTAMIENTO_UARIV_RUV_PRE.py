import json
import time
import pyodbc
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# Guarda el tiempo de inicio
start_time = time.time()

# Configurar la codificación Unicode
encoding = "ISO-8859-1"

with open('config.json') as config_file:
    config = json.load(config_file)

DIRECTORY_PATH = config['DIRECTORY_PATH']
DB_SERVER = config['DB_SERVER']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_DATABASE = "PRD_QPREP_UBPD"
DB_SCHEMA = "dbo"
DB_TABLE = "UARIV_UNI_VIC_LB"

db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)

Session = sessionmaker(bind=engine)
session = Session()

# Nombre del procedimiento almacenado
stored_procedure_name = 'CONSULTA_UARIV_EXCLUYENDO_DESPLA_FORZA'
#############
query = f"EXEC {stored_procedure_name}"
result = session.execute(query)
session.commit()


# Esperar a que el procedimiento almacenado termine (ajusta el tiempo de espera según sea necesario)
tiempo_espera = 5  # segundos
while True:
    # Verificar si el procedimiento ha terminado consultando la tabla de resultados
    DB_TABLE = "UARIV_UNI_VIC_LB_"
    result_count = session.execute(f'SELECT count(*) FROM {DB_DATABASE}.{DB_SCHEMA}.{DB_TABLE}').scalar()

    if result_count > 0:
        break

    # Esperar y luego volver a verificar
    time.sleep(tiempo_espera)


# Cargar los resultados en un DataFrame de pandas
#DB_TABLE = "UARIV_UNI_VIC_LB_"
#df_uariv_pre = pd.read_sql_query(f'SELECT * FROM {DB_DATABASE}.{DB_SCHEMA}.{DB_TABLE}', engine)
#len(df_uariv_pre)
# Cerrar la sesión
session.close()
##############
end_time = time.time()

# Calcula el tiempo transcurrido
elapsed_time = end_time - start_time

print(f"Tiempo transcurrido: {elapsed_time/60} segundos")
