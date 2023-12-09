import os
import yaml
import json
import time
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

fecha_inicio = datetime.now()
# Guarda el tiempo de inicio
start_time = time.time()


#import config
with open('config.json') as config_file:
    config = json.load(config_file)

DIRECTORY_PATH = config['DIRECTORY_PATH']
DB_SERVER = config['DB_SERVER']
DB_INSTANCE = config['DB_INSTANCE']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_DATABASE = "UNIVERSO_PDD"
DB_SCHEMA = "orq_salida"
DB_TABLE = "INMLCF_DES_DATOS_DE_REGISTRO"

encoding = "ISO-8859-1"
# La codificación ISO-8859-1

# Conexión a la base de datos usando pyodbc
# Configurar la cadena de conexion
db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}\\{DB_INSTANCE}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)

sql_query = f"SELECT * FROM {DB_DATABASE}.{DB_SCHEMA}.{DB_TABLE} WITH (NOLOCK)"
df_rnd = pd.read_sql(sql_query, engine)