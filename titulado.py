import pandas as pd
from sqlalchemy import create_engine

DIRECTORY_PATH = "C:/A_UBPD/Fuentes/OrquestadorUniverso/"

df_valida = pd.read_stata(DIRECTORY_PATH+
    "archivos depurados/BD_UBPD_RSB.dta")

DB_SERVER= "localhost"
DB_USERNAME="sa"
DB_PASSWORD="ClaveSqlserver1"

db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)

DB_DATABASE = "UNIVERSO_PDD"
DB_SCHEMA = "depurados"
DB_TABLE = "BD_UBPD_RSB"


with engine.connect() as conn, conn.begin():
    conn.execute(f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{DB_SCHEMA}') BEGIN EXEC('CREATE SCHEMA {DB_SCHEMA}') END")

    
df_valida.to_sql(name=DB_TABLE, con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)





df_valida = pd.read_stata("fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")

db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)

df_valida.to_sql('BD_UBPD_RSB', con=engine, if_exists='replace', index=False)