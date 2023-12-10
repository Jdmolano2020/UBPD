import os
import json
import time
import yaml
import pandas as pd
from sqlalchemy import create_engine
import homologacion.limpieza
import homologacion.fecha
import homologacion.nombres
import homologacion.documento
import homologacion.etnia
import homologacion.nombre_completo

# Guarda el tiempo de inicio
start_time = time.time()


with open('config.json') as config_file:
    config = json.load(config_file)

DIRECTORY_PATH = config['DIRECTORY_PATH']

delete_file_path = os.path.join("fuentes secundarias",
                                "V_JEP_MACROCASO_003.csv")

if DIRECTORY_PATH:
    if os.path.exists(delete_file_path):
        os.remove(delete_file_path)
DIRECTORY_PATH = config['DIRECTORY_PATH']
DB_SERVER = config['DB_SERVER']
DB_INSTANCE = config['DB_INSTANCE']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_DATABASE = "UNIVERSO_PDD"

encoding = "ISO-8859-1"
# La codificación ISO-8859-1

# Conexión a la base de datos usando pyodbc
# Configurar la cadena de conexion
db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}\\{DB_INSTANCE}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)

# Macrocaso 03
excel_path = os.path.join(
    DIRECTORY_PATH, "fuentes secundarias",
    "13092021_VíctimasCaso003_2002a2008.xlsx")

sheet_excel = "Dato a Dato por fuente 6402"
df_003 = pd.read_excel(excel_path, sheet_name=sheet_excel, header=0)
################################
nrow_df = len(df_003)
print("Registros iniciales:", nrow_df)
nrow_df_ini = nrow_df
# Convertir nombres de columnas a minúsculas (lower)
df_003.columns = [col.lower() for col in df_003.columns]

# Etiquetar y eliminar observaciones duplicadas
df_003['duplicates_reg'] = df_003.duplicated()
df_003 = df_003[~df_003['duplicates_reg']]
################################
nrow_df = len(df_003)
print("Registros poseliminacion duplicados:", nrow_df)

# Crear la nueva variable 'tabla_origen' concatenando
# "MACROCASO_003_" y el valor de 'fuente'
df_003['tabla_origen'] = "MACROCASO_003" + "_" + df_003['fuente'].astype(str)

# Eliminar la columna 'fuente'
df_003.drop('fuente', axis=1, inplace=True)

# Crear la nueva variable 'in_macro003' e inicializarla con el valor 1
df_003['in_macro003'] = 1

# Crear la nueva columna 'codigo_unico_fuente' concatenando
# las columnas id_dedup e id_fuente
df_003['codigo_unico_fuente'] = df_003[
    'id_dedup'].astype(str) + " - " + df_003['id_fuente']

# Ordenar el DataFrame por la columna 'codigo_unico_fuente'
df_003.sort_values('codigo_unico_fuente', inplace=True)

# Eliminar las columnas id_dedup e id_fuente
df_003.drop(['id_dedup', 'id_fuente'], axis=1, inplace=True)

# Reordenar el DataFrame primero por 'tabla' y luego por 'codigo_unico_fuente'
df_003.sort_values(['tabla_origen', 'codigo_unico_fuente'], inplace=True)

# exportar actual dataframe a .csv
V_JEP_MACROCASO_003_file_path = os.path.join(
    DIRECTORY_PATH, "archivos depurados", "V_JEP_MACROCASO_003.csv")
df_003.to_csv(V_JEP_MACROCASO_003_file_path, index=False)

# Datos sobre las personas dadas por desaparecidos
# Nombres y apellidos
df_003['nombre_completo'] = ""

# Corrección del uso de artículos y preposiciones en los nombres
# Eliminar nombres y apellidos que solo tienen una letra inicial
homologacion.nombres.nombres_validos(df_003, primer_nombre='primer_nombre',
                                     segundo_nombre='seg_nombre',
                                     primer_apellido='primer_apellido',
                                     segundo_apellido='seg_apellido',
                                     nombre_completo='nombre_completo')


# 3. Homologación de estructura, formato y contenido
# Datos sobre los hechos
# Genere 'codigo_dane_municipio_' como cadena con formato "%05.0f"
df_003['codigo_dane_municipio_'] = df_003[
    'codigo_dane_municipio'].apply(lambda x: "{:05.0f}".format(x))

# Elimine la columna original 'codigo_dane_municipio'
df_003.drop(columns=['codigo_dane_municipio'], inplace=True)

# Renombre 'codigo_dane_municipio_' a 'codigo_dane_municipio'
df_003.rename(columns={'codigo_dane_municipio_': 'codigo_dane_municipio'},
              inplace=True)

# Reemplace 'codigo_dane_municipio' con "05042"
# cuando depto=="ANTIOQUIA" & municipio=="SANTAFE DE ANTIOQUIA"
df_003.loc[(df_003['depto'] == "ANTIOQUIA") &
           (df_003['municipio'] == "SANTAFE DE ANTIOQUIA"),
           'codigo_dane_municipio'] = "05042"

# Elimine las columnas 'municipio' y 'depto'
df_003.drop(columns=['municipio', 'depto'], inplace=True)

dane = pd.read_stata(
    DIRECTORY_PATH + "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")

# Realizar la unión (left join) con "dane"
df_003 = pd.merge(
    df_003, dane[['codigo_dane_municipio', 'municipio', 'departamento']],
    how='left', left_on=['codigo_dane_municipio'],
    right_on=['codigo_dane_municipio'], indicator=True).replace(
                   {'left_only': 1, 'right_only': 2, 'both': 3})

# Elimine las filas donde no hay coincidencia en la fusión (_merge==2)
df_003 = df_003[df_003['_merge'] != 2]


# Renombre las columnas resultantes de la fusión
df_003.rename(columns={'municipio': 'municipio_ocurrencia',
                       'departamento': 'departamento_ocurrencia'},
              inplace=True)

# Elimine las columnas '_merge'
df_003.drop(columns=['_merge'], inplace=True)

# obtener los primeros dos caracteres que corresponden al depto
df_003['codigo_dane_departamento'] = df_003['codigo_dane_municipio'].str[:2]
df_003['pais_ocurrencia'] = "COLOMBIA"

df_003.rename(columns={
    'annoh': 'fecha_ocur_anio',
    'mesh': 'fecha_ocur_mes',
    'diah': 'fecha_ocur_dia'}, inplace=True)

# fecha de ocurrencia
df_003['fecha_ocur_anio'] = pd.to_numeric(df_003['fecha_ocur_anio'],
                                          errors='coerce')
df_003['fecha_ocur_mes'] = pd.to_numeric(df_003['fecha_ocur_mes'],
                                         errors='coerce')
df_003['fecha_ocur_dia'] = pd.to_numeric(df_003['fecha_ocur_dia'],
                                         errors='coerce')
homologacion.fecha.fechas_validas(df_003, fecha_dia='fecha_ocur_dia',
                                  fecha_mes='fecha_ocur_mes',
                                  fecha_anio='fecha_ocur_anio',
                                  fecha='fecha_desaparicion_dtf',
                                  fechat='fecha_desaparicion')

# tratamiento a fecha ocur anio para transformaciones
# Reemplazar "." con cadena vacía
df_003['fecha_ocur_anio'] = df_003['fecha_ocur_anio'].replace('.', '')

df_003['fecha_ocur_anio'] = df_003['fecha_ocur_anio'].apply(
    lambda x: '' if (isinstance(x, int) or
                     isinstance(x, float)) and len(str(x)) != 4 else x)

df_003['fecha_ocur_anio'] = df_003['fecha_ocur_anio'].apply(
    lambda x: x.replace("18", "19", 1) if isinstance(x, str)
    and x.startswith("18") else x)
df_003['fecha_ocur_anio'] = df_003['fecha_ocur_anio'].apply(
    lambda x: x.replace("179", "197", 1) if isinstance(x, str)
    and x.startswith("179") else x)
df_003['fecha_ocur_anio'] = df_003['fecha_ocur_anio'].apply(
    lambda x: x.replace("169", "196", 1) if isinstance(x, str)
    and x.startswith("169") else x)
df_003['fecha_ocur_anio'] = df_003['fecha_ocur_anio'].apply(
    lambda x: x.replace("159", "195", 1) if isinstance(x, str)
    and x.startswith("159") else x)

df_003['fecha_ocur_anio_dtf'] = pd.to_numeric(df_003['fecha_ocur_anio'],
                                              errors='coerce')

df_003['fecha_desaparicion_dtf'] = df_003[
    'fecha_desaparicion_dtf'].dt.strftime('%d')

df_003.drop(columns=['fecha_ocur_anio_dtf'], inplace=True)

# Datos sobre las personas dadas por desparecidas
# Renombre columnas
df_003.rename(columns={'seg_nombre': 'segundo_nombre',
                       'seg_apellido': 'segundo_apellido'}, inplace=True)

df_003.rename(columns={'num_documento': 'documento'}, inplace=True)
df_003['documento'] = df_003['documento'].astype(str)
# Documento de identificación
homologacion.documento.documento_valida(df_003, documento='documento')

# 4. Eliminación de Registros No Identificados (registros
# sin datos suficientes para la individualización de las víctimas)
# Crear la variable 'non_miss' contando los valores no perdidos por fila
nrow_df_fin = len(df_003)
df_003['non_miss'] = df_003[['primer_nombre', 'segundo_nombre',
                             'primer_apellido', 'segundo_apellido']].apply(
                                 lambda row: row.apply(
                                     lambda x: x != '').sum(), axis=1)

# Crear la variable 'rni' basada en 'non_miss'
df_003['rni'] = df_003['non_miss'] < 2

df_003.loc[(df_003['codigo_dane_departamento'] == "") &
           (df_003['fecha_ocur_anio'] == "") &
           (df_003['documento'] == ""), 'rni'] = 1

df_003['rni_'] = df_003.groupby('codigo_unico_fuente')['rni'].transform('sum')
df_003['N'] = df_003.groupby('codigo_unico_fuente').cumcount() + 1

nrow_df = len(df_003)
print("Registros :", nrow_df)

df_003_rni = df_003[df_003['rni'] == 1]
nrow_df = len(df_003_rni)
print("Registros no identificados:", nrow_df)

BD_MACRO_003_PNI_file_path = os.path.join(
    DIRECTORY_PATH, "archivos depurados", "BD_MACRO_003_PNI.csv")
df_003_rni.to_csv(BD_MACRO_003_PNI_file_path, index=False)
nrow_df_no_ident = len(df_003_rni)
DB_SCHEMA = "version5"
DB_TABLE = "BD_MACRO_003_PNI"
chunk_size = 1000
with engine.connect() as conn, conn.begin():
    df_003_rni.to_sql(name=DB_TABLE, con=engine,
                      schema=DB_SCHEMA, if_exists='replace', index=False,
                      chunksize=chunk_size)

# Eliminar registros donde 'rni_' es igual a 'N'
df_003.drop(df_003[df_003['rni_'] == df_003['N']].index, inplace=True)

nrow_df = len(df_003)
print("Registros identificados:", nrow_df)
# Eliminar las columnas 'non_miss', 'N',
# y todas las columnas que comienzan con 'rni'
df_003.drop(columns=df_003.columns[
    df_003.columns.str.startswith('rni')].tolist() + ['non_miss', 'N'],
    inplace=True)

# 5. Identificación de registros únicos
# Ordenar el DataFrame por 'codigo_unico_fuente'
df_003.sort_values(by='codigo_unico_fuente', inplace=True)
nrow_df_ident = len(df_003)
n_duplicados = nrow_df_ini - nrow_df_ident
# Guardar el DataFrame ordenado en un archivo CSV
# (puedes ajustar el formato según tus necesidades)
BD_MACRO_003_file_path = os.path.join(
    DIRECTORY_PATH, "archivos depurados", "BD_MACRO_003.csv")
df_003.to_csv(BD_MACRO_003_file_path, index=False)

DB_SCHEMA = "version5"
DB_TABLE = "BD_MACRO_003"

with engine.connect() as conn, conn.begin():
    df_003.to_sql(name=DB_TABLE, con=engine,
                  schema=DB_SCHEMA, if_exists='replace', index=False,
                  chunksize=chunk_size)

# Registra el tiempo de finalización
end_time = time.time()

# Calcula el tiempo transcurrido
elapsed_time = end_time - start_time

print(f"Tiempo transcurrido: {elapsed_time/60} segundos")
fecha_fin = time.time()

log = {
    "fecha_inicio": str(start_time),
    "fecha_fin": str(fecha_fin),
    "tiempo_ejecucion": str(fecha_fin - start_time),
    'filas_iniciales_df': nrow_df_ini,
    'filas_final_df': nrow_df_fin,
    'filas_df_ident': nrow_df_ident,
    'filas_df_no_ident': nrow_df_no_ident,
    'n_duplicados': n_duplicados,
}

log_file_path = os.path.join(
    DIRECTORY_PATH, "log", "resultado_df_inml_cad.yaml")
with open(log_file_path, 'w') as file:
    yaml.dump(log, file)
