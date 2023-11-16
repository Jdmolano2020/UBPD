import os
import json
import time
import subprocess
import pandas as pd
import numpy as np
import homologacion.etnia
import homologacion.fecha
import homologacion.nombres
import homologacion.limpieza
import homologacion.documento
import homologacion.nombre_completo
from sqlalchemy import create_engine, text
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_ELN
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_FARC
import FASE1_HOMOLOGACION_CAMPO_OTRAS_GUERRILLAS
import FASE1_HOMOLOGACION_CAMPO_BANDAS_CRIMINALES
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_PARAMILITARES
import FASE1_HOMOLOGACION_CAMPO_FUERZA_PUBLICA_Y_AGENTES_DEL_ESTADO


def clean_text(text):
    if text is None or text.isna().any():
        text = text.astype(str)      
    text = text.apply(homologacion.limpieza.normalize_text)
    return text

# Guarda el tiempo de inicio
start_time = time.time()


#import config
with open('config.json') as config_file:
    config = json.load(config_file)

DIRECTORY_PATH = config['DIRECTORY_PATH']
DB_SERVER = config['DB_SERVER']
DB_DATABASE = config['DB_DATABASE']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_SCHEMA = "orq_salida"
DB_TABLE = "RSB"

# Cambiar de directorio
archivo_a_borrar = os.path.join("fuentes secundarias",
                                "V_UBPD_RSB.csv")

    
if DIRECTORY_PATH:
    if os.path.exists(archivo_a_borrar):
        os.remove(archivo_a_borrar)

encoding = "ISO-8859-1"
# La codificación ISO-8859-1 no es necesaria en Python ya que usa Unicode por defecto

# Conexión a la base de datos usando pyodbc
# Configurar la cadena de conexion
db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)
# Cargar datos desde la base de datos
sql_query = f'SELECT * FROM {DB_SCHEMA}.{DB_TABLE}'
df = pd.read_sql(sql_query, engine)


# Convertir nombres de columnas a minúsculas (lower)
df.columns = [col.lower() for col in df.columns]
    
# Guardar el DataFrame en un archivo
csv_file_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias", "V_UBPD_RSB.csv")

df.to_csv(csv_file_path, index=False)

# Etiquetar y eliminar observaciones duplicadas
df['duplicates_reg'] = df.duplicated()
df = df[~df['duplicates_reg']]
#cadena de nombres de columnas que inician por anio_nacimiento
year_of_birth_columns = df.filter(regex='^anio_nacimiento')
column_names = year_of_birth_columns.columns.tolist()

# 1. Ordenar el DataFrame
columns_to_sort = [
    "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido",
    "documento", "sexo", "edad_des_inf", "edad_des_sup"] + list(column_names) + [ 
    "mes_nacimiento", 
    "dia_nacimiento", "iden_orientacionsexual", "iden_pertenenciaetnica",
    "pais_de_ocurrencia", "codigo_dane_departamento", "codigo_dane_municipio",
    "fecha_ocur_dia", "fecha_ocur_mes", "fecha_ocur_anio", "tipo_de_hecho",
    "presunto_responsable", "descripcion_relato", "codigo_unico_fuente"
]
df.columns = [col.replace(" ", "_") for col in df.columns]
df.sort_values(columns_to_sort, inplace=True)#quitar ordenamientos

try:
    df.rename(columns={'fuente': 'tabla_origen'}, inplace=True)
except KeyError:
    # Si la columna 'fuente' no existe, se captura el error y no se realiza ninguna acción
    pass

# Crear una nueva columna "in_ubpd" y asignarle el valor constante 1
df['in_ubpd'] = 1
df['codigo_unico_fuente_'] = 'codigo_unico_fuente'

#Asigna valor posterior a formatear a 6 digitos en texto relleno a la izquierda con ceros
df['codigo_unico_fuente_'] = df['codigo_unico_fuente'].apply(lambda x: f"{int(x):06}")
df.drop(columns=['codigo_unico_fuente'], inplace=True)

df.rename(columns={'codigo_unico_fuente_': 'codigo_unico_fuente'}, inplace=True)

#Definición de ruta y exportacion de dataframe actual
# Guardar el DataFrame en un archivo
csv_file_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias", "V_UBPD_RSB.csv")
df.to_csv(csv_file_path, index=False)

#1. Seleccionar variables que serán homologadas para la integración
# Exclusion de las que no se van a tener en cuenta
df.drop(columns=["tipo_de_otro_nombre", "otro_nombre", "iden_orientacionsexual", "lude_territoriocolectivo", "rein_nombre", "anio_nacimiento_ini", "anio_nacimiento_fin", "tipo_de_documento"], inplace=True)

#Variables a aplicar script depuracion
columns_to_normalize = ["nombre_completo", "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido", "pais_de_ocurrencia", "presunto_responsable", "sexo", "tipo_de_hecho"]

#2. Normalización de los campos de texto
df[columns_to_normalize] = df[columns_to_normalize].apply(clean_text)

na_equal = {
    'ND': "",
    'NA': "",
    'NR': ""
}

equal_conditions = {value: lambda x, val=value: str(x) == val for value in na_equal.keys()}

# Aplicar la transformación de igualdad
for col in columns_to_normalize:
    for value, func in equal_conditions.items():
        df[col] = df[col].apply(lambda x: '' if func(x) else x)
        
na_content = {
    'NO APLICA': lambda x: ('NO ' in str(x)) and ('APLICA' in str(x)),
    'NULL': lambda x: 'NULL' in str(x),
    'SIN INFOR': lambda x: ('SIN' in str(x)) and ('INFOR' in str(x)),
    'NO SABE': lambda x: ('NO' in str(x)) and ('SABE' in str(x)),
    'DESCONOCIDO': lambda x: 'DESCONOCIDO' in str(x),
    'POR ESTABLECER': lambda x: ('POR' in str(x)) and ('ESTABLECER' in str(x)),
    'SIN DEFINIR': lambda x: ('SIN' in str(x)) and ('DEFINIR' in str(x)),
    'SIN ESTABLECER': lambda x: ('SIN' in str(x)) and ('ESTABLECER' in str(x)),
    'POR DEFINIR': lambda x: ('POR' in str(x)) and ('DEFINIR' in str(x)),
}

# Aplicar transformación
for col in df.columns:
    for condition, func in na_content.items():
        df[col] = df[col].apply(lambda x: '' if func(x) else x)

#3. Homologación de estructura, formato y contenido
#Datos sobre los hechos	
#Lugar de ocurrencia- País/Departamento/Muncipio
df.rename(columns={
    'pais_de_ocurrencia': 'pais_ocurrencia',
    'departamento_de_ocurrencia': 'departamento_ocurrencia',
    'municipio_de_ocurrencia': 'municipio_ocurrencia'
}, inplace=True)

# Realizar un merge con el archivo DIVIPOLA_departamentos_122021.dta
dane = pd.read_stata(DIRECTORY_PATH + "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")

# Realizar la unión (left join) con "dane"
df = pd.merge(df, dane, how='left', left_on=['codigo_dane_departamento', 'codigo_dane_municipio'],
                right_on=['codigo_dane_departamento', 'codigo_dane_municipio'])


# fecha de ocurrencia 
df['fecha_ocur_anio'] = pd.to_numeric(df['fecha_ocur_anio'], errors='coerce')
df['fecha_ocur_mes'] = pd.to_numeric(df['fecha_ocur_mes'], errors='coerce')
df['fecha_ocur_dia'] = pd.to_numeric(df['fecha_ocur_dia'], errors='coerce')
homologacion.fecha.fechas_validas (df,fecha_dia = 'fecha_ocur_dia', 
                                   fecha_mes = 'fecha_ocur_mes',
                                   fecha_anio = 'fecha_ocur_anio',
                                   fecha = 'fecha_desaparicion_dtf',
                                   fechat= 'fecha_desaparicion')

# Borrar registros si el año tiene menos de 4 dígitos
df = df[df['fecha_ocur_anio'].apply(lambda x: len(x) == 4 if isinstance(x, str) else False)]

# Corrección de errores tipográficos
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].apply(lambda x: x.replace("18", "19", 1) if isinstance(x, str) and x.startswith("18") else x)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].apply(lambda x: x.replace("179", "197", 1) if isinstance(x, str) and x.startswith("179") else x)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].apply(lambda x: x.replace("169", "196", 1) if isinstance(x, str) and x.startswith("169") else x)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].apply(lambda x: x.replace("159", "195", 1) if isinstance(x, str) and x.startswith("159") else x)

# Convertir la columna "fecha_ocur_anio" a tipo de datos numérico
df['fecha_ocur_anio'] = pd.to_numeric(df['fecha_ocur_anio'], errors='coerce')

# Generar una nueva columna "fecha_ocur_anio_dtf" como número real
df['fecha_ocur_anio_dtf'] = df['fecha_ocur_anio'].astype(float)

# Concatenar las columnas de año, mes y día si todas contienen valores no nulos
df['fecha_desaparicion'] = df.apply(lambda row: f"{row['fecha_ocur_anio']}{row['fecha_ocur_mes']}{row['fecha_ocur_dia']}" if all(row[['fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia']].notnull()) else "", axis=1)

# Convertir la columna 'fecha_desaparicion' a formato datetime
df['fecha_desaparicion_dtf'] = pd.to_datetime(df['fecha_desaparicion'], errors='coerce')

# Formatear la columna 'fecha_desaparicion_dtf' como fecha (%d representa el formato de día-mes-año)
df['fecha_desaparicion_dtf'] = df['fecha_desaparicion_dtf'].dt.strftime('%d-%m-%Y')

# Eliminar las columnas de año, mes y día originales
df.drop(['fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia'], axis=1, inplace=True)

# Convertir la columna "presunto_responsable" a cadena
df['presunto_responsable'] = df['presunto_responsable'].astype(str)
# Reemplazar las celdas que contienen un punto (".") con un valor vacío ("")
df['presunto_responsable'] = np.where(df['presunto_responsable'].isna(),"", df['presunto_responsable'])

# Tipo de responsable
# Paramilitares
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_PARAMILITARES.homologar_paramilitares(df)
# Bandas criminales y grupos armados posdesmovilización
FASE1_HOMOLOGACION_CAMPO_BANDAS_CRIMINALES.homologar_bandas_criminales(df)
# Fuerza pública y agentes del estado
FASE1_HOMOLOGACION_CAMPO_FUERZA_PUBLICA_Y_AGENTES_DEL_ESTADO.homologar_fuerzapublica(df)
# FARC
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_FARC.homologar_farc(df)
# ELN
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_ELN.homologar_eln(df)
# Otra guerrilla y grupo guerrillero no determinado
FASE1_HOMOLOGACION_CAMPO_OTRAS_GUERRILLAS.homologar_otras_guerrillas(df)
# Otro actor- PENDIENTE
# Aplicar las condiciones y asignar 1 a pres_resp_otro cuando todas las condiciones se cumplan.
df['pres_resp_otro'] = 0
df.loc[(df['presunto_responsable'] != "") &
       (df['presunto_responsable'] != "X") &
       (df['presunto_responsable'].str.contains("PENDIENTE") == False) &
       (df['presunto_responsable'].str.contains("INFORMACION") == False) &
       (df['presunto_responsable'].str.contains("DETERMINAR") == False) &
       (df['presunto_responsable'].str.contains("PRECISAR") == False) &
       (df['presunto_responsable'].str.contains("REFIERE") == False) &
       (df['presunto_responsable'].str.contains("IDENTIFICADA") == False) &
       (df['pres_resp_paramilitares'].isna()) &
       (df['pres_resp_grupos_posdesmov'].isna()) &
       (df['pres_resp_agentes_estatales'].isna()) &
       (df['pres_resp_guerr_farc'].isna()) &
       (df['pres_resp_guerr_eln'].isna()) &
       (df['pres_resp_guerr_otra'].isna()), 'pres_resp_otro'] = 1


# Tipos de hechos de interés: Desaparición Forzada, Secuestro y Reclutamiento
# Convertir la columna "tipo_de_hecho" a cadena
df['tipo_de_hecho'] = df['tipo_de_hecho'].astype(str)
# Crear variables binarias basadas en "tipo_de_hecho"
df['TH_DF'] = (df['tipo_de_hecho'].str.contains("DESAPARICION", case=False) & df['tipo_de_hecho'].str.contains("FORZADA", case=False)).astype(int)
df['TH_SE'] = (df['tipo_de_hecho'].str.contains("SECUESTRO", case=False)).astype(int)
df['TH_RU'] = (df['tipo_de_hecho'].str.contains("RECLUTAMIENTO", case=False)).astype(int)
df['TH_OTRO'] = ((df['TH_DF'] == 0) & (df['TH_SE'] == 0) & (df['TH_RU'] == 0)).astype(int)
# Convertir el texto en la columna "descripcion_relato" a mayúsculas
df['descripcion_relato'] = df['descripcion_relato'].str.upper()


# Datos sobre las personas dadas por desaparecidos
# Nombres y apellidos
# Corrección del uso de artículos y preposiciones en los nombres
# Eliminar nombres y apellidos que solo tienen una letra inicial
homologacion.nombres.nombres_validos (df , primer_nombre = 'primer_nombre',
                 segundo_nombre = 'segundo_nombre',
                 primer_apellido = 'primer_apellido',
                 segundo_apellido = 'segundo_apellido',
                 nombre_completo = 'nombre_completo')

# Documento de identificación
homologacion.documento.documento_valida (df, documento = 'documento')

df['sexo'].replace('INTERSEXUAL', 'INTERSEX', inplace=True)

# Pertenencia_etnica [NARP; INDIGENA; RROM; MESTIZO]
# Renombrar la columna
homologacion.etnia.etnia_valida (df, etnia = 'iden_pertenenciaetnica')

# Validar rango de fecha de nacimiento
#Identificar inconsistencias en el año de nacimiento
homologacion.fecha.fechas_validas (df,fecha_dia = 'dia_nacimiento', fecha_mes = 'mes_nacimiento', fecha_anio = 'anio_nacimiento', fechat = 'fecha_nacimiento', fecha = 'fecha_nacimiento_dft')

# Validar rango de edad
# Primero, convierte las columnas 'edad_des_sup' y 'edad_des_inf' a numéricas
df[['edad_des_sup', 'edad_des_inf']] = df[['edad_des_sup', 'edad_des_inf']].apply(pd.to_numeric, errors='coerce')


df['edad'] = 0
# Luego, aplica las condiciones y realiza los reemplazos
df['edad'] = np.where((df['edad_des_sup'].notnull()) & (df['edad_des_inf'].isnull()), df['edad_des_sup'], df['edad'])


df['edad'] = np.where((df['edad_des_inf'].notnull()) & (df['edad_des_sup'].isnull()), df['edad_des_inf'], df['edad'])
df['edad'] = np.where((np.abs(df['edad_des_sup'] - df['edad_des_inf']) <= 10) &
                      (df['edad_des_sup'].notnull()) & (df['edad_des_inf'].notnull()) &
                      (df['edad_des_sup'] >= df['edad_des_inf']), df['edad_des_sup'], df['edad'])
df['edad'] = np.where((np.abs(df['edad_des_sup'] - df['edad_des_inf']) <= 10) &
                      (df['edad_des_sup'].notnull()) & (df['edad_des_inf'].notnull()) &
                      (df['edad_des_inf'] > df['edad_des_sup']), df['edad_des_inf'], df['edad'])
df['edad'] = np.where(df['edad'] > 100, np.nan, df['edad'])
df['edad'] = np.where(((df['edad_des_inf'] == 0) & (df['edad_des_sup'] == 0)) |
                      ((df['edad_des_inf'].isnull()) & (df['edad_des_sup'] == 0)) |
                      ((df['edad_des_inf'] == 0) & (df['edad_des_sup'].isnull())), 0, df['edad'])

# Finalmente, elimina las columnas 'edad_des_sup' y 'edad_des_inf'
df.drop(['edad_des_sup', 'edad_des_inf'], axis=1, inplace=True)
####hasta aca corre

df.['fecha_ocur_anio'] = np.where((df['fecha_ocur_anio'].str.len() < 1),
                                  "0", df['fecha_ocur_anio'])
df.['anio_nacimiento'] = np.where((df['anio_nacimiento'].str.len() < 1),
                                  "0", df['anio_nacimiento'])



df['fecha_nacimiento_dft'] = pd.to_datetime(df['fecha_nacimiento_dft'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
df['fecha_desaparicion_dtf'] = pd.to_datetime(df['fecha_desaparicion_dtf'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')


# Realizar operaciones con las fechas
df['edad_desaparicion_est'] =  df['fecha_desaparicion_dtf'] - df['fecha_nacimiento_dft'] 


df['edad_desaparicion_est'] = ((df['fecha_desaparicion_dtf'].dt.year - df['fecha_nacimiento_dft'].dt.year) -
                                ((df['fecha_desaparicion_dtf'].dt.month - df['fecha_nacimiento_dft'].dt.month) +
                                (df['fecha_desaparicion_dtf'].dt.day - df['fecha_nacimiento_dft'].dt.day)) / 12).round()


df['dif_edad'] = np.abs(df['edad_desaparicion_est'] - df['edad'])
p90 = df['dif_edad'].quantile(0.90)
df['inconsistencia_fechas'] = np.where(((df['edad_desaparicion_est'] < 0) | (df['edad_desaparicion_est'] > 100)) & (df['edad_desaparicion_est'].notna()), True, False)
df['inconsistencia_fechas'] = np.where((df['dif_edad'] > p90) & (df['dif_edad'].notna()), 2, df['inconsistencia_fechas'])
df['inconsistencia_fechas'] = np.where((df['fecha_nacimiento_dft'] == df['fecha_desaparicion_dtf']) & (df['fecha_nacimiento_dft'].notna()) & (df['fecha_desaparicion_dtf'].notna()), 3, df['inconsistencia_fechas'])
# Limpiar valores en columnas relacionadas con fechas y edad


#date_cols = ['fecha_nacimiento_dft', 'fecha_nacimiento', 'anio_nacimiento',
            # 'mes_nacimiento', 'dia_nacimiento','edad','edad_desaparicion_est']

# Iterar sobre las columnas que contienen 'nacimiento' o 'edad'
for col in df.columns:
    if 'nacimiento' in col or 'edad' in col:
        # Reemplazar con valor nulo para las filas con inconsistencia_fechas diferente de 0
        df.loc[df['inconsistencia_fechas'] != 0, col] = np.nan

# Reemplazar la columna 'edad' con 'edad_desaparicion_est' donde sea necesario
df.loc[df['edad_desaparicion_est'].notnull() & df['edad'].isnull(), 'edad'] = df['edad_desaparicion_est']

df.drop(['edad_desaparicion_est'], axis=1, inplace=True)

# Convertir la columna 'situacion_actual_des' a cadena
df['situacion_actual_des'] = df['situacion_actual_des'].astype(str)
# Reemplazar con cadena vacía los valores que son igual a '.'
df.loc[df['situacion_actual_des'] == '.', 'situacion_actual_des'] = ''

# Ejecuta el script
subprocess.run(['python', 'FASE1_ALISTAMIENTO_UBPD_DTPECV.py'])

nrow_df = len(df)
print("Registros despues left dane depto muni:",nrow_df)

# Guarda el tiempo de finalización
end_time = time.time()

# Calcula el tiempo transcurrido
elapsed_time = end_time - start_time

print(f'Tiempo transcurrido: {elapsed_time/60} segundos')


































