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
from sqlalchemy import create_engine
from FASE1_APARECIO_MUERTO_RSB import asigna_aparecio_muerto
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
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_DATABASE = "UNIVERSO_PDD"
DB_SCHEMA = "orq_salida"
DB_TABLE = "RSB"

# Cambiar de directorio
archivo_a_borrar = os.path.join("fuentes secundarias",
                                "V_UBPD_RSB.csv")

    
if DIRECTORY_PATH:
    if os.path.exists(archivo_a_borrar):
        os.remove(archivo_a_borrar)

encoding = "ISO-8859-1"
# La codificación ISO-8859-1

# Conexión a la base de datos usando pyodbc
# Configurar la cadena de conexion
db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)
# Cargar datos desde la base de datos
sql_query = f'SELECT * FROM {DB_DATABASE}.{DB_SCHEMA}.{DB_TABLE}'
df_rsb = pd.read_sql(sql_query, engine)

#38903
# Convertir nombres de columnas a minúsculas (lower)
df_rsb.columns = [col.lower() for col in df_rsb.columns]
    
# Etiquetar y eliminar observaciones duplicadas
df_rsb['duplicates_reg'] = df_rsb.duplicated()
df_rsb = df_rsb[~df_rsb['duplicates_reg']]

#cadena de nombres de columnas que inician por anio_nacimiento
year_columns = [col for col in df_rsb.columns if col.startswith('anio_nacimiento')]
age_columns = [col for col in df_rsb.columns if col.startswith('edad')]

# 1. Ordenar el DataFrame
columns_to_sort = [
    "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido",
    "documento", "sexo"] + age_columns + year_columns + ["mes_nacimiento", 
    "dia_nacimiento", "iden_orientacionsexual", "iden_pertenenciaetnica",
    "pais_de_ocurrencia", "codigo_dane_departamento", "codigo_dane_municipio",
    "fecha_ocur_dia", "fecha_ocur_mes", "fecha_ocur_anio", "tipo_de_hecho",
    "presunto_responsable", "descripcion_relato", "codigo_unico_fuente"
    ]
df_rsb.columns = [col.replace(" ", "_") for col in df_rsb.columns]
df_rsb.sort_values(columns_to_sort, inplace=True)

try:
    df_rsb.rename(columns={'fuente': 'tabla_origen'}, inplace=True)
except KeyError:
    pass

# Crear una nueva columna "in_ubpd" y asignarle el valor constante 1
df_rsb['in_ubpd'] = 1

#Asigna valor posterior a formatear a 6 digitos en texto relleno a la izquierda con ceros
df_rsb['codigo_unico_fuente_'] = df_rsb['codigo_unico_fuente'].apply(lambda x: f"{int(x):06}")
df_rsb.drop(columns=['codigo_unico_fuente'], inplace=True)

df_rsb.rename(columns={'codigo_unico_fuente_': 'codigo_unico_fuente'}, inplace=True)

# Guardar el DataFrame en un archivo
csv_file_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias", "V_UBPD_RSB.csv")
df_rsb.to_csv(csv_file_path, index=False)

#1. Seleccionar variables que serán homologadas para la integración
# Exclusion de las que no se van a tener en cuenta
df_rsb.drop(columns=["tipo_de_otro_nombre", "otro_nombre", "iden_orientacionsexual", "lude_territoriocolectivo", "rein_nombre", "anio_nacimiento_ini", "anio_nacimiento_fin", "tipo_de_documento"], inplace=True)

#Variables a aplicar script depuracion
columns_to_normalize = ["nombre_completo", "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido", "pais_de_ocurrencia", "presunto_responsable", "sexo", "tipo_de_hecho"]

#2. Normalización de los campos de texto
df_rsb[columns_to_normalize] = df_rsb[columns_to_normalize].apply(clean_text)

na_equal = {
    'ND': "",
    'NA': "",
    'NR': ""
}

equal_conditions = {value: lambda x, val=value: str(x) == val for value in na_equal.keys()}

# Aplicar la transformación de igualdad
for col in columns_to_normalize:
    for value, func in equal_conditions.items():
        df_rsb[col] = df_rsb[col].apply(lambda x: '' if func(x) else x)
        
na_content = {
    'NO APLICA': lambda x: ('NO ' in str(x)) and ('APLICA' in str(x)),
    'NULL': lambda x: 'NULL' in str(x),
    'SIN INFOR': lambda x: ('SIN' in str(x)) and ('INFOR' in str(x)),
    'NO SABE': lambda x: ('NO' in str(x)) and ('SABE' in str(x)),
    'DESCONOCID': lambda x: 'DESCONOCID' in str(x),
    'POR ESTABLECER': lambda x: ('POR' in str(x)) and ('ESTABLECER' in str(x)),
    'SIN DEFINIR': lambda x: ('SIN' in str(x)) and ('DEFINIR' in str(x)),
    'SIN ESTABLECER': lambda x: ('SIN' in str(x)) and ('ESTABLECER' in str(x)),
    'POR DEFINIR': lambda x: ('POR' in str(x)) and ('DEFINIR' in str(x)),
}

# Aplicar transformación
for col in columns_to_normalize:
    for condition, func in na_content.items():
        df_rsb[col] = df_rsb[col].apply(lambda x: '' if func(x) else x)

#3. Homologación de estructura, formato y contenido
#Datos sobre los hechos	
#Lugar de ocurrencia- País/Departamento/Muncipio
df_rsb.rename(columns={
    'pais_de_ocurrencia': 'pais_ocurrencia',
    'departamento_de_ocurrencia': 'departamento_ocurrencia',
    'municipio_de_ocurrencia': 'municipio_ocurrencia'
}, inplace=True)

# Realizar un merge con el archivo DIVIPOLA_departamentos_122021.dta
dane = pd.read_stata(DIRECTORY_PATH + "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")

# Realizar la unión (left join) con "dane"
df_rsb = pd.merge(df_rsb, dane, how='left', left_on=['codigo_dane_departamento', 'codigo_dane_municipio'],
                right_on=['codigo_dane_departamento', 'codigo_dane_municipio'], indicator = True)
df_rsb['_merge'] = df_rsb['_merge'].map({'left_only': 1, 'right_only': 2, 'both': 3})

df_rsb = df_rsb[df_rsb['_merge'] != 2]
#38903

#Ordenar el DataFrame
columns_to_sort = ['tabla_origen', 'codigo_unico_fuente', 'nombre_completo', 'codigo_dane_departamento','departamento_ocurrencia']
df_rsb.sort_values(by=columns_to_sort, ascending=[True, True, True, False, True], inplace=True)

df_rsb.drop(columns=["departamento_ocurrencia", "_merge", "municipio_ocurrencia"], inplace=True)

df_rsb.rename(columns={
    'departamento': 'departamento_ocurrencia',
    'municipio': 'municipio_ocurrencia'
}, inplace=True)

# fecha de ocurrencia 
df_rsb['fecha_ocur_anio'] = pd.to_numeric(df_rsb['fecha_ocur_anio'], errors='coerce')
df_rsb['fecha_ocur_mes'] = pd.to_numeric(df_rsb['fecha_ocur_mes'], errors='coerce')
df_rsb['fecha_ocur_dia'] = pd.to_numeric(df_rsb['fecha_ocur_dia'], errors='coerce')
homologacion.fecha.fechas_validas (df_rsb,fecha_dia = 'fecha_ocur_dia', 
                                   fecha_mes = 'fecha_ocur_mes',
                                   fecha_anio = 'fecha_ocur_anio',
                                   fecha = 'fecha_desaparicion_dtf',
                                   fechat= 'fecha_desaparicion')

# Borrar registros si el año tiene menos de 4 dígitos
df_rsb = df_rsb[df_rsb['fecha_ocur_anio'].apply(lambda x: len(x) == 4 if isinstance(x, str) else False)]

# Corrección de errores tipográficos
df_rsb['fecha_ocur_anio'] = df_rsb['fecha_ocur_anio'].apply(lambda x: x.replace("18", "19", 1) if isinstance(x, str) and x.startswith("18") else x)
df_rsb['fecha_ocur_anio'] = df_rsb['fecha_ocur_anio'].apply(lambda x: x.replace("179", "197", 1) if isinstance(x, str) and x.startswith("179") else x)
df_rsb['fecha_ocur_anio'] = df_rsb['fecha_ocur_anio'].apply(lambda x: x.replace("169", "196", 1) if isinstance(x, str) and x.startswith("169") else x)
df_rsb['fecha_ocur_anio'] = df_rsb['fecha_ocur_anio'].apply(lambda x: x.replace("159", "195", 1) if isinstance(x, str) and x.startswith("159") else x)

# Convertir la columna "fecha_ocur_anio" a tipo de datos numérico
df_rsb['fecha_ocur_anio'] = pd.to_numeric(df_rsb['fecha_ocur_anio'], errors='coerce')

# Generar una nueva columna "fecha_ocur_anio_dtf" como número real
df_rsb['fecha_ocur_anio_dtf'] = df_rsb['fecha_ocur_anio'].astype(float)

# Concatenar las columnas de año, mes y día si todas contienen valores no nulos
df_rsb['fecha_desaparicion'] = df_rsb.apply(lambda row: f"{row['fecha_ocur_anio']}{row['fecha_ocur_mes']}{row['fecha_ocur_dia']}" if all(row[['fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia']].notnull()) else "", axis=1)

# Convertir la columna 'fecha_desaparicion' a formato datetime
df_rsb['fecha_desaparicion_dtf'] = pd.to_datetime(df_rsb['fecha_desaparicion'], errors='coerce')

# Tipo de responsable
# Paramilitares
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_PARAMILITARES.homologar_paramilitares(df_rsb)
# Bandas criminales y grupos armados posdesmovilización
FASE1_HOMOLOGACION_CAMPO_BANDAS_CRIMINALES.homologar_bandas_criminales(df_rsb)
# Fuerza pública y agentes del estado
FASE1_HOMOLOGACION_CAMPO_FUERZA_PUBLICA_Y_AGENTES_DEL_ESTADO.homologar_fuerzapublica(df_rsb)
# FARC
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_FARC.homologar_farc(df_rsb)
# ELN
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_ELN.homologar_eln(df_rsb)
# Otra guerrilla y grupo guerrillero no determinado
FASE1_HOMOLOGACION_CAMPO_OTRAS_GUERRILLAS.homologar_otras_guerrillas(df_rsb)
# Otro actor- PENDIENTE
# Aplicar las condiciones y asignar 1 a pres_resp_otro cuando todas las condiciones se cumplan.
df_rsb['pres_resp_otro'] = 0
df_rsb.loc[(df_rsb['presunto_responsable'] != "") &
       (df_rsb['presunto_responsable'] != "X") &
       (df_rsb['presunto_responsable'].str.contains("PENDIENTE") == False) &
       (df_rsb['presunto_responsable'].str.contains("INFORMACION") == False) &
       (df_rsb['presunto_responsable'].str.contains("DETERMINAR") == False) &
       (df_rsb['presunto_responsable'].str.contains("PRECISAR") == False) &
       (df_rsb['presunto_responsable'].str.contains("REFIERE") == False) &
       (df_rsb['presunto_responsable'].str.contains("IDENTIFICADA") == False) &
       (df_rsb['pres_resp_paramilitares'].isna()) &
       (df_rsb['pres_resp_grupos_posdesmov'].isna()) &
       (df_rsb['pres_resp_agentes_estatales'].isna()) &
       (df_rsb['pres_resp_guerr_farc'].isna()) &
       (df_rsb['pres_resp_guerr_eln'].isna()) &
       (df_rsb['pres_resp_guerr_otra'].isna()), 'pres_resp_otro'] = 1

# Convertir la columna "presunto_responsable" a cadena
df_rsb['presunto_responsable'] = df_rsb['presunto_responsable'].astype(str)
# Reemplazar las celdas que contienen un punto (".") con un valor vacío ("")
df_rsb['presunto_responsable'] = np.where(df_rsb['presunto_responsable'].isna(),"", df_rsb['presunto_responsable'])
df_rsb.drop(['presunto_responsable'], axis=1, inplace=True)


# Tipos de hechos de interés: Desaparición Forzada, Secuestro y Reclutamiento
# Convertir la columna "tipo_de_hecho" a cadena
df_rsb['tipo_de_hecho'] = df_rsb['tipo_de_hecho'].astype(str)
# Crear variables binarias basadas en "tipo_de_hecho"
df_rsb['TH_DF'] = (df_rsb['tipo_de_hecho'].str.contains("DESAPARICION", case=False) & df_rsb['tipo_de_hecho'].str.contains("FORZADA", case=False)).astype(int)
df_rsb['TH_SE'] = (df_rsb['tipo_de_hecho'].str.contains("SECUESTRO", case=False)).astype(int)
df_rsb['TH_RU'] = (df_rsb['tipo_de_hecho'].str.contains("RECLUTAMIENTO", case=False)).astype(int)
df_rsb['TH_OTRO'] = ((df_rsb['TH_DF'] == 0) & (df_rsb['TH_SE'] == 0) & (df_rsb['TH_RU'] == 0) & ~df_rsb['tipo_de_hecho'].str.contains("SIN", case=False) & ~df_rsb['tipo_de_hecho'].str.contains("DETERMINAR", case=False) & df_rsb['tipo_de_hecho'] != "").astype(int)
df_rsb.drop(['tipo_de_hecho'], axis=1, inplace=True)
# Convertir el texto en la columna "descripcion_relato" a mayúsculas
df_rsb['descripcion_relato'] = df_rsb['descripcion_relato'].str.upper()


# Datos sobre las personas dadas por desaparecidos
# Nombres y apellidos
# Corrección del uso de artículos y preposiciones en los nombres
# Eliminar nombres y apellidos que solo tienen una letra inicial
homologacion.nombres.nombres_validos (df_rsb , primer_nombre = 'primer_nombre',
                 segundo_nombre = 'segundo_nombre',
                 primer_apellido = 'primer_apellido',
                 segundo_apellido = 'segundo_apellido',
                 nombre_completo = 'nombre_completo')

# Documento de identificación
homologacion.documento.documento_valida (df_rsb, documento = 'documento')

df_rsb['sexo'].replace('INTERSEXUAL', 'INTERSEX', inplace=True)

# Pertenencia_etnica [NARP; INDIGENA; RROM; MESTIZO]
# Renombrar la columna
homologacion.etnia.etnia_valida (df_rsb, etnia = 'iden_pertenenciaetnica')

# Validar rango de fecha de nacimiento
#Identificar inconsistencias en el año de nacimiento
homologacion.fecha.fechas_validas (df_rsb,fecha_dia = 'dia_nacimiento', fecha_mes = 'mes_nacimiento', fecha_anio = 'anio_nacimiento', fechat = 'fecha_nacimiento', fecha = 'fecha_nacimiento_dft')

# Validar rango de edad
# Primero, convierte las columnas 'edad_des_sup' y 'edad_des_inf' a numéricas
df_rsb[['edad_des_sup', 'edad_des_inf']] = df_rsb[['edad_des_sup', 'edad_des_inf']].apply(pd.to_numeric, errors='coerce')


df_rsb['edad'] = 0
# Luego, aplica las condiciones y realiza los reemplazos
df_rsb['edad'] = np.where((df_rsb['edad_des_sup'].notnull()) & (df_rsb['edad_des_inf'].isnull()), df_rsb['edad_des_sup'], df_rsb['edad'])


df_rsb['edad'] = np.where((df_rsb['edad_des_inf'].notnull()) & (df_rsb['edad_des_sup'].isnull()), df_rsb['edad_des_inf'], df_rsb['edad'])
df_rsb['edad'] = np.where((np.abs(df_rsb['edad_des_sup'] - df_rsb['edad_des_inf']) <= 10) &
                      (df_rsb['edad_des_sup'].notnull()) & (df_rsb['edad_des_inf'].notnull()) &
                      (df_rsb['edad_des_sup'] >= df_rsb['edad_des_inf']), df_rsb['edad_des_sup'], df_rsb['edad'])
df_rsb['edad'] = np.where((np.abs(df_rsb['edad_des_sup'] - df_rsb['edad_des_inf']) <= 10) &
                      (df_rsb['edad_des_sup'].notnull()) & (df_rsb['edad_des_inf'].notnull()) &
                      (df_rsb['edad_des_inf'] > df_rsb['edad_des_sup']), df_rsb['edad_des_inf'], df_rsb['edad'])
df_rsb['edad'] = np.where(df_rsb['edad'] > 100, np.nan, df_rsb['edad'])
df_rsb['edad'] = np.where(((df_rsb['edad_des_inf'] == 0) & (df_rsb['edad_des_sup'] == 0)) |
                      ((df_rsb['edad_des_inf'].isnull()) & (df_rsb['edad_des_sup'] == 0)) |
                      ((df_rsb['edad_des_inf'] == 0) & (df_rsb['edad_des_sup'].isnull())), 0, df_rsb['edad'])

# elimina las columnas 'edad_des_sup' y 'edad_des_inf'
df_rsb.drop(['edad_des_sup', 'edad_des_inf'], axis=1, inplace=True)


# Convertir la columna "fecha_ocur_anio" a tipo de datos caracter
df_rsb['fecha_ocur_anio'] = df_rsb['fecha_ocur_anio'].astype(str)
df_rsb['anio_nacimiento'] = df_rsb['anio_nacimiento'].astype(str)

# Verificar consistencia de anio_nacimiento, mes_nacimiento
df_rsb['fecha_ocur_anio'] = np.where((df_rsb['fecha_ocur_anio'].str.len() < 1),
                                   "0", df_rsb['fecha_ocur_anio'])
df_rsb['anio_nacimiento'] = np.where((df_rsb['anio_nacimiento'].str.len() < 1),
                                   "0", df_rsb['anio_nacimiento'])

# Calcular la edad estimada al desaparecer
df_rsb['edad_desaparicion_est'] = (
    (df_rsb['fecha_desaparicion_dtf'].dt.year - df_rsb['fecha_nacimiento_dft'].dt.year) -
    ((df_rsb['fecha_desaparicion_dtf'].dt.month - df_rsb['fecha_nacimiento_dft'].dt.month) +
    (df_rsb['fecha_desaparicion_dtf'].dt.day - df_rsb['fecha_nacimiento_dft'].dt.day)) / 12).round()

#validamos valores validos de edad al momento de desaparecer
df_rsb['edad_desaparicion_est'] = np.where(df_rsb['edad_desaparicion_est'] > 100, np.nan, df_rsb['edad_desaparicion_est'])

# Verificar que la edad esté dentro del rango [1, 100]
df_rsb['edad_desaparicion_est'] = np.where(
    (df_rsb['edad_desaparicion_est'].between(1, 100, inclusive='both') |
     df_rsb['edad_desaparicion_est'].isna()),
    df_rsb['edad_desaparicion_est'],
    np.nan)


df_rsb['fecha_ocur_anio'] = np.where((df_rsb['fecha_ocur_anio'].str.len() == 1),
                                   "", df_rsb['fecha_ocur_anio'])
df_rsb['anio_nacimiento'] = np.where((df_rsb['anio_nacimiento'].str.len() == 1),
                                   "", df_rsb['anio_nacimiento'])

#Consistencia entre las fechas (fechas de nacimiento y de ocurrencia de los hechos) y la edad al momento de desaparición reportadas
df_rsb['dif_edad'] = np.abs(df_rsb['edad_desaparicion_est'] - df_rsb['edad'])
p90 = df_rsb['dif_edad'].quantile(0.90)
df_rsb['inconsistencia_fechas'] = np.where(((df_rsb['edad_desaparicion_est'] < 0) | 
       (df_rsb['edad_desaparicion_est'] > 100)) & 
       (df_rsb['edad_desaparicion_est'].notna()), True, False)
df_rsb['inconsistencia_fechas'] = np.where((df_rsb['dif_edad'] > p90) & 
       (df_rsb['dif_edad'].notna()), 2, 
       df_rsb['inconsistencia_fechas'])
df_rsb['inconsistencia_fechas'] = np.where((df_rsb['fecha_nacimiento_dft'] == 
       df_rsb['fecha_desaparicion_dtf']) & 
      (df_rsb['fecha_nacimiento_dft'].notna()) & 
      (df_rsb['fecha_desaparicion_dtf'].notna()), 3, 
      df_rsb['inconsistencia_fechas'])

# Iterar sobre las columnas que contienen 'nacimiento' o 'edad'
for col in df_rsb.columns:
    if 'nacimiento' in col or 'edad' in col:
        # Reemplazar con valor nulo para las filas con inconsistencia_fechas diferente de 0
        df_rsb.loc[df_rsb['inconsistencia_fechas'] != 0, col] = np.nan

# Reemplazar la columna 'edad' con 'edad_desaparicion_est' donde sea necesario
df_rsb.loc[df_rsb['edad_desaparicion_est'].notnull() & df_rsb['edad'].isnull(), 'edad'] = df_rsb['edad_desaparicion_est']

df_rsb.drop(['edad_desaparicion_est'], axis=1, inplace=True)

# Convertir la columna 'situacion_actual_des' a cadena
df_rsb['situacion_actual_des'] = df_rsb['situacion_actual_des'].astype(str)
# Reemplazar con cadena vacía los valores que son igual a '.'
df_rsb.loc[df_rsb['situacion_actual_des'] == '.', 'situacion_actual_des'] = ''

# Ejecuta el script
subprocess.run(['python', 'FASE1_ALISTAMIENTO_UBPD_DTPECV.py'])

dignas_path = os.path.join(DIRECTORY_PATH, "archivos depurados", "BD_UBPD_DTPECV_identificados.csv")
df_csv = pd.read_csv(dignas_path)
df_csv['documento'] = df_csv['documento'].astype(str)

# Realizar un merge con el archivo de identificados de entregas dignas
df_rsb = pd.merge(df_rsb, df_csv, how='left', left_on=['documento'],
                right_on=['documento'], indicator = 'm_dtpecv', suffixes=('', '_derecha'))

df_rsb['m_dtpecv'] = df_rsb['m_dtpecv'].map({'left_only': 1, 'right_only': 2, 'both': 3})

# Reemplazar valores en la columna 'situacion_actual_des'
# cuando 'situacion_actual_des' está vacía y 'm_dtpecv' indica que en ambas tablas hubo coincidencia
df_rsb.loc[(df_rsb['situacion_actual_des'] == '') & (df_rsb['m_dtpecv'] == 3), 'situacion_actual_des'] = 'Apareció Muerto'


# Eliminación de la columna de indicador
df_rsb.drop('m_dtpecv', axis=1, inplace=True)
df_rsb = df_rsb[df_rsb.columns]

#asignamos valores al dataframe para casos puntuales ya identificados como aparecidos muertos
asigna_aparecio_muerto(df_rsb)

# Ordenar el DataFrame por codigo_unico_fuente y situacion_actual_des en orden descendente
df_rsb.sort_values(by=['codigo_unico_fuente', 'situacion_actual_des'], ascending=[True, False], inplace=True)

# Reemplazar situacion_actual_des con el valor de la primera observación dentro de cada bloque
df_rsb['situacion_actual_des'] = df_rsb.groupby('codigo_unico_fuente')['situacion_actual_des'].transform('first')


# Eliminación de Registros No Identificados
non_miss = df_rsb[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']].count(axis=1)
df_rsb['rni'] = (non_miss < 2).astype(int)

df_rsb.loc[(df_rsb['primer_nombre'] == "") | (df_rsb['primer_apellido'] == ""), 'rni'] = 1
df_rsb.loc[(df_rsb['codigo_dane_departamento'] == "") & (df_rsb['fecha_ocur_anio'] == "") & (df_rsb['documento'] == "") & (df_rsb['fecha_nacimiento'] == ""), 'rni'] = 1

# Calcular la suma acumulativa de rni dentro de cada grupo definido por codigo_unico_fuente
df_rsb['rni_'] = df_rsb.groupby('codigo_unico_fuente')['rni'].cumsum()

# Calcular el número de observaciones dentro de cada grupo
df_rsb['N'] = df_rsb.groupby('codigo_unico_fuente').cumcount() + 1

#obtenemos una copia del dataframe principal para trasformarlo y exportar su contenido como no identificados
df_rsb_copy = df_rsb.copy()

#Mantener solo las filas donde rni es igual a 1
df_rsb_copy = df_rsb_copy[df_rsb_copy['rni'] == 1]

# Mantener solo la primera observación dentro de cada bloque definido por codigo_unico_fuente
df_rsb_copy = df_rsb_copy.groupby('codigo_unico_fuente').head(1)

#Definición de ruta y exportacion de dataframe actual
# Guardar el DataFrame en un archivo
csv_file_path = os.path.join(DIRECTORY_PATH, "archivos depurados", "BD_UBPD_RSB_PNI.csv")
df_rsb_copy.to_csv(csv_file_path, index=False)

#retomamos el original o principal
#conservamos los que son diferentes segun la comparacion, ó lo que es igual excluimos los que son igual
#se comparan la suma acumulada del tro del grupo y el conteo del total delgrupo, si son iguales quedan por fuera
# se tienen en cuenta los que son registros identificables por que tienen suficiente informacion
df_rsb = df_rsb[df_rsb['rni_'] != df_rsb['N']]

# Obtener todas las columnas que comienzan con "TH"
th_columns = [col for col in df_rsb.columns if col.startswith('TH')]
pres_resp_columns = [col for col in df_rsb.columns if col.startswith('pres_resp_')]
# 1. Seleccionar un conjunto específico de columnas
selected_columns = ['tabla_origen', 'codigo_unico_fuente', 'nombre_completo', 'primer_nombre', 'segundo_nombre',
                    'primer_apellido', 'segundo_apellido', 'documento', 'sexo', 'iden_pertenenciaetnica',
                    'fecha_nacimiento', 'anio_nacimiento', 'mes_nacimiento', 'dia_nacimiento', 'edad',
                    'fecha_desaparicion', 'fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia', 'pais_ocurrencia',
                    'codigo_dane_departamento', 'departamento_ocurrencia', 'codigo_dane_municipio',
                    'municipio_ocurrencia'] + th_columns + pres_resp_columns + ['situacion_actual_des', 'descripcion_relato',
                    'in_ubpd']

# Crear una copia explícita del DataFrame
df_rsb = df_rsb[selected_columns].copy()

# Ordenar el DataFrame
df_rsb.sort_values(['tabla_origen', 'codigo_unico_fuente', 'nombre_completo', 'primer_nombre', 'segundo_nombre',
                         'primer_apellido', 'segundo_apellido', 'documento', 'sexo', 'iden_pertenenciaetnica',
                         'fecha_nacimiento', 'fecha_desaparicion', 'edad', 'pais_ocurrencia',
                         'codigo_dane_departamento', 'departamento_ocurrencia', 'codigo_dane_municipio',
                         'municipio_ocurrencia'] + th_columns + pres_resp_columns + ['situacion_actual_des', 'descripcion_relato',
                         'in_ubpd'], inplace=True)

# Crear la variable temporal 'nonmiss' utilizando rownonmiss
selected_nonmiss_columns = ['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido', 'documento',
                             'sexo', 'iden_pertenenciaetnica', 'fecha_nacimiento', 'fecha_desaparicion', 'edad',
                             'pais_ocurrencia', 'codigo_dane_departamento', 'codigo_dane_municipio', 'TH_DF', 'TH_SE',
                             'TH_RU', 'TH_OTRO'] + pres_resp_columns + ['situacion_actual_des', 'descripcion_relato', 'in_ubpd']

df_rsb['nonmiss'] = df_rsb[selected_nonmiss_columns].notnull().all(axis=1)


# Ordenar el DataFrame
df_rsb.sort_values(['codigo_unico_fuente', 'documento', 'nonmiss'], ascending=[True, False, False], inplace=True)

#  Mantener solo la primera observación por cada valor único en 'codigo_unico_fuente'
df_rsb = df_rsb.groupby('codigo_unico_fuente').head(1)

# Eliminar la columna 'nonmiss'
df_rsb.drop('nonmiss', axis=1, inplace=True)

# Ordenar DataFrame por 'codigo_unico_fuente'
df_rsb.sort_values(['codigo_unico_fuente'], inplace=True)

#Definición de ruta y exportacion de dataframe actual
# Guardar el DataFrame en un archivo
csv_file_path = os.path.join(DIRECTORY_PATH, "archivos depurados", "BD_UBPD_RSB.csv")
df_rsb.to_csv(csv_file_path, index=False)


#################
DB_SCHEMA = "version5"
DB_TABLE = "BD_UBPD_RSB"

with engine.connect() as conn, conn.begin():
    conn.execute(f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{DB_SCHEMA}') BEGIN EXEC('CREATE SCHEMA {DB_SCHEMA}') END")

    
df_rsb.to_sql(name=DB_TABLE, con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
##################

nrow_df = len(df_rsb)
print("Registros despues left dane depto muni:",nrow_df)
#identificados 13551
nrow_df = len(df_rsb_copy)
print("Registros despues left dane depto muni:",nrow_df)
#no identificados 97
# Registra el tiempo de finalización
end_time = time.time()

# Calcula el tiempo transcurrido
elapsed_time = end_time - start_time

print(f"Tiempo transcurrido: {elapsed_time/60} segundos")