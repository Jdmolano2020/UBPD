import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import unicodedata
import re
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_PARAMILITARES
import FASE1_HOMOLOGACION_CAMPO_FUERZA_PUBLICA_Y_AGENTES_DEL_ESTADO
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_FARC 
import FASE1_HOMOLOGACION_CAMPO_BANDAS_CRIMINALES
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_ELN
import FASE1_HOMOLOGACION_CAMPO_OTRAS_GUERRILLAS

def normalize_text(text):
    # Eliminar acentos
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    # Convertir a mayúsculas
    text = text.upper()
    # Eliminar espacios al inicio y al final
    text = text.strip()
    # Eliminar caracteres no ASCII
    text = ''.join([c if ord(c) < 128 else ' ' for c in text])
    # Mantener solo caracteres alfanuméricos y espacios
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    # Eliminar espacios duplicados
    text = ' '.join(text.strip().split())
    return text

def clean_text(text):
    if text is None or text.isna().any():
        return text      
    text = text.apply(normalize_text)
    return text

def concat_values(*args):
    return ' '.join(arg for arg in args if arg.strip())


# parametros programa stata
parametro_ruta = ""
parametro_cantidad = ""
# Establecer la ruta de trabajo
ruta =  "C:/Users/HP/Documents/UBPD/HerramientaAprendizaje/Fuentes/OrquestadorUniverso"  # Cambia esto según tu directorio

# Verificar si `1` es una cadena vacía y ajustar el directorio de trabajo
# en consecuencia
if parametro_ruta == "":
    os.chdir(ruta)
else:
    os.chdir(parametro_ruta)
# Borrar el archivo "fuentes secundarias\V_ICMP.dta"
archivo_a_borrar = os.path.join("fuentes secundarias",
                                "V_ICMP.dta")
if os.path.exists(archivo_a_borrar):
    os.remove(archivo_a_borrar)
# Configurar la codificación Unicode
encoding = "ISO-8859-1"
# 1. Conexión al repositorio de información (Omitir esta sección en Python)
# 2. Cargue de datos y creación de id_registro (Omitir esta sección en Python)
# Establecer la conexión ODBC
db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)
# JEP-CEV: Resultados integración de información (CA_DESAPARICION)
# Cargue de datos
query = "EXECUTE [dbo].[CONSULTA_V_ICMP]"
df = pd.read_sql_query(query, engine)
# Aplicar filtro si `2` no es una cadena vacía parametro cantidad registros
if parametro_cantidad != "":
    limite = int(parametro_cantidad)
    df = df[df.index < limite]
# Guardar el DataFrame en un archivo
archivo_csv = os.path.join("fuentes secundarias",
                           "V_ICMP.csv")
df.to_csv(archivo_csv, index=False)
# Cambiar directorio de trabajo
# #os.chdir(os.path.join(ruta, "fuentes secundarias"))
# Traducir la codificación Unicode
# #archivo_a_traducir = "V_ICMP.dta"
# #archivo_utf8 = archivo_a_traducir.replace(".dta", "_utf8.dta")
# #if os.path.exists(archivo_a_traducir):
# #    os.system(
# #        f'unicode translate "{archivo_a_traducir}" "{archivo_utf8}" transutf8')
# #    os.remove(archivo_a_traducir)
# Crear un identificador de registro
# #df = pd.read_csv(archivo_csv)
df.columns = df.columns.str.lower()
df['duplicates_reg'] = df.duplicated()
df = df[~df['duplicates_reg']]

# Ordenar el DataFrame por las columnas especificadas
columnas_ordenadas = ['primer_nombre', 'segundo_nombre', 'primer_apellido',
                      'segundo_apellido', 'documento', 'sexo', 'edad_des_inf',
                      'edad_des_sup', 'anio_nacimiento', 'mes_nacimiento',
                      'dia_nacimiento', 'iden_pertenenciaetnica',
                      'codigo_dane_departamento','codigo_dane_municipio',
                      'fecha_ocur_dia','fecha_ocur_mes', 'fecha_ocur_anio',
                      'presunto_responsable', 'codigo_unico_fuente']

df = df.sort_values(by=columnas_ordenadas)
# Renombrar una columna
df.rename(columns={'fuente': 'tabla_origen'}, inplace=True)
# Origen de los datos
df['tabla_origen'] = 'ICMP'
# Origen
df['in_icmp'] = 1

df['codigo_unico_fuente'] = df['codigo_unico_fuente'].apply(lambda x: f'{x:08.0f}')
# Guardar el DataFrame final en un archivo
# #df.to_stata(archivo_utf8, write_index=False)
# Cambiar el nombre de las columnas a minúsculas
# #df.columns = df.columns.str.lower()
# #df.to_stata(archivo_utf8, index=False)
# 1.Selección de variable a homologar
# Normalización de los campos
columns_to_normalize = ['nombre_completo', 'primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido',
                         'pais_ocurrencia', 'sexo']
df[columns_to_normalize] = df[columns_to_normalize].apply(clean_text)

na_values = {
    'NO APLICA': None,
    'NULL': None,
    'ND': None,
    'NA': None,
    'SIN INFOR': None,
    'SIN DETERM': None,
    'POR DEFINIR': None
}

df[columns_to_normalize] = df[columns_to_normalize].replace(na_values)

df['pais_ocurrencia'].replace({"UNITED STATES": "ESTADOS UNIDOS"}, inplace=True)
# Realizar un merge con el archivo DIVIPOLA_departamentos_122021.dta
dane = pd.read_stata(
    "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")
# Renombrar columnas
dane = dane.rename(columns={
    'codigo_dane_departamento': 'codigo_dane_departamento',
    'departamento': 'departamento_ocurrencia',
    'codigo_dane_municipio': 'codigo_dane_municipio',
    'municipio': 'municipio_ocurrencia'
})
# Realizar la unión (left join) con "dane"
df = pd.merge(df, dane, how='left', left_on=['codigo_dane_departamento', 'codigo_dane_municipio'],
                right_on=['codigo_dane_departamento', 'codigo_dane_municipio'])
nrow_df = len(df)
print("Registros despues left dane depto muni:",nrow_df)
# Fecha de ocurrencia
df['fecha_ocur_dia_0'] = df['fecha_ocur_dia'].astype(str)
df['fecha_ocur_dia_0'].fillna(value="0", inplace=True)
df['fecha_ocur_dia_0'] = df['fecha_ocur_dia_0'].replace(np.nan, '0')
df['fecha_ocur_dia_0'] = df['fecha_ocur_dia_0'].str.replace("nan", "0", regex=True)
df['fecha_ocur_dia_0'] = df['fecha_ocur_dia_0'].str.replace(".0", "", regex=True).str.zfill(2) 

df['fecha_ocur_mes_0'] = df['fecha_ocur_mes'].astype(str)
df['fecha_ocur_mes_0'].fillna(value="0", inplace=True)
df['fecha_ocur_mes_0'] = df['fecha_ocur_mes_0'].replace(np.nan, '0')
df['fecha_ocur_mes_0'] = df['fecha_ocur_mes_0'].str.replace("nan", "0", regex=True)
df['fecha_ocur_mes_0'] = df['fecha_ocur_mes_0'].str.replace(".0", "", regex=True).str.zfill(2) 

df['fecha_ocur_anio_0'] = df['fecha_ocur_anio'].astype(str).str.slice(0, 4)
df['fecha_ocur_anio_0'] = df['fecha_ocur_anio_0'].str.replace("nan", "0000", regex=True)
df['fecha_ocur_anio_0'] = df['fecha_ocur_anio_0'].str.replace("18", "19", n=1)
df['fecha_ocur_anio_0'] = df['fecha_ocur_anio_0'].str.replace("179", "197", n=1)
df['fecha_ocur_anio_0'] = df['fecha_ocur_anio_0'].str.replace("169", "196", n=1)
df['fecha_ocur_anio_0'] = df['fecha_ocur_anio_0'].str.replace("159", "195", n=1)

df['fecha_desaparicion_0'] = df['fecha_ocur_anio_0'] + df['fecha_ocur_mes_0'] + df['fecha_ocur_dia_0']
df['fecha_desaparicion_dtf'] = pd.to_datetime(df['fecha_desaparicion_0'], format='%Y%m%d', errors='coerce')
df['fecha_ocur_anio_0'] = np.where((df['fecha_desaparicion_dtf'].isna()),None, df['fecha_ocur_anio_0'])
df['fecha_ocur_mes_0'] = np.where(df['fecha_desaparicion_dtf'].isna(),None, df['fecha_ocur_mes_0'])
df['fecha_ocur_dia_0'] = np.where(df['fecha_desaparicion_dtf'].isna(),None, df['fecha_ocur_dia_0'])
df.loc[df['fecha_desaparicion_dtf'].isna(), 'fecha_desaparicion_dtf'] = None

df.drop(columns=['fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia', 'fecha_desaparicion_0'], inplace=True)
df.rename(columns={'fecha_ocur_anio_0': 'fecha_ocur_anio', 
                   'fecha_ocur_mes_0': 'fecha_ocur_mes', 
                   'fecha_ocur_dia_0': 'fecha_ocur_dia'}, inplace=True)
# Guardar el DataFrame en un archivo
# #df.to_stata("archivos depurados/BD_FGN_INACTIVOS.dta", index=False)
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
df['i'] = (df['segundo_nombre'].isin(["DEL", "DE", "DE LAS", "DE LOS"]))
df.loc[df['i'], 'segundo_nombre'] = df['segundo_nombre'] + " " + df['primer_apellido']
df.loc[df['i'], 'primer_apellido'] = df['segundo_apellido']
df.loc[df['i'], 'segundo_apellido'] = ""
df.drop(columns=['i'], inplace=True)

# Reemplazar valores en primer_apellido
df['i'] = (df['primer_apellido'].isin(["DEL", "DE", "DE LAS", "DE LOS"]))
df.loc[df['i'], 'primer_apellido'] = df['primer_apellido'] + " " + df['segundo_apellido']
df.loc[df['i'], 'segundo_apellido'] = ""
df.drop(columns=['i'], inplace=True)
# Reemplazar primer apellido por segundo apellido cuando el primer campo está vacío
df['i'] = (df['primer_apellido'] == "") & (df['segundo_apellido'] != "")
df.loc[df['i'], 'primer_apellido'] = df['segundo_apellido']
df.loc[df['i'], 'segundo_apellido'] = ""
df.drop(columns=['i'], inplace=True)
# Eliminar nombres y apellidos cuando solo se registra la letra inicial
cols_to_clean = ['primer_nombre', 'primer_apellido', 'segundo_nombre', 'segundo_apellido']
for col in cols_to_clean:
    df.loc[df[col].str.len() == 1, col] = ""
# Nombre completo
cols_nombre = [ 'segundo_nombre', 'primer_apellido', 'segundo_apellido']
# Inicializa la columna 'nombre_completo' con el valor de 'primer_nombre'
df['nombre_completo_'] = df['primer_nombre']

for col in cols_nombre:
    df['nombre_completo_'] = df['nombre_completo_'] + " " + df[col].fillna("")  # Concatenar nombres y apellidos no vacíos
    
df['nombre_completo_'] = df['nombre_completo_'].str.strip()  # Eliminar espacios en blanco al principio y al final
df['nombre_completo_'] = df['nombre_completo_'].str.replace('  ', ' ', regex=True)  # Reemplazar espacios dobles por espacios simples
# Eliminar columna nombre_completo original
df.drop(columns=['nombre_completo'], inplace=True)
# Renombrar columna
df.rename(columns={'nombre_completo_': 'nombre_completo'}, inplace=True)
# Documento de identificación
df['documento'] = df['documento'].str.upper()

for i in range(256):
    if i not in [32, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 209]:
        df['documento'] = df['documento'].str.replace(chr(i), '', regex=True)
df['documento_dep'] = df['documento']

for i in range(48, 58):
    df['documento_dep'] = df['documento_dep'].str.replace(chr(i), '', regex=True)

df['documento'] = df.apply(lambda row: '' if row['documento_dep'] == row['documento'] and row['documento'] != '' else row['documento'], axis=1)

for i in range(256):
    if i not in [32, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57]:
        df['documento'] = df.apply(lambda row: row['documento'].replace(chr(i), '') if ' ' in row['documento'] and row['documento_dep'] != row['documento'] and row['documento'] != '' and row['documento_dep'] != '' else row['documento'], axis=1)

df['documento'] = df['documento'].str.strip()
df['documento'] = df['documento'].str.replace('   ', ' ', regex=True)
df['documento'] = df['documento'].str.replace('  ', ' ', regex=True)
df.drop(columns=['documento_dep'], inplace=True)
df['documento'] = df.apply(lambda row: '' if len(row['documento']) < 4 else row['documento'], axis=1)
# Pertenencia_etnica [NARP; INDIGENA; RROM; MESTIZO]
# Renombrar la columna
df.rename(columns={'iden_pertenenciaetnica': 'iden_pertenenciaetnica_'}, inplace=True)
# Reemplazar valores en la columna 'iden_pertenenciaetnica'
df['iden_pertenenciaetnica'] = np.where(df['iden_pertenenciaetnica_'] == 'Afrocolombiano', 'NARP', df['iden_pertenenciaetnica_'])
df['iden_pertenenciaetnica'] = np.where(df['iden_pertenenciaetnica_'].str.contains('Indígena|Nasa'), 'INDIGENA', df['iden_pertenenciaetnica'])
df['iden_pertenenciaetnica'] = np.where(df['iden_pertenenciaetnica_'].str.contains('Ninguno'), 'NINGUNA', df['iden_pertenenciaetnica'])
# Eliminar la columna original
df.drop(columns=['iden_pertenenciaetnica_'], inplace=True)
# Validar rango de fecha de nacimiento
df['anio_nacimiento'] = np.where(df['anio_nacimiento'] < 1900, np.nan, df['anio_nacimiento'])
df['mes_nacimiento'] = np.where((df['mes_nacimiento'] < 1) | (df['mes_nacimiento'] > 12) & (df['mes_nacimiento'] != np.nan), np.nan, df['mes_nacimiento'])
df['dia_nacimiento'] = np.where((df['dia_nacimiento'] < 1) | (df['dia_nacimiento'] > 31) & (df['dia_nacimiento'] != np.nan), np.nan, df['dia_nacimiento'])
# Crear columna 'fecha_nacimiento' en formato datetime
df['fecha_dia'] = df['dia_nacimiento'].astype(str)
df['fecha_dia'].fillna(value="0", inplace=True)
df['fecha_dia'] = df['fecha_dia'].replace(np.nan, '0')
df['fecha_dia'] = df['fecha_dia'].str.replace("nan", "0", regex=True)
df['fecha_dia'] = df['fecha_dia'].str.replace(".0", "", regex=True).str.zfill(2) 

df['fecha_mes'] = df['mes_nacimiento'].astype(str)
df['fecha_mes'].fillna(value="0", inplace=True)
df['fecha_mes'] = df['fecha_mes'].replace(np.nan, '0')
df['fecha_mes'] = df['fecha_mes'].str.replace("nan", "0", regex=True)
df['fecha_mes'] = df['fecha_mes'].str.replace(".0", "", regex=True).str.zfill(2) 

df['fecha_anio'] = df['anio_nacimiento'].astype(str).str.slice(0, 4)
df['fecha_anio'] = df['fecha_anio'].str.replace("nan", "0000", regex=True)


df['fecha_ymd'] = df['fecha_anio'] + df['fecha_mes'] + df['fecha_dia']

df['fecha_ymd_dtf'] = pd.to_datetime(df['fecha_ymd'], format='%Y%m%d', errors='coerce')
df['fecha_anio'] = np.where((df['fecha_ymd_dtf'].isna()),None, df['fecha_anio'])
df['fecha_mes'] = np.where(df['fecha_ymd_dtf'].isna(),None, df['fecha_mes'])
df['fecha_dia'] = np.where(df['fecha_ymd_dtf'].isna(),None, df['fecha_dia'])
df.loc[df['fecha_ymd_dtf'].isna(), 'fecha_ymd_dtf'] = None

df.drop(columns=['anio_nacimiento', 'mes_nacimiento', 'dia_nacimiento', 'fecha_ymd'], inplace=True)
df.rename(columns={'fecha_anio': 'anio_nacimiento', 
                   'fecha_mes': 'mes_nacimiento', 
                   'fecha_dia': 'dia_nacimiento',
                   'fecha_ymd_dtf':'fecha_nacimiento'}, inplace=True)
# Validar rango de edad
df['edad_des_inf'].fillna(value=0, inplace=True)
df['edad_des_sup'].fillna(value=0, inplace=True)
df.loc[(df['edad_des_inf'] != 0) & (df['edad_des_sup'] == 0), 'edad'] = df['edad_des_inf']
df.loc[(df['edad_des_inf'] == 0) & (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_sup']
df.loc[(df['edad_des_inf'] < df['edad_des_sup']) & (df['edad_des_inf'] != 0) & (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_inf']

df.loc[(df['edad_des_inf'] >= df['edad_des_sup']) & (df['edad_des_inf'] != 0) & (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_sup']
df['edad'] = np.where(df['edad'] > 100, np.nan, df['edad'])
df['edad'] = np.where((df['edad_des_inf'] == 0) & (df['edad_des_sup'] == 0), 0, df['edad'])
# Eliminar columnas de edad desaparición
# #df.drop(columns=['edad_des_inf', 'edad_des_sup'], inplace=True)
# Calcular edad_desaparicion_est y detectar inconsistencias
df['edad_desaparicion_est'] = ((df['fecha_desaparicion_dtf'].dt.year - df['fecha_nacimiento'].dt.year) -
                                ((df['fecha_desaparicion_dtf'].dt.month - df['fecha_nacimiento'].dt.month) +
                                (df['fecha_desaparicion_dtf'].dt.day - df['fecha_nacimiento'].dt.day)) / 12).round()
df['dif_edad'] = np.abs(df['edad_desaparicion_est'] - df['edad'])
p90 = df['dif_edad'].quantile(0.90)
df['inconsistencia_fechas'] = np.where(((df['edad_desaparicion_est'] < 0) | (df['edad_desaparicion_est'] > 100)) & (df['edad_desaparicion_est'].notna()), True, False)
df['inconsistencia_fechas'] = np.where((df['dif_edad'] > p90) & (df['dif_edad'].notna()), 2, df['inconsistencia_fechas'])
df['inconsistencia_fechas'] = np.where((df['fecha_nacimiento'] == df['fecha_desaparicion_dtf']) & (df['fecha_nacimiento'].notna()) & (df['fecha_desaparicion_dtf'].notna()), 3, df['inconsistencia_fechas'])
# Limpiar valores en columnas relacionadas con fechas y edad
date_cols = ['fecha_nacimiento', 'fecha_desaparicion_dtf', 'fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia']
for col in date_cols:
    df[col] = df[col].where(df['inconsistencia_fechas'] == 0)
    
age_cols = ['edad', 'edad_desaparicion_est']
for col in age_cols:
    df[col] = df[col].where(df['inconsistencia_fechas'] == 0)
# Eliminar columnas auxiliares y con inconsistencias
# #df.drop(columns=['edad_desaparicion_est', 'dif_edad', 'inconsistencia_fechas'], inplace=True)
# Limpiar valores en la columna 'situacion_actual_des'
df['situacion_actual_des'] = df['situacion_actual_des'].replace('.', '')

# Identificación de registros que no refieren a personas individualizables (datos almacenados en campos de identificación que refieren a otras entidades)
# Crear una nueva columna 'non_miss' que cuenta la cantidad de columnas no nulas para cada fila
df['non_miss'] = df[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']].count(axis=1)
# Crear una nueva columna 'rni' que indica si la fila debe ser eliminada (1) o no (0)
df['rni'] = 0
# Marcar filas con menos de 2 columnas no nulas (debes ajustar el valor 2 según tus criterios)
df.loc[df['non_miss'] < 2, 'rni'] = 1
# Marcar filas con nombres muy cortos que puedan ser siglas o abreviaturas
df.loc[(df['primer_nombre'].str.len() < 3) & (df['segundo_nombre'].str.len() < 3) & (df['primer_apellido'].str.len() < 3) & (df['segundo_apellido'].str.len() < 3), 'rni'] = 1
# Crear una lista de palabras clave y marcar filas que contienen esas palabras en las columnas de nombres y apellidos
keywords = ["GUERRIL", "FRENTE", "BLOQ", "ORGANIZ", "ACTOR", "ARMADO", "ARMADA", "INTEGRANTE", "SOLDADO", "BATALLON", "BRIGADA", "COLUMNA", "COMANDANTE", "TENIENTE", "CAPITAN", "DRAGONEANTE", "EJERCITO", "SARGENTO", "DIJIN", "SIJIN", "INTENDENTE", "GENERAL", "MILITAR", "BRIGADIER", "FP"]
for keyword in keywords:
    df.loc[(df['primer_nombre'].str.contains(keyword)) | (df['segundo_nombre'].str.contains(keyword)) | (df['primer_apellido'].str.contains(keyword)) | (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Marcar filas que contienen "CORONEL" en el primer nombre
df.loc[df['primer_nombre'].str.contains("CORONEL"), 'rni'] = 1
# Marcar filas que contienen "POLICIA" en el primer nombre o "POLICIA" como el único nombre
df.loc[(df['primer_nombre'].str.contains("POLICIA")) | ((df['primer_nombre'] == "POLICIA") & (df['segundo_nombre'].isna()) & (df['primer_apellido'].isna()) & (df['segundo_apellido'].isna())), 'rni'] = 1
# Marcar filas que contienen "FUERZA PUBLICA" en cualquiera de las cuatro columnas de nombres y apellidos
df.loc[(df[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']].apply(lambda x: x.str.contains("FUERZA PUBLICA")).any(axis=1)), 'rni'] = 1
# Crear una lista de palabras clave para hechos investigados por oficio y marcar filas que contienen esas palabras en las columnas de nombres y apellidos
inv_oficiosas = ["DIREC", "OFICI", "RADICA", "ASIG", "COMPULS", "COPIA", "SECCION", "ADMIN", "PUBLIC", "ESTADO", "DEFENSOR", "JUZGADO", "CIRCUITO"]
for keyword in inv_oficiosas:
    df.loc[(df['primer_nombre'].str.contains(keyword)) | (df['segundo_nombre'].str.contains(keyword)) | (df['primer_apellido'].str.contains(keyword)) | (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Crear una lista de palabras clave para términos que indican que no ha sido posible individualizar a la persona y marcar filas que contienen esas palabras en las columnas de nombres y apellidos
no_ident = ["IDENTIFICAD", "INFORMAC", "PERSONA", "DESCONOC", "CNI", "MASCULINO", "FEMENINO", "DEFINIR", "ESTABLECER"]
for keyword in no_ident:
    df.loc[(df['primer_nombre'].str.contains(keyword)) | (df['segundo_nombre'].str.contains(keyword)) | (df['primer_apellido'].str.contains(keyword)) | (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Crear una lista de palabras clave para términos relacionados con comunidades y marcar filas que contienen esas palabras en las columnas de nombres y apellidos
comunidad = ["COMUNIDAD", "ASOCIACION", "ASIACION", "ASENT", "CORREG", "VERED", "CONSEJO", "CONSORC", "COMISI", "COMIT", "CABECERA", "MUNICIPIO", "REGIMEN", "CONSTITUC"]
for keyword in comunidad:
    df.loc[(df['primer_nombre'].str.contains(keyword)) | (df['segundo_nombre'].str.contains(keyword)) | (df['primer_apellido'].str.contains(keyword)) | (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Marcar filas que contienen "ZONA" o "RURAL" como nombres o apellidos
df.loc[((df['primer_nombre'].str.contains("ZONA")) & (df['primer_nombre'].str.len() == 4)) | ((df['segundo_nombre'].str.contains("ZONA")) & (df['segundo_nombre'].str.len() == 4)) | ((df['primer_apellido'].str.contains("ZONA")) & (df['primer_apellido'].str.len() == 4)) | ((df['segundo_apellido'].str.contains("ZONA")) & (df['segundo_apellido'].str.len() == 4)), 'rni'] = 1
df.loc[((df['primer_nombre'].str.contains("RURAL")) & (df['primer_nombre'].str.len() == 5)) | ((df['segundo_nombre'].str.contains("RURAL")) & (df['segundo_nombre'].str.len() == 5)) | ((df['primer_apellido'].str.contains("RURAL")) & (df['primer_apellido'].str.len() == 5)) | ((df['segundo_apellido'].str.contains("RURAL")) & (df['segundo_apellido'].str.len() == 5)), 'rni'] = 1
# Marcar filas que contienen "ALIAS" al principio del primer nombre
df.loc[df['primer_nombre'].str.startswith("ALIAS "), 'rni'] = 1
# Marcar filas que contienen "ACTA" en cualquiera de las cuatro columnas de nombres y apellidos
df.loc[(df[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']].apply(lambda x: x.str.contains("ACTA")).any(axis=1)), 'rni'] = 1
# Marcar filas que contienen "SIN" en cualquiera de las cuatro columnas de nombres y apellidos
df.loc[(df[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']].apply(lambda x: x.str.contains("SIN")).any(axis=1)), 'rni'] = 1
# Marcar filas que contienen "POR" en cualquiera de las cuatro columnas de nombres y apellidos
df.loc[(df[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']].apply(lambda x: x.str.contains("POR")).any(axis=1)), 'rni'] = 1
# Marcar filas con valores comunes de no identificación como "NN", "N", "XX" y "X"
df.loc[((df['primer_nombre'].isin(["NN", "N", "XX", "X"])) | (df['segundo_nombre'].isin(["NN", "N", "XX", "X"])) | (df['primer_apellido'].isin(["NN", "N", "XX", "X"])) | (df['segundo_apellido'].isin(["NN", "N", "XX", "X"]))), 'rni'] = 1
# Marcar filas con todas las columnas de nombres y apellidos vacías
df.loc[(df['primer_nombre'].isna()) & (df['segundo_nombre'].isna()) & (df['primer_apellido'].isna()) & (df['segundo_apellido'].isna()) & (df['codigo_dane_departamento'].isna()) & (df['fecha_ocur_anio'].isna()) & (df['documento'].isna()) & (df['fecha_nacimiento'].isna()), 'rni'] = 1
# Guardar las filas marcadas como rni en un archivo
df_rni = df[df['rni'] == 1]
db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)# Escribir los DataFrames en las tablas correspondientes en la base de datos

# #df_rni.to_stata("archivos depurados/BD_FGN_INACTIVOS_PNI.dta")
df_rni.to_sql('BD_ICMP_PNI', con=engine, if_exists='replace', index=False)
# #df_rni.to_csv("archivos depurados/BD_ICMP_PNI.csv", index=False)
# Eliminar las filas marcadas como rni del DataFrame original
df = df[df['rni'] == 0]
df.drop(columns=['non_miss', 'rni'], inplace=True)
# 5. Identificación de registros únicos	
# Seleccionar las columnas que deseas mantener
columnas = ['tabla_origen', 'codigo_unico_fuente', 'nombre_completo', 'primer_nombre', 'segundo_nombre', 
            'primer_apellido', 'segundo_apellido', 'documento', 'sexo', 'iden_pertenenciaetnica', 
            'fecha_nacimiento', 'anio_nacimiento', 'mes_nacimiento', 'dia_nacimiento', 'edad', 
            'fecha_desaparicion_dtf', 'fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia', 
            'codigo_dane_departamento', 'departamento_ocurrencia', 'codigo_dane_municipio', 
            'municipio_ocurrencia', 
            'TH_DF',  'TH_SE','TH_RU',
            'pres_resp_paramilitares',
            'pres_resp_grupos_posdesmov', 'pres_resp_agentes_estatales', 'pres_resp_guerr_farc',
            'pres_resp_guerr_eln', 'pres_resp_guerr_otra', 'pres_resp_otro',
            'situacion_actual_des', 'descripcion_relato']
df = df[columnas]
# Ordenar el DataFrame por 'codigo_unico_fuente', 'documento' y 'nonmiss'
df.sort_values(by=['codigo_unico_fuente', 'documento'], ascending=[True, True], inplace=True)
# Mantener solo el primer registro para cada 'codigo_unico_fuente'
df.drop_duplicates(subset=['codigo_unico_fuente'], keep='first', inplace=True)

df.to_sql('BD_ICMP', con=engine, if_exists='replace', index=False)

# #df.to_stata("archivos depurados/BD_ICMP.dta", index=False)



