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
ruta =  "C:/Users/HP/Documents/UBPD/HerramientaAprendizaje/Fuentes/OrquestadorUniverso"

# Verificar si `1` es una cadena vacía y ajustar el directorio de trabajo
# en consecuencia
if parametro_ruta == "":
    os.chdir(ruta)
else:
    os.chdir(parametro_ruta)
# Borrar el archivo "fuentes secundarias\V_FGN_INACTIVOS.dta"
archivo_a_borrar = os.path.join("fuentes secundarias",
                                "V_FGN_INACTIVOS.dta")
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
query = "EXECUTE [dbo].[CONSULTA_V_FGN_INACTIVOS]"
df = pd.read_sql_query(query, engine)
# Aplicar filtro si `2` no es una cadena vacía parametro cantidad registros
if parametro_cantidad != "":
    limite = int(parametro_cantidad)
    df = df[df.index < limite]
# Guardar el DataFrame en un archivo
archivo_csv = os.path.join("fuentes secundarias",
                           "V_FGN_INACTIVOS.csv")
df.to_csv(archivo_csv, index=False)
# Cambiar directorio de trabajo
# #os.chdir(os.path.join(ruta, "fuentes secundarias"))
# Traducir la codificación Unicode
# #archivo_a_traducir = "V_FGN_INACTIVOS.dta"
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
columnas_ordenadas = ['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido', 'nombre_completo',
                      'documento', 'edad_des_inf', 'edad_des_sup', 'dia_nacimiento', 'mes_nacimiento',
                      'anio_nacimiento_ini', 'anio_nacimiento_fin', 'sexo', 'codigo_dane_departamento',
                      'codigo_dane_municipio', 'fecha_ocur_dia', 'fecha_ocur_mes', 'fecha_ocur_anio',
                      'tipo_de_hecho', 'presunto_responsable', 'codigo_unico_fuente']

df = df.sort_values(by=columnas_ordenadas)

# Renombrar una columna
df.rename(columns={'fuente': 'tabla_origen'}, inplace=True)
# Origen de los datos
df['tabla_origen'] = 'FGN_EXP_INACTIVOS'
# Origen
df['in_fgn_inactivos'] = 1
# Número observaciones en la tabla
numero_observaciones = len(df)
# 1. Seleccionar variables que serán homologadas para la integración
# #columnas_a_eliminar = ['lugar_ocurr_territorio_colectivo', 'departamento_de_ocurrencia',
# #                       'municipio_de_ocurrencia', 'tipo_documento', 'tipo_otro_nombre', 'otro_nombre']

# #df = df.drop(columns=columnas_a_eliminar)
# 2. Normalización de los campos de texto
# Eliminación de acento, "NO APLICA", "NULL"

# Convertir todas las variables a mayúsculas
variables_a_convertir = ['tabla_origen',
                         'nombre_completo',
                         'primer_nombre',
                         'segundo_nombre',
                         'primer_apellido',
                         'segundo_apellido',
                         'sexo',
                         'iden_orientacionsexual',
                         'iden_pertenenciaetnica_',
                         'tipo_de_hecho',
                         'descripcion_relato',
                         'presunto_responsable',
                         'situacion_actual_des']

df[variables_a_convertir] = df[variables_a_convertir].apply(clean_text)

na_values = {
    'NO DETERMINADO DESDE FUENTE': np.nan,
    'SIN INFORMACION': np.nan,
    'SIN DATOS EN ARCHIVO FUENTE': np.nan,
    'NO APLICA': np.nan,
    'NULL': np.nan,
    'ND': np.nan,
    'NA': np.nan,
    'NR': np.nan,
    'SIN INFOR': np.nan,
    'NO SABE': np.nan,
    'DESCONOCIDO': np.nan,
    'POR DEFINIR': np.nan,
    'POR ESTABLECER': np.nan,
    'NONE': np.nan,
    'SIN INFORMACION EN EL EXPEDIENTE': np.nan,
    'NaT': np.nan
}

df[variables_a_convertir] = df[variables_a_convertir].replace(na_values)

# 3. Homologación de estructura, formato y contenido
# Datos sobre los hechos	
# Lugar de ocurrencia- País/Departamento/Muncipio
# Crear la variable 'pais_ocurrencia' basada en 'pais_de_ocurrencia'
df['pais_ocurrencia'] = np.where((df['pais_de_ocurrencia'] == '57'),'COLOMBIA',None)
# #df.drop(columns=['pais_de_ocurrencia'], inplace=True)
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

# Procesamiento de fechas de ocurrencia

df['ymd_hecho'] = df['fecha_ocur_anio'].astype(str)

df['fecha_ocur_anio'] = df['ymd_hecho'].str[0:4]
df['fecha_ocur_mes'] = df['ymd_hecho'].str[5:7]
df['fecha_ocur_dia'] = df['ymd_hecho'].str[8:10]
# #df['fecha_ocur_mes'] = pd.to_datetime(df['fecha_ocur_anio'], format='%Y').dt.month
# #df['fecha_ocur_dia'] = pd.to_datetime(df['fecha_ocur_anio'], format='%Y').dt.day
# Eliminar fechas de ocurrencia que no cumplen con los rangos válidos
df['fecha_ocur_mes'].replace('0', np.nan, inplace=True)
df['fecha_ocur_dia'].replace('0', np.nan, inplace=True)
# #df.drop(index=df[(df['fecha_ocur_mes'] < 1) | (df['fecha_ocur_mes'] > 12)].index, inplace=True)
# #df.drop(index=df[(df['fecha_ocur_dia'] < 1) | (df['fecha_ocur_dia'] > 31)].index, inplace=True)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].str.replace('18', '19', n=1)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].str.replace('179', '197', n=1)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].str.replace('169', '196', n=1)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].str.replace('159', '195', n=1)

df['fecha_desaparicion_0'] = df['fecha_ocur_anio'] + "-" + df['fecha_ocur_mes'] + "-" + df['fecha_ocur_dia']
df['fecha_desaparicion'] = pd.to_datetime(df['fecha_desaparicion_0'], format='%Y-%m-%d', errors='coerce')
# #df['fecha_desaparicion'] = df['fecha_ocur_anio'].str.replace('NaT--', '', n=1)
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
# Tipo de hecho
# Crear la variable TH_DF
df['TH_DF'] = 0
df.loc[(df['tipo_de_hecho'].str.contains("DESAPARICION")) & (df['tipo_de_hecho'].str.contains("FORZADA")), 'TH_DF'] = 1
# Crear la variable TH_SE
df['TH_SE'] = 0
df.loc[df['tipo_de_hecho'].str.contains("SECUESTRO"), 'TH_SE'] = 1
# Crear la variable TH_RU
df['TH_RU'] = 0
df.loc[df['tipo_de_hecho'].str.contains("RECLUTAMIENTO"), 'TH_RU'] = 1
# Crear la variable TH_OTRO
df['TH_OTRO'] = 0
df.loc[(df['TH_DF'] == 0) & (df['TH_RU'] == 0) & (df['TH_SE'] == 0) &
       (df['tipo_de_hecho'].str.contains("SIN") == False) &
       (df['tipo_de_hecho'].str.contains("DETERMINAR") == False) &
       (df['tipo_de_hecho'] != ""), 'TH_OTRO'] = 1

# Eliminar la columna tipo_de_hecho
# #df.drop(columns=['tipo_de_hecho'], inplace=True)
# Relato
# Convertir la variable descripcion_relato a mayúsculas
df['descripcion_relato'] = df['descripcion_relato'].str.upper()
# Datos sobre las personas dadas por desparecidas
# Nombres y apelllidos
# Corrección del uso de artículos y preposiciones en los nombres
# Reemplazar valores en segundo_nombre
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
# Documento
# Eliminar símbolos y caracteres especiales
# Convertir todos los caracteres a mayúsculas
df['documento_'] = df['documento'].str.upper()
# Eliminar caracteres no permitidos en el documento
for i in range(256):
    if (i != 32) and (i < 48 or i > 57) and (i < 65 or i > 90) and (i != 209):
        char = chr(i)
        df['documento_'] = df['documento_'].str.replace(char, '', regex=False)
# Eliminar cadenas de texto sin números
for i in range(48, 58):
    char = chr(i)
    df['documento_dep'] = df['documento_'].str.replace(char, '', regex=False)
df['documento_'] = df.apply(lambda row: row['documento_'] if row['documento_dep'] == row['documento_'] else "", axis=1)
# Eliminar cadenas complejas de texto tipo anotaciones
df['documento_'] = df['documento_'].str.replace(r'[^0-9]', '', regex=True)

# Borrar registros de documentos de identificación iguales a '0'
# #df = df[~df['documento_'].astype(str).str.isnumeric() | (df['documento_'] != "0")]
# Limpiar espacios en blanco al principio y al final, y reducir espacios múltiples a uno solo
df['documento_'] = df['documento_'].str.strip()
df['documento_'] = df['documento_'].str.replace('   ', ' ')
df['documento_'] = df['documento_'].str.replace('  ', ' ')
# Eliminar las columnas documento y documento_dep
df.drop(columns=['documento', 'documento_dep'], inplace=True)
# Renombrar la columna documento_
df.rename(columns={'documento_': 'documento'}, inplace=True)
# Pertenencia_etnica [NARP; INDIGENA; RROM; MESTIZO]
# Renombrar la columna
df.rename(columns={'iden_pertenenciaetnica_': 'iden_pertenenciaetnica'}, inplace=True)
# Recodificar valores
df['iden_pertenenciaetnica'].replace({"AFROCOLOMBIANO": "NARP",
                                      "AFROCOLOMBIANOA": "NARP",
                                      "AFROCOLOMBIANA": "NARP",
                                      "PALENQUERO": "NARP",
                                      "RAIZAL": "NARP",
                                      "NINGUNA": "MESTIZO"}, inplace=True)
# Fecha de nacimiento- Validar rango
# Eliminar columnas que empiezan con "anio_nacimiento"
columns_to_drop = [col for col in df.columns if col.startswith("anio_nacimiento")]
df.drop(columns=columns_to_drop, inplace=True)
# Crear una columna "anio_nacimiento" vacía
df['anio_nacimiento'] = ""
# Convertir las columnas dia_nacimiento y mes_nacimiento a cadenas
df['dia_nacimiento'] = df['dia_nacimiento'].astype(str)
df['mes_nacimiento'] = df['mes_nacimiento'].astype(str)
# Reemplazar valores vacíos en mes_nacimiento
df.loc[df['mes_nacimiento'] == ".", 'mes_nacimiento'] = ""
# Reemplazar valores vacíos en dia_nacimiento
df.loc[df['dia_nacimiento'] == ".", 'dia_nacimiento'] = ""
# Crear una columna "fecha_nacimiento" vacía
df['fecha_nacimiento'] = ""
# Edad
# Validación de rango
# Calcular la variable "edad" en función de las condiciones especificadas
df['edad'] = 0  # Crear la columna "edad" inicialmente con valor 0
# Calcular "edad" en función de las condiciones especificadas
df.loc[(df['edad_des_inf'] != 0) & (df['edad_des_sup'] == 0), 'edad'] = df['edad_des_inf']
df.loc[(df['edad_des_inf'] == 0) & (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_sup']
df.loc[(df['edad_des_inf'] < df['edad_des_sup']) & (df['edad_des_inf'] != 0) & (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_inf']
df.loc[(df['edad_des_inf'] >= df['edad_des_sup']) & (df['edad_des_inf'] != 0) & (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_sup']
# Reemplazar valores mayores de 100 con valores faltantes
df.loc[df['edad'] > 100, 'edad'] = None  # None representa un valor faltante en Pandas
# Reemplazar "edad" con 0 cuando ambas "edad_des_inf" y "edad_des_sup" son iguales a 0
df.loc[(df['edad_des_inf'] == 0) & (df['edad_des_sup'] == 0), 'edad'] = 0
# Eliminar las columnas "edad_des_inf" y "edad_des_sup"
df.drop(columns=['edad_des_inf', 'edad_des_sup'], inplace=True)
# 4. Identificación y eliminación de Registros No Identificados (registros sin datos suficientes para la individualización de las víctimas) 
# Calcular la variable "non_miss" que cuenta la cantidad de columnas no faltantes por fila
df['non_miss'] = df[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']].count(axis=1)
# Calcular la variable "rni" en función de las condiciones especificadas
df['rni'] = (df['non_miss'] < 2) | (df['primer_nombre'] == "") | (df['primer_apellido'] == "") | ((df['codigo_dane_departamento'] == "") & (df['fecha_ocur_anio'] == "") & (df['documento'] == ""))
# Calcular "rni_" y "N" por grupo de "codigo_unico_fuente"
df['rni_'] = df.groupby('codigo_unico_fuente')['rni'].transform('sum')
df['N'] = df.groupby('codigo_unico_fuente')['codigo_unico_fuente'].transform('count')
# Guardar registros no individualizables en un nuevo archivo
df_rni = df[df['rni'] == 1].copy()
# Guardar resultados en la base de datos de destino
db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)# Escribir los DataFrames en las tablas correspondientes en la base de datos

# #df_rni.to_stata("archivos depurados/BD_FGN_INACTIVOS_PNI.dta")
df_rni.to_sql('BD_FGN_INACTIVOS_PNI', con=engine, if_exists='replace', index=False)


# Eliminar registros no individualizables o sin suficientes datos para la integración
df = df[df['rni_'] != df['N']]
# Eliminar columnas auxiliares
df.drop(columns=['non_miss', 'rni_', 'N'], inplace=True)
# 5. Identificación de registros/filas únicas	
# Seleccionar las columnas especificadas
columns_to_keep = ['tabla_origen', 'codigo_unico_fuente', 'nombre_completo', 'primer_nombre', 'segundo_nombre',
                   'primer_apellido', 'segundo_apellido', 'documento', 'sexo', 'iden_pertenenciaetnica', 'edad',
                   'fecha_desaparicion', 'fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia', 'pais_ocurrencia',
                   'codigo_dane_departamento', 'departamento_ocurrencia', 'codigo_dane_municipio', 'municipio_ocurrencia',
                   'TH_DF', 'TH_SE', 'TH_RU', 'TH_OTRO', 'situacion_actual_des', 'descripcion_relato', 'in_fgn_inactivos']

df = df[columns_to_keep]
# Ordenar el DataFrame
df.sort_values(by=['codigo_unico_fuente', 'documento'], ascending=[False, False], inplace=True)
# Mantener el registro más completo por cada persona identificada de forma única
df.drop_duplicates(subset='codigo_unico_fuente', keep='first', inplace=True)
# Eliminar columnas auxiliares
df.drop(columns=['situacion_actual_des'], inplace=True)
# Guardar el DataFrame en un archivo
# #df.to_stata("archivos depurados/BD_FGN_INACTIVOS.dta")
df.to_sql('BD_FGN_INACTIVOS', con=engine, if_exists='replace', index=False)
# Contar el número de registros
count = len(df)
# Crear una variable de grupo 'g' basada en 'codigo_unico_fuente'
df['g'] = df.groupby('codigo_unico_fuente').ngroup()
# Calcular el resumen por grupo 'g'
group_summary = df.groupby('g').size().reset_index(name='count')
# Eliminar la variable de grupo 'g'
df.drop(columns=['g'], inplace=True)

