import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import re

def concat_values(*args):
    return ' '.join(arg for arg in args if arg.strip())

def clean_text(text):
    return re.sub(r'[^A-Z0-9 ]', '', text.upper())

# Asegúrate de importar la biblioteca pandas antes de usar df (DataFrame) y su columna 'var'

# parametros programa stata
parametro_ruta = ""
parametro_cantidad = ""
# Establecer la ruta de trabajo
ruta = "C:/Users/HP/Documents/UBPD/HerramientaAprendizaje/Fuentes/OrquestadorUniverso" # Cambia esto según tu directorio

# Verificar si `1` es una cadena vacía y ajustar el directorio de trabajo
# en consecuencia
if parametro_ruta == "":
    os.chdir(ruta)
else:
    os.chdir(parametro_ruta)

# Borrar el archivo "fuentes secundarias\V_JEP_CEV_CA_DESAPARICION.dta"
archivo_a_borrar = os.path.join("fuentes secundarias",
                                "V_JEP_CEV_CA_DESAPARICION.dta")
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
query = "EXECUTE [dbo].[CONSULTA_V_JEP_CEV]"

df = pd.read_sql_query(query, engine)
# Aplicar filtro si `2` no es una cadena vacía parametro cantidad registros
if parametro_cantidad != "":
    limite = int(parametro_cantidad)
    df = df[df.index < limite]
# Guardar el DataFrame en un archivo
archivo_csv = os.path.join("Fuentes secundarias",
                           "V_JEP_CEV_CA_DESAPARICION.csv")
df.to_csv(archivo_csv, index=False)
# # validar envi a un archivo para cambiar en encoding
# Cambiar directorio de trabajo
# #os.chdir(os.path.join(ruta, "Fuentes secundarias"))
# Traducir la codificación Unicode
# #archivo_a_traducir = "V_JEP_CEV_CA_DESAPARICION.csv"
# #archivo_utf8 = archivo_a_traducir.replace(".csv", "_utf8.csv")
# #if os.path.exists(archivo_a_traducir):
# #    os.system(
# #        f'unicode translate "{archivo_a_traducir}" "{archivo_utf8}" transutf8')
# #    os.remove(archivo_a_traducir)
# Crear un identificador de registro
df = pd.read_csv(archivo_csv)
df.columns = df.columns.str.lower()
df['duplicates_reg'] = df.duplicated()
df = df[~df['duplicates_reg']]
# Más manipulación de datos (Omitir esta sección en Python)
# No requiere ordenar el datafrane
# Origen de los datos
df['tabla_origen'] = "JEP_CEV"
# Código de identificación de la tabla de origen
df.rename(columns={'match_group_id': 'codigo_unico_fuente'}, inplace=True)
# Guardar el DataFrame final en un archivo
# #df.to_stata(archivo_utf8, write_index=False)
# Cambiar el nombre de las columnas a minúsculas
df.columns = df.columns.str.lower()

# 1. Seleccionar variables que serán homologadas para la integración
variables_a_mantener = [
    'nombre_1', 'nombre_2', 'apellido_1', 'apellido_2',
    'nombre_apellido_completo', 'cedula', 'otro_documento', 'edad',
    'yy_nacimiento', 'mm_nacimiento', 'dd_nacimiento', 'sexo', 'edad',
    'yy_nacimiento', 'mm_nacimiento', 'dd_nacimiento', 'etnia',
    'dept_code_hecho', 'muni_code_hecho', 'yy_hecho', 'ymd_hecho',
    'tipohecho', 'perp_agentes_estatales', 'perp_grupos_posdesmv_paramilitar',
    'perp_paramilitares', 'perp_guerrilla_eln', 'perp_guerrilla_farc',
    'perp_guerrilla_otra', 'perp_otro', 'codigo_unico_fuente',
    'in_ruv', 'in_vp_das', 'in_urt', 'in_uph', 'in_up', 'in_sindicalistas',
    'in_sijyp', 'in_ponal', 'in_pgn', 'in_personeria', 'in_paislibre',
    'in_onic', 'in_oacp', 'in_mindefensa', 'in_jmp', 'in_inml', 'in_icbf',
    'in_forjandofuturos', 'in_fgn', 'in_ejercito', 'in_credhos', 'in_conase',
    'in_cnmh', 'in_comunidades_negras', 'in_cev', 'in_cecoin', 'in_ccj',
    'in_cceeu', 'in_caribe', 'narrativo_hechos',  'tabla_origen'
]
# #df = df[variables_a_mantener]

cols_a_normalizar = ['sexo', 'etnia', 'nombre_1', 'nombre_apellido_completo',
                     'nombre_2', 'apellido_1', 'apellido_2', 'tipohecho']
for col in cols_a_normalizar:
    df[col] = df[col].str.upper()
    df[col] = df[col].str.replace("Á", "A")
    df[col] = df[col].str.replace("É", "E")
    df[col] = df[col].str.replace("Í", "I")
    df[col] = df[col].str.replace("Ó", "O")
    df[col] = df[col].str.replace("Ú", "U")
    df[col] = df[col].str.replace("Ü", "U")
    df[col] = df[col].str.replace("Ñ", "N")
    df[col] = df[col].str.replace("   ", " ")
    df[col] = df[col].str.replace("  ", " ")
    df[col] = df[col].str.strip()

    for i in range(0, 210):
        if (i != 32) and (i < 48 or i > 57) and (i < 65 or i > 90) and i != 209:
            char_to_remove = chr(i)
            df[col] = df[col].str.replace(char_to_remove, "", regex=True)
    df[col].replace(["NO APLICA", "NULL", "ND", "NA", "NR",
                     "SIN INFORMACION", "NO SABE", "DESCONOCIDO",
                     "POR DEFINIR", "SIN ESTABLECER"], "", inplace=True)
# 3. Homologación de estructura, formato y contenido
df['pais_ocurrencia'] = "COLOMBIA"
df['codigo_dane_departamento'] = df['dept_code_hecho'].apply(
    lambda x: str(int(x)).zfill(2) if not pd.isna(x) else "")
df.drop(columns=['dept_code_hecho'], inplace=True)

df['codigo_dane_municipio'] = df['muni_code_hecho'].apply(
    lambda x: str(int(x)).zfill(5) if not pd.isna(x) else "")
df.drop(columns=['muni_code_hecho'], inplace=True)

dane = pd.read_stata(
    "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")

df = pd.merge(df, dane, how='left', left_on=['codigo_dane_departamento', 'codigo_dane_municipio'],
                right_on=['codigo_dane_departamento', 'codigo_dane_municipio'])

df.rename(columns={'departamento': 'departamento_ocurrencia'}, inplace=True)

# Fecha de ocurrencia
df['ymd_hecho'] = df['ymd_hecho'].astype(str)
df['fecha_ocur_anio'] = df['ymd_hecho'].str[0:4]
df['fecha_ocur_mes'] = df['ymd_hecho'].str[4:6]
df['fecha_ocur_dia'] = df['ymd_hecho'].str[6:8]
df.rename(columns={'ymd_hecho': 'fecha_desaparicion'}, inplace=True)

df['fecha_ocur_mes'] = df['fecha_ocur_mes'].fillna("0")
df['fecha_ocur_dia'] = df['fecha_ocur_dia'].fillna("0")

df['mm_hecho'] = df['fecha_ocur_mes']
df['dd_hecho'] = df['fecha_ocur_dia']

df['fecha_hecho_0'] = df['dd_hecho'] + '-' + df['mm_hecho'] + '-' + df['fecha_ocur_anio']
df['fecha_desaparicion_dtf'] = pd.to_datetime(df['fecha_hecho_0'], format="%d-%m-%Y", errors='coerce')
# #df['fecha_desaparicion_dtf'] = df['fecha_desaparicion_dtf'].dt.strftime('%d')

# #df.drop(['dd_hecho', 'mm_hecho', 'fecha_ocur_anio'], axis=1, inplace=True)
# Presuntos responsables
df.rename(
    columns={
        'perp_agentes_estatales': 'pres_resp_agentes_estatales',
        'perp_grupos_posdesmv_paramilitar': 'pres_resp_grupos_posdesmov',
        'perp_paramilitares': 'pres_resp_paramilitares',
        'perp_guerrilla_eln': 'pres_resp_guerr_eln',
        'perp_guerrilla_farc': 'pres_resp_guerr_farc',
        'perp_guerrilla_otra': 'pres_resp_guerr_otra',
        'perp_otro': 'pres_resp_otro'},
    inplace=True)

df['pres_resp_guerr_otra'] = np.where(
    ((df['perp_guerrilla'] == 1.0)  &  (df['pres_resp_guerr_otra'] == 0.0)),1,0)

# Tipo de hecho
df['TH_DF'] = df['tipohecho'].str.contains('DESAPARICION').astype(int)
df['TH_SE'] = df['tipohecho'].str.contains('SECUESTRO').astype(int)
df['TH_RU'] = df['tipohecho'].str.contains('RECLUTAMIENTO').astype(int)
df.drop(['tipohecho'], axis=1, inplace=True)
# Relato
df.rename(columns={'narrativo_hechos': 'descripcion_relato'}, inplace=True)
df['descripcion_relato'] = df['descripcion_relato'].str.upper()

th_df_counts = df["TH_DF"].value_counts()
th_se_counts = df["TH_SE"].value_counts()
th_ru_counts = df["TH_RU"].value_counts()

# Datos sobre las personas dadas por desparecidas
# Nombres y apellidos
# Renombrar columnas
df.rename(columns={'nombre_1': 'primer_nombre',
                   'nombre_2': 'segundo_nombre',
                   'apellido_1': 'primer_apellido',
                   'apellido_2': 'segundo_apellido',
                   'nombre_apellido_completo': 'nombre_completo'},
          inplace=True)
# Corrección del uso de artículos y preposiciones en los nombres
condicion = (df['segundo_nombre'].isin(['DEL', 'DE', 'DE LAS', 'DE LOS']))
df.loc[condicion, 'segundo_nombre'] = df['segundo_nombre'] + \
    " " + df['primer_apellido']
df.loc[condicion, 'primer_apellido'] = df['segundo_apellido']
df.loc[condicion, 'segundo_apellido'] = ""
condicion = (df['primer_apellido'].isin(['DEL', 'DE', 'DE LAS', 'DE LOS']))
df.loc[condicion, 'primer_apellido'] = df['primer_apellido'] + \
    " " + df['segundo_apellido']
df.loc[condicion, 'segundo_apellido'] = ""
# Eliminar nombres y apellidos cuando solo se registra la letra inicial
columnas_a_verificar = [
    'primer_nombre',
    'primer_apellido',
    'segundo_nombre',
    'segundo_apellido']
for columna in columnas_a_verificar:
    df.loc[df[columna].str.len() == 1, columna] = ""
# Reemplazar primer apellido por segundo apellido cuando el primer campo
# se encuentra vacío
condicion = (df['primer_apellido'] == "") & (df['segundo_apellido'] != "")
df.loc[condicion, 'primer_apellido'] = df['segundo_apellido']
df.loc[condicion, 'segundo_apellido'] = ""

df['primer_nombre'] = df['primer_nombre'].fillna('')
df['primer_apellido'] = df['primer_apellido'].fillna('')
df['segundo_nombre'] = df['segundo_nombre'].fillna('')
df['segundo_apellido'] = df['segundo_apellido'].fillna('')

# Validación de la variable "nombre completo"
df['nombre_completo_'] = df.apply(lambda row: concat_values(
    row['primer_nombre'], row['segundo_nombre'],
    row['primer_apellido'], row['segundo_apellido']),
    axis=1)
df['nombre_completo_'] = df['nombre_completo_'].str.strip()
df['nombre_completo_'] = df['nombre_completo_'].str.replace('  ', ' ')
# Renombrar la columna final
df.rename(columns={'nombre_completo_': 'nombre_completo'}, inplace=True)

# Documento
# Eliminar símbolos y caracteres especiales
df['cedula'] = df['cedula'].astype(str)
df['cedula'] = df['cedula'].fillna('')
df['documento'] = df['cedula'].apply(clean_text)
# Eliminar cadenas de texto sin números
for i in range(48, 58):
    char = chr(i)
    df['documento'] = df['documento'].str.replace(char, '',regex=True)
# Eliminar cadenas complejas de texto tipo anotaciones
for i in range(256):
    char = chr(i)
    if char not in "0123456789":
        df['documento'] = df['documento'].str.replace(char, '',regex=True)
        
# Eliminar registros de documentos de identificación iguales a '0'
# #df = df[df['documento'] != '0']
# Limpiar y eliminar espacios adicionales
df['documento'] = df['documento'].str.strip()
df['documento'] = df['documento'].str.replace('   ', ' ')
df['documento'] = df['documento'].str.replace('  ', ' ')
# Eliminar columnas temporales
# df.drop(columns=['cedula'], inplace=True)
# Transformar la columna 'sexo'
df['sexo'].replace({'OTRO': 'INTERSEX'}, inplace=True)
# Transformar la columna 'iden_pertenenciaetnica' y renombrarla
df.rename(columns={'iden_pertenenciaetnica': 'etnia'}, inplace=True)
# Reemplazar los valores en 'etnia'
df['etnia'].replace({'MESTIZO': 'NINGUNA', 'ROM': 'RROM'}, inplace=True)
# 237
# Fecha de nacimiento
# Reemplazar valores fuera de rango con NaN

df.loc[df['yy_nacimiento'] < 1900, 'yy_nacimiento'] = None
df.loc[df['yy_nacimiento'].isna(), 'yy_nacimiento'] = None

df.loc[df['yy_nacimiento'].astype(float) < 1900, 'yy_nacimiento'] = None

df.loc[(df['mm_nacimiento'] < 1) | (df['mm_nacimiento'] > 12),
       'mm_nacimiento'] = None
df.loc[(df['dd_nacimiento'] < 1) | (df['dd_nacimiento'] > 31),
       'dd_nacimiento'] = None
# Crear la columna 'fecha_nacimiento_dtf'

df['mm_nacimiento'] = df['mm_nacimiento'].fillna("0")
df['dd_nacimiento'] = df['dd_nacimiento'].fillna("0")
df['yy_nacimiento'] = df['yy_nacimiento'].fillna("0")

df['yy_nacimiento'] = df['yy_nacimiento'].astype(int)
df['dd_nacimiento'] = df['dd_nacimiento'].astype(int)
df['mm_nacimiento'] = df['mm_nacimiento'].astype(int)
df['yy_nacimiento'] = df['yy_nacimiento'].astype(str)
df['dd_nacimiento'] = df['dd_nacimiento'].astype(str)
df['mm_nacimiento'] = df['mm_nacimiento'].astype(str)


df['fecha_nacimiento_0'] = df['dd_nacimiento'] + '-' + df['mm_nacimiento'] + '-' + df['yy_nacimiento']
df['fecha_nacimiento_dtf'] = pd.to_datetime(df['fecha_nacimiento_0'], format="%d-%m-%Y", errors='coerce')

# Crear las columnas 'anio_nacimiento', 'mes_nacimiento' y 'dia_nacimiento'
df['anio_nacimiento'] = df['yy_nacimiento'].astype(str).str.zfill(4)
df['mes_nacimiento'] = df['mm_nacimiento'].astype(str).str.zfill(2)
df['dia_nacimiento'] = df['dd_nacimiento'].astype(str).str.zfill(2)

df['anio_nacimiento'] = df['anio_nacimiento'].fillna("0")
df['mes_nacimiento'] = df['mes_nacimiento'].fillna("0")
df['dia_nacimiento'] = df['dia_nacimiento'].fillna("0")

# Crear la columna 'fecha_nacimiento' mediante concatenación
df['fecha_nacimiento_0'] = df['dia_nacimiento'] + '-' + df['mes_nacimiento'] + '-' + df['anio_nacimiento']
df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento_0'], format="%d-%m-%Y", errors='coerce')

# Filtrar las filas donde todas las columnas tienen valores
# #df = df[(df['anio_nacimiento'] != '') & (
# #   df['mes_nacimiento'] != '') & (df['dia_nacimiento'] != '')]
# Eliminar las columnas temporales
# #df.drop(columns=['yy_nacimiento', 'mm_nacimiento', 'dd_nacimiento'],
# #        inplace=True)
# 258
# Edad
# Validación de rango
# Reemplazar valores de 'edad' fuera de rango con NaN
df.loc[(df['edad'] < 0) | (df['edad'] > 100), 'edad'] = None
# Crear la columna 'edad_desaparicion_est'
df['edad_desaparicion_est'] = (
    (df['fecha_desaparicion_dtf'].dt.year * 12 + df['fecha_desaparicion_dtf'].dt.month) - (
        df['fecha_nacimiento_dtf'].dt.year * 12 + df['fecha_nacimiento_dtf'].dt.month)) // 12
# Calcular la diferencia absoluta entre 'edad' y 'edad_desaparicion_est'
df['dif_edad'] = abs(df['edad_desaparicion_est'] - df['edad'])

# Crear la columna 'inconsistencia_fechas'
# en función de las condiciones especificadas
df['inconsistencia_fechas'] = (
    (df['edad_desaparicion_est'] < 0) | (
        df['edad_desaparicion_est'] > 100)) & (
            df['edad_desaparicion_est'].notna())
# Reemplazar valores en 'inconsistencia_fechas' con 3
# si se cumple la condición adicional
df.loc[(df['fecha_nacimiento_dtf'] == df['fecha_desaparicion_dtf']) &
       (df['fecha_nacimiento_dtf'].notna()), 'inconsistencia_fechas'] = 3
# Filtrar las filas donde 'edad' no es nulo ni cero y
# 'edad_desaparicion_est' no es nulo ni cero
# #df = df[(df['edad'].notna()) & (df['edad'] != 0) &
# #        (df['edad_desaparicion_est'].notna())
# #        & (df['edad_desaparicion_est'] != 0)]
# Filtrar las filas donde 'edad_desaparicion_est' está fuera de rango
df.loc[(df['edad_desaparicion_est'] < 0) | (df['edad_desaparicion_est'] > 100),
       'inconsistencia_fechas'] = 3
# Reemplazar 'fecha_nacimiento_dtf' con NaN en las filas con
# 'inconsistencia_fechas' diferente de cero
df.loc[df['inconsistencia_fechas'] != 0, 'fecha_nacimiento_dtf'] = None
# Reemplazar columnas relacionadas con la fecha con cadenas vacías
# en las filas con 'inconsistencia_fechas' diferente de cero
columnas_fecha = ['anio_nacimiento', 'mes_nacimiento', 'dia_nacimiento',
                  'fecha_nacimiento']
for columna in columnas_fecha:
    df.loc[df['inconsistencia_fechas'] != 0, columna] = ''
# Eliminar la columna 'inconsistencia_fechas' si ya no es necesaria
# #df.drop(columns=['inconsistencia_fechas'], inplace=True)
# 277
# 4. Identificación y eliminación de Registros No Identificados
# Crear una columna 'non_miss' que cuente la cantidad de valores no nulos
# en las columnas especificadas
df['non_miss'] = df[['primer_nombre', 'segundo_nombre', 'primer_apellido',
                     'segundo_apellido']].count(axis=1)

# Crear una columna 'rni' basada en la condición de que 'non_miss'
# sea menor que 2
df['rni'] = (df['non_miss'] < 2).astype(int)

# Aplicar condiciones adicionales para 'rni'
df.loc[(df['primer_nombre'] == "") | (df['primer_apellido'] == ""), 'rni'] = 1
df.loc[(df['codigo_dane_departamento'] == "") & (df['fecha_ocur_anio'] == "")
       & (df['documento'] == "") & (df['fecha_nacimiento'] == ""), 'rni'] = 1

conteo = df["rni"].value_counts()

# Calcular 'rni_' y 'N' para cada grupo de 'codigo_unico_fuente'
df['rni_'] = df.groupby('codigo_unico_fuente')['rni'].transform('sum')
df['N'] = df.groupby('codigo_unico_fuente').cumcount() + 1

# 287
# Personas no identificadas que podrían integrarse posteriormente
# al Universo mediante una comparación de nombres
# Filtrar las filas donde 'rni_' es igual a 'N'
df_posterior = df[df['rni_'] == df['N']]
# Guardar el DataFrame filtrado en un archivo
df_posterior.to_csv("archivos depurados/BD_CEV_JEP_PNI.csv", index=False)
# Contar las filas del DataFrame
count = len(df)
# Crear una columna 'g' basada en 'codigo_unico_fuente'
df['g'] = df['codigo_unico_fuente'].astype('category').cat.codes
# Calcular la suma de 'g'
sum_g = df['g'].sum()
# Eliminar la columna 'g'
df.drop(columns=['g'], inplace=True)
# Eliminar registros donde 'rni_' es igual a 'N'
df = df[df['rni_'] != df['N']]
# 300
# 5. Identificación de filas únicas
# Crear una lista con las columnas que deseas mantener
columnas_a_mantener = [
    'tabla_origen', 'codigo_unico_fuente', 'nombre_completo', 'primer_nombre',
    'segundo_nombre', 'primer_apellido', 'segundo_apellido', 'documento',
    'sexo', 'etnia', 'fecha_nacimiento', 'anio_nacimiento',
    'mes_nacimiento', 'dia_nacimiento', 'edad', 'fecha_desaparicion',
    'fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia', 'pais_ocurrencia',
    'codigo_dane_departamento', 'departamento_ocurrencia',
    'codigo_dane_municipio', 'municipio', 
    'TH_DF','TH_SE', 'TH_RU', 
    'pres_resp_agentes_estatales', 'pres_resp_grupos_posdesmov',
    'pres_resp_paramilitares', 'pres_resp_guerr_eln', 'pres_resp_guerr_farc',
    'pres_resp_guerr_otra',  'pres_resp_otro',
    'descripcion_relato', 
    'in_ruv', 'in_vp_das', 'in_urt', 'in_uph', 'in_up', 'in_sindicalistas',
    'in_sijyp', 'in_ponal', 'in_pgn', 'in_personeria', 'in_paislibre',
    'in_onic', 'in_oacp', 'in_mindefensa', 'in_jmp', 'in_inml', 'in_icbf',
    'in_forjandofuturos', 'in_fgn', 'in_ejercito', 'in_credhos', 'in_conase',
    'in_cnmh', 'in_comunidades_negras', 'in_cev', 'in_cecoin', 'in_ccj',
    'in_cceeu', 'in_caribe','rni','non_miss' ]
# Filtrar las columnas que deseas mantener en el DataFrame
df = df[columnas_a_mantener]
# Crear una columna 'nonmiss' que cuente la cantidad de valores no nulos
# en las columnas especificadas
columnas_no_nulas = [
    'primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido',
    'nombre_completo', 'documento', 'sexo', 'edad', 'dia_nacimiento',
    'mes_nacimiento', 'anio_nacimiento', 'etnia', 'TH_DF',
    'TH_SE', 'TH_RU', 'fecha_ocur_dia', 'fecha_ocur_mes', 'fecha_ocur_anio',
    'codigo_dane_departamento', 'codigo_dane_municipio', 
    'pres_resp_agentes_estatales', 'pres_resp_grupos_posdesmov',
    'pres_resp_paramilitares', 'pres_resp_guerr_eln', 'pres_resp_guerr_farc',
    'pres_resp_guerr_otra',  'pres_resp_otro', 
    'descripcion_relato', 
    'in_ruv', 'in_vp_das', 'in_urt', 'in_uph', 'in_up', 'in_sindicalistas',
    'in_sijyp', 'in_ponal', 'in_pgn', 'in_personeria', 'in_paislibre',
    'in_onic', 'in_oacp', 'in_mindefensa', 'in_jmp', 'in_inml', 'in_icbf',
    'in_forjandofuturos', 'in_fgn', 'in_ejercito', 'in_credhos', 'in_conase',
    'in_cnmh', 'in_comunidades_negras', 'in_cev', 'in_cecoin', 'in_ccj',
    'in_cceeu', 'in_caribe']
df['nonmiss'] = df[columnas_no_nulas].count(axis=1)
# Ordenar el DataFrame
df = df.sort_values(by=['codigo_unico_fuente', 'rni', 'documento', 'nonmiss'])
# Mantener solo la primera fila de cada grupo 'codigo_unico_fuente'
df = df.drop_duplicates(subset=['codigo_unico_fuente'], keep='first')
# Eliminar columnas temporales
# #df.drop(columns=['rni*', 'nonmiss'], inplace=True)
df.to_csv("archivos depurados/BD_CEV_JEP.csv", index=False)
# 318



