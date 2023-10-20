import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_PARAMILITARES
import FASE1_HOMOLOGACION_CAMPO_FUERZA_PUBLICA_Y_AGENTES_DEL_ESTADO
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_FARC 
import FASE1_HOMOLOGACION_CAMPO_BANDAS_CRIMINALES
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_ELN
import FASE1_HOMOLOGACION_CAMPO_OTRAS_GUERRILLAS

def concat_values(*args):
    return ' '.join(arg for arg in args if arg.strip())

def clean_func(x):
    if x is None:
        x = ' '
    x1 = x.astype(str)
    x2 = x1.str.upper()
    return x2
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
variables_a_convertir = ['tabla_origen', 'nombre_completo', 'primer_nombre', 'segundo_nombre', 'primer_apellido',
                         'segundo_apellido', 'sexo', 'iden_orientacionsexual', 'iden_pertenenciaetnica_', 'tipo_de_hecho',
                         'descripcion_relato', 'presunto_responsable', 'situacion_actual_des']

df[variables_a_convertir] = df[variables_a_convertir].apply(lambda x: clean_func(x))


# Reemplazar valores vacíos en todas las columnas con palabras clave
palabras_clave = ['NO DETERMINADO DESDE FUENTE', 'SIN INFORMACIÓN', 'SIN INFORMACION', 'SIN DATOS EN ARCHIVO FUENTE']

for var in df.columns:
    df[var] = df[var].apply(lambda x: np.nan if any(keyword in str(x) for keyword in palabras_clave) else x)

# Limpieza de caracteres especiales y espacios en variables específicas
variables_a_limpiar = ['nombre_completo', 'primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido',
                       'nombre_completo', 'presunto_responsable', 'sexo', 'tipo_de_hecho', 'iden_pertenenciaetnica_']

for var in variables_a_limpiar:
    df[var] = df[var].str.replace('Á', 'A').str.replace('É', 'E').str.replace('Í', 'I').str.replace('Ó', 'O')\
        .str.replace('Ú', 'U').str.replace('Ü', 'U').str.replace('Ñ', 'N').str.replace('   ', ' ').str.replace('  ', ' ')\
        .str.strip()
# Remover caracteres no permitidos
for var in variables_a_limpiar:
    for i in range(210):
        if chr(i) not in [' ', '0-9', 'A-Z', 'Á', 'É', 'Í', 'Ó', 'Ú', 'Ü', 'Ñ', 'ñ']:
            df[var] = df[var].str.replace(chr(i), '')
# Reemplazar valores basados en palabras clave en variables específicas
palabras_clave_reemplazar = ['NO APLICA', 'NULL', 'ND', 'NA', 'NR', 'SIN INFOR', 'NO SABE', 'DESCONOCID', 'POR DEFINIR', 'POR ESTABLECER']

for var in variables_a_limpiar:
    df[var] = df[var].apply(lambda x: np.nan if any(keyword in str(x) for keyword in palabras_clave_reemplazar) else x)

# 3. Homologación de estructura, formato y contenido
# Datos sobre los hechos	
# Lugar de ocurrencia- País/Departamento/Muncipio
# Crear la variable 'pais_ocurrencia' basada en 'pais_de_ocurrencia'
df['pais_ocurrencia'] = np.where(df['pais_de_ocurrencia'] == 57, 'COLOMBIA',
                                   None)
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

df['fecha_desaparicion'] = df['fecha_ocur_anio'] + "-" + df['fecha_ocur_mes'] + "-" + df['fecha_ocur_dia']
df['fecha_desaparicion'] = df['fecha_ocur_anio'].str.replace('NaT--', '', n=1)

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
