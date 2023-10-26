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

