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
    'NO APLICA': np.nan,
    'NULL': np.nan,
    'ND': np.nan,
    'NA': np.nan,
    'SIN INFOR': np.nan,
    'SIN DETERM': np.nan,
    'POR DEFINIR': np.nan
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
df['fecha_ocur_anio'].fillna(value='0', inplace=True)
df['fecha_ocur_mes'].fillna(value='0', inplace=True)
df['fecha_ocur_dia'].fillna(value='0', inplace=True)

#df['fecha_ocur_anio'] = df['fecha_ocur_anio'].astype(int).astype(str)
#df['fecha_ocur_mes'] = df['fecha_ocur_mes'].astype(int).astype(str)
#df['fecha_ocur_dia'] = df['fecha_ocur_dia'].astype(int).astype(str)


# #df['fecha_ocur_mes'] = df['fecha_ocur_mes'].str.zfill(2)
# #df['fecha_ocur_dia'] = df['fecha_ocur_dia'].str.zfill(2)
# #df['fecha_ocur_anio'] = df['fecha_ocur_anio'].apply(lambda x: x if len(str(x)) == 4 else "")
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].str.replace("18", "19", n=1)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].str.replace("179", "197", n=1)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].str.replace("169", "196", n=1)
df['fecha_ocur_anio'] = df['fecha_ocur_anio'].str.replace("159", "195", n=1)
df['fecha_desaparicion_0'] = df['fecha_ocur_anio'].astype(int).astype(str) + df['fecha_ocur_mes'].astype(int).astype(str) + df['fecha_ocur_dia'].astype(int).astype(str)
df['fecha_desaparicion_dtf'] = pd.to_datetime(df['fecha_desaparicion_0'], format='%Y%m%d', errors='coerce')
# #df.drop(columns=['fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia'], inplace=True)
# #df.dropna(subset=['fecha_desaparicion_dtf'], inplace=True)
# Guardar el DataFrame en un archivo
# #df.to_stata("archivos depurados/BD_FGN_INACTIVOS.dta", index=False)
