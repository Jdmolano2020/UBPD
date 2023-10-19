###################################################
# Autor: Esneyder
# Titulo: Homologacion de la fuente de información de la V_CNMH_SE
# Fecha: 21/06/2023
##
#

###################################################
# Cambia el path base intentando leer desde los parametros de invocacion

import sys
import os
import pandas as pd
import hashlib
import unicodedata
import re
from datetime import datetime
from sqlalchemy import create_engine
import numpy as np
import time
import yaml

# creacion de las funciones requeridas
# creacion de las funciones requeridas
def funcion_hash(row):
    return hashlib.sha1(str(row).encode()).hexdigest()

# Función de limpieza
def clean_func(x, na_values):
    # Transliterar a ASCII y convertir a mayúsculas
    x1 = x.str.encode('ascii', 'ignore').str.decode('ascii').str.upper()
    # Quitar espacios al inicio y al final
    x2 = x1.str.strip()
    # Reemplazar valores NA con NaN
    x2.replace(na_values, np.nan, inplace=True, regex=True)
    # Dejar solo caracteres alfanuméricos y espacios
    x3 = x2.str.replace(r'[^A-Z0-9 ]', ' ')
    # Quitar espacios adicionales
    x4 = x3.str.replace(r'\s+', ' ')
    return x4

# Define una función para limpiar nombres y apellidos
def limpiar_nombres_apellidos(nombre_completo):
    if nombre_completo in ["PERSONA SIN IDENTIFICAR", "NA"]:
        return None, None, None, None
    
    # Divide el nombre completo en tokens
    tokens = re.split(r'\s+', nombre_completo.strip())
    
    primer_nombre, segundo_nombre, primer_apellido, segundo_apellido = None, None, None, None
    
    # Elimina preposiciones
    preposiciones = ["DE", "DEL", "DE LAS", "DE LA", "DE LOS", "VAN", "LA", "VIUDA DE", "VIUDA", "SAN", "DA"]
    tokens = [token for token in tokens if token not in preposiciones]
    
    if len(tokens) == 4:
        primer_nombre, segundo_nombre, primer_apellido, segundo_apellido = tokens
    elif len(tokens) == 3:
        primer_nombre, primer_apellido, segundo_apellido = tokens
    elif len(tokens) == 2:
        primer_nombre, primer_apellido = tokens
    
    return primer_nombre, segundo_nombre, primer_apellido, segundo_apellido
# Limpiar todas las variables
# =============================================================================
# for variable in list(locals()):
#     del locals()[variable]
# =============================================================================
# Obtener los argumentos de la línea de comandos
args = sys.argv
if len(args) > 1:
    # Detecta si se proporciona la ruta base como argumento
    ruta_base = args[1]
else:
    # En caso contrario, define una ruta por defecto
    ruta_base = "C:/Users/HP/Documents/UBPD/HerramientaAprendizaje/Fuentes/OrquestadorUniverso"
# Cambiar el directorio de trabajo a la ruta base
os.chdir(ruta_base)
n_sample = ""
if len(args) > 2:
    # Detecta si se proporciona el número de muestras como argumento
    n_sample = args[2]
# 32
# Establecer la ruta base
ruta_base = "C:/Users/HP/Documents/UBPD/HerramientaAprendizaje/Fuentes/OrquestadorUniverso"

# Obtener la fecha y hora actual
fecha_inicio = datetime.now()
# 88
# Lectura del archivo DIVIPOLA
dane = pd.read_stata("fuentes secundarias\\tablas complementarias\\DIVIPOLA_municipios_122021.dta")
# Renombrar columnas
dane = dane.rename(columns={
    'codigo_dane_departamento': 'codigo_dane_departamento',
    'departamento': 'departamento_ocurrencia',
    'codigo_dane_municipio': 'codigo_dane_municipio',
    'municipio': 'municipio_ocurrencia'
})
# Eliminar la columna 'categoria_divipola'
dane = dane.drop(columns=['categoria_divipola'])
# Agregar nuevas filas
nuevas_filas = pd.DataFrame({
    'codigo_dane_departamento': ["94", "99", "99"],
    'departamento_ocurrencia': ["GUAINÍA", "VICHADA", "VICHADA"],
    'codigo_dane_municipio': ["94663", "99572", "99760"],
    'municipio_ocurrencia': ["MAPIRIPANA", "SANTA RITA", "SAN JOSÉ DE OCUNE"]
})
dane = pd.concat([dane, nuevas_filas], ignore_index=True)
# Crear DataFrame 'dane_depts' con las columnas 'codigo_dane_departamento'
# y 'departamento_ocurrencia' únicas
dane_depts = dane[['codigo_dane_departamento', 'departamento_ocurrencia']].drop_duplicates()
# Configurar la conexión a la base de datos (asegúrate de proporcionar los detalles correctos)
# db_url = "mssql+pyodbc://orquestacion.universo:Ubpd2022*@172.16.10.10/UNIVERSO_PDD?driver=ODBC+Driver+17+for+SQL+Server"
db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)
# Crear la consulta SQL
n_sample_p = f"top({n_sample})" if n_sample != "" else ""
query = "EXECUTE [dbo].[CONSULTA_V_CNMH_SE]"
# Ejecutar la consulta y cargar los datos en un DataFrame
cnmh = pd.read_sql(query, engine)
# Obtener el número de filas en el DataFrame cnmh
nrow_cnmh = len(cnmh)
# Obtener el número de casos y personas
n_casos = pd.read_sql("select count(*) from [dbo].[V_CNMH_SE_C]", engine).iloc[0, 0]
n_personas = pd.read_sql("select count(*) from [dbo].[V_CNMH_SE]", engine).iloc[0, 0]
# Obtener el número de casos sin personas
n_casos_sin_personas = pd.read_sql("select count(*) from [dbo].[V_CNMH_SE_C] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_RU])", engine).iloc[0, 0]
# Obtener los casos sin personas
casos_sin_personas = pd.read_sql("select * from [dbo].[V_CNMH_SE_C] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_SE_C])", engine)
# Obtener el número de personas sin casos
n_personas_sin_casos = pd.read_sql("select count(*) from [dbo].[V_CNMH_SE] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_SE])", engine).iloc[0, 0]
# Limpieza de nombres de columnas (clean_names no es necesario en pandas)
cnmh.columns = cnmh.columns.str.lower()
# Creación del ID único para cada registro
cnmh['id_registro'] = cnmh.apply(funcion_hash, axis=1)
cnmh['tabla_origen'] = "CNMH_SE"


