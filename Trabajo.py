import os
from sqlalchemy import create_engine
import pandas as pd


def concat_values(*args):
    return ' '.join(arg for arg in args if arg.strip())


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
archivo_csv = os.path.join("fuentes secundarias",
                           "V_JEP_CEV_CA_DESAPARICION.csv")
df.to_csv(archivo_csv, index=False)
# Cambiar directorio de trabajo
os.chdir(os.path.join(ruta, "fuentes secundarias"))
# Traducir la codificación Unicode
archivo_a_traducir = "V_JEP_CEV_CA_DESAPARICION.dta"
archivo_utf8 = archivo_a_traducir.replace(".dta", "_utf8.dta")
if os.path.exists(archivo_a_traducir):
    os.system(
        f'unicode translate "{archivo_a_traducir}" "{archivo_utf8}" transutf8')
    os.remove(archivo_a_traducir)
# Crear un identificador de registro
df = pd.read_stata(archivo_utf8, encoding=encoding)
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
df.to_stata(archivo_utf8, write_index=False)
# Cambiar el nombre de las columnas a minúsculas
df.columns = df.columns.str.lower()

# 1. Seleccionar variables que serán homologadas para la integración
variables_a_mantener = [
    'nombre_1', 'nombre_2', 'apellido_1', 'apellido_2',
    'nombre_apellido_completo', 'cedula', 'otro_documento', 'edad',
    'yy_nacimiento', 'mm_nacimiento', 'dd_nacimiento', 'sexo', 'edad',
    'yy_nacimiento', 'mm_nacimiento', 'dd_nacimiento', 'etnia',
    'dept_code_hecho', 'muni_code_hecho', 'yy_hecho', 'ymd_hecho',
    'tipohecho', 'perp_*', 'codigo_unico_fuente', 'in*', 'narrativo_hechos',
    'tabla_origen'
]
df = df[variables_a_mantener]
