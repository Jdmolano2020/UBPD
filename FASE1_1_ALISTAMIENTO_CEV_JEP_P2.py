import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import homologacion.limpieza
import homologacion.fecha
import homologacion.nombres
import homologacion.documento
import homologacion.etnia
import homologacion.nombre_completo


# creacion de las funciones requeridas
def clean_text(text):
    if text is None or text.isna().any():
        text = text.astype(str)
    text = text.apply(homologacion.limpieza.normalize_text)
    return text


def concat_values(*args):
    return ' '.join(arg for arg in args if arg.strip())


# parametros programa stata
parametro_ruta = ""
parametro_cantidad = ""
# Establecer la ruta de trabajo
ruta = "C:/Users/HP/Documents/UBPD/HerramientaAprendizaje/Fuentes/OrquestadorUniverso" 
# Cambia esto según tu directorio

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
nrow_df = len(df)
print("Registros despues cargue fuente: ", nrow_df)
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
nrow_df = len(df)
print("Registros despues eliminar duplicados: ", nrow_df)

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
# Definir una lista de valores a reemplazar
na_values = {
    "NO APLICA": None,
    "NULL": None,
    "ND": None,
    "NA": None,
    "NAN": None,
    "NR": None,
    "SIN INFORMACION": None,
    "NO SABE": None,
    "DESCONOCIDO": None,
    "POR DEFINIR": None,
    "SIN ESTABLECER": None,
}

cols_a_normalizar = ['sexo', 'etnia', 'nombre_1', 'nombre_apellido_completo',
                     'nombre_2', 'apellido_1', 'apellido_2', 'cedula']

df[cols_a_normalizar] = df[cols_a_normalizar].apply(clean_text)
df[cols_a_normalizar] = df[cols_a_normalizar].replace(na_values)

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

df = pd.merge(df, dane, how='left',
              left_on=['codigo_dane_departamento',
                       'codigo_dane_municipio'],
              right_on=['codigo_dane_departamento', 'codigo_dane_municipio'])

nrow_df = len(df)
print("Registros despues cruzar divipola: ", nrow_df)
df.rename(columns={'departamento': 'departamento_ocurrencia'}, inplace=True)

# fecha de ocurrencia
df['ymd_hecho'] = df['ymd_hecho'].astype(str)
df['fecha_ocur_anio'] = df['ymd_hecho'].str[0:4]
df['fecha_ocur_mes'] = df['ymd_hecho'].str[4:6]
df['fecha_ocur_dia'] = df['ymd_hecho'].str[6:8]

df['fecha_ocur_anio'] = pd.to_numeric(df['fecha_ocur_anio'], errors='coerce')
df['fecha_ocur_mes'] = pd.to_numeric(df['fecha_ocur_mes'], errors='coerce')
df['fecha_ocur_dia'] = pd.to_numeric(df['fecha_ocur_dia'], errors='coerce')
homologacion.fecha.fechas_validas(df, fecha_dia='fecha_ocur_dia',
                                  fecha_mes='fecha_ocur_mes',
                                  fecha_anio='fecha_ocur_anio',
                                  fecha='fecha_desaparicion_dtf',
                                  fechat='fecha_desaparicion')
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

df['pres_resp_guerr_otra'] = np.where(((df['perp_guerrilla'] == 1.0) &
                                       (df['pres_resp_guerr_otra'] == 0.0)),
                                      1, 0)

# Tipo de hecho
df['tipohecho'].replace(
    {"DesapariciÃ³n - Secuestro - Reclutamiento": "DESAPARICION - SECUESTRO - RECLUTAMIENTO",
     "DesapariciÃ³n - Secuestro":  "DESAPARICION - SECUESTRO",
     "DesapariciÃ³n": "DESAPARICION",
     "DesapariciÃ³n - Reclutamiento": "DESAPARICION - SECUESTRO - RECLUTAMIENTO"
     }, inplace=True)




df['TH_DF'] = df['tipohecho'].str.contains('DESAPARICION').astype(int)
df['TH_SE'] = df['tipohecho'].str.contains('SECUESTRO').astype(int)
df['TH_RU'] = df['tipohecho'].str.contains('RECLUTAMIENTO').astype(int)
# df.drop(['tipohecho'], axis=1, inplace=True)
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

homologacion.nombres.nombres_validos(df, primer_nombre='primer_nombre',
                                     segundo_nombre='segundo_nombre',
                                     primer_apellido='primer_apellido',
                                     segundo_apellido='segundo_apellido',
                                     nombre_completo='nombre_completo')
# Documento
# Eliminar espacios en blanco al principio
# y al final de la columna numero_documento
df['cedula'] = df['cedula'].astype(str)
df['documento'] = df['cedula'].fillna('')
df['documento'] = df['documento'].str.strip()
homologacion.documento.documento_valida(df, documento='documento')
# Transformar la columna 'sexo'
df['sexo'].replace({'OTRO': 'INTERSEX'}, inplace=True)
# Transformar la columna 'iden_pertenenciaetnica' y renombrarla
df['iden_pertenenciaetnica'] = df['etnia']
# Pertenencia étnica
homologacion.etnia.etnia_valida(df, etnia='iden_pertenenciaetnica')
# 237
# Fecha de nacimiento
df_r = df[
    df['codigo_unico_fuente'] == '0003c22c6df379bf4f780d84230415100cf99e59']

df['anio_nacimiento'] = pd.to_numeric(df['yy_nacimiento'], errors='coerce')
df['mes_nacimiento'] = pd.to_numeric(df['mm_nacimiento'], errors='coerce')
df['dia_nacimiento'] = pd.to_numeric(df['dd_nacimiento'], errors='coerce')

homologacion.fecha.fechas_validas(df, fecha_dia='dia_nacimiento',
                                  fecha_mes='mes_nacimiento',
                                  fecha_anio='anio_nacimiento',
                                  fecha='fecha_nacimiento_dtf',
                                  fechat='fecha_nacimiento')

# Edad
# Validación de rango
# Reemplazar valores de 'edad' fuera de rango con NaN
df.loc[(df['edad'] < 0) | (df['edad'] > 100), 'edad'] = None
# Crear la columna 'edad_desaparicion_est'
df['edad_desaparicion_est'] = (
    (df['fecha_desaparicion_dtf'].dt.year * 12 +
     df['fecha_desaparicion_dtf'].dt.month) -
    (df['fecha_nacimiento_dtf'].dt.year * 12 +
     df['fecha_nacimiento_dtf'].dt.month)) // 12
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
nrow_df = len(df)
print("Registros despues eliminar RNI: ", nrow_df)
# 300
# 5. Identificación de filas únicas
# Crear una lista con las columnas que deseas mantener
df['campo_novacios_p1'] = np.where(
    ((df['nombre_completo'].notna()) &
     (df['nombre_completo'].str.len() > 0)), 1100, 0)

df['campo_novacios_p2'] = np.where(
    ((df['primer_nombre'].notna()) &
     (df['primer_nombre'].str.len() > 0)), 1000, 0)

df['campo_novacios_p3'] = np.where(
    ((df['segundo_nombre'].notna()) &
     (df['segundo_nombre'].str.len() > 0)), 700, 0)

df['campo_novacios_p4'] = np.where(
    ((df['primer_apellido'].notna()) &
     (df['primer_apellido'].str.len() > 0)), 900, 0)

df['campo_novacios_p5'] = np.where(
    ((df['segundo_apellido'].notna()) &
     (df['segundo_apellido'].str.len() > 0)), 600, 0)

df['campo_novacios_p6'] = np.where(
    ((df['documento'].notna()) &
     (df['documento'].str.len() > 0)), 800, 0)

df['campo_novacios_p7'] = np.where(
    ((df['fecha_nacimiento'].notna()) &
     (df['fecha_nacimiento'].str.len() > 0)), 500, 0)

df['campo_novacios_p8'] = np.where(
    ((df['fecha_desaparicion'].notna()) &
     (df['fecha_desaparicion'].str.len() > 0)), 400, 0)

df['campo_novacios_p9'] = np.where(
    ((df['sexo'].notna()) &
     (df['sexo'].str.len() > 0)), 300, 0)

df['campo_novacios_p10'] = np.where(
    ((df['edad'].notna()) &
     (df['edad'].astype(str).str.len() > 0)), 200, 0)

df['campo_novacios_p11'] = np.where(
    ((df['iden_pertenenciaetnica'].notna()) &
     (df['iden_pertenenciaetnica'].str.len() > 0)), 100, 0)

cols_to_sum = ['campo_novacios_p1', 'campo_novacios_p2',
               'campo_novacios_p3', 'campo_novacios_p4',
               'campo_novacios_p5', 'campo_novacios_p6',
               'campo_novacios_p7', 'campo_novacios_p8',
               'campo_novacios_p9', 'campo_novacios_p10',
               'campo_novacios_p11']
df['campo_novacios'] = 0
for col in cols_to_sum:
    df['campo_novacios'] = df['campo_novacios'] + df[col]

columnas_a_mantener = [
    'tabla_origen', 'codigo_unico_fuente', 'nombre_completo', 'primer_nombre',
    'segundo_nombre', 'primer_apellido', 'segundo_apellido', 'documento',
    'sexo', 'etnia', 'fecha_nacimiento', 'anio_nacimiento',
    'mes_nacimiento', 'dia_nacimiento', 'edad', 'fecha_desaparicion',
    'fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia', 'pais_ocurrencia',
    'codigo_dane_departamento', 'departamento_ocurrencia',
    'codigo_dane_municipio', 'municipio',
    'iden_pertenenciaetnica',
    'TH_DF', 'TH_SE', 'TH_RU',
    'pres_resp_agentes_estatales', 'pres_resp_grupos_posdesmov',
    'pres_resp_paramilitares', 'pres_resp_guerr_eln', 'pres_resp_guerr_farc',
    'pres_resp_guerr_otra',  'pres_resp_otro',
    'descripcion_relato',
    'in_ruv', 'in_vp_das', 'in_urt', 'in_uph', 'in_up', 'in_sindicalistas',
    'in_sijyp', 'in_ponal', 'in_pgn', 'in_personeria', 'in_paislibre',
    'in_onic', 'in_oacp', 'in_mindefensa', 'in_jmp', 'in_inml', 'in_icbf',
    'in_forjandofuturos', 'in_fgn', 'in_ejercito', 'in_credhos', 'in_conase',
    'in_comunidades_negras', 'in_cev', 'in_cecoin', 'in_ccj',
    'in_cceeu', 'in_caribe', 'rni', 'non_miss', 'campo_novacios']
# Filtrar las columnas que deseas mantener en el DataFrame
df = df[columnas_a_mantener]

cols_to_clean = ['descripcion_relato', 'sexo', 'municipio']
for col in cols_to_clean:
    df[col] = df[col].fillna("")

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
    'in_comunidades_negras', 'in_cev', 'in_cecoin', 'in_ccj',
    'in_cceeu', 'in_caribe']
df['nonmiss'] = df[columnas_no_nulas].count(axis=1)
# Ordenar el DataFrame
df = df.sort_values(by=['codigo_unico_fuente', 'campo_novacios', 'rni',
                        'nombre_completo',
                        'nonmiss'], ascending=False)
nrow_df = len(df)
print("Registros despues ordenar: ", nrow_df)
# Mantener solo la primera fila de cada grupo 'codigo_unico_fuente'
df = df.drop_duplicates(subset=['codigo_unico_fuente'], keep='first')
nrow_df = len(df)
print("Registros despues eliminar duplicados codigo_unico_fuente: ", nrow_df)

# Eliminar columnas temporales
# #df.drop(columns=['rni*', 'nonmiss'], inplace=True)
df.to_csv("archivos depurados/BD_CEV_JEP.csv", index=False)
# 318
df.to_sql(name="BD_CEV_JEP", con=engine, if_exists="replace", index=False)
