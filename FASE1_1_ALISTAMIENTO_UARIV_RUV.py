import os
import json
import time
import yaml
import numpy as np
import pandas as pd
import hashlib
from datetime import datetime
import homologacion.etnia
import homologacion.fecha
import homologacion.nombres
import homologacion.limpieza
import homologacion.documento
import homologacion.nombre_completo
from sqlalchemy import create_engine
from FASE1_CORRIGE_FECHA_OCURRENCIA_UARIV import corrige_fecha_ocurrencia


def clean_text(text):
    if text is None or text.isna().any():
        text = text.astype(str)
    text = text.apply(homologacion.limpieza.normalize_text)
    return text


# Función para generar un hash SHA-1 a partir de todas las columnas de una fila
def funcion_hash(row):
    # Concatena todas las columnas de la fila en una cadena
    row_as_string = ''.join(map(str, row))

    # Aplica la función hash SHA-1
    hash_result = hashlib.sha1(row_as_string.encode()).hexdigest()

    return hash_result


start_time = time.time()
# Configurar la codificación Unicode
encoding = "ISO-8859-1"

with open('config.json') as config_file:
    config = json.load(config_file)

DIRECTORY_PATH = config['DIRECTORY_PATH']
DB_SERVER = config['DB_SERVER']
DB_INSTANCE = config['DB_INSTANCE']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_DATABASE = "PRD_QPREP_UBPD"
DB_SCHEMA = "dbo"
DB_TABLE = "UARIV_UNI_VIC_LB_"

# Realizar un merge con el archivo DIVIPOLA_departamentos_122021.dta
dane_file_path = os.path.join(
    DIRECTORY_PATH, "fuentes secundarias", "tablas complementarias",
    "DIVIPOLA_municipios_122021.dta")
dane = pd.read_stata(dane_file_path)

# Renombrar columnas
dane = dane.rename(columns={
    'codigo_dane_departamento': 'codigo_dane_departamento',
    'departamento': 'departamento_ocurrencia',
    'codigo_dane_municipio': 'codigo_dane_municipio',
    'municipio': 'municipio_ocurrencia'})

# Seleccionar columnas y eliminar 'categoria_divipola'
dane = dane.drop(columns=['categoria_divipola'])
# Crear un DataFrame con las filas adicionales
additional_data = pd.DataFrame({
    'codigo_dane_departamento': [np.nan, "94", "99", "99"],
    'departamento_ocurrencia': [np.nan, "GUAINIA", "VICHADA", "VICHADA"],
    'codigo_dane_municipio': [np.nan, "94663", "99572", "99760"],
    'municipio_ocurrencia': [np.nan, "MAPIRIPANA", "SANTA RITA",
                             "SAN JOSE DE OCUNE"]})

# Concatenar los DataFrames
dane = pd.concat([dane, additional_data], ignore_index=True)

# Seleccionar las columnas 'codigo_dane_departamento'
# y 'departamento_ocurrencia' y eliminar duplicados
dane_depts = dane[['codigo_dane_departamento',
                  'departamento_ocurrencia']].drop_duplicates()

db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}\\{DB_INSTANCE}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)
# Cargar datos desde la base de datos

sql_query = f'SELECT * FROM {DB_DATABASE}.{DB_SCHEMA}.{DB_TABLE}'
df_uariv = pd.read_sql(sql_query, engine)
nrow_df_ini = len(df_uariv)
print("Registros despues cargue fuente: ", nrow_df_ini)

# Columnas de interés
interest_columns = [
    "VILB_IDPERSONA", "VILB_IDHOGAR", "VILB_TIPODOCUMENTO",
    "VILB_DOCUMENTO", "VILB_PRIMERNOMBRE", "VILB_SEGUNDONOMBRE",
    "VILB_PRIMERAPELLIDO", "VILB_SEGUNDOAPELLIDO",
    "VILB_FECHANACIMIENTO", "VILB_EXPEDICIONDOCUMENTO",
    "VILB_FECHAEXPEDICIONDOCUMENTO", "VILB_PERTENENCIAETNICA",
    "VILB_GENERO", "VILB_TIPOHECHO", "VILB_HECHO", "VILB_FECHAOCURRENCIA",
    "VILB_CODDANEMUNICIPIOOCURRENCIA", "VILB_ZONAOCURRENCIA",
    "VILB_UBICACIONOCURRENCIA", "VILB_PRESUNTOACTOR",
    "VILB_PRESUNTOVICTIMIZANTE", "VILB_FECHAREPORTE",
    "VILB_TIPOPOBLACION", "VILB_TIPOVICTIMA",
    "VILB_PAIS", "VILB_CIUDAD", "VILB_ESTADOVICTIMA", "VILB_NOMBRECOMPLETO",
    "VILB_IDSINIESTRO", "VILB_IDMIJEFE",
    "VILB_CODIGOHECHO", "VILB_DISCAPACIDAD", "VILB_DESCRIPCIONDISCAPACIDAD",
    "VILB_FUENTE"]

# Selección de columnas de interés
df_uariv = df_uariv[interest_columns]

# Crear una nueva columna 'id_registro' con hash para cada fila
df_uariv['id_registro'] = df_uariv.apply(funcion_hash, axis=1)

df_uariv["tabla_origen"] = "UARIV_RUV"
df_uariv["codigo_unico_fuente"] = df_uariv["VILB_IDPERSONA"].astype(str) + "_" + df_uariv["VILB_FUENTE"]

# Seleccionar solo la columna 'id_registro'
id_registros = df_uariv[['id_registro']]

# Definir una lista de valores a reemplazar
na_values = {
    "[SIN INFORMACION]": "",
    "ND": "",
    "AI": ""}

# Definir la lista de variables a limpiar
clean_columns = [
    "VILB_TIPODOCUMENTO", "VILB_PRIMERNOMBRE", "VILB_SEGUNDONOMBRE",
    "VILB_PRIMERAPELLIDO", "VILB_SEGUNDOAPELLIDO", "VILB_EXPEDICIONDOCUMENTO",
    "VILB_PERTENENCIAETNICA", "VILB_GENERO", "VILB_TIPOHECHO", "VILB_HECHO",
    "VILB_ZONAOCURRENCIA", "VILB_UBICACIONOCURRENCIA", "VILB_PRESUNTOACTOR",
    "VILB_PRESUNTOVICTIMIZANTE", "VILB_TIPOPOBLACION", "VILB_TIPOVICTIMA",
    "VILB_PAIS", "VILB_CIUDAD", "VILB_ESTADOVICTIMA", "VILB_NOMBRECOMPLETO",
    "VILB_DESCRIPCIONDISCAPACIDAD"
]

# Seleccionar todas las columnas excepto 'id_registro'
df_uariv = df_uariv.drop('id_registro', axis=1)

df_uariv = df_uariv.astype(str)

# Aplicar limpieza para columnas de tipo caracter
df_uariv = df_uariv.apply(
    lambda col: col.str.strip().str.upper()
    if col.name in clean_columns else col)

# Aplicar las transformaciones a las columnas de tipo 'str'
df_uariv[clean_columns] = df_uariv[clean_columns].apply(clean_text)
df_uariv[clean_columns] = df_uariv[clean_columns].replace(na_values)

# Unir la columna 'id_registro' al DataFrame
df_uariv = pd.concat([df_uariv, id_registros], axis=1)

# homologacion de estructura, formato y contenido
# Datos sobre los hechos
# lugar de ocurrencia
# Definición de la lista de códigos de municipios específicos
only_departamento = [
    2368, 2381, 5000, 8000, 13000, 15000, 17000, 18000, 19000, 20000, 23000,
    25000, 27000, 41000, 44000, 47000, 50000, 52000, 54000, 66000, 68000,
    70000, 73000, 76000, 81000, 85000, 86000, 91000, 95000, 99000]

# Aplicar transformaciones en el DataFrame 'uariv'
df_uariv['codigo_dane_municipio'] = np.where(
    pd.to_numeric(df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']) < 1000, np.nan,
    np.where(
        pd.to_numeric(df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']) == 1349,
        '13490',
        np.where(pd.to_numeric(
            df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']).isin([
                27086, 2727086]), '27615',
                np.where(pd.to_numeric(
                    df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']).isin(
                        only_departamento), np.nan,
                        np.where(pd.to_numeric(
                            df_uariv[
                                'VILB_CODDANEMUNICIPIOOCURRENCIA']) < 9000,
                            '0' + df_uariv[
                                'VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str),
                            df_uariv[
                                'VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(
                                    str))))))

# Realizar un left join con el DataFrame 'dane'
df_uariv = df_uariv.merge(dane, on='codigo_dane_municipio',
                          how='left', suffixes=('', '_sec'))

# Seleccionar las columnas necesarias y eliminar 'departamento_ocurrencia'
# df_uariv.drop(columns=['departamento_ocurrencia'], inplace=True)

# Aplicar transformaciones adicionales
df_uariv['codigo_dane_departamento'] = np.where(
    (pd.to_numeric(df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']) > 0) &
    (pd.to_numeric(df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']) < 10),
    df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str).str.zfill(2),
    np.where(((pd.to_numeric(
        df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']) >= 10) &
        (pd.to_numeric(df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']) < 100)) |
        (pd.to_numeric(
            df_uariv[
                'VILB_CODDANEMUNICIPIOOCURRENCIA']).isin(only_departamento)),
        df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str).str[:2],
        df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str).str[:2]))

df_uariv['codigo_dane_departamento'] = np.where(
    pd.to_numeric(df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA']) < 9000,
    '0' + df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str).str[:1],
    df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str).str[:2])

df_uariv['codigo_dane_departamento'] = np.where(
    df_uariv['codigo_dane_departamento'] == '80',
    '08', df_uariv['codigo_dane_departamento'])

# Realizar un left join con el DataFrame 'dane_depts'
df_uariv = df_uariv.merge(dane_depts, on='codigo_dane_departamento',
                          how='left', suffixes=('', '_sec'))
# 1870582 despues del left con dane departamento
# Realizar las últimas transformaciones
df_uariv['pais_ocurrencia'] = np.where(
    df_uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] == 0,
    'COLOMBIA', df_uariv['VILB_PAIS'])

columns_to_normalize = ["departamento_ocurrencia", "municipio_ocurrencia"]

# 2. Normalización de los campos de texto
df_uariv[columns_to_normalize] = df_uariv[
    columns_to_normalize].apply(clean_text)
df_uariv[columns_to_normalize] = df_uariv[
    columns_to_normalize].replace(na_values)


# Filtrar para mantener solo los registros con 'codigo_dane_departamento' y
# 'codigo_dane_municipio' presentes en 'dane'
df_uariv = df_uariv[
    df_uariv['codigo_dane_departamento'].isin(
        dane['codigo_dane_departamento'])]
# 1768666 excluyendo los departamentos que no estan en dane divipola

df_uariv = df_uariv[df_uariv[
    'codigo_dane_municipio'].isin(dane['codigo_dane_municipio'])]

# llevar a cabo mapeo de fechas
corrige_fecha_ocurrencia(df_uariv)

df_uariv_copy = df_uariv.copy()
###############################
# df_uariv=df_uariv_copy.copy()

df_uariv['fecha_desaparicion'] = df_uariv['VILB_FECHAOCURRENCIA'].astype(str)
df_uariv['fecha_ocur_anio'] = pd.to_numeric(
    df_uariv['fecha_desaparicion'].str[6:10], errors='coerce')
df_uariv['fecha_ocur_mes'] = pd.to_numeric(
    df_uariv['fecha_desaparicion'].str[3:5], errors='coerce')
df_uariv['fecha_ocur_dia'] = pd.to_numeric(
    df_uariv['fecha_desaparicion'].str[0:2], errors='coerce')

# transformaciones sobre fecha de ocurrencia
homologacion.fecha.fechas_validas(df_uariv, fecha_dia='fecha_ocur_dia',
                                  fecha_mes='fecha_ocur_mes',
                                  fecha_anio='fecha_ocur_anio',
                                  fecha='fecha_desaparicion_dtf',
                                  fechat='fecha_desaparicion')

# Reemplazar si la longitud no es 4 con cadena vacía
# df_uariv['fecha_ocur_anio'] = df_uariv['fecha_ocur_anio'].apply(
#     lambda x: '' if (isinstance(x, int) or isinstance(x, float)) and
#     len(str(x)) != 4 else x)

# Definir los valores a considerar como 'sin información' en actores
no_information_actors = [
    'NO DEFINIDO', '', 'NAN', 'NO IDENTIFICA',
    'SIN INFORMACION CONFLICTO ARMADO',
    'NO IDENTIFICA CONFLICTO ARMADO', 'OTROS VIOLENCIA GENERALIZADA',
    'SIN INFORMACION', 'NO IDENTIFICA RELACION CERCANA Y SUFICIENTE',
    'NO IDENTIFICA VIOLENCIA GENERALIZADA',
    'NO IDENTIFICA RELACION CERCANA Y SUFICIENTE',
    'SIN INFORMACION RELACION CERCANA Y SUFICIENTE',
    'NO IDENTIFICA CONFLICTO ARMADO', 'CONFLICTO ARMADO',
    '0', 'SIN INFORMACION VIOLENCIA GENERALIZADA']


# Reemplazar los valores en VILB_PRESUNTOACTOR con vacio
# si están en sin_informacion_actores
df_uariv['VILB_PRESUNTOACTOR'] = np.where(
    df_uariv['VILB_PRESUNTOACTOR'].isin(no_information_actors),
    "", df_uariv['VILB_PRESUNTOACTOR'])

# Crear columnas de presuntos responsables y asignar 0 o 1
# basado en ciertas condiciones
df_uariv['pres_resp_agentes_estatales'] = np.where(
    df_uariv['VILB_PRESUNTOACTOR'].str.contains(
        'FUERZA PUBLICA|AGENTE DEL ESTADO'), 1, 0)
df_uariv['pres_resp_grupos_posdesmov'] = 0
df_uariv['pres_resp_paramilitares'] = np.where(
    df_uariv['VILB_PRESUNTOACTOR'].str.contains(
        'AUTODEF|PARAMI|AUC|A.U.C'), 1, 0)
df_uariv['pres_resp_guerr_eln'] = np.where(
    df_uariv['VILB_PRESUNTOACTOR'].str.contains('ELN'), 1, 0)
df_uariv['pres_resp_guerr_farc'] = np.where(
    df_uariv['VILB_PRESUNTOACTOR'].str.contains('FARC'), 1, 0)
df_uariv['pres_resp_guerr_otra'] = np.where(
    (df_uariv['VILB_PRESUNTOACTOR'].str.contains(
        'GRUPOS GUERRILLEROS|GUERRILLA') &
        ~df_uariv['VILB_PRESUNTOACTOR'].str.contains('FARC|ELN')) |
    df_uariv['VILB_PRESUNTOACTOR'].str.contains('EPL'), 1, 0)

# Calcular la suma de las columnas de presuntos responsables
df_uariv['tmp'] = df_uariv.filter(like='pres_resp_').sum(axis=1)

# Crear la columna 'pres_resp_otro' basado en la suma obtenida
df_uariv['pres_resp_otro'] = np.where(df_uariv['tmp'] > 0, 1, 0)

# Eliminar la columna temporal 'tmp'
df_uariv.drop('tmp', axis=1, inplace=True)

pres_resp_cols = df_uariv.filter(like='pres_resp_')
pres_resp_sum = pres_resp_cols.sum()

# estandarizar tipo de hecho
other_facts = [
    "HOMICIDIO", "ACTO TERRORISTA ATENTADOS COMBATES ENFRENTAM",
    "PERDIDA DE BIENES MUEBLES O INMUEBLES",
    "AMENAZA",
    "DELITOS CONTRA LA LIBERTAD Y LA INTEGRIDAD SEXUAL ",
    "MINAS ANTIPERSONAL MUNICION SIN EXPLOTAR Y ARTEFA",
    "TORTURA",
    "LESIONES PERSONALES FISICAS",
    "ABANDONO O DESPOJO FORZADO DE TIERRAS",
    "LESIONES PERSONALES PSICOLOGICAS",
    "CONFINAMIENTO"]

df_uariv["TH_DF"] = np.where(
    df_uariv["VILB_HECHO"] == "DESAPARICION FORZADA", 1,
    np.where(df_uariv["VILB_HECHO"].isna(), np.nan,
             np.where(df_uariv["VILB_HECHO"] == "SIN INFORMACION", np.nan, 0)))

df_uariv["TH_SE"] = np.where(
    df_uariv["VILB_HECHO"] == "SECUESTRO", 1,
    np.where(df_uariv["VILB_HECHO"].isna(), np.nan,
             np.where(df_uariv["VILB_HECHO"] == "SIN INFORMACION", np.nan, 0)))

df_uariv["TH_RU"] = np.where(
    df_uariv["VILB_HECHO"] == "VINCULACION DE NINOS NINAS Y ADOLESCENTES A ACTIVI", 1,
    np.where(df_uariv["VILB_HECHO"].isna(), np.nan,
             np.where(df_uariv["VILB_HECHO"] == "SIN INFORMACION", np.nan, 0)))

df_uariv["TH_OTRO"] = np.where(
    df_uariv["VILB_HECHO"].isin(other_facts), 1,
    np.where(df_uariv["VILB_HECHO"].isna(), np.nan,
             np.where(df_uariv["VILB_HECHO"] == "SIN INFORMACION", np.nan, 0)))

# relato
# Asignación de valores a las columnas
df_uariv["descripcion_relato"] = ""
df_uariv["situacion_actual_des"] = "Sin informacion"

# Datos sobre las personas dadas por desaparecidas
# Normalización de nombres y apellidos")
# Se corrige los nombres de la presunta víctima
# se arma nombre completo
# df_uariv[['VILB_PRIMER_NOMBRE',
#     'VILB_SEGUNDO_NOMBRE',
#     'VILB_PRIMER_APELLIDO',
#     'VILB_SEGUNDO_APELLIDO']] = df_uariv['VILB_NOMBRECOMPLETO'].apply(
#           lambda x: pd.Series(
#               homologacion.nombre_completo.limpiar_nombre_completo(x)))


homologacion.nombres.nombres_validos(df_uariv,
                                     primer_nombre='VILB_PRIMERNOMBRE',
                                     segundo_nombre='VILB_SEGUNDONOMBRE',
                                     primer_apellido='VILB_PRIMERAPELLIDO',
                                     segundo_apellido='VILB_SEGUNDOAPELLIDO',
                                     nombre_completo='VILB_NOMBRECOMPLETO')


df_uariv.rename(columns={'VILB_DOCUMENTO': 'documento'}, inplace=True)
# Documento de identificación
homologacion.documento.documento_valida(df_uariv, documento='documento')

df_uariv['VILB_FECHANACIMIENTO_'] = df_uariv['VILB_FECHANACIMIENTO']
df_uariv['VILB_FECHANACIMIENTO'] = pd.to_datetime(
    df_uariv['VILB_FECHANACIMIENTO'], errors='coerce', format="%d/%m/%Y")

# implementaciOn de las reglas de la registraduria
# Si es CC o TI solo deben ser numéricos
df_uariv['documento_CC_TI_no_numerico'] = np.where(
    df_uariv['documento'].str.contains('[A-Z]') &
    df_uariv['VILB_TIPODOCUMENTO'].isin(['TI', 'CC']), 1, 0)
df_uariv['documento'] = np.where(
    df_uariv['documento'].str.contains('[A-Z]') &
    df_uariv['VILB_TIPODOCUMENTO'].isin(['TI', 'CC']), np.nan,
    df_uariv['documento'])

# Número de cedula o tarjeta de identidad longitud mayor
# a 10 debe ser mayor a 1.000.000.000
df_uariv['documento_CC_TI_mayor1KM'] = np.where(
    (df_uariv['documento'].str.len() >= 10) &
    (pd.to_numeric(df_uariv['documento'], errors='coerce') <= 1000000000) &
    df_uariv['VILB_TIPODOCUMENTO'].isin(['TI', 'CC']), 1, 0)

# Número de tarjeta de identidad de 10 o 11 caracteres
df_uariv['documento_TI_10_11_caract'] = np.where(
    ~((df_uariv['documento'].str.len() == 10) |
      (df_uariv['documento'].str.len() == 11)) &
    (df_uariv['VILB_TIPODOCUMENTO'] == 'TI'), 1, 0)

# Si TI es de longitud 11, los 6 primeros dígitos corresponden a la
# fecha de nacimiento (aammdd)
df_uariv['documento_TI_11_caract_fecha_nac'] = np.where(
    ~((df_uariv['documento'].str[:6] == df_uariv[
        'VILB_FECHANACIMIENTO'].dt.strftime('%y%m%d')) &
        (df_uariv['documento'].str.len() == 11) &
        (df_uariv['VILB_TIPODOCUMENTO'] == 'TI')), 1, 0)

# El número de CC de un hombre con longitud de 4 a 8 debe estar
# entre 1 y 19.999.999 o 70.000.000 y 99.999.999
df_uariv['documento_CC_hombre_consistente'] = np.where(
    ~(pd.to_numeric(df_uariv['documento'],
                    errors='coerce').isin(range(1, 20000000)) |
      pd.to_numeric(df_uariv['documento'],
                    errors='coerce').isin(range(70000000, 100000000))) &
    df_uariv['documento'].str.len().isin(range(4, 9)) &
    (df_uariv['VILB_TIPODOCUMENTO'] == 'CC') &
    (df_uariv['VILB_GENERO'] == 'HOMBRE'), 1, 0)

# El número de CC de una mujer de longitud 8 debe estar entre
# 20.000.000 y 69.999.999
df_uariv['documento_CC_mujer_consistente'] = np.where(
    ~(pd.to_numeric(df_uariv['documento'],
                    errors='coerce').isin(range(20000000, 70000000))) &
    (df_uariv['documento'].str.len() == 8) &
    (df_uariv['VILB_TIPODOCUMENTO'] == 'CC') &
    (df_uariv['VILB_GENERO'] == 'MUJER'), 1, 0)

# La CC de una mujer debe estar entre 8 o 10 caracteres
df_uariv['documento_CC_mujer_consistente2'] = np.where(
    ~(df_uariv['documento'].str.len().isin(list(range(4, 9)) + [10])) &
    (df_uariv['VILB_TIPODOCUMENTO'] == 'CC') &
    (df_uariv['VILB_GENERO'] == 'MUJER'), 1, 0)

# La CC de un hombre debe tener una longitud de 4, 5, 6, 7, 8 o 10 caracteres
df_uariv['documento_CC_hombre_consistente2'] = np.where(
    ~df_uariv['documento'].str.len().isin([4, 5, 6, 7, 8, 10]) &
    (df_uariv['VILB_TIPODOCUMENTO'] == 'CC') &
    (df_uariv['VILB_GENERO'] == 'HOMBRE'), 1, 0)

# Si la TI es de 11 caracteres, el 10mo caracter debe
# ser 1, 3, 5, 7, 9 si es mujer
df_uariv['documento_TI_mujer_consistente'] = np.where(
    ~(df_uariv['documento'].str[10:11].isin([str(i)
                                             for i in range(1, 11, 2)])) &
    (df_uariv['documento'].str.len() == 11) &
    (df_uariv['VILB_TIPODOCUMENTO'] == 'TI') &
    (df_uariv['VILB_GENERO'] == 'MUJER'), 1, 0)

# Si la TI es de 11 caracteres, el 10mo caracter debe
# ser 2, 4, 6, 8, 0 si es hombre
df_uariv['documento_TI_hombre_consistente'] = np.where(
    ~(df_uariv['documento'].str[10:11].isin([str(i)
                                             for i in range(0, 11, 2)])) &
    (df_uariv['documento'].str.len() == 11) &
    (df_uariv['VILB_TIPODOCUMENTO'] == 'TI') &
    (df_uariv['VILB_GENERO'] == 'HOMBRE'), 1, 0)

# df_uariv.drop(columns=['VILB_FECHANACIMIENTO'], inplace=True)
# df_uariv.rename(columns={'VILB_FECHANACIMIENTO_':
# 'VILB_FECHANACIMIENTO'}, inplace=True)

# Resumen de documentos
# resumen_documento = df_uariv.loc[
# :, df_uariv.columns.str.startswith('documento_')].copy()
# resumen_documento = resumen_documento[
# df_uariv.filter(like='documento_').sum(axis=1) > 0]
# resumen_documento = resumen_documento[
# ['documento_', 'VILB_TIPODOCUMENTO', 'VILB_DOCUMENTO', 'documento',
# 'VILB_GENERO', 'VILB_FECHANACIMIENTO']]


# Guardar el DataFrame en un archivo
# log_file_path = os.path.join(
# DIRECTORY_PATH, "log", "revision_documentos.csv")
# resumen_documento.to_csv(log_file_path, sep=';', index=False)

# Sexo
df_uariv['sexo'] = df_uariv['VILB_GENERO'].apply(
    lambda x: 'OTRO' if x in ['LGBTI', 'INTERSEXUAL'] else x)
df_uariv['sexo'] = df_uariv['sexo'].replace('NO INFORMA', pd.NA)

# Pertenencia étnica
conditions = [
    (df_uariv['VILB_PERTENENCIAETNICA'] == 'NINGUNA'),
    (df_uariv['VILB_PERTENENCIAETNICA'].isin([
        'NEGROA O AFROCOLOMBIANOA',
        'RAIZAL DEL ARCHIPIELAGO DE SAN ANDRES Y PROVIDENCI', 'PALENQUERO',
        'AFROCOLOMBIANO ACREDITADO RA'])),
    (df_uariv['VILB_PERTENENCIAETNICA'].isin([
        'INDIGENA', 'INDIGENA ACREDITADO RA'])),
    (df_uariv['VILB_PERTENENCIAETNICA'].isin([
        'GITANOA ROM', 'GITANO RROM ACREDITADO RA']))]
choices = ['MESTIZO', 'NARP', 'INDIGENA', 'RROM']
df_uariv['iden_pertenenciaetnica'] = np.select(
    conditions, choices, default=pd.NA)

df_uariv['anio_nacimiento'] = pd.to_datetime(
    df_uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime("%Y")
df_uariv['mes_nacimiento'] = pd.to_datetime(
    df_uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime("%m")
df_uariv['dia_nacimiento'] = pd.to_datetime(
    df_uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime("%d")
df_uariv['fecha_nacimiento_dtf'] = pd.to_datetime(
    df_uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y")
df_uariv['fecha_nacimiento'] = pd.to_datetime(
    df_uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime("%Y%m%d")


# Ajuste del año de nacimiento
df_uariv['anio_nacimiento'] = np.where(
    df_uariv['anio_nacimiento'].astype(float) < 1905,
    np.nan, df_uariv['anio_nacimiento'])
df_uariv['anio_nacimiento'] = np.where(
    df_uariv['anio_nacimiento'].astype(float) > 2022,
    np.nan, df_uariv['anio_nacimiento'])

meses = ["01", "02", "03", "04", "05", "06", "07",
         "08", "09", "10", "11", "12"]
dias = ["01", "02", "03", "04", "05", "06", "07",
        "08", "09"] + [str(i) for i in range(10, 32)]

# Verificación de meses y días
df_uariv = df_uariv[df_uariv['mes_nacimiento'].isin(meses)]
df_uariv = df_uariv[df_uariv['dia_nacimiento'].isin(dias)]
# 1768638 registros despues de quitar para los cuales los meses
# o los dias son invalidos

hora = datetime.now()
print(hora)
# Calculando la edad
df_uariv['edad'] = np.where(
    (df_uariv['fecha_ocur_anio'].isna() | df_uariv['anio_nacimiento'].isna()) |
    (df_uariv['fecha_ocur_anio'].astype(float) <= df_uariv[
        'anio_nacimiento'].astype(float)), np.nan,
    df_uariv['fecha_ocur_anio'].astype(float) - df_uariv[
        'anio_nacimiento'].astype(float))

# Ajuste de edad
df_uariv['edad'] = np.where(df_uariv['edad'] > 100, np.nan, df_uariv['edad'])

# Eliminar duplicados
n_1 = len(df_uariv)
df_uariv = df_uariv.drop_duplicates()

n_duplicados = n_1 - len(df_uariv)

# Excluir víctimas indirectas
n_2 = len(df_uariv)
df_uariv = df_uariv[df_uariv['VILB_TIPOVICTIMA'] != "INDIRECTA"]
# 912885 registros posterior a excluir los tipo victima indirecta

n_indirectas = n_2 - len(df_uariv)

# Excluir personas jurídicas
n_3 = len(df_uariv)
df_uariv = df_uariv[~df_uariv['VILB_TIPODOCUMENTO'].isin(["NIT"])]
# 912879 registros posterior a excluir personas juridicas
n_juridicas = n_3 - len(df_uariv)

df_uariv.rename(columns={
    'VILB_NOMBRECOMPLETO': 'nombre_completo',
    'VILB_PRIMERNOMBRE': 'primer_nombre',
    'VILB_SEGUNDONOMBRE': 'segundo_nombre',
    'VILB_PRIMERAPELLIDO': 'primer_apellido',
    'VILB_SEGUNDOAPELLIDO': 'segundo_apellido'}, inplace=True)

# Campos requeridos
campos_requeridos = [
    'id_registro', 'tabla_origen', 'codigo_unico_fuente',
    'nombre_completo', 'primer_nombre', 'segundo_nombre', 'primer_apellido',
    'segundo_apellido', 'documento', 'fecha_nacimiento', 'anio_nacimiento',
    'mes_nacimiento', 'dia_nacimiento', 'edad', 'iden_pertenenciaetnica',
    'sexo', 'fecha_desaparicion', 'fecha_desaparicion_dtf', 'fecha_ocur_dia',
    'fecha_ocur_mes', 'fecha_ocur_anio', 'TH_DF', 'TH_SE', 'TH_RU', 'TH_OTRO',
    'descripcion_relato', 'pais_ocurrencia', 'codigo_dane_departamento',
    'departamento_ocurrencia', 'codigo_dane_municipio', 'municipio_ocurrencia',
    'pres_resp_paramilitares', 'pres_resp_grupos_posdesmov',
    'pres_resp_agentes_estatales', 'pres_resp_guerr_farc',
    'pres_resp_guerr_eln', 'pres_resp_guerr_otra', 'pres_resp_otro',
    'situacion_actual_des']

# Verificar campos requeridos y seleccionar columnas
df_uariv = df_uariv[campos_requeridos].copy()

# Filtrar y eliminar filas con id_registro nulo
df_uariv = df_uariv.dropna(subset=['id_registro'])

# Eliminar duplicados manteniendo la primera ocurrencia
df_uariv = df_uariv.drop_duplicates(subset=campos_requeridos[1:], keep='first')
# 912121 registros posterior a quitar dulicados

# Filtrar filas duplicadas en el campo codigo_unico_fuente
temp = df_uariv[
    df_uariv['codigo_unico_fuente'].isin(
        df_uariv[df_uariv.duplicated(
            'codigo_unico_fuente')]['codigo_unico_fuente'])]

# Filtrar y verificar unicidad del campo codigo_unico_fuente
df_uariv = df_uariv.dropna(subset=['codigo_unico_fuente'])
df_uariv = df_uariv[~df_uariv.duplicated('codigo_unico_fuente')]

# Verificar que el número de filas sea igual a la cantidad
# única de codigo_unico_fuente
assert len(df_uariv) == len(df_uariv['codigo_unico_fuente'].unique())

# Identificación de registros
df_uariv_ident = df_uariv[
    (~df_uariv['primer_nombre'].isna() |
     ~df_uariv['segundo_nombre'].isna()) &
    (~df_uariv['primer_apellido'].isna() |
     ~df_uariv['segundo_apellido'].isna()) &
    (~df_uariv['documento'].isna() |
     ~df_uariv['fecha_ocur_anio'].isna() |
     ~df_uariv['departamento_ocurrencia'].isna())]

# Registros no identificados
df_uariv_no_ident = df_uariv[
    ~df_uariv['id_registro'].isin(df_uariv_ident['id_registro'])]

# Número de filas en cada conjunto
nrow_df_fin = len(df_uariv)
nrow_df_ident = len(df_uariv_ident)
nrow_df_no_ident = len(df_uariv_no_ident)

print("Número de filas en uariv:", nrow_df_fin)
print("Número de filas en df_uariv_ident:", nrow_df_ident)
print("Número de filas en df_uariv_no_ident:", nrow_df_no_ident)


DB_SCHEMA = "version5"
DB_TABLE = "UARIV_VIVANTO"

# exportar a la BBDD los registros identificados
with engine.connect() as conn, conn.begin():
    df_uariv_ident.to_sql(name=DB_TABLE, con=engine,
                          schema=DB_SCHEMA, if_exists='replace', index=False)

DB_TABLE = "UARIV_VIVANTO_PNI"
# exportar a la BBDD los registros no identificados
with engine.connect() as conn, conn.begin():
    df_uariv_no_ident.to_sql(name=DB_TABLE, con=engine,
                             schema=DB_SCHEMA, if_exists='replace',
                             index=False)


# Registra el tiempo de finalización
end_time = time.time()
log = {
    "fecha_inicio": str(start_time),
    "fecha_fin": str(end_time),
    "tiempo_ejecucion": str(end_time - start_time),
    'filas_iniciales_df': nrow_df_ini,
    'filas_final_df': nrow_df_fin,
    'filas_df_ident': nrow_df_ident,
    'filas_df_no_ident': nrow_df_no_ident,
    'n_duplicados': n_duplicados,
}

log_file_path = os.path.join(
    DIRECTORY_PATH, "log", "resultado_df_uariv_vivanto.yaml")
with open(log_file_path, 'w') as file:
    yaml.dump(log, file)

# Calcula el tiempo transcurrido
elapsed_time = end_time - start_time

print(f"Tiempo transcurrido: {elapsed_time/60} segundos")
