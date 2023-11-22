import os
import re
import sys
import json
import string
import pyodbc
import logging
import numpy as np
import pandas as pd
from hashlib import sha1
from datetime import datetime
import homologacion.etnia
import homologacion.fecha
import homologacion.nombres
import homologacion.limpieza
import homologacion.documento
import homologacion.nombre_completo
from sqlalchemy import create_engine


fecha_inicio = datetime.now()

def clean_text(text):
    if text is None or text.isna().any():
        text = text.astype(str)
    text = text.apply(homologacion.limpieza.normalize_text)
    return text

# creacion de las funciones requeridas
def funcion_hash(row):
    return hashlib.sha1(str(row).encode()).hexdigest()

# Configurar la codificación Unicode
encoding = "ISO-8859-1"

with open('config.json') as config_file:
    config = json.load(config_file)

# Realizar un merge con el archivo DIVIPOLA_departamentos_122021.dta
dane = pd.read_stata(DIRECTORY_PATH + "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")

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

################

DIRECTORY_PATH = config['DIRECTORY_PATH']
DB_SERVER = config['DB_SERVER']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_DATABASE = "PRD_QPREP_UBPD"
DB_SCHEMA = "dbo"
DB_TABLE = "UARIV_UNI_VIC_LB"


# Conexión a la base de datos usando pyodbc
# Configurar la cadena de conexion
db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(db_url)
# Cargar datos desde la base de datos
sql_query = f"SELECT * FROM {DB_DATABASE}.{DB_SCHEMA}.{DB_TABLE} where VILB_HECHO <> 'DESPLAZAMIENTO FORZADO'"
df_uariv = pd.read_sql(sql_query, engine)

# Columnas de interés
interest_columns = ["VILB_IDPERSONA", "VILB_IDHOGAR", "VILB_TIPODOCUMENTO",
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
                    "VILB_CODIGOHECHO", "VILB_DISCAPACIDAD", "VILB_DESCRIPCIONDISCAPACIDAD", "VILB_FUENTE"]

# Selección de columnas de interés
df_uariv = df_uariv[interest_columns]


# Creación del hash único para cada registro
logging.info("Se incia la creación del hash único")

# Función para calcular el hash
def calcular_hash(data):
    hash_object = sha1(str(data).encode())
    return hash_object.hexdigest()

# Suponiendo que uariv es un DataFrame de pandas
uariv["id_registro"] = uariv.apply(calcular_hash, axis=1)
uariv["tabla_origen"] = "UARIV_RUV"
uariv["codigo_unico_fuente"] = uariv["VILB_IDPERSONA"] + "_" + uariv["VILB_FUENTE"]

# Seleccionar solo la columna 'id_registro'
id_registros = uariv[['id_registro']]

# Definir la lista de valores NA
na_values = ["ND", "AI", "[SIN INFORMACION]"]

# Definir la lista de variables a limpiar
variables_limpieza = [
    "VILB_TIPODOCUMENTO", "VILB_PRIMERNOMBRE", "VILB_SEGUNDONOMBRE",
    "VILB_PRIMERAPELLIDO", "VILB_SEGUNDOAPELLIDO", "VILB_EXPEDICIONDOCUMENTO",
    "VILB_PERTENENCIAETNICA", "VILB_GENERO", "VILB_TIPOHECHO", "VILB_HECHO",
    "VILB_ZONAOCURRENCIA", "VILB_UBICACIONOCURRENCIA", "VILB_PRESUNTOACTOR",
    "VILB_PRESUNTOVICTIMIZANTE", "VILB_TIPOPOBLACION", "VILB_TIPOVICTIMA",
    "VILB_PAIS", "VILB_CIUDAD", "VILB_ESTADOVICTIMA", "VILB_NOMBRECOMPLETO",
    "VILB_DESCRIPCIONDISCAPACIDAD"
]

# Seleccionar todas las columnas excepto 'id_registro'
uariv = uariv.drop('id_registro', axis=1)

# Aplicar limpieza para columnas de tipo caracter
uariv = uariv.apply(lambda col: col.str.strip().str.upper() if col.name in variables_limpieza else col)

# Aplicar la función de limpieza a columnas específicas
uariv[variables_limpieza] = uariv[variables_limpieza].apply(lambda col: clean_func(col, na_values))

# Unir la columna 'id_registro' al DataFrame
uariv = pd.concat([uariv, id_registros], axis=1)

# Definición de la lista de códigos de municipios específicos
solo_departamento = [2368, 2381, 5000, 8000, 13000, 15000, 17000, 18000, 19000, 20000, 23000, 25000,
                     27000, 41000, 44000, 47000, 50000, 52000, 54000, 66000, 68000, 70000, 73000, 76000,
                     81000, 85000, 86000, 91000, 95000, 99000]

# Aplicar transformaciones en el DataFrame 'uariv'
uariv['codigo_dane_municipio'] = np.where(uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] < 1000, np.nan,
                                          np.where(uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] == 1349, '13490',
                                                   np.where(uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].isin([27086, 2727086]), '27615',
                                                            np.where(uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].isin(solo_departamento), np.nan,
                                                                     np.where(uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] < 9000,
                                                                              '0' + uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str),
                                                                              uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str)))))))

# Realizar un left join con el DataFrame 'dane'
uariv = uariv.merge(dane, on='codigo_dane_municipio', how='left')

# Seleccionar las columnas necesarias y eliminar 'departamento_ocurrencia'
uariv.drop(columns=['departamento_ocurrencia'], inplace=True)

# Aplicar transformaciones adicionales
uariv['codigo_dane_departamento'] = np.where((uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] > 0) & (uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] < 10),
                                             uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str).str.zfill(2),
                                             np.where(((uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] >= 10) & (uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] < 100)) | 
                                                       (uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].isin(solo_departamento)),
                                                       uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'].astype(str).str[:2],
                                                       uariv['codigo_dane_departamento']))

uariv['codigo_dane_departamento'] = np.where(uariv['codigo_dane_departamento'] == '80', '08', uariv['codigo_dane_departamento'])

# Realizar un left join con el DataFrame 'dane_depts'
uariv = uariv.merge(dane_depts, on='codigo_dane_departamento', how='left')

# Realizar las últimas transformaciones
uariv['pais_ocurrencia'] = np.where(uariv['VILB_CODDANEMUNICIPIOOCURRENCIA'] == 0, 'COLOMBIA', uariv['VILB_PAIS'])
uariv[['departamento_ocurrencia', 'municipio_ocurrencia']] = uariv[['departamento_ocurrencia', 'municipio_ocurrencia']].apply(lambda col: col.apply(lambda x: clean_func(x, na_values)))

# Filtrar para mantener solo los registros con 'codigo_dane_departamento' y 'codigo_dane_municipio' presentes en 'dane'
uariv = uariv[uariv['codigo_dane_departamento'].isin(dane['codigo_dane_departamento'])]
uariv = uariv[uariv['codigo_dane_municipio'].isin(dane['codigo_dane_municipio'])]


# Definir las condiciones y sus respectivos reemplazos
conditions = [
    (uariv['VILB_FECHAOCURRENCIA'] == "01/01/0001"),
    (uariv['VILB_FECHAOCURRENCIA'] == "10/04/0997"),
    # ... (resto de las condiciones)
]

replacements = [
    "01/01/2001",
    "10/04/1997",
    # ... (resto de los reemplazos)
]

# Aplicar las condiciones y reemplazos
uariv['VILB_FECHAOCURRENCIA'] = pd.Series(np.select(conditions, replacements, default=uariv['VILB_FECHAOCURRENCIA']))

# Definir meses y días para validaciones posteriores
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
dias = ["01", "02", "03", "04", "05", "06", "07", "08", "09"] + [str(i) for i in range(10, 32)]

# Transformaciones
uariv['fecha_desaparicion'] = pd.to_datetime(uariv['VILB_FECHAOCURRENCIA'], format="%d/%m/%Y").dt.strftime('%Y%m%d')
uariv['fecha_desaparicion_dtf'] = pd.to_datetime(uariv['VILB_FECHAOCURRENCIA'], format="%d/%m/%Y")
uariv['fecha_ocur_anio'] = pd.to_datetime(uariv['VILB_FECHAOCURRENCIA'], format="%d/%m/%Y").dt.strftime('%Y')
uariv['fecha_ocur_mes'] = pd.to_datetime(uariv['VILB_FECHAOCURRENCIA'], format="%d/%m/%Y").dt.strftime('%m')
uariv['fecha_ocur_dia'] = pd.to_datetime(uariv['VILB_FECHAOCURRENCIA'], format="%d/%m/%Y").dt.strftime('%d')

# Validaciones
uariv = uariv[uariv['fecha_ocur_anio'].isin([str(i) for i in range(1900, 2023)])]
uariv = uariv[uariv['fecha_ocur_mes'].isin(meses)]
uariv = uariv[uariv['fecha_ocur_dia'].isin(dias)]


# Definir los valores a considerar como 'sin información' en actores
sin_informacion_actores = ['NO DEFINIDO', '', 'NO IDENTIFICA', 'SIN INFORMACION CONFLICTO ARMADO',
                           'NO IDENTIFICA CONFLICTO ARMADO', 'OTROS VIOLENCIA GENERALIZADA',
                           'SIN INFORMACION', 'NO IDENTIFICA RELACION CERCANA Y SUFICIENTE',
                           'NO IDENTIFICA VIOLENCIA GENERALIZADA',
                           'NO IDENTIFICA RELACION CERCANA Y SUFICIENTE',
                           'SIN INFORMACION RELACION CERCANA Y SUFICIENTE',
                           'NO IDENTIFICA CONFLICTO ARMADO', 'CONFLICTO ARMADO',
                           '0', 'SIN INFORMACION VIOLENCIA GENERALIZADA']

# Log para indicar la identificación y estandarización de los presuntos responsables
logging.info("Se identifica y estandariza los presuntos responsables")


# Reemplazar los valores en VILB_PRESUNTOACTOR con NA si están en sin_informacion_actores
uariv['VILB_PRESUNTOACTOR'] = np.where(uariv['VILB_PRESUNTOACTOR'].isin(sin_informacion_actores), np.nan, uariv['VILB_PRESUNTOACTOR'])

# Crear columnas de presuntos responsables y asignar 0 o 1 basado en ciertas condiciones
uariv['pres_resp_agentes_estatales'] = np.where(uariv['VILB_PRESUNTOACTOR'].str.contains('FUERZA PUBLICA|AGENTE DEL ESTADO'), 1, 0)
uariv['pres_resp_grupos_posdesmov'] = 0
uariv['pres_resp_paramilitares'] = np.where(uariv['VILB_PRESUNTOACTOR'].str.contains('AUTODEF|PARAMI|AUC|A.U.C'), 1, 0)
uariv['pres_resp_guerr_eln'] = np.where(uariv['VILB_PRESUNTOACTOR'].str.contains('ELN'), 1, 0)
uariv['pres_resp_guerr_farc'] = np.where(uariv['VILB_PRESUNTOACTOR'].str.contains('FARC'), 1, 0)
uariv['pres_resp_guerr_otra'] = np.where((uariv['VILB_PRESUNTOACTOR'].str.contains('GRUPOS GUERRILLEROS|GUERRILLA') &
                                          ~uariv['VILB_PRESUNTOACTOR'].str.contains('FARC|ELN')) |
                                         uariv['VILB_PRESUNTOACTOR'].str.contains('EPL'), 1, 0)

# Calcular la suma de las columnas de presuntos responsables
uariv['tmp'] = uariv.filter(like='pres_resp_').sum(axis=1)

# Crear la columna 'pres_resp_otro' basado en la suma obtenida
uariv['pres_resp_otro'] = np.where(uariv['tmp'] > 0, 1, 0)

# Eliminar la columna temporal 'tmp'
uariv.drop('tmp', axis=1, inplace=True)

pres_resp_cols = uariv.filter(like='pres_resp_')
pres_resp_sum = pres_resp_cols.sum()

logging.info("se estandariza el tipo de hecho")

otros_hechos = ["HOMICIDIO",
                "ACTO TERRORISTA / ATENTADOS / COMBATES / ENFRENTAM",
                "PERDIDA DE BIENES MUEBLES O INMUEBLES",
                "AMENAZA",
                "DELITOS CONTRA LA LIBERTAD Y LA INTEGRIDAD SEXUAL ",
                "MINAS ANTIPERSONAL, MUNICION SIN EXPLOTAR Y ARTEFA",
                "TORTURA",
                "LESIONES PERSONALES FISICAS",
                "ABANDONO O DESPOJO FORZADO DE TIERRAS",
                "LESIONES PERSONALES PSICOLOGICAS",
                "CONFINAMIENTO"]

uariv["TH_DF"] = np.where(uariv["VILB_HECHO"] == "DESAPARICION FORZADA", 1,
                         np.where(uariv["VILB_HECHO"].isna(), np.nan,
                                  np.where(uariv["VILB_HECHO"] == "SIN INFORMACION", np.nan, 0))))

uariv["TH_SE"] = np.where(uariv["VILB_HECHO"] == "SECUESTRO", 1,
                         np.where(uariv["VILB_HECHO"].isna(), np.nan,
                                  np.where(uariv["VILB_HECHO"] == "SIN INFORMACION", np.nan, 0))))

uariv["TH_RU"] = np.where(uariv["VILB_HECHO"] == "VINCULACION DE NIÑOS NIÑAS Y ADOLESCENTES A ACTIVI", 1,
                         np.where(uariv["VILB_HECHO"].isna(), np.nan,
                                  np.where(uariv["VILB_HECHO"] == "SIN INFORMACION", np.nan, 0))))

uariv["TH_OTRO"] = np.where(uariv["VILB_HECHO"].isin(otros_hechos), 1,
                           np.where(uariv["VILB_HECHO"].isna(), np.nan,
                                    np.where(uariv["VILB_HECHO"] == "SIN INFORMACION", np.nan, 0))))

# Asignación de valores a las columnas
uariv["descripcion_relato"] = ""
uariv["situacion_actual_des"] = "Sin información"

logging.info("Normalización de nombres y apellidos")
logging.info("Se corrige los nombres de la presunta víctima")

# Normalización de nombres y apellidos
uariv["primer_nombre"] = uariv["VILB_PRIMERNOMBRE"].str.strip().str.replace('\s+', ' ')
uariv["segundo_nombre"] = uariv["VILB_SEGUNDONOMBRE"].str.strip().str.replace('\s+', ' ')
uariv["primer_apellido"] = uariv["VILB_PRIMERAPELLIDO"].str.strip().str.replace('\s+', ' ')
uariv["segundo_apellido"] = uariv["VILB_SEGUNDOAPELLIDO"].str.strip().str.replace('\s+', ' ')

# Eliminar nombres y apellidos cuando solo se registra la letra inicial
uariv[["primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido"]] = \
    uariv[["primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido"]].apply(
        lambda x: np.where(x.str.len() == 1, np.nan, x))

uariv["nombre_completo"] = uariv[["primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido"]] \
    .apply(lambda x: ' '.join(x.dropna()), axis=1).str.replace(" NA ", " ").str.strip()

uariv["tipo_documento"] = uariv["VILB_TIPODOCUMENTO"].str.strip().str.replace('\s+', ' ')

# Definir la lista de preposiciones
preposiciones = ["DE", "DEL", "DE LAS", "DE LA", "DE LOS", "VAN", "LA", "VIUDA DE", "VIUDA", "SAN", "DA"]

# Supongamos que 'uariv' es tu DataFrame en Python

uariv['segundo_nombre'] = np.where(uariv['segundo_nombre'].isin(preposiciones),
                                   uariv['segundo_nombre'] + ' ' + uariv['primer_apellido'],
                                   uariv['segundo_nombre'])

uariv['primer_apellido'] = np.where(uariv['segundo_nombre'].isin(preposiciones),
                                    uariv['segundo_apellido'],
                                    uariv['primer_apellido'])

uariv['segundo_apellido'] = np.where(uariv['segundo_nombre'].isin(preposiciones),
                                     np.nan,
                                     uariv['segundo_apellido'])

uariv['primer_apellido'] = np.where(uariv['primer_apellido'].isin(preposiciones),
                                    uariv['primer_apellido'] + ' ' + uariv['segundo_apellido'],
                                    uariv['primer_apellido'])

uariv['segundo_apellido'] = np.where(uariv['primer_apellido'].isin(preposiciones),
                                     np.nan,
                                     uariv['segundo_apellido'])

uariv['primer_apellido'] = np.where(uariv['primer_apellido'].isna(),
                                    uariv['segundo_apellido'],
                                    uariv['primer_apellido'])

# Documento
uariv['documento'] = uariv['VILB_DOCUMENTO'].str.strip().str.replace('\s+', '')

# Eliminar símbolos y caracteres especiales
uariv['documento'] = uariv['documento'].apply(lambda x: re.sub("[^A-Z0-9]", "", x))

# Eliminar cadenas de texto sin números y borrar registros de documentos de identificación iguales a '0'
uariv['documento_solo_cadena_texto'] = np.where(uariv['documento'].str.contains('[0-9]'), 0, 1)
uariv['documento'] = np.where(~uariv['documento'].str.contains('[0-9]'), np.nan,
                              np.where(uariv['documento'].astype(float) == 0, np.nan, uariv['documento']))

# Implementación de las reglas de la Registraduría
logging.info("Se implementa la revisión de la Registraduría")

# Si es CC o TI solo deben ser numéricos
uariv['documento_CC_TI_no_numerico'] = np.where(uariv['documento'].str.contains('[A-Z]') & uariv['VILB_TIPODOCUMENTO'].isin(['TI', 'CC']), 1, 0)
uariv['documento'] = np.where(uariv['documento'].str.contains('[A-Z]') & uariv['VILB_TIPODOCUMENTO'].isin(['TI', 'CC']), np.nan, uariv['documento'])

# Número de cedula o tarjeta de identidad longitud mayor a 10 debe ser mayor a 1.000.000.000
uariv['documento_CC_TI_mayor1KM'] = np.where((uariv['documento'].str.len() >= 10) & (pd.to_numeric(uariv['documento'], errors='coerce') <= 1000000000) & uariv['VILB_TIPODOCUMENTO'].isin(['TI', 'CC']), 1, 0)

# Número de tarjeta de identidad de 10 o 11 caracteres
uariv['documento_TI_10_11_caract'] = np.where(~((uariv['documento'].str.len() == 10) | (uariv['documento'].str.len() == 11)) & (uariv['VILB_TIPODOCUMENTO'] == 'TI'), 1, 0)

# Si TI es de longitud 11, los 6 primeros dígitos corresponden a la fecha de nacimiento (aammdd)
uariv['documento_TI_11_caract_fecha_nac'] = np.where(~((uariv['documento'].str[:6] == pd.to_datetime(uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime('%y%m%d')) & (uariv['documento'].str.len() == 11) & (uariv['VILB_TIPODOCUMENTO'] == 'TI')), 1, 0)

# El número de CC de un hombre con longitud de 4 a 8 debe estar entre 1 y 19.999.999 o 70.000.000 y 99.999.999
uariv['documento_CC_hombre_consistente'] = np.where(~(pd.to_numeric(uariv['documento'], errors='coerce').isin(range(1, 20000000)) | pd.to_numeric(uariv['documento'], errors='coerce').isin(range(70000000, 100000000))) & uariv['documento'].str.len().isin(range(4, 9)) & (uariv['VILB_TIPODOCUMENTO'] == 'CC') & (uariv['VILB_GENERO'] == 'HOMBRE')), 1, 0)

# El número de CC de una mujer de longitud 8 debe estar entre 20.000.000 y 69.999.999
uariv['documento_CC_mujer_consistente'] = np.where(~pd.to_numeric(uariv['documento'], errors='coerce').isin(range(20000000, 70000000)) & (uariv['documento'].str.len() == 8) & (uariv['VILB_TIPODOCUMENTO'] == 'CC') & (uariv['VILB_GENERO'] == 'MUJER')), 1, 0)

# La CC de una mujer debe estar entre 8 o 10 caracteres
uariv['documento_CC_mujer_consistente2'] = np.where(~uariv['documento'].str.len().isin([8, 10]) & (uariv['VILB_TIPODOCUMENTO'] == 'CC') & (uariv['VILB_GENERO'] == 'MUJER')), 1, 0)

# La CC de un hombre debe tener una longitud de 4, 5, 6, 7, 8 o 10 caracteres
uariv['documento_CC_hombre_consistente2'] = np.where(~uariv['documento'].str.len().isin([4, 5, 6, 7, 8, 10]) & (uariv['VILB_TIPODOCUMENTO'] == 'CC') & (uariv['VILB_GENERO'] == 'HOMBRE')), 1, 0)

# Si la TI es de 11 caracteres, el 10mo caracter debe ser 1, 3, 5, 7, 9 si es mujer
uariv['documento_TI_mujer_consistente'] = np.where(~uariv['documento'].str[9:10].isin([str(i) for i in range(1, 10, 2)]) & (uariv['documento'].str.len() == 11) & (uariv['VILB_TIPODOCUMENTO'] == 'TI') & (uariv['VILB_GENERO'] == 'MUJER')), 1, 0)

# Si la TI es de 11 caracteres, el 10mo caracter debe ser 2, 4, 6, 8, 0 si es hombre
uariv['documento_TI_hombre_consistente'] = np.where(~uariv['documento'].str[9:10].isin([str(i) for i in range(0, 10, 2)]) & (uariv['documento'].str.len() == 11) & (uariv['VILB_TIPODOCUMENTO'] == 'TI') & (uariv['VILB_GENERO'] == 'HOMBRE')), 1, 0)


# Resumen de documentos
resumen_documento = uariv.loc[:, uariv.columns.str.startswith('documento_')].copy()
resumen_documento = resumen_documento[uariv.filter(like='documento_').sum(axis=1) > 0]
resumen_documento = resumen_documento[['documento_', 'VILB_TIPODOCUMENTO', 'VILB_DOCUMENTO', 'documento', 'VILB_GENERO', 'VILB_FECHANACIMIENTO']]
resumen_documento.to_csv("log/revision_documentos.csv", sep=';', index=False)

# Summarize documento_ columns
documento_summary = resumen_documento.filter(like='documento_').apply(lambda x: x.sum())
print(documento_summary)

# Número de filas en resumen_documento
print(resumen_documento.shape[0])

# Sexo
uariv['sexo'] = uariv['VILB_GENERO'].apply(lambda x: 'OTRO' if x in ['LGBTI', 'INTERSEXUAL'] else x)
uariv['sexo'] = uariv['sexo'].replace('NO INFORMA', pd.NA)

# Pertenencia étnica
conditions = [
    (uariv['VILB_PERTENENCIAETNICA'] == 'NINGUNA'), 
    (uariv['VILB_PERTENENCIAETNICA'].isin(['NEGROA O AFROCOLOMBIANOA', 'RAIZAL DEL ARCHIPIELAGO DE SAN ANDRES Y PROVIDENCI', 'PALENQUERO', 'AFROCOLOMBIANO ACREDITADO RA'])), 
    (uariv['VILB_PERTENENCIAETNICA'].isin(['INDIGENA', 'INDIGENA ACREDITADO RA'])), 
    (uariv['VILB_PERTENENCIAETNICA'].isin(['GITANOA ROM', 'GITANO RROM ACREDITADO RA']))
]
choices = ['MESTIZO', 'NARP', 'INDIGENA', 'RROM']
uariv['iden_pertenenciaetnica'] = np.select(conditions, choices, default=pd.NA)

uariv['anio_nacimiento'] = pd.to_datetime(uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime("%Y")
uariv['mes_nacimiento'] = pd.to_datetime(uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime("%m")
uariv['dia_nacimiento'] = pd.to_datetime(uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime("%d")
uariv['fecha_nacimiento_dtf'] = pd.to_datetime(uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y")
uariv['fecha_nacimiento'] = pd.to_datetime(uariv['VILB_FECHANACIMIENTO'], format="%d/%m/%Y").dt.strftime("%Y%m%d")


# Ajuste del año de nacimiento
uariv['anio_nacimiento'] = np.where(uariv['anio_nacimiento'].astype(float) < 1905, np.nan, uariv['anio_nacimiento'])
uariv['anio_nacimiento'] = np.where(uariv['anio_nacimiento'].astype(float) > 2022, np.nan, uariv['anio_nacimiento'])

meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
dias = ["01", "02", "03", "04", "05", "06", "07", "08", "09"] + [str(i) for i in range(10, 32)]

# Verificación de meses y días
uariv = uariv[uariv['mes_nacimiento'].isin(meses)]
uariv = uariv[uariv['dia_nacimiento'].isin(dias)]

# Calculando la edad
uariv['edad'] = np.where((uariv['fecha_ocur_anio'].isna() | uariv['anio_nacimiento'].isna()) |
                        (uariv['fecha_ocur_anio'].astype(float) <= uariv['anio_nacimiento'].astype(float)),
                        np.nan,
                        uariv['fecha_ocur_anio'].astype(float) - uariv['anio_nacimiento'].astype(float))

# Ajuste de edad
uariv['edad'] = np.where(uariv['edad'] > 100, np.nan, uariv['edad'])

# Eliminar duplicados
n_1 = len(uariv)
uariv = uariv.drop_duplicates()

n_duplicados = n_1 - len(uariv)

# Excluir víctimas indirectas
n_2 = len(uariv)
uariv = uariv[uariv['VILB_TIPOVICTIMA'] != "INDIRECTA"]

n_indirectas = n_2 - len(uariv)

# Excluir personas jurídicas
n_3 = len(uariv)
uariv = uariv[~uariv['VILB_TIPODOCUMENTO'].isin(["NIT"])]

n_juridicas = n_3 - len(uariv)


# Campos requeridos
campos_requeridos = ['id_registro', 'tabla_origen', 'codigo_unico_fuente',
                     'nombre_completo', 'primer_nombre', 'segundo_nombre', 'primer_apellido',
                     'segundo_apellido', 'documento', 'fecha_nacimiento', 'anio_nacimiento',
                     'mes_nacimiento', 'dia_nacimiento', 'edad', 'iden_pertenenciaetnica', 'sexo',
                     'fecha_desaparicion', 'fecha_desaparicion_dtf', 'fecha_ocur_dia', 'fecha_ocur_mes',
                     'fecha_ocur_anio', 'TH_DF', 'TH_SE', 'TH_RU', 'TH_OTRO', 'descripcion_relato',
                     'pais_ocurrencia', 'codigo_dane_departamento', 'departamento_ocurrencia',
                     'codigo_dane_municipio', 'municipio_ocurrencia', 'pres_resp_paramilitares',
                     'pres_resp_grupos_posdesmov', 'pres_resp_agentes_estatales', 'pres_resp_guerr_farc',
                     'pres_resp_guerr_eln', 'pres_resp_guerr_otra', 'pres_resp_otro', 'situacion_actual_des']

# Verificar campos requeridos y seleccionar columnas
uariv = uariv[campos_requeridos].copy()

# Filtrar y eliminar filas con id_registro nulo
uariv = uariv.dropna(subset=['id_registro'])

# Eliminar duplicados manteniendo la primera ocurrencia
uariv = uariv.drop_duplicates(subset=campos_requeridos[1:], keep='first')

# Filtrar filas duplicadas en el campo codigo_unico_fuente
temp = uariv[uariv['codigo_unico_fuente'].isin(uariv[uariv.duplicated('codigo_unico_fuente')]['codigo_unico_fuente'])]

# Filtrar y verificar unicidad del campo codigo_unico_fuente
uariv = uariv.dropna(subset=['codigo_unico_fuente'])
uariv = uariv[~uariv.duplicated('codigo_unico_fuente')]

# Verificar que el número de filas sea igual a la cantidad única de codigo_unico_fuente
assert len(uariv) == len(uariv['codigo_unico_fuente'].unique())


# Mensaje de información
logging.info("Se realiza la identificación y eliminación de los registros no identificados")

# Identificación de registros
uariv_ident = uariv[
    (~uariv['primer_nombre'].isna() | ~uariv['segundo_nombre'].isna()) &
    (~uariv['primer_apellido'].isna() | ~uariv['segundo_apellido'].isna()) &
    (~uariv['documento'].isna() | ~uariv['fecha_ocur_anio'].isna() | ~uariv['fecha_ocur_anio'].isna() | ~uariv['departamento_ocurrencia'].isna())
]

# Registros no identificados
uariv_no_ident = uariv[~uariv['id_registro'].isin(uariv_ident['id_registro'])]

# Número de filas en cada conjunto
print("Número de filas en uariv:", len(uariv))
print("Número de filas en uariv_ident:", len(uariv_ident))
print("Número de filas en uariv_no_ident:", len(uariv_no_ident))
