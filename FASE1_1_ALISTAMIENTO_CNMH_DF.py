###################################################
# Autor: Esneyder
# Titulo: Homologacion de la fuente de informacion de la V_CNMH_DF
# Fecha: 23/12/2022
# Fecha Ajuste: 25/05/2023
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
import pyodbc
import numpy as np
import time
import yaml

# creacion de las funciones requeridas


def generate_hash(df):
    return df.apply(lambda x: hashlib.sha1(str(x).encode()).hexdigest(),
                    axis=1)
# Limpiar datos


def clean_text(text, na_values):
    # Eliminar acentos
    text = ''.join(c for c in unicodedata.normalize(
        'NFD', text) if unicodedata.category(c) != 'Mn')
    # Convertir a mayúsculas
    text = text.upper()
    # Eliminar espacios al inicio y al final
    text = text.strip()
    # Reemplazar nulos con NA
    if text in na_values:
        text = None
    # Eliminar caracteres no ASCII
    text = ''.join([c if ord(c) < 128 else ' ' for c in text])
    # Mantener solo caracteres alfanuméricos y espacios
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    # Eliminar espacios duplicados
    text = ' '.join(text.split())
    return text


# Limpiar todas las variables
for variable in list(locals()):
    del locals()[variable]
# Obtener los argumentos de la línea de comandos
args = sys.argv
if len(args) > 1:
    # Detecta si se proporciona la ruta base como argumento
    ruta_base = args[1]
else:
    # En caso contrario, define una ruta por defecto
    ruta_base = "E:/OrquestadorNatalia"
# Cambiar el directorio de trabajo a la ruta base
os.chdir(ruta_base)
n_sample = ""
if len(args) > 2:
    # Detecta si se proporciona el número de muestras como argumento
    n_sample = args[2]
# 32
# Establecer la ruta base
ruta_base = "E:/OrquestadorNatalia"

# Obtener la fecha y hora actual
fecha_inicio = datetime.now()
# Establecimiento de la conexion a la base de datos
# Listar los drivers ODBC instalados
drivers = [driver for driver in pyodbc.drivers()]
# Conectar a la base de datos SQL Server
connection_string = (
    r"Driver={SQL Server};"
    r"Server=172.16.10.10;"
    r"Database=UNIVERSO_PDD;"
    r"UID=orquestacion.universo;"
    r"PWD=Ubpd2022*;"
    r"Charset=latin1;"
)

try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    # Obtener una lista de tablas en la base de datos
    cursor.execute(
        "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_SCHEMA")
    tables = cursor.fetchall()
    for table in tables:
        print(table)
    # Listar otros objetos disponibles en la conexión ODBC
    objects = conn.listtables()
    for obj in objects:
        print(obj)
    # Cerrar la conexión
    cursor.close()
    conn.close()
except Exception as e:
    print("Error:", str(e))
# Cargar la tabla DIVIPOLA
dane = pd.read_stata(
    "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")
# Renombrar columnas
dane = dane.rename(columns={
    'codigo_dane_departamento': 'codigo_dane_departamento',
    'departamento': 'departamento_ocurrencia',
    'codigo_dane_municipio': 'codigo_dane_municipio',
    'municipio': 'municipio_ocurrencia'
})
# Seleccionar columnas y eliminar 'categoria_divipola'
dane = dane.drop(columns=['categoria_divipola'])
# Crear un DataFrame con las filas adicionales
additional_data = pd.DataFrame({
    'codigo_dane_departamento': [np.nan, "94", "99", "99"],
    'departamento_ocurrencia': [np.nan, "GUAINIA", "VICHADA", "VICHADA"],
    'codigo_dane_municipio': [np.nan, "94663", "99572", "99760"],
    'municipio_ocurrencia': [np.nan, "MAPIRIPANA", "SANTA RITA",
                             "SAN JOSE DE OCUNE"]
})
# Concatenar los DataFrames
dane = pd.concat([dane, additional_data], ignore_index=True)

# Seleccionar las columnas 'codigo_dane_departamento'
# y 'departamento_ocurrencia' y eliminar duplicados
dane_depts = dane[['codigo_dane_departamento',
                   'departamento_ocurrencia']].drop_duplicates()
# Lectura de la tabla de la cnmh resumida a no desplazamiento forzado
# Definir la cadena de conexión
connection_string = (
    r"Driver={SQL Server};"
    r"Server=172.16.10.10;"
    r"Database=UNIVERSO_PDD;"
    r"UID=orquestacion.universo;"
    r"PWD=Ubpd2022*;"
    r"charset=latin1;"
)

# Conectar a la base de datos
try:
    conn = pyodbc.connect(connection_string)
except pyodbc.Error as e:
    print(f"Error de conexión a la base de datos: {str(e)}")
    conn = None

n_sample_p = ""
if n_sample:
    n_sample_p = f"TOP ({n_sample})"

# Consulta SQL para obtener datos de CNMH_DF
query = f"""
    SELECT {n_sample_p} * FROM [dbo].[V_CNMH_DF] personas
    LEFT JOIN [dbo].[V_CNMH_DF_C] casos ON casos.IdCaso = personas.IdCaso
"""
cnmh = pd.read_sql(query, conn)

# Consulta SQL para obtener el número de casos
query_casos = "SELECT COUNT(*) FROM [dbo].[V_CNMH_DF_C]"
n_casos = pd.read_sql(query_casos, conn).iloc[0, 0]

# Consulta SQL para obtener el número de personas
query_personas = "SELECT COUNT(*) FROM [dbo].[V_CNMH_DF]"
n_personas = pd.read_sql(query_personas, conn).iloc[0, 0]

# Consulta SQL para obtener el número de casos sin personas
query_casos_sin_personas = """
    SELECT COUNT(*) FROM [dbo].[V_CNMH_DF_C]
    WHERE IdCaso NOT IN (SELECT IdCaso FROM [dbo].[V_CNMH_DF])
"""
n_casos_sin_personas = pd.read_sql(query_casos_sin_personas, conn).iloc[0, 0]

# Consulta SQL para obtener casos sin personas
query_casos_sin_personas_data = """
    SELECT * FROM [dbo].[V_CNMH_DF_C]
    WHERE IdCaso NOT IN (SELECT IdCaso FROM [dbo].[V_CNMH_DF])
"""
casos_sin_personas = pd.read_sql(query_casos_sin_personas_data, conn)

# Cerrar la conexión a la base de datos
conn.close()
# creacion del hash unico para cada registro
# Utiliza clean_names() para convertir los nombres de las columnas a minúsculas
# y reemplazar espacios con guiones bajos
cnmh.columns = cnmh.columns.str.lower().str.replace(' ', '_')
# Utiliza distinct() para eliminar filas duplicadas
cnmh = cnmh.drop_duplicates()
# Obtener el número de filas en el DataFrame cnmh
nrow_cnmh = len(cnmh)
# Obtener el número de filas únicas basadas en la concatenación de columnas
filas_unicas = cnmh['id_caso'] + cnmh['id'] + cnmh['identificador_caso']
num_filas_unicas = len(filas_unicas.unique())
# Obtener la longitud de la columna 'id'
longitud_columna_id = len(cnmh['id'])
# Crear una nueva columna 'id_registro' con el hash
cnmh['id_registro'] = cnmh.apply(generate_hash, axis=1)
# Crear una nueva columna 'tabla_origen' con el valor "CNMH_DF"
cnmh['tabla_origen'] = "CNMH_DF"
# Crear una nueva columna 'codigo_unico_fuente' con la
# concatenación de columnas
cnmh['codigo_unico_fuente'] = cnmh['id_caso'] + "_" + \
    cnmh['identificador_caso'] + "_" + cnmh['id']
# normalizacion de los campos de texto
# Seleccionar la columna 'id_registro'
id_registros = cnmh['id_registro']
# Definir una lista de valores a reemplazar
na_values = ["ND", "AI", "[SIN INFORMACION]"]

variables_limpieza = [
    "estado", "zon_id_lugar_del_hecho", "municipio_caso", "depto_caso",
    "nacionalidad", "tipo_documento", "nombres_apellidos",
    "sobre_nombre_alias", "sexo", "orientacion_sexual",
    "descripcion_edad", "etnia", "descripcion_etnia", "discapacidad",
    "ocupacion_victima", "descripcion_otra_ocupacion_victima",
    "calidad_victima", "cargo_rango_funcionario_publico",
    "cargo_empleado_sector_privado", "tipo_poblacion_vulnerable",
    "descripcion_otro_tipo_poblacion_vulnerable", "organizacion_civil",
    "militante_politico", "descripcion_otro_militante_politico",
    "grupo", "descripcion_grupo", "espeficicacion_presunto_responsable",
    "observaciones_grupo_armado1", "rango_fuerzas_armadas",
    "descripcion_rango_fuerzas_armadas_estatales", "rango_grupo_armado",
    "descripcion_rango_grupo_armado", "acciones_busqueda_familias",
    "actv_mec_bus", "situacion_actual_victima",
    "fuente_informacion_desaparicion", "rad_sen_jud", "infjev", "confesion",
    "viol_cuerpo_nombre", "sig_viol_cuerpo", "signos_violencia_sexual",
    "desc_sig_vs", "disposicion_cuerpo", "d_disp_cuerpo", "depto_aparic",
    "vereda_sitio_ap", "esc_aparic", "reg_hechos_gao", "entidad_recep_denun",
    "mun_denun", "depto_denun"]

# Quitar la columna 'id_registro'
cnmh = cnmh.drop(columns=['id_registro'])
# Aplicar las transformaciones a las columnas de tipo 'str'
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(
    lambda x: x.str.strip().str.upper())
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(
    lambda x: clean_text(x, na_values))
# Suponiendo que 'id_registros' es una Serie de Pandas
cnmh['id_registro'] = id_registros
# homologacion de estructura, formato y contenido
# Datos sobre los hechos
# lugar de ocurrencia
# Lista de variables a limpiar
variables_limpieza_dane = ["departamento_ocurrencia", "municipio_ocurrencia"]
# Aplicar transformaciones a las columnas de tipo 'str'
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(
    lambda x: x.str.strip().str.upper())
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(
    lambda x: clean_text(x, na_values))
# Suponiendo que 'na_values' es una lista de valores a reemplazar
for value in na_values:
    dane = dane.replace(value, pd.NA)
# Obtener valores únicos de 'codigo_dane_departamento'
dane = dane["codigo_dane_departamento"].unique()
# 200
# Crear una nueva columna 'pais_ocurrencia'
cnmh['pais_ocurrencia'] = np.where(cnmh['depto_caso'] == 'EXTERIOR', None,
                                   'COLOMBIA')

# Realizar mapeo de valores en las columnas 'depto_caso' y 'municipio_caso'
mapeo_depto = {
    'SIN INFORMACION': None,
    'EXTERIOR': None,
    'CUCUTA': 'SAN JOSE DE CUCUTA',
    'ARMERO GUAYABAL': 'ARMERO',
    'TOLU VIEJO': 'SAN JOSE DE TOLUVIEJO',
    'CUASPUD': 'CUASPUD CARLOSAMA',
    'BARRANCO MINAS': 'BARRANCOMINAS',
    'MOMPOS': 'SANTA CRUZ DE MOMPOX',
    'BELEN DE BAJIRA': 'RIOSUCIO',
    'PIENDAMO': 'PIENDAMO TUNIA',
    'SOTARA': 'SOTARA PAISPAMBA',
    'FRONTERA VENEZUELA': None,
    'FRONTERA PANAMA': None,
    'FRONTERA BRASIL': None,
    'GUICAN': 'GUICAN DE LA SIERRA',
    'FRONTERA': None,
    'FRONTERA ECUADOR': None,
    'YACARATE': 'YAVARATE'
}

cnmh['depto_caso'] = cnmh['depto_caso'].map(mapeo_depto)
# Realizar el mismo mapeo para 'municipio_caso'
cnmh['municipio_caso'] = cnmh['municipio_caso'].map(mapeo_depto)
# A continuación, aseguramos que los valores de 'depto_caso' y 'municipio_caso'
# existan en el DataFrame 'dane'
# Crear un conjunto de valores únicos en 'departamento_ocurrencia' y
# 'municipio_ocurrencia' de 'dane'
depto_ocurrencia_set = set(dane['departamento_ocurrencia'])
municipio_ocurrencia_set = set(dane['municipio_ocurrencia'])
# Asegurar que los valores en 'depto_caso' y 'municipio_caso'
# estén en los conjuntos creados anteriormente
cnmh = cnmh[cnmh['depto_caso'].isin(
    depto_ocurrencia_set) & cnmh['municipio_caso'].isin(municipio_ocurrencia_set)]
# Realizar una operación de left join con el DataFrame 'dane'
cnmh = pd.merge(cnmh, dane, left_on=['depto_caso', 'municipio_caso'],
                right_on=['departamento_ocurrencia', 'municipio_ocurrencia'],
                how='left', suffixes=('', '_dane'))
# fecha de ocurrencia
# 232
# Convertir valores NA en 'mesh' a "0"
cnmh['mesh'] = cnmh['mesh'].fillna("0")
# Añadir ceros iniciales a 'diah' si es menor que 10
cnmh['diah'] = cnmh['diah'].apply(lambda x: f"0{x}" if int(x) < 10 else x)
# Añadir ceros iniciales a 'mesh' si es menor que 10
cnmh['mesh'] = cnmh['mesh'].apply(lambda x: f"0{x}" if int(x) < 10 else x)
# Asegurarse de que 'annoh' sea al menos "1900"
cnmh['annoh'] = cnmh['annoh'].apply(lambda x: "1900" if int(x) < 1000 else x)
# Crear una nueva columna 'fecha_hecho_0' con la fecha en formato "dd-mm-yyyy"
cnmh['fecha_hecho_0'] = cnmh['diah'] + '-' + cnmh['mesh'] + '-' + cnmh['annoh']
# Convertir 'fecha_hecho_0' a tipo de dato Date
cnmh['fecha_hecho'] = pd.to_datetime(cnmh['fecha_hecho_0'], format="%d-%m-%Y")
# Formatear 'fecha_desaparicion' como "YYYYMMDD"
cnmh['fecha_desaparicion'] = cnmh['fecha_hecho'].dt.strftime("%Y%m%d")
# Convertir 'fecha_hecho' a tipo de dato Date
cnmh['fecha_desaparicion_dtf'] = pd.to_datetime(cnmh['fecha_hecho'])
# Copiar 'annoh' a 'fecha_ocur_anio'
cnmh['fecha_ocur_anio'] = cnmh['annoh']
# Copiar 'mesh' a 'fecha_ocur_mes'
cnmh['fecha_ocur_mes'] = cnmh['mesh']
# Copiar 'diah' a 'fecha_ocur_dia'
cnmh['fecha_ocur_dia'] = cnmh['diah']
# Reemplazar valores en 'fecha_ocur_anio' según condiciones
cnmh.loc[cnmh['fecha_ocur_anio'].astype(
    float) < 1900, 'fecha_ocur_anio'] = None
cnmh.loc[cnmh['fecha_ocur_anio'].isna(), 'fecha_ocur_anio'] = None
# Reemplazar valores en 'fecha_ocur_mes' según condiciones
cnmh.loc[cnmh['fecha_ocur_mes'] == "00", 'fecha_ocur_mes'] = None
cnmh.loc[cnmh['fecha_ocur_mes'].isna(), 'fecha_ocur_mes'] = None
# Reemplazar valores en 'fecha_ocur_dia' según condiciones
cnmh.loc[cnmh['fecha_ocur_dia'] == "00", 'fecha_ocur_dia'] = None
cnmh.loc[cnmh['fecha_ocur_dia'] == "", 'fecha_ocur_dia'] = None
cnmh.loc[cnmh['fecha_ocur_dia'].isna(), 'fecha_ocur_dia'] = None
# 258
# Definir listas de meses y días
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
         "12"]
dias = ["01", "02", "03", "04", "05", "06", "07", "08",
        "09"] + [str(i) for i in range(10, 32)]
# Realizar las aserciones
cnmh = cnmh[cnmh['fecha_ocur_anio'].isin(map(str, range(1900, 2023)))]
cnmh = cnmh[cnmh['fecha_ocur_mes'].isin(meses)]
cnmh = cnmh[cnmh['fecha_ocur_dia'].isin(dias)]

# 266
# presuntos responsables
sin_informacion_actores = ['NO DEFINIDO', '', 'NO IDENTIFICA',
                           'SIN INFORMACIÓN CONFLICTO ARMADO',
                           'NO IDENTIFICA CONFLICTO ARMADO',
                           'OTROS VIOLENCIA GENERALIZADA',
                           'SIN INFORMACIÓN',
                           'NO IDENTIFICA RELACIÓN CERCANA Y SUFICIENTE',
                           'NO IDENTIFICA VIOLENCIA GENERALIZADA',
                           'NO IDENTIFICA RELACIÓN CERCANA Y SUFICIENTE ',
                           'SIN INFORMACIÓN RELACIÓN CERCANA Y SUFICIENTE',
                           'NO IDENTIFICA CONFLICTO ARMADO ',
                           'CONFLICTO ARMADO', '0',
                           'SIN INFORMACIÓN VIOLENCIA GENERALIZADA', '',
                           "NO IDENTIFICADO"]

cnmh['espeficicacion_presunto_responsable'] = cnmh['espeficicacion_presunto_responsable'].apply(
    lambda x: None if x in sin_informacion_actores else x)
# 284
responsables_cols = ["perpetrador_identificado", "presunto_reponsable",
                     "descripcion_presunto_responsable1",
                     "espeficicacion_presunto_responsable_2",
                     "observaciones_presunto_responsable1"]
# Aplicar la función de limpieza a las columnas de responsables
for col in responsables_cols:
    cnmh[col] = cnmh[col].apply(
        lambda x: None if x in sin_informacion_actores else x)
# 291
# Crear columnas de pres_resp_ y establecer valores iniciales en 0
pres_resp_columns = [
    "pres_resp_agentes_estatales",
    "pres_resp_grupos_posdesmov",
    "pres_resp_paramilitares",
    "pres_resp_guerr_eln",
    "pres_resp_guerr_farc",
    "pres_resp_guerr_otra"]
# Actualizar las columnas de pres_resp_ basadas en las condiciones
cnmh.loc[cnmh["grupo"].str.contains("FUERZA PÚBLICA|AGENTE DEL ESTADO|GENTE EXTRANJERO"),
         "pres_resp_agentes_estatales"] = 1
cnmh.loc[cnmh["grupo"].str.contains(
    "POSDESMOVILIZACION"), "pres_resp_grupos_posdesmov"] = 1
cnmh.loc[cnmh["grupo"].str.contains("AUTODEF|PARAMI|AUC|A.U.C|PARAMILITAR"),
         "pres_resp_paramilitares"] = 1
cnmh.loc[cnmh["grupo"].str.contains("ELN"), "pres_resp_guerr_eln"] = 1
cnmh.loc[cnmh["grupo"].str.contains(
    "GUERRILLA") & cnmh["descripcion_grupo"].str.contains("ELN"), "pres_resp_guerr_eln"] = 1
cnmh.loc[cnmh["grupo"].str.contains("FARC"), "pres_resp_guerr_farc"] = 1
cnmh.loc[cnmh["grupo"].str.contains("GUERRILLA") & cnmh["descripcion_grupo"].str.contains(
    "FARC"), "pres_resp_guerr_farc"] = 1
cnmh.loc[cnmh["grupo"].str.contains("GRUPOS GUERRILLEROS") & ~(cnmh["grupo"].str.contains(
    "FARC|ELN")) & ~(cnmh["descripcion_grupo"].str.contains("FARC|ELN")), "pres_resp_guerr_otra"] = 1
cnmh.loc[cnmh["grupo"].str.contains("GUERRILLA") & ~(cnmh["grupo"].str.contains("FARC|ELN")) & ~(
    cnmh["descripcion_grupo"].str.contains("FARC|ELN")), "pres_resp_guerr_otra"] = 1
cnmh.loc[cnmh["grupo"].str.contains("GUERRILLA") & ~(
    cnmh["descripcion_grupo"].str.contains("FARC|ELN")), "pres_resp_guerr_otra"] = 1
cnmh.loc[cnmh["grupo"].str.contains("EPL"), "pres_resp_guerr_otra"] = 1
cnmh.loc[cnmh["grupo"].str.contains("GUERRILLA") & cnmh["descripcion_grupo"].str.contains(
    "EPL"), "pres_resp_guerr_otra"] = 1

# Calcular la columna "tmp" y actualizar "pres_resp_otro"
# Revisar la sumatoria de las columnas con prefijo "pres_resp_"
cnmh["tmp"] = cnmh[pres_resp_columns].sum(axis=1)
cnmh["pres_resp_otro"] = cnmh["tmp"].apply(lambda x: 1 if x > 0 else 0)
# Eliminar la columna temporal "tmp"
cnmh.drop(columns=["tmp"], inplace=True)
# 325
# tipo de hecho

otros_hechos = ["CAPTURA", "CAPTURA - ACUSACIÓN DE TERRORISMO",
                "CAPTURADO CON ESPOSAS", "IÓNTENTO DE OCULTAMIENTO DE CADÁVER",
                "MASACRE - DAÑO A BIEN CIVIL", "OCULTAMIENTO",
                "OCULTAMIENTO DE CADAVER",
                "OCULTAMIENTO DE CADÁVER", "OCULTAMIENTO DEL CADÁVER"]

cnmh["TH_DF"] = (cnmh["tipo_desaparicion"].str.contains("DESAPARICIÓN FORZADA|DESAPARICIÓN") |
                 cnmh["otro_hecho_simultaneo"].str.contains("DESAPARICIÓN|DESAPARECIO")).astype(int)

cnmh["TH_SE"] = (cnmh["otro_hecho_simultaneo"].str.contains(
    "SECUESTRO|SECUSTRO|RETENCIONES|RETENCIÓN|DETENCIÓN|DETENCION|RESTRICCIÓN DE LA LIBERTAD|RESTRICCIÓN DE MOVILIDAD INDIVIDUAL")).astype(int)

cnmh["TH_RU"] = (cnmh["otro_hecho_simultaneo"].str.contains(
    "RECLUTAMIENTO")).astype(int)

cnmh["TH_OTRO"] = (
    cnmh["otro_hecho_simultaneo"].str.contains(
        '|'.join(otros_hechos))).astype(int)

# 358
th_df_counts = cnmh["TH_DF"].value_counts()
th_se_counts = cnmh["TH_SE"].value_counts()
th_ru_counts = cnmh["TH_RU"].value_counts()
th_otro_counts = cnmh["TH_OTRO"].value_counts()
# 365
# relato
# Mutación para limpiar la descripción del caso
cnmh["descripcion_relato"] = cnmh["descripcion_del_caso"].str.strip()
# Mutación para modificar la situación actual de la víctima
cnmh["situacion_actual_des"] = cnmh["situacion_actual_victima"].replace({
    "APARECIO MUERTO": "Aparecio Muerto",
    "SIGUE DESAPAR": "Continua desaparecido",
    "VIVA": "Aparecio Vivo",
    "MUERTA": "Aparecio Muerto",
    "DESCONOCIDA": None,
    "APARECIO VIVO": "Aparecio Vivo",
    "DESAPARECIDO FORZADO": "Continua desaparecido"
})
# 385
# Datos sobre las personas dadas por desaparecidas
# nombres y apellidos
# Mutación para limpiar y estandarizar el nombre completo
cnmh["nombre_completo"] = cnmh["nombres_apellidos"].str.strip()
cnmh["nombre_completo"] = cnmh["nombre_completo"].replace(
    {"PERSONA SIN IDENTIFICAR": None})
cnmh["nombre_completo"] = cnmh["nombre_completo"].str.replace(
    r'\bNA\b', '', regex=True).str.strip()
# 396
preposiciones = ["DE", "DEL", "DE LAS", "DE LA", "DE LOS", "VAN",
    "LA", "VIUDA DE", "VIUDA", "SAN", "DA"]
cnmh["primer_nombre"] = np.nan
cnmh["segundo_nombre"] = np.nan
cnmh["primer_apellido"] = np.nan
cnmh["segundo_apellido"] = np.nan

for i in range(len(cnmh)):
    token = cnmh["nombre_completo"].iloc[i].split()
    n_iter = 0
    while any(elem in token for elem in preposiciones):
        for k in range(len(token)):
            if token[k] in preposiciones:
                token[k + 1] = token[k] + " " + token[k + 1]
                token.pop(k)
        n_iter += 1
        if n_iter > 10:
            break

    while len(token) > 4:
        for k in range(len(token)):
            if k == 1:
                token[k + 1] = token[k] + " " + token[k + 1]
                token.pop(k)
        n_iter += 1
        if n_iter > 10:
            break

    if all(elem is None for elem in token):
        cnmh.at[i, "primer_nombre"] = np.nan
        cnmh.at[i, "segundo_nombre"] = np.nan
        cnmh.at[i, "primer_apellido"] = np.nan
        cnmh.at[i, "segundo_apellido"] = np.nan
    else:
        if len(token) == 4:
            cnmh.at[i, "primer_nombre"] = token[0]
            cnmh.at[i, "segundo_nombre"] = token[1]
            cnmh.at[i, "primer_apellido"] = token[2]
            cnmh.at[i, "segundo_apellido"] = token[3]
        elif len(token) == 3:
            cnmh.at[i, "primer_nombre"] = token[0]
            cnmh.at[i, "segundo_nombre"] = None
            cnmh.at[i, "primer_apellido"] = token[1]
            cnmh.at[i, "segundo_apellido"] = token[2]
        elif len(token) == 2:
            cnmh.at[i, "primer_nombre"] = token[0]
            cnmh.at[i, "segundo_nombre"] = None
            cnmh.at[i, "primer_apellido"] = token[1]
            cnmh.at[i, "segundo_apellido"] = None
        elif len(token) == 1:
            cnmh.at[i, "primer_nombre"] = token[0]
            cnmh.at[i, "segundo_nombre"] = None
            cnmh.at[i, "primer_apellido"] = None
            cnmh.at[i, "segundo_apellido"] = None
# 475
# Primero, asegúrate de que los valores de tipo_documento estén limpios
cnmh['tipo_documento'] = cnmh['tipo_documento'].str.strip()
# Define una lista de preposiciones
preposiciones = [
    "DE",
    "DEL",
    "DE LAS",
    "DE LA",
    "DE LOS",
    "VAN",
    "LA",
    "VIUDA DE",
    "VIUDA",
    "SAN",
    "DA"]
# Realiza las transformaciones necesarias en las columnas de nombres y
# apellidos
cnmh['segundo_nombre'] = cnmh['segundo_nombre'].apply(
    lambda x: x if x not in preposiciones else x + " " + cnmh['primer_apellido'])
cnmh['primer_apellido'] = cnmh['primer_apellido'].apply(
    lambda x: x if x not in preposiciones else cnmh['segundo_apellido'])
cnmh['segundo_apellido'] = cnmh['segundo_apellido'].apply(
    lambda x: None if x in preposiciones else x)
# Elimina nombres y apellidos cuando solo se registra la letra inicial
columnas_nombre_apellido = [
    'primer_nombre',
    'segundo_nombre',
    'primer_apellido',
    'segundo_apellido']
cnmh[columnas_nombre_apellido] = cnmh[columnas_nombre_apellido].apply(
    lambda x: x if x.str.len() > 1 else None)
# Reemplaza primer apellido por segundo apellido cuando el primer campo se
# encuentra vacío
cnmh['primer_apellido'].fillna(cnmh['segundo_apellido'], inplace=True)
# 503
# Documento
cnmh['documento'] = cnmh['numero_documento'].str.strip(
).str.replace("[^A-Z0-9]", "", regex=True)
# Eliminar cadenas de texto sin números y borrar registros de documentos
# de identificación iguales a '0'
cnmh['documento'] = cnmh['documento'].apply(
    lambda x: None if not re.search(
        r'\d', x) or x == '0' else x)
# Crear una nueva columna para indicar si el documento contiene solo texto
cnmh['documento_solo_cadena_texto'] = cnmh['documento'].apply(
    lambda x: 0 if re.search(r'\d', x) else 1)
# Convertir la columna 'documento' a tipo numérico si contiene solo números
cnmh['documento'] = pd.to_numeric(cnmh['documento'], errors='coerce')
# 518
# implementación de las reglas de la registraduria
# Columna: documento_CC_TI_no_numerico
cnmh['documento_CC_TI_no_numerico'] = cnmh.apply(
    lambda row: 1 if re.search(
        r'[A-Z]',
        row['documento']) and row['tipo_documento'] in [
            "TARJETA DE IDENTIDAD",
            "CEDULA DE CIUDADANIA"] else 0,
    axis=1)
# Columna: documento
cnmh['documento'] = cnmh.apply(
    lambda row: None if re.search(
        r'[A-Z]',
        row['documento']) and row['tipo_documento'] in [
            "TARJETA DE IDENTIDAD",
            "CEDULA DE CIUDADANIA"] else row['documento'],
    axis=1)
# Columna: documento_CC_TI_mayor1KM
cnmh['documento_CC_TI_mayor1KM'] = cnmh.apply(
    lambda row: 1 if len(
        row['documento']) >= 10 and int(
            row['documento']) <= 1000000000 and row['tipo_documento'] in [
                "TARJETA DE IDENTIDAD",
                "CEDULA DE CIUDADANIA"] else 0,
    axis=1)
# Columna: documento_TI_10_11_caract
cnmh['documento_TI_10_11_caract'] = cnmh.apply(
    lambda row: 1 if len(
        row['documento']) not in [10, 11] 
    and row['tipo_documento'] == "TARJETA DE IDENTIDAD" else 0,  axis=1)
# Columna: documento_TI_11_caract_fecha_nac
cnmh['documento_TI_11_caract_fecha_nac'] = cnmh.apply(
    lambda row: 1 if len(row['documento']) == 11 
    and row['documento'][:6] != row['fecha_nacimiento'].strftime("%y%m%d") 
    and row['tipo_documento'] == "TARJETA DE IDENTIDAD" else 0, axis=1)
# Columna: documento_CC_hombre_consistente
cnmh['documento_CC_hombre_consistente'] = cnmh.apply(
    lambda row: 1 if (
        int(
            row['documento']) not in range(
                1,
                20000000) and int(
                    row['documento']) not in range(70000000,100000000)) 
                    and len(
                            row['documento']) in [4, 5, 6, 7, 8]
                            and row['tipo_documento'] == "CEDULA DE CIUDADANIA"
                            and row['sexo'] == "H" else 0,  axis=1)
# Columna: documento_CC_mujer_consistente
cnmh['documento_CC_mujer_consistente'] = cnmh.apply(
    lambda row: 1 if (
        int(
            row['documento']) not in range(20000000,70000000)) 
                    and len(
                    row['documento']) == 8
                    and row['tipo_documento'] == "CEDULA DE CIUDADANIA"
                    and row['sexo'] == "M" else 0,   axis=1)
# Columna: documento_CC_mujer_consistente2
cnmh['documento_CC_mujer_consistente2'] = cnmh.apply(
    lambda row: 1 if len(
        row['documento']) not in [ 8, 10] 
    and row['tipo_documento'] == "CEDULA DE CIUDADANIA" 
    and row['sexo'] == "M" else 0,   axis=1)
# Columna: documento_CC_hombre_consistente2
cnmh['documento_CC_hombre_consistente2'] = cnmh.apply(
    lambda row: 1 if len(
        row['documento']) not in [4, 5, 6, 7,  8, 10] 
            and row['tipo_documento'] == "CEDULA DE CIUDADANIA" 
            and row['sexo'] == "H" else 0,  axis=1)
# Columna: documento_TI_mujer_consistente
cnmh['documento_TI_mujer_consistente'] = cnmh.apply(
    lambda row: 1 if row['documento'][9] not in [
        '1', '3', '5', '7', '9'] and len(
            row['documento']) == 11 
            and row['tipo_documento'] == "TARJETA DE IDENTIDAD" 
            and row['sexo'] == "M" else 0,  axis=1)
# Columna: documento_TI_hombre_consistente
cnmh['documento_TI_hombre_consistente'] = cnmh.apply(
    lambda row: 1 if row['documento'][9] not in ['0', '2', '4', '6',  '8'] 
    and len(row['documento']) == 11 
    and row['tipo_documento'] == "TARJETA DE IDENTIDAD" 
    and row['sexo'] == "H" else 0,   axis=1)
# 583
# Filtrar y seleccionar columnas relevantes
resumen_documento = cnmh[cnmh.filter(like="documento_").sum(axis=1) > 0][["documento_CC_TI_no_numerico",
                                                                          "documento_CC_TI_mayor1KM",
                                                                          "documento_TI_10_11_caract",
                                                                          "documento_TI_11_caract_fecha_nac",
                                                                          "documento_CC_hombre_consistente",
                                                                          "documento_CC_mujer_consistente",
                                                                          "documento_CC_mujer_consistente2",
                                                                          "documento_CC_hombre_consistente2",
                                                                          "documento_TI_mujer_consistente",
                                                                          "documento_TI_hombre_consistente",
                                                                          "tipo_documento",
                                                                          "numero_documento",
                                                                          "documento",
                                                                          "sexo",
                                                                          "fecha_nacimiento"]]
# Suponiendo que ya tienes el DataFrame resumen_documento en Python
# Seleccionar columnas que comienzan con "documento_", y algunas otras columnas
columnas_seleccionadas = resumen_documento.filter(like="documento_").columns.tolist(
) + ["tipo_documento", "numero_documento", "documento", "sexo", "fecha_nacimiento"]
resumen_documento_seleccionado = resumen_documento[columnas_seleccionadas]
# Guardar el DataFrame resultante en un archivo CSV
resumen_documento_seleccionado.to_csv(
    "log/revision_documentos_cnmh_df.csv", sep=";", index=False)
# Resumen de las columnas que comienzan con "documento_"
resumen_columnas_documento = resumen_documento_seleccionado.filter(
    like="documento_").agg('sum')
# Número de filas en el DataFrame
n_filas = len(resumen_documento_seleccionado)

# 587
# sexo
cnmh['sexo'] = np.where(cnmh['sexo'] == "S", np.nan,
                        np.where(cnmh['sexo'] == "I", "OTRO",
                        np.where(cnmh['sexo'] == "H", "HOMBRE",
                                 np.where(cnmh['sexo'] == "M", "MUJER",
                                 cnmh['sexo']))))
# 611
# pertenencia etnica
cnmh['iden_pertenenciaetnica'] = np.where(
    cnmh['etnia'] == "", "NINGUNA", np.where(
        cnmh['etnia'].isin(
            [
                "AFROCOLOMBIANO", "PALENQUERO", "RAIZAL"]), "NARP", np.where(
                    cnmh['etnia'] == "INDIGENA", "INDIGENA", np.where(
                        cnmh['etnia'] == "ROM", "RROM", np.nan))))
# 622
# fecha de nacimiento
# Copiar la columna fecha_nacimiento a fecha_nacimiento_original
cnmh['fecha_nacimiento_original'] = cnmh['fecha_nacimiento']

# Extraer el año, mes y día de la fecha de nacimiento
cnmh['anio_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'],
                                         format="%Y-%m-%d").dt.strftime("%Y")
cnmh['mes_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'],
                                        format="%Y-%m-%d").dt.strftime("%m")
cnmh['dia_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'],
                                        format="%Y-%m-%d").dt.strftime("%d")
cnmh['fecha_nacimiento_dtf'] = pd.to_datetime(
    cnmh['fecha_nacimiento_original'], format="%Y-%m-%d")
# Convertir la fecha de nacimiento a formato YYYYMMDD
cnmh['fecha_nacimiento'] = pd.to_datetime(
    cnmh['fecha_nacimiento_original'],
    format="%Y-%m-%d").dt.strftime("%Y%m%d")

# Filtrar y corregir los valores de anio_nacimiento fuera del rango
cnmh['anio_nacimiento'] = np.where(
    pd.to_numeric(
        cnmh['anio_nacimiento']) < 1905,
    np.nan,
    cnmh['anio_nacimiento'])
cnmh['anio_nacimiento'] = np.where(
    pd.to_numeric(
        cnmh['anio_nacimiento']) > 2022,
    np.nan,
    cnmh['anio_nacimiento'])

# Verificar que los valores de anio_nacimiento, mes_nacimiento y
# dia_nacimiento estén en conjuntos válidos
valid_years = list(map(str, range(1900, 2023)))
valid_months = list(map(str, range(1, 13)))
valid_days = list(map(str, range(1, 32)))

cnmh['anio_nacimiento'] = np.where(cnmh['anio_nacimiento'].isin(valid_years),
                                   cnmh['anio_nacimiento'], np.nan)
cnmh['mes_nacimiento'] = np.where(cnmh['mes_nacimiento'].isin(valid_months),
                                  cnmh['mes_nacimiento'], np.nan)
cnmh['dia_nacimiento'] = np.where(cnmh['dia_nacimiento'].isin(valid_days),
                                  cnmh['dia_nacimiento'], np.nan)

# Calcular la edad
cnmh['edad'] = np.where(
    (cnmh['fecha_ocur_anio'].isna() | cnmh['anio_nacimiento'].isna()),
    np.nan,
    np.where(
        cnmh['fecha_ocur_anio'].astype(float) <= cnmh['anio_nacimiento'].astype(float),
        np.nan,
        cnmh['fecha_ocur_anio'].astype(float) -
        cnmh['anio_nacimiento'].astype(float)))
cnmh['edad'] = np.where(cnmh['edad'] > 100, np.nan, cnmh['edad'])

# Verificar que la edad esté dentro del rango [1, 100]
cnmh['edad'] = np.where(
    (cnmh['edad'].between(
        1,
        100,
        inclusive=True) | cnmh['edad'].isna()),
    cnmh['edad'],
    np.nan)

# 658
# Identificación de filas únicas
n_1 = len(cnmh)
cnmh = cnmh.drop_duplicates()
n_duplicados = n_1 - len(cnmh)
# Excluir victimas indirectas
n_2 = len(cnmh)
n_indirectas = n_2 - len(cnmh)
# Excluir personas jurídicas
n_3 = len(cnmh)
cnmh = cnmh[cnmh['calidad_victima'] != "PERSONA JURIDICA"]
n_juridicas = n_3 - len(cnmh)

# Crear tabla de datos únicamente con los campos requeridos para la
# comparación de nombres
campos_requeridos = ['id_registro', 'tabla_origen', 'codigo_unico_fuente',
                     'nombre_completo', 'primer_nombre', 'segundo_nombre',
                     'primer_apellido', 'segundo_apellido', 'documento',
                     'fecha_nacimiento', 'anio_nacimiento', 'mes_nacimiento',
                     'dia_nacimiento', 'edad', 'iden_pertenenciaetnica',
                     'sexo', 'fecha_desaparicion', 'fecha_desaparicion_dtf',
                     'fecha_ocur_dia', 'fecha_ocur_mes', 'fecha_ocur_anio',
                     'TH_DF', 'TH_SE', 'TH_RU', 'TH_OTRO',
                     'descripcion_relato', 'pais_ocurrencia',
                     'codigo_dane_departamento', 'departamento_ocurrencia',
                     'codigo_dane_municipio', 'municipio_ocurrencia',
                     'pres_resp_paramilitares', 'pres_resp_grupos_posdesmov',
                     'pres_resp_agentes_estatales', 'pres_resp_guerr_farc',
                     'pres_resp_guerr_eln', 'pres_resp_guerr_otra',
                     'pres_resp_otro', 'situacion_actual_des']

# Verificar qué campos requeridos no están en el DataFrame
campos_no_encontrados = [
    campo for campo in campos_requeridos if campo not in cnmh.columns]

# Verificar si el DataFrame tiene todas las columnas requeridas
if len(campos_no_encontrados) > 0:
    raise ValueError(
        f"Campos requeridos no encontrados: {', '.join(campos_no_encontrados)}")

# Seleccionar y mantener solo las columnas requeridas
cnmh = cnmh[campos_requeridos]

# Eliminar duplicados basados en todas las columnas excepto id_registro
cnmh = cnmh.drop_duplicates(subset=campos_requeridos[1:], keep="first")

# Verificar que el número de filas sea igual al número de valores únicos en
# la columna codigo_unico_fuente
if len(cnmh) != len(cnmh['codigo_unico_fuente'].unique()):
    raise ValueError(
        "El número de filas no coincide con el número de valores únicos en la columna codigo_unico_fuente")

# 715
# Identificacion y eliminacion de Registros No Identificados

# Filtrar registros con nombres y apellidos no nulos, y al menos uno de
# los otros campos no nulos
cnmh_ident = cnmh[(~cnmh['primer_nombre'].isna() | ~cnmh['segundo_nombre'].isna()) &
                  (~cnmh['primer_apellido'].isna() | ~cnmh['segundo_apellido'].isna()) &
                  (~cnmh['documento'].isna() | ~cnmh['fecha_ocur_anio'].isna() |
                   ~cnmh['departamento_ocurrencia'].isna())]
# Filtrar registros que no cumplen con los criterios anteriores
cnmh_no_ident = cnmh[~cnmh['id_registro'].isin(cnmh_ident['id_registro'])]

# Obtener el número de filas en cada conjunto
nrow_cnmh = len(cnmh)
nrow_cnmh_ident = len(cnmh_ident)
nrow_cnmh_no_ident = len(cnmh_no_ident)

# Guardar el resultado en la base de datos de destino (SQL Server en este caso)
con2 = pyodbc.connect(driver="{SQL Server}",
                      server="172.16.10.10",
                      database="UNIVERSO_PDD",
                      uid="orquestacion.universo",
                      pwd="Ubpd2022*")

# Escribir cnmh_ident en la tabla orq_salida.CNMH_DF
cnmh_ident.to_sql(name="CNMH_DF", con=con2, if_exists="replace",
                  index=False, schema="orq_salida")

# Escribir cnmh_no_ident en la tabla orq_salida.CNMH_DF_PNI
cnmh_no_ident.to_sql(name="CNMH_DF_PNI", con=con2, if_exists="replace",
                     index=False, schema="orq_salida")

fecha_fin = time.time()

# Crear un diccionario con los datos del log
log = {
    'fecha_inicio': str(fecha_inicio),
    'fecha_fin': str(fecha_fin),
    'tiempo_ejecucion': fecha_fin - fecha_inicio,
    'n_casos': n_casos,  # Asegúrate de definir n_casos antes de esta línea
    'n_personas': n_personas,  # Asegúrate de definir n_personas antes de esta línea
    # Asegúrate de definir n_casos_sin_personas antes de esta línea
    'n_casos_sin_personas': n_casos_sin_personas,
    'filas_iniciales_cnmh': nrow_cnmh,
    'filas_final_cnmh': nrow_cnmh,
    'filas_cnmh_ident': nrow_cnmh_ident,
    'filas_cnmh_no_ident': nrow_cnmh_no_ident,
    # Asegúrate de definir n_duplicados antes de esta línea
    'n_duplicados': n_duplicados,
    # Asegúrate de definir n_indirectas antes de esta línea
    'n_indirectas': n_indirectas,
    'n_juridicas': n_juridicas  # Asegúrate de definir n_juridicas antes de esta línea
}

# Guardar el log en un archivo YAML
with open('log/resultado_cnmh.yaml', 'w') as file:
    yaml.dump(log, file)
# 776
