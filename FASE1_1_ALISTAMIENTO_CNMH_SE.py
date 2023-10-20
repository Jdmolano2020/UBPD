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
import pyodbc
import numpy as np
import time
import yaml

# creacion de las funciones requeridas
def generate_hash(df):
    return df.apply(lambda x: hashlib.sha1(str(x).encode()).hexdigest(),
                    axis=1)
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
    cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_SCHEMA")
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
connection_string = (
    r"Driver={SQL Server};"
    r"Server=172.16.10.10;"
    r"Database=UNIVERSO_PDD;"
    r"UID=orquestacion.universo;"
    r"PWD=Ubpd2022*;"
    r"Charset=latin1;"
)
con = pyodbc.connect(connection_string)
# Crear la consulta SQL
n_sample_p = f"top({n_sample})" if n_sample != "" else ""
query = f"SELECT {n_sample_p} * from [dbo].[V_CNMH_SE] personas left join [dbo].[V_CNMH_SE_C] casos on casos.IdCaso = personas.IdCaso"
# Ejecutar la consulta y cargar los datos en un DataFrame
cnmh = pd.read_sql(query, con)
# Obtener el número de filas en el DataFrame cnmh
nrow_cnmh = len(cnmh)
# Obtener el número de casos y personas
n_casos = pd.read_sql("select count(*) from [dbo].[V_CNMH_SE_C]", con).iloc[0, 0]
n_personas = pd.read_sql("select count(*) from [dbo].[V_CNMH_SE]", con).iloc[0, 0]
# Obtener el número de casos sin personas
n_casos_sin_personas = pd.read_sql("select count(*) from [dbo].[V_CNMH_SE_C] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_RU])", con).iloc[0, 0]
# Obtener los casos sin personas
casos_sin_personas = pd.read_sql("select * from [dbo].[V_CNMH_SE_C] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_SE_C])", con)
# Obtener el número de personas sin casos
n_personas_sin_casos = pd.read_sql("select count(*) from [dbo].[V_CNMH_SE] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_SE])", con).iloc[0, 0]
# Limpieza de nombres de columnas (clean_names no es necesario en pandas)
cnmh.columns = cnmh.columns.str.lower()
# Creación del ID único para cada registro
cnmh['id_registro'] = cnmh.apply(generate_hash, axis=1)
cnmh['tabla_origen'] = "CNMH_SE"
cnmh['codigo_unico_fuente'] = cnmh['id_caso'] + "_" + cnmh['identificador_caso'] + "_" + cnmh['id']
con.close()
# normalización de los campos de texto
# Lista de valores NA
na_values = ["ND", "AI", "[SIN INFORMACION]"]  # Puedes ampliar según sea necesario

# Lista de columnas que serán limpiadas
variables_limpieza = [
    "estado", "zon_id_lugar_del_hecho", "muninicio_caso", "depto_caso", "nacionalidad",
    "tipo_documento", "nombres_apellidos", "sobre_nombre_alias", "sexo",
    "orientacion_sexual", "descripcion_edad", "etnia", "descripcion_etnia",
    "discapacidad", "ocupacion_victima", "descripcion_otra_ocupacion_victima",
    "calidad_victima", "cargo_rango_funcionario_publico", "cargo_empleado_sector_privado",
    "tipo_poblacion_vulnerable", "descripcion_otro_tipo_poblacion_vulnerable",
    "organizacion_civil", "militante_politico", "descripcion_otro_militante_politico",
    "situacion_actual_victima", "observaciones_situacion_actual_de_la_victima",
    "circustancia_muerte_en_cautiverio",
    "descripcion_otra_circustancia_muerte_en_cautiverio", "tipo_liberacion",
    "descripcion_otro_tipo_liberacion", "dias_cautiverio", "no_veces_secuestrado",
    "hechos_simultaneos_durante_periodo", "otro_hecho_simultaneos_durante_periodo",
    "grupo", "descripcion_grupo", "espeficicacion_presunto_responsable",
    "observaciones_grupo_armado1", "rango_fuerzas_armadas",
    "descripcion_rango_fuerzas_armadas_estatales", "rango_grupo_armado",
    "descripcion_rango_grupo_armado", "zon_id_lugar_del_hecho_2",
    "muninicio_caso_2", "depto_caso_2", "region", "cabecera_municipal", "comuna",
    "barrio", "area_rural", "corregimiento", "vereda", "codigo_centro_poblado",
    "centro_poblado", "tipo_centro_poblado", "sitio",
    "territorio_colectivo", "resguardo", "modalidad", "descripcion_de_la_modalidad",
    "modalidad_de_secuestro", "tipo_secuestro", "finalidad_del_secuestro",
    "descripcion_otra_finalidad", "exigencia_para_la_liberacion",
    "descripcion_otra_exigencia", "porte_listas", "ingreso_vivienda_finca",
    "encapuchados", "perpetrador_identificado", "presunto_reponsable",
    "descripcion_presunto_responsable1", "espeficicacion_presunto_responsable_2",
    "observaciones_presunto_responsable", "abandono_despojo_forzado_tierras",
    "amenaza_intimidacion", "ataque_contra_mision_medica",
    "confinamiento_restriccion_movilidad", "desplazamiento_forzado", "extorsion",
    "lesionados_civiles", "pillaje", "tortura", "violencia_basada_genero",
    "otro_hecho_simultaneo", "total_civiles", "total_combatientes",
    "total_civiles_combatientes", "grafitis_letreros", "vinculos_familiares",
    "mujeres_embarazadas", "descripcion_del_caso", "usuario", "estado_2",
    "tipo_caso", "caso_maestro"
]


# Limpieza y manipulación de datos
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(
    lambda x: x.str.strip().str.upper())
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(lambda x: clean_func(x, na_values))
# homologación de estructura, formato y contenido
# Datos sobre los hechos
# lugar de ocurrencia
# Definir las variables de limpieza DANE
variables_limpieza_dane = ["departamento_ocurrencia", "municipio_ocurrencia"]
# Limpieza y manipulación de datos en 'dane'
dane = dane.apply(lambda col: col.str.upper() if col.name in variables_limpieza_dane else col)
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].applymap(lambda x: np.nan if x in na_values else x)
# Obtener valores únicos del departamento en 'dane'
unique_departamentos = dane['codigo_dane_departamento'].unique()
# Realizar la manipulación de 'cnmh'
cnmh['pais_ocurrencia'] = np.where(cnmh['depto_caso'] == 'EXTERIOR', np.nan, 'COLOMBIA')
# Mapear valores en 'depto_caso' y 'muninicio_caso'
mapeo_deptos_mun = {
    'SIN INFORMACION': np.nan,
    '': np.nan,
    'EXTERIOR': np.nan,
    'CUCUTA': 'SAN JOSE DE CUCUTA',
    'ARMERO GUAYABAL': 'ARMERO',
    'TOLU VIEJO': 'SAN JOSE DE TOLUVIEJO',
    'CUASPUD': 'CUASPUD CARLOSAMA',
    'BARRANCO MINAS': 'BARRANCOMINAS',
    'MOMPOS': 'SANTA CRUZ DE MOMPOX',
    'BELEN DE BAJIRA': 'RIOSUCIO',
    'PIENDAMO': 'PIENDAMO TUNIA',
    'SOTARA': 'SOTARA PAISPAMBA',
    'FRONTERA VENEZUELA': np.nan,
    'FRONTERA PANAMA': np.nan,
    'FRONTERA BRASIL': np.nan,
    'GUICAN': 'GUICAN DE LA SIERRA',
    'FRONTERA': np.nan,
    'FRONTERA ECUADOR': np.nan,
    'YACARATE': 'YAVARATE',
    'PAPUNAUA': 'PAPUNAHUA'
}

cnmh['depto_caso'] = cnmh['depto_caso'].map(mapeo_deptos_mun)
cnmh['muninicio_caso'] = cnmh['muninicio_caso'].map(mapeo_deptos_mun)
# Verificar valores en 'depto_caso' y 'muninicio_caso' contra 'dane'
cnmh = cnmh[cnmh['depto_caso'].isin(unique_departamentos) & cnmh['muninicio_caso'].isin(dane['municipio_ocurrencia'])]
# Realizar la unión (left join) con 'dane' por departamento y municipio
cnmh = pd.merge(cnmh, dane, left_on=['depto_caso', 'muninicio_caso'], right_on=['codigo_dane_departamento', 'municipio_ocurrencia'], how='left')

# fecha de ocurrencia 
# Definir meses y días válidos
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
dias = ["01", "02", "03", "04", "05", "06", "07", "08", "09"] + list(map(str, range(10, 32)))
# Realizar transformaciones de fechas y limpieza
cnmh['mesh'] = cnmh['mesh'].fillna("0").apply(lambda x: "0" + str(int(x)) if int(x) < 10 else str(int(x)))
cnmh['diah'] = cnmh['diah'].apply(lambda x: "0" + str(int(x)) if int(x) < 10 else str(int(x)))
cnmh['annoh'] = cnmh['annoh'].apply(lambda x: "1900" if int(x) < 1000 else x)
cnmh['fecha_hecho_0'] = cnmh['diah'] + "-" + cnmh['mesh'] + "-" + cnmh['annoh']
cnmh['fecha_hecho'] = pd.to_datetime(cnmh['fecha_hecho_0'], format="%d-%m-%Y")
cnmh['fecha_desaparicion'] = cnmh['fecha_hecho'].dt.strftime("%Y%m%d")
cnmh['fecha_desaparicion_dtf'] = cnmh['fecha_hecho']
cnmh['fecha_ocur_anio'] = cnmh['annoh']
cnmh['fecha_ocur_mes'] = cnmh['mesh']
cnmh['fecha_ocur_dia'] = cnmh['diah']
# Limpieza de fechas
cnmh['fecha_ocur_anio'] = np.where(cnmh['fecha_ocur_anio'].astype(int) < 1900, np.nan, cnmh['fecha_ocur_anio'])
cnmh['fecha_ocur_mes'] = np.where(cnmh['fecha_ocur_mes'] == "00", np.nan, cnmh['fecha_ocur_mes'])
cnmh['fecha_ocur_dia'] = np.where(cnmh['fecha_ocur_dia'] == "00", np.nan, cnmh['fecha_ocur_dia'])
# Verificación de fechas
cnmh = cnmh[cnmh['fecha_ocur_anio'].isin(map(str, range(1900, 2023)))]
cnmh = cnmh[cnmh['fecha_ocur_mes'].isin(meses)]
cnmh = cnmh[cnmh['fecha_ocur_dia'].isin(dias)]
#presuntos responsables
# Lista de valores a considerar como "sin información" para espeficicacion_presunto_responsable
sin_informacion_actores = ['NO DEFINIDO', '', 'NO IDENTIFICA', 'SIN INFORMACIÓN CONFLICTO ARMADO',
                           'NO IDENTIFICA CONFLICTO ARMADO', 'OTROS VIOLENCIA GENERALIZADA',
                           'SIN INFORMACIÓN', 'NO IDENTIFICA RELACIÓN CERCANA Y SUFICIENTE',
                           'NO IDENTIFICA VIOLENCIA GENERALIZADA',
                           'NO IDENTIFICA RELACIÓN CERCANA Y SUFICIENTE ',
                           'SIN INFORMACIÓN RELACIÓN CERCANA Y SUFICIENTE',
                           'NO IDENTIFICA CONFLICTO ARMADO', 'CONFLICTO ARMADO',
                           '0', 'SIN INFORMACIÓN VIOLENCIA GENERALIZADA', '', "NO IDENTIFICADO",
                           'DESCONOCIDO']
# Realizar transformaciones y limpieza
cnmh['espeficicacion_presunto_responsable'] = np.where(cnmh['espeficicacion_presunto_responsable'].isin(sin_informacion_actores), np.nan, cnmh['espeficicacion_presunto_responsable'])
# Lista de columnas relacionadas con los presuntos responsables
responsables = ["perpetrador_identificado", "presunto_reponsable",
                "descripcion_presunto_responsable1", "espeficicacion_presunto_responsable_2",
                "observaciones_presunto_responsable1"]
# Crear columnas para cada tipo de presunto responsable y asignar valores 0 o 1
cnmh['pres_resp_agentes_estatales'] = np.where((cnmh['grupo'].str.contains("FUERZA PÚBLICA|AGENTE DEL ESTADO|GENTE EXTRANJERO", flags=re.IGNORECASE)) | (cnmh['presunto_reponsable'].str.contains("AGENTE DEL ESTADO", flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_grupos_posdesmov'] = np.where((cnmh['grupo'].str.contains("POSDESMOVILIZACION", flags=re.IGNORECASE)) | (cnmh['presunto_reponsable'].str.contains("POSDESMOVILIZACION", flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_paramilitares'] = np.where((cnmh['grupo'].str.contains("AUTODEF|PARAMI|AUC|A.U.C|PARAMILITAR", flags=re.IGNORECASE)) | (cnmh['presunto_reponsable'].str.contains("PARAMILITAR", flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_guerr_eln'] = np.where((cnmh['grupo'].str.contains("ELN|GUERRILLA", flags=re.IGNORECASE)) & (cnmh['descripcion_grupo'].str.contains("ELN", flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_guerr_farc'] = np.where((cnmh['grupo'].str.contains("FARC|GUERRILLA", flags=re.IGNORECASE)) & (cnmh['descripcion_grupo'].str.contains("FARC", flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_guerr_otra'] = np.where((cnmh['grupo'].str.contains("GRUPOS GUERRILLEROS", flags=re.IGNORECASE) & ~(cnmh['grupo'].str.contains("FARC|ELN", flags=re.IGNORECASE))) |
                                        (cnmh['presunto_reponsable'].str.contains("GUERRILLA", flags=re.IGNORECASE) & ~(cnmh['descripcion_grupo'].str.contains("FARC|ELN", flags=re.IGNORECASE))) |
                                        (cnmh['descripcion_presunto_responsable1'].str.contains("EPL", flags=re.IGNORECASE)), 1, 0)
# Calcular columna 'pres_resp_otro' basada en la suma de las columnas anteriores

cnmh['pres_resp_otro'] = np.where(
    (cnmh['grupo'].str.contains("GRUPO ARMADO NO IDENTIFICADO", flags=re.IGNORECASE))  |
    (cnmh['presunto_reponsable'].str.contains("GRUPO ARMADO NO", flags=re.IGNORECASE))  |
    (cnmh['presunto_reponsable'].str.contains("BANDOLERISMO", flags=re.IGNORECASE))  |
    (cnmh['presunto_reponsable'].str.contains("AGENTE EXTRANJERO", flags=re.IGNORECASE))  |
    (cnmh['presunto_reponsable'].str.contains("CRIMEN ORGANIZADO", flags=re.IGNORECASE))  |
    (cnmh['presunto_reponsable'].str.contains("OTRO CUAL", flags=re.IGNORECASE)) , 1, 0)

# Mostrar la suma de presuntos responsables en cada categoría
sum_pres_resp = cnmh[['pres_resp_agentes_estatales', 'pres_resp_grupos_posdesmov', 'pres_resp_paramilitares',
                      'pres_resp_guerr_eln', 'pres_resp_guerr_farc', 'pres_resp_guerr_otra', 'pres_resp_otro']].sum()
# tipo de hecho
# Lista de otros hechos
otros_hechos = ["OCULTAMIENTO DE CADAVER", "OCULTAMIENTO DEL CADAVER", "VENTA DEL SECUESTRADO"]
# Realizar transformaciones y cálculos
cnmh['TH_DF'] = np.where(cnmh['otro_hecho_simultaneo'].str.contains("DESAPARICION FORZADA", flags=re.IGNORECASE), 1, 0)
cnmh['TH_SE'] = 1  # Columna TH_SE siempre se establece en 1
cnmh['TH_RU'] = np.where(cnmh['otro_hecho_simultaneo'].str.contains("DESMOVILIZADO", flags=re.IGNORECASE), 1, 0)
cnmh['TH_OTRO'] = np.where(cnmh['otro_hecho_simultaneo'].isin(otros_hechos), 1, 0)
# Mostrar la tabla de frecuencias de las columnas TH_DF, TH_SE, TH_RU, TH_OTRO
table_TH_DF = cnmh['TH_DF'].value_counts()
table_TH_SE = cnmh['TH_SE'].value_counts()
table_TH_RU = cnmh['TH_RU'].value_counts()
table_TH_OTRO = cnmh['TH_OTRO'].value_counts()
#relato 
# Limpiar la descripción del caso
cnmh['descripcion_relato'] = cnmh['descripcion_del_caso'].str.strip()
# Mapear la situación actual de la víctima
cnmh['situacion_actual_des'] = cnmh['situacion_actual_victima'].map({
    "APARECIO MUERTO": "Apareció Muerto",
    "SIGUE DESAPAR": "Continúa desaparecido",
    "VIVA": "Apareció Vivo",
    "MUERTA": "Apareció Muerto",
    "DESCONOCIDA": None,
    "APARECIO VIVO": "Apareció Vivo",
    "DESAPARECIDO FORZADO": "Continúa desaparecido"
})
# Datos sobre las personas dadas por desaparecidas
# nombres y apellidos
# Limpiar y procesar el nombre completo
cnmh['nombre_completo'] = cnmh['nombres_apellidos'].str.strip()
cnmh['nombre_completo'] = np.where(cnmh['nombre_completo'] == "PERSONA SIN IDENTIFICAR", None, cnmh['nombre_completo'])
cnmh['nombre_completo'] = cnmh['nombre_completo'].str.replace(" NA ", "", regex=True)
# Definir listado de preposiciones
preposiciones = ["DE", "DEL", "DE LAS", "DE LA", "DE LOS", "VAN", "LA", "VIUDA DE", "VIUDA", "SAN", "DA"]
# Separar el nombre completo en primer_nombre, segundo_nombre, primer_apellido y segundo_apellido
cnmh[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']] = cnmh['nombre_completo'].str.split(' ', n=3, expand=True)
# Eliminar preposiciones en los apellidos
cnmh['primer_apellido'] = cnmh['primer_apellido'].apply(lambda x: x if x not in preposiciones else None)
cnmh['segundo_apellido'] = cnmh['segundo_apellido'].apply(lambda x: x if x not in preposiciones else None)

preposiciones = ["DE", "DEL", "DE LAS", "DE LA", "DE LOS", "VAN", "LA", "VIUDA DE", "VIUDA", "SAN", "DA"]

for i, row in cnmh.iterrows():
    nombre_completo = row['nombre_completo']
    tokens = nombre_completo.split()   
    # Elimina preposiciones
    tokens = [token for token in tokens if token not in preposiciones]
    primer_nombre, segundo_nombre, primer_apellido, segundo_apellido = None, None, None, None
    
    while len(tokens) > 4:
        for k in range(len(tokens)):
            if k == 1:
                tokens[k + 1] = tokens[k] + ' ' + tokens[k + 1]
                del tokens[k]
    
    if len(tokens) == 4:
        primer_nombre, segundo_nombre, primer_apellido, segundo_apellido = tokens
    elif len(tokens) == 3:
        primer_nombre, primer_apellido, segundo_apellido = tokens
    elif len(tokens) == 2:
        primer_nombre, primer_apellido = tokens
    
    # Asigna los valores resultantes
    cnmh.at[i, 'primer_nombre'] = primer_nombre
    cnmh.at[i, 'segundo_nombre'] = segundo_nombre
    cnmh.at[i, 'primer_apellido'] = primer_apellido
    cnmh.at[i, 'segundo_apellido'] = segundo_apellido

# Rellena los valores NaN con None
cnmh = cnmh.fillna({'primer_nombre': None, 'segundo_nombre': None, 'primer_apellido': None, 'segundo_apellido': None})

# Aplicar la función para limpiar tipo_documento
cnmh['tipo_documento'] = cnmh['tipo_documento'].str.strip().str.upper()

# Aplicar la función para eliminar preposiciones en nombres y apellidos
cnmh['segundo_nombre'] = np.where(cnmh['segundo_nombre'].isin(preposiciones),
                                  cnmh['segundo_nombre'] + ' ' + cnmh['primer_apellido'],
                                  cnmh['segundo_nombre'])
cnmh['primer_apellido'] = np.where(cnmh['segundo_nombre'].isin(preposiciones),
                                   cnmh['segundo_apellido'],
                                   cnmh['primer_apellido'])
cnmh['segundo_apellido'] = np.where(cnmh['segundo_nombre'].isin(preposiciones),
                                    np.nan,
                                    cnmh['segundo_apellido'])

# Eliminar nombres y apellidos cuando solo se registra la letra inicial
names_columns = ['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']
cnmh[names_columns] = cnmh[names_columns].apply(lambda col: col.apply(lambda x: None if len(str(x)) == 1 else x))
# Reemplazar primer apellido por segundo apellido cuando el primero está vacío
cnmh['primer_apellido'].fillna(cnmh['segundo_apellido'], inplace=True)
# Documento
# Aplicar str.strip() y str.upper() para limpiar y convertir a mayúsculas
cnmh['documento'] = cnmh['numero_documento'].str.strip().str.upper()
# Eliminar símbolos y caracteres especiales
cnmh['documento'] = cnmh['documento'].str.replace(r'[^A-Z0-9]', '', regex=True)
# Eliminar cadenas de texto sin números y documentos igual a '0'
cnmh['documento'] = np.where(cnmh['documento'] == '', None, cnmh['documento'])
cnmh['documento_solo_cadena_texto'] = np.where(cnmh['documento'].str.contains(r'[0-9]') == False, 0, 1)
# Convertir documento a NA si no contiene números o es igual a '0'
cnmh['documento'] = np.where((cnmh['documento'].str.contains(r'[0-9]') == False) | (cnmh['documento'] == '0'), None, cnmh['documento'])
# implementacion de las reglas de la registraduria
# Crear columnas con las condiciones especificadas
cnmh['documento_CC_TI_no_numerico'] = np.where(cnmh['documento'].str.contains(r'[A-Z]') & (cnmh['tipo_documento'].isin(["TARJETA DE IDENTIDAD", "CEDULA DE CIUDADANIA"])), 1, 0)
cnmh['documento'] = np.where(cnmh['documento'].str.contains(r'[A-Z]') & (cnmh['tipo_documento'].isin(["TARJETA DE IDENTIDAD", "CEDULA DE CIUDADANIA"])), None, cnmh['documento'])
cnmh['documento_CC_TI_mayor1KM'] = np.where((cnmh['documento'].str.len() >= 10) & (cnmh['documento'].astype(float) <= 1000000000) & (cnmh['tipo_documento'].isin(["TARJETA DE IDENTIDAD", "CEDULA DE CIUDADANIA"])), 1, 0)
cnmh['documento_TI_10_11_caract'] = np.where(~((cnmh['documento'].str.len() == 10) | (cnmh['documento'].str.len() == 11)) & (cnmh['tipo_documento'] == "TARJETA DE IDENTIDAD"), 1, 0)
cnmh['documento_TI_11_caract_fecha_nac'] = np.where(~((cnmh['documento'].str.slice(0, 6) == cnmh['fecha_nacimiento'].dt.strftime('%y%m%d')) & (cnmh['documento'].str.len() == 11) & (cnmh['tipo_documento'] == "TARJETA DE IDENTIDAD")), 1, 0)
cnmh['documento_CC_hombre_consistente'] = np.where(~(cnmh['documento'].astype(float).isin(range(1, 20000000)) | cnmh['documento'].astype(float).isin(range(70000000, 100000000))) & (cnmh['documento'].str.len().isin(range(4, 9))) & (cnmh['tipo_documento'] == "CEDULA DE CIUDADANIA") & (cnmh['sexo'] == "H"), 1, 0)
cnmh['documento_CC_mujer_consistente'] = np.where(~(cnmh['documento'].astype(float).isin(range(20000000, 70000000))) & (cnmh['documento'].str.len() == 8) & (cnmh['tipo_documento'] == "CEDULA DE CIUDADANIA") & (cnmh['sexo'] == "M"), 1, 0)
cnmh['documento_CC_mujer_consistente2'] = np.where(~(cnmh['documento'].str.len().isin([8, 10])) & (cnmh['tipo_documento'] == "CEDULA DE CIUDADANIA") & (cnmh['sexo'] == "M"), 1, 0)
cnmh['documento_CC_hombre_consistente2'] = np.where(~(cnmh['documento'].str.len().isin([4, 5, 6, 7, 8, 10])) & (cnmh['tipo_documento'] == "CEDULA DE CIUDADANIA") & (cnmh['sexo'] == "H"), 1, 0)
cnmh['documento_TI_mujer_consistente'] = np.where(~(cnmh['documento'].str.slice(9, 10).astype(int).isin(range(1, 10, 2))) & (cnmh['documento'].str.len() == 11) & (cnmh['tipo_documento'] == "TARJETA DE IDENTIDAD") & (cnmh['sexo'] == "M"), 1, 0)
cnmh['documento_TI_hombre_consistente'] = np.where(~(cnmh['documento'].str.slice(9, 10).astype(int).isin(range(0, 11, 2))) & (cnmh['documento'].str.len() == 11) & (cnmh['tipo_documento'] == "TARJETA DE IDENTIDAD") & (cnmh['sexo'] == "H"), 1, 0)
# Crear un DataFrame resumen con las filas que cumplen al menos una condición
resumen_documento = cnmh[cnmh.filter(like='documento_').sum(axis=1) > 0][['documento_CC_TI_no_numerico', 'documento', 'sexo', 'fecha_nacimiento']]
# Guardar el DataFrame en un archivo CSV con formato adecuado
resumen_documento.to_csv("log/revision_documentos_cnmh_se.csv", sep=";", index=False)
# Contar la cantidad de filas en resumen_documento
cantidad_filas = len(resumen_documento)
# Resumen de las columnas de documento_
resumen_documento.filter(like='documento_').sum()
# sexo
cnmh['sexo'] = cnmh['sexo'].apply(lambda x: 'HOMBRE' if x == 'H' else ('MUJER' if x == 'M' else 'NO INFORMA'))
# Para la columna 'etnia'
cnmh['iden_pertenenciaetnica'] = cnmh['etnia'].apply(lambda x: 'NINGUNA' if x == '' else ('NARP' if x in ["AFROCOLOMBIANO", "PALENQUERO", "RAIZAL"] else ('INDIGENA' if x == "INDIGENA" else ('RROM' if x == "ROM" else None))))
# Opcional: Reemplazar valores NA por 'NO INFORMA'
cnmh['iden_pertenenciaetnica'].fillna('NO INFORMA', inplace=True)
# fecha de nacimiento
# Copiar la columna 'fecha_nacimiento' a 'fecha_nacimiento_original'
cnmh['fecha_nacimiento_original'] = cnmh['fecha_nacimiento']
# Convertir 'fecha_nacimiento' a formato de año, mes y día separados
cnmh['anio_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'], format='%Y-%m-%d').dt.year.astype(str)
cnmh['mes_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'], format='%Y-%m-%d').dt.month.map("{:02}".format).astype(str)
cnmh['dia_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'], format='%Y-%m-%d').dt.day.map("{:02}".format).astype(str)
# Convertir 'fecha_nacimiento_original' a tipo datetime
cnmh['fecha_nacimiento_dtf'] = pd.to_datetime(cnmh['fecha_nacimiento_original'], format='%Y-%m-%d')
# Formatear 'fecha_nacimiento' como YYYYMMDD
cnmh['fecha_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'], format='%Y-%m-%d').dt.strftime('%Y%m%d')
# Filtrar años de nacimiento fuera del rango 1905-2022 y reemplazarlos con NA
cnmh['anio_nacimiento'] = cnmh['anio_nacimiento'].apply(lambda x: x if 1905 <= int(x) <= 2022 else None)
# Asegurarse de que 'anio_nacimiento', 'mes_nacimiento' y 'dia_nacimiento' estén en los conjuntos correctos
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
dias = ["01", "02", "03", "04", "05", "06", "07", "08", "09"] + [str(i) for i in range(10, 32)]
cnmh = cnmh[cnmh['anio_nacimiento'].isin([str(i) for i in range(1900, 2023)])]
cnmh = cnmh[cnmh['mes_nacimiento'].isin(meses)]
cnmh = cnmh[cnmh['dia_nacimiento'].isin(dias)]
# edad
# Calcula la edad y maneja los casos especiales
cnmh['edad'] = np.where((cnmh['fecha_ocur_anio'].isna() | cnmh['anio_nacimiento'].isna()), np.nan,
                        cnmh['fecha_ocur_anio'].astype(int) - cnmh['anio_nacimiento'].astype(int))

# Reemplaza edades fuera del rango 1-100 con NaN
cnmh['edad'] = np.where((cnmh['edad'] > 100) | (cnmh['edad'] < 1), np.nan, cnmh['edad'])
# Asegura que la columna 'edad' esté dentro del rango 1-100 (permitiendo valores NaN)
cnmh = cnmh[(cnmh['edad'] >= 1) | (cnmh['edad'] <= 100) | (cnmh['edad'].isna())]
# Identificación de filas unicas
# Contar filas originales
n_1 = len(cnmh)
# Eliminar duplicados
cnmh = cnmh.drop_duplicates()
# Contar duplicados eliminados
n_duplicados = n_1 - len(cnmh)
# Excluir víctimas indirectas
n_2 = len(cnmh)
# Contar víctimas indirectas excluidas
n_indirectas = n_2 - len(cnmh)
# Excluir personas jurídicas
n_3 = len(cnmh)
# Contar personas jurídicas excluidas
n_juridicas = n_3 - len(cnmh)
# Crear una lista con los campos requeridos para la comparación de nombres
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
# Verificar si los campos requeridos están presentes en el DataFrame
campos_faltantes = [campo for campo in campos_requeridos if campo not in cnmh.columns]
# Seleccionar solo los campos requeridos
cnmh = cnmh[campos_requeridos]
# Eliminar duplicados basados en 'id_registro'
cnmh = cnmh.drop_duplicates(subset=['id_registro'], keep='first')
# Verificar que el número de registros sea igual al número de códigos únicos de fuente
cnmh_codigos_unicos = cnmh['codigo_unico_fuente'].nunique()
# Identificación y eliminacin de Registros No Identificados
# Filtrar registros con información de identificación
cnmh_ident = cnmh[(~cnmh['primer_nombre'].isna() | ~cnmh['segundo_nombre'].isna()) &
                  (~cnmh['primer_apellido'].isna() | ~cnmh['segundo_apellido'].isna()) &
                  (~cnmh['documento'].isna() | ~cnmh['fecha_ocur_anio'].isna() |
                   ~cnmh['fecha_ocur_anio'].isna() | ~cnmh['departamento_ocurrencia'].isna())]
# Filtrar registros sin información de identificación
cnmh_no_ident = cnmh[~cnmh['id_registro'].isin(cnmh_ident['id_registro'])]
nrow_cnmh = len(cnmh)
nrow_cnmh_ident = len(cnmh_ident)
nrow_cnmh_no_ident = len(cnmh_no_ident)
# Guardar resultados en la base de datos de destino
server = '172.16.10.10'
database = 'UNIVERSO_PDD'
username = 'orquestacion.universo'
password = 'Ubpd2022*'

# Crear la conexión a la base de datos SQL Server
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
# Escribir los DataFrames en las tablas correspondientes en la base de datos
cnmh_ident.to_sql('CNMH_SE', con=conn, if_exists='replace', index=False)
cnmh_no_ident.to_sql('CNMH_SE_PNI', con=conn, if_exists='replace', index=False)
# Cerrar la conexión a la base de datos
conn.close()
# Registrar información de ejecución
import yaml
from datetime import datetime

fecha_fin = datetime.now()

log = {
    "fecha_inicio": str(fecha_inicio),
    "fecha_fin": str(fecha_fin),
    "tiempo_ejecucion": str(fecha_fin - fecha_inicio),
    "n_casos": n_casos,  # Debes definir n_casos antes de esta parte
    "n_personas": n_personas,  # Debes definir n_personas antes de esta parte
    "n_casos_sin_personas": n_casos_sin_personas,
    'filas_iniciales_cnmh': nrow_cnmh,
    'filas_final_cnmh': nrow_cnmh,
    'filas_cnmh_ident': nrow_cnmh_ident,
    'filas_cnmh_no_ident': nrow_cnmh_no_ident,
    'n_duplicados': n_duplicados,
    'n_indirectas': n_indirectas,
    'n_juridicas': n_juridicas
}

with open('log/resultado_cnmh_se.yaml', 'w') as file:
    yaml.dump(log, file)
