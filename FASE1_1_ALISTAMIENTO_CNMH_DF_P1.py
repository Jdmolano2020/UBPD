import sys
import os
import pandas as pd
import hashlib
import re
from datetime import datetime
from sqlalchemy import create_engine
import numpy as np
import yaml

# creacion de las funciones requeridas
def funcion_hash(row):
    return hashlib.sha1(str(row).encode()).hexdigest()

# Función de limpieza
def clean_func(x, na_values):
    if x is None:
        x = ' '
    x = x.str.replace("Á", "A")
    x = x.str.replace("É", "E")
    x = x.str.replace("Í", "I")
    x = x.str.replace("Ó", "O")
    x = x.str.replace("Ú", "U")
    x = x.str.replace("Ü", "U")
    x = x.str.replace("Ñ", "N")
    # Transliterar a ASCII y convertir a mayúsculas
    x1 = x.str.encode('ascii', 'ignore').str.decode('ascii').str.upper()
    # Quitar espacios al inicio y al final
    x2 = x1.str.strip()
    # Dejar solo caracteres alfanuméricos y espacios
    x3 = x2.str.replace(r'[^A-Z0-9 ]', ' ', regex=True)
    # Quitar espacios adicionales
    x4 = x3.str.replace(r'\s+', ' ', regex=True)
    return x4

# Define una función para limpiar nombres y apellidos
def limpiar_nombres_apellidos(nombre_completo):
    if nombre_completo is None:
        return None, None, None, None
    
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
# Establecimiento de la conexion a la base de datos
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
db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)

n_sample_p = ""
if n_sample:
    n_sample_p = f"TOP ({n_sample})"

# Consulta SQL para obtener datos de CNMH_DF
query = """
    EXECUTE [dbo].[CONSULTA_V_CNMH_DF]
"""
cnmh = pd.read_sql(query, engine)

# Consulta SQL para obtener el número de casos
query_casos = "SELECT COUNT(*) FROM [dbo].[V_CNMH_DF_C]"
n_casos = pd.read_sql(query_casos, engine).iloc[0, 0]

# Consulta SQL para obtener el número de personas
query_personas = "SELECT COUNT(*) FROM [dbo].[V_CNMH_DF]"
n_personas = pd.read_sql(query_personas, engine).iloc[0, 0]

# Consulta SQL para obtener el número de casos sin personas
query_casos_sin_personas = """
    SELECT COUNT(*) FROM [dbo].[V_CNMH_DF_C]
    WHERE IdCaso NOT IN (SELECT IdCaso FROM [dbo].[V_CNMH_DF])
"""
n_casos_sin_personas = pd.read_sql(query_casos_sin_personas, engine).iloc[0, 0]

# Consulta SQL para obtener casos sin personas
query_casos_sin_personas_data = """
    SELECT * FROM [dbo].[V_CNMH_DF_C]
    WHERE IdCaso NOT IN (SELECT IdCaso FROM [dbo].[V_CNMH_DF])
"""
casos_sin_personas = pd.read_sql(query_casos_sin_personas_data, engine)


# creacion del hash unico para cada registro
# Utiliza clean_names() para convertir los nombres de las columnas a minúsculas
# y reemplazar espacios con guiones bajos
cnmh.columns = cnmh.columns.str.lower().str.replace(' ', '_')
# Utiliza distinct() para eliminar filas duplicadas
cnmh = cnmh.drop_duplicates()
# Obtener el número de filas en el DataFrame cnmh
nrow_cnmh = len(cnmh)
# Obtener el número de filas únicas basadas en la concatenación de columnas
filas_unicas = cnmh['idcaso'] + cnmh['id'] + cnmh['identificadorcaso']
num_filas_unicas = len(filas_unicas.unique())
# Obtener la longitud de la columna 'id'
longitud_columna_id = len(cnmh['id'])
# Crear una nueva columna 'id_registro' con el hash
cnmh['id_registro'] = cnmh.apply(funcion_hash, axis=1)
# Crear una nueva columna 'tabla_origen' con el valor "CNMH_DF"
cnmh['tabla_origen'] = "CNMH_DF"
# Crear una nueva columna 'codigo_unico_fuente' con la
# normalizacion de los campos de texto

# Definir una lista de valores a reemplazar
na_values = {
    "SIN INFORMACION": np.nan,
    "ND": np.nan,
    "AI": np.nan
}

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

# Aplicar las transformaciones a las columnas de tipo 'str'
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(
    lambda x: x.str.strip().str.upper())
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(
    lambda x: clean_func(x, na_values))
cnmh[variables_limpieza] = cnmh[variables_limpieza].replace(na_values)
# homologacion de estructura, formato y contenido
# Datos sobre los hechos
# lugar de ocurrencia
# Lista de variables a limpiar
variables_limpieza_dane = ["departamento_ocurrencia", "municipio_ocurrencia"]
# Aplicar transformaciones a las columnas de tipo 'str'
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(
    lambda x: x.str.strip().str.upper())
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(
    lambda x: clean_func(x, na_values))
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].replace(na_values)
# Obtener valores únicos de 'codigo_dane_departamento'

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

# A continuación, aseguramos que los valores de 'depto_caso' y 'municipio_caso'
cnmh["depto_caso"] = cnmh["depto_caso"].replace(mapeo_depto)
cnmh["municipio_caso"] = cnmh["municipio_caso"].replace(mapeo_depto)

# Realizar la unión (left join) con "dane"
cnmh = pd.merge(cnmh, dane, how='left', left_on=['depto_caso', 'municipio_caso'],
                right_on=['departamento_ocurrencia', 'municipio_ocurrencia'])
nrow_cnmh = len(cnmh)
print("Registros despues left dane depto muni:",nrow_cnmh)
cnmh_ndp = cnmh[cnmh["departamento_ocurrencia"].isna()]
# fecha de ocurrencia
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
cnmh['fecha_hecho'] = pd.to_datetime(cnmh['fecha_hecho_0'], format="%d-%m-%Y", errors='coerce')
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
cnmh_ocur = cnmh[cnmh["fecha_ocur_anio"].astype(str).isin(map(str, range(1900, 2023))) &
    cnmh["fecha_ocur_mes"].astype(str).isin(meses) &
    cnmh["fecha_ocur_dia"].astype(str).isin(dias) ]


cnmh_nocur = cnmh.merge(cnmh_ocur, on=['id_registro'], how='left', indicator=True)
cnmh_nocur = cnmh_nocur[cnmh_nocur['_merge'] == 'left_only']
cnmh_nocur = cnmh_nocur.drop(columns=['_merge'])
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
cnmh['pres_resp_agentes_estatales'] = np.where(
    (cnmh['grupo'].str.contains("FUERZA PUBLICA") |
     cnmh['grupo'].str.contains("AGENTE DEL ESTADO") |
     cnmh['grupo'].str.contains("GENTE EXTRANJERO") ), 1, 0)

cnmh['pres_resp_grupos_posdesmov'] = np.where(
    (cnmh['grupo'].str.contains("POSDESMOVILIZACION") ), 1, 0)

cnmh['pres_resp_paramilitares'] = np.where(
    (cnmh['grupo'].str.contains("AUTODEF") |
     cnmh['grupo'].str.contains("PARAMI") |
     cnmh['grupo'].str.contains("AUC") |
     cnmh['grupo'].str.contains("PARAMILITAR") ), 1, 0)

cnmh['pres_resp_guerr_eln'] = np.where(
    (cnmh['grupo'].str.contains("ELN") |
     (cnmh['grupo'].str.contains("GUERRILLA") & 
      cnmh["descripcion_grupo"].str.contains("ELN"))), 1, 0)

cnmh['pres_resp_guerr_farc'] = np.where(
    (cnmh['grupo'].str.contains("FARC") |
     (cnmh['grupo'].str.contains("GUERRILLA") &
      cnmh['descripcion_grupo'].str.contains("FARC"))), 1, 0
)

cnmh['pres_resp_guerr_otra'] = np.where(
    ((cnmh['grupo'].str.contains("GRUPOS GUERRILLEROS") &
      ~(cnmh["grupo"].str.contains("FARC|ELN")) & 
      ~(cnmh["descripcion_grupo"].str.contains("FARC|ELN")))|
     (cnmh['grupo'].str.contains("GUERRILLA") &
      ~(cnmh["grupo"].str.contains("FARC|ELN")) &
      ~(cnmh['descripcion_grupo'].str.contains("FARC|ELN"))) |
     (cnmh['grupo'].str.contains("GUERRILLA") &
      ~(cnmh["descripcion_grupo"].str.contains("FARC|ELN"))) |
     (cnmh['grupo'].str.contains("EPL")) |
     (cnmh['grupo'].str.contains("GUERRILLA") &
       cnmh['descripcion_grupo'].str.contains("EPL"))), 1, 0)
     
# Calcular la columna "tmp" y actualizar "pres_resp_otro"
# Revisar la sumatoria de las columnas con prefijo "pres_resp_"
cnmh["tmp"] = cnmh[pres_resp_columns].sum(axis=1)
cnmh["pres_resp_otro"] = cnmh["tmp"].apply(lambda x: 1 if x > 0 else 0)
# Eliminar la columna temporal "tmp"
cnmh.drop(columns=["tmp"], inplace=True)

otros_hechos = ["CAPTURA", "CAPTURA - ACUSACION DE TERRORISMO",
                "CAPTURADO CON ESPOSAS", "IONTENTO DE OCULTAMIENTO DE CADAVER",
                "MASACRE - DANO A BIEN CIVIL", "OCULTAMIENTO",
                "OCULTAMIENTO DE CADAVER",
                "OCULTAMIENTO DE CADAVER", "OCULTAMIENTO DEL CADAVER"]

cnmh["TH_DF"] = np.where((cnmh["tipo_desaparicion"].str.contains("DESAPARICION FORZADA|DESAPARICION") |
                 cnmh["otro_hecho_simultaneo"].str.contains("DESAPARICIÓN|DESAPARECIO")), 1, 0)

cnmh["TH_SE"] = np.where((cnmh["otro_hecho_simultaneo"].str.contains(
    "SECUESTRO|SECUSTRO|RETENCIONES|RETENCION|DETENCION|DETENCION|RESTRICCION DE LA LIBERTAD|RESTRICCION DE MOVILIDAD INDIVIDUAL")), 1, 0)

cnmh["TH_RU"] = np.where((cnmh["otro_hecho_simultaneo"].str.contains(
    "RECLUTAMIENTO")), 1, 0)

cnmh["TH_OTRO"] = np.where((
    cnmh["otro_hecho_simultaneo"].str.contains(
        '|'.join(otros_hechos))), 1, 0)

th_df_counts = cnmh["TH_DF"].value_counts()
th_se_counts = cnmh["TH_SE"].value_counts()
th_ru_counts = cnmh["TH_RU"].value_counts()
th_otro_counts = cnmh["TH_OTRO"].value_counts()

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

cnmh['nombre_completo'] = cnmh['nombres_apellidos'].str.strip()  # Elimina espacios en blanco al principio y al final de la cadena
cnmh['nombre_completo'] = cnmh['nombre_completo'].fillna('')
# Aplica la función a la columna "nombre_completo"
cnmh[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']] = cnmh['nombre_completo'].apply(lambda x: pd.Series(limpiar_nombres_apellidos(x)))

# Primero, asegúrate de que los valores de tipo_documento estén limpios
cnmh['tipo_documento'] = cnmh['tipo_documento'].str.strip()
# Define una lista de preposiciones
preposiciones = ["DE", "DEL", "DE LAS", "DE LA", "DE LOS", "VAN",
                 "LA", "VIUDA DE", "VIUDA", "SAN", "DA"]
# Realiza las transformaciones necesarias en las columnas de nombres y
# apellidos
cnmh['segundo_nombre'] = cnmh['segundo_nombre'].apply(
    lambda x: x if x not in preposiciones else x + " " + cnmh['primer_apellido'])
cnmh['primer_apellido'] = cnmh['primer_apellido'].apply(
    lambda x: x if x not in preposiciones else cnmh['segundo_apellido'])
cnmh['segundo_apellido'] = cnmh['segundo_apellido'].apply(
    lambda x: None if x in preposiciones else x)
# Elimina nombres y apellidos cuando solo se registra la letra inicial
cols_to_clean = ['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']
cnmh[cols_to_clean] = cnmh[cols_to_clean].apply(lambda x: np.where(x.str.len() == 1, np.nan, x))
# Reemplaza primer apellido por segundo apellido cuando el primer campo se
# encuentra vacío
cnmh['primer_apellido'] = np.where(cnmh['primer_apellido'].isna(), cnmh['segundo_apellido'], cnmh['primer_apellido'])
# Documento
cnmh['documento'] = cnmh['numero_documento'].str.strip(
).str.replace("[^A-Z0-9]", "", regex=True)
# Eliminar cadenas de texto sin números y borrar registros de documentos
# de identificación iguales a '0'
cnmh['documento'] = cnmh['documento'].apply(
    lambda x: None if not re.search( r'\d', x) or x == '0' else x)

# Crear una nueva columna para indicar si el documento contiene solo texto
cnmh['documento_solo_cadena_texto'] = np.where(cnmh['documento'].str.isnumeric() == False, 1, 0)
# Convertir la columna 'documento' a tipo numérico si contiene solo números
# cnmh['documento'] = pd.to_numeric(cnmh['documento'], errors='coerce')
# Aplicar las reglas de validación de la Registraduría
cnmh['documento'] = np.where((cnmh['documento'].str.isnumeric() == False) , np.nan, cnmh['documento'])

# implementación de las reglas de la registraduria
# Columna: documento_CC_TI_no_numerico
cnmh['documento_CC_TI_no_numerico'] = np.where(
    (cnmh['documento'].str.isalpha()) &
    (cnmh['tipo_documento'].isin(['TARJETA DE IDENTIDAD',
                                  'CEDULA DE CIUDADANIA'])), 1, 0)
# Columna: documento
cnmh['documento'] = np.where(
    (cnmh['documento'].str.isalpha()) &
    (cnmh['tipo_documento'].isin(['TARJETA DE IDENTIDAD', 'CEDULA DE CIUDADANIA'])), np.nan, cnmh['documento'])

# Columna: documento_CC_TI_mayor1KM
cnmh['documento_CC_TI_mayor1KM'] = np.where(
    (cnmh['documento'].str.len() >= 10) &
    (cnmh['documento'].str.isnumeric()) &
    (cnmh['documento'].astype(float) <= 1000000000) &
    (cnmh['tipo_documento'].isin(['TARJETA DE IDENTIDAD', 'CEDULA DE CIUDADANIA'])), 1, 0)

                
# Columna: documento_TI_10_11_caract
cnmh['documento_TI_10_11_caract'] = np.where(
    (~cnmh['documento'].str.len().isin([10, 11])) &
    (cnmh['tipo_documento'] == 'TARJETA DE IDENTIDAD'), 1, 0)
# Columna: documento_TI_11_caract_fecha_nac
cnmh['documento_TI_11_caract_fecha_nac'] = np.where(
    (cnmh['documento'].str.len() == 11) &
    (~cnmh['documento'].str[:6].eq(cnmh['fecha_nacimiento'].dt.strftime('%y%m%d'))) &
    (cnmh['tipo_documento'] == 'TARJETA DE IDENTIDAD'), 1, 0)
# Columna: documento_CC_hombre_consistente
cnmh['documento_CC_hombre_consistente'] = np.where(
    (~cnmh['documento'].astype(float).isin(range(1, 20000000))) &
    (~cnmh['documento'].astype(float).isin(range(70000000, 100000000))) &
    (cnmh['documento'].str.len().isin(range(4, 9))) &
    (cnmh['tipo_documento'] == 'CEDULA DE CIUDADANIA') &
    (cnmh['sexo'] == 'H'), 1, 0)

# Columna: documento_CC_mujer_consistente
cnmh['documento_CC_mujer_consistente'] = np.where(
    (~cnmh['documento'].astype(float).isin(range(20000000, 70000000))) &
    (cnmh['documento'].str.len() == 8) &
    (cnmh['tipo_documento'] == 'CEDULA DE CIUDADANIA') &
    (cnmh['sexo'] == 'M'), 1, 0)

# Columna: documento_CC_mujer_consistente2
cnmh['documento_CC_mujer_consistente2'] = np.where(
    (~cnmh['documento'].str.len().isin([8, 10])) &
    (cnmh['tipo_documento'] == 'CEDULA DE CIUDADANIA') &
    (cnmh['sexo'] == 'M'), 1, 0)

# Columna: documento_CC_hombre_consistente2
cnmh['documento_CC_hombre_consistente2'] = np.where(
    (~cnmh['documento'].str.len().isin([4, 5, 6, 7, 8, 10])) &
    (cnmh['tipo_documento'] == 'CEDULA DE CIUDADANIA') &
    (cnmh['sexo'] == 'H'), 1, 0)

# Columna: documento_TI_mujer_consistente
cnmh['documento_TI_mujer_consistente'] = np.where(
    (~cnmh['documento'].str[9].isin([1,3,5,7,9])) &
    (cnmh['documento'].str.len() == 11) &
    (cnmh['tipo_documento'] == 'TARJETA DE IDENTIDAD') &
    (cnmh['sexo'] == 'M'), 1, 0)
            
# Columna: documento_TI_hombre_consistente
cnmh['documento_TI_hombre_consistente'] = np.where(
    (~cnmh['documento'].str[9].isin([0,2,4,6,8])) &
    (cnmh['documento'].str.len() == 11) &
    (cnmh['tipo_documento'] == 'TARJETA DE IDENTIDAD') &
    (cnmh['sexo'] == 'H'), 1, 0)
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
conteo = cnmh["iden_pertenenciaetnica"].value_counts()
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
db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)
# Escribir cnmh_ident en la tabla orq_salida.CNMH_DF
cnmh_ident.to_sql(name="CNMH_DF", con=engine, if_exists="replace",
                  index=False)

# Escribir cnmh_no_ident en la tabla orq_salida.CNMH_DF_PNI
cnmh_no_ident.to_sql(name="CNMH_DF_PNI", con=engine, if_exists="replace",
                     index=False)

fecha_fin = datetime.now()

# Crear un diccionario con los datos del log
log = {
    'fecha_inicio': str(fecha_inicio),
    'fecha_fin': str(fecha_fin),
    'tiempo_ejecucion': str(fecha_fin - fecha_inicio),
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



