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
    if nombre_completo is None or pd.notna(nombre_completo):
       nombre_completo = ""
    
    if nombre_completo in ["PERSONA SIN IDENTIFICAR", "NA"] or pd.notna(nombre_completo):
       nombre_completo = ""
    
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
# for variable in list(locals()):
#    del locals()[variable]

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
# Listar los drivers ODBC instalados
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
# 110
# Configurar la conexión a la base de datos (asegúrate de proporcionar los detalles correctos)
# db_url = "mssql+pyodbc://orquestacion.universo:Ubpd2022*@172.16.10.10/UNIVERSO_PDD?driver=ODBC+Driver+17+for+SQL+Server"
db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)

# Crear la consulta SQL
n_sample_p = f"top({n_sample})" if n_sample != "" else ""
# #query = f"SELECT {n_sample_p} * FROM [dbo].[V_CNMH_RU] personas left join [dbo].[V_CNMH_RU_C] casos on casos.IdCaso = personas.IdCaso"
query = "EXECUTE CONSULTA_V_CNMH_RU"
# Ejecutar la consulta y cargar los datos en un DataFrame
cnmh = pd.read_sql(query, con = engine)
# Obtener el número de filas en el DataFrame cnmh
nrow_cnmh = len(cnmh)
print("Registros en la consulta tabla :",nrow_cnmh)
# Obtener el número de casos y personas
n_casos = pd.read_sql("select count(*) from [dbo].[V_CNMH_RU_C]", con = engine).iloc[0, 0]
n_personas = pd.read_sql("select count(*) from [dbo].[V_CNMH_RU]", con = engine).iloc[0, 0]
# Obtener el número de casos sin personas
n_casos_sin_personas = pd.read_sql("select count(*) from [dbo].[V_CNMH_RU_C] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_RU])", con = engine).iloc[0, 0]
# Obtener los casos sin personas
casos_sin_personas = pd.read_sql("select * from [dbo].[V_CNMH_RU_C] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_RU])", con = engine)
# Obtener el número de personas sin casos
n_personas_sin_casos = pd.read_sql("select count(*) from [dbo].[V_CNMH_RU] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_RU_C])", con = engine).iloc[0, 0]
# Limpieza de nombres de columnas (clean_names no es necesario en pandas)
cnmh.columns = cnmh.columns.str.lower()
# Creación del ID único para cada registro
cnmh['id_registro'] = cnmh.apply(funcion_hash, axis=1)
cnmh['tabla_origen'] = "CNMH_RU"
# se reeplaza esta isntruccion y se usa desde la base de datos
# con procedimiento almacenado
# cnmh['codigo_unico_fuente'] = cnmh['id_caso'] + "_" + cnmh['identificador_caso'] + "_" + cnmh['id']
##con.close()
# 147
# Valores NA
na_values = {
    "SIN INFORMACION": np.nan,
    "ND": np.nan,
    "AI": np.nan
}
# Lista de variables a limpiar
variables_limpieza = ["zon_id_lugar_del_hecho", "muninicio_caso", "depto_caso", "nacionalidad",
                      "tipo_documento", "nombres_apellidos", "sobre_nombre_alias", "sexo",
                      "orientacion_sexual", "descripcion_edad", "etnia", "descripcion_etnia",
                      "discapacidad", "ocupacion_victima", "descripcion_otra_ocupacion_victima",
                      "calidad_victima", "tipo_poblacion_vulnerable", "descripcion_otro_tipo_poblacion_vulnerable",
                      "militante_politico", "descripcion_otro_militante_politico", "caletero", "camp",
                      "coci", "coman", "comba", "conta", "entre", "esco", "ogaoextor", "fab_arm", "minas",
                      "guar", "inform", "patru", "radio", "rasp", "ser_salud", "sica", "tra_org", "tra_dro",
                      "tra_armas", "sin_ofi", "otro_ofi", "hechos_simultaneos_durante_cautiverio", "des_h_sim_per",
                      "tipo_salida", "des_sal", "motivo_salida", "annofh", "grupo", "descripcion_grupo",
                      "espeficicacion_presunto_responsable", "observaciones_grupo_armado1", "rango_fuerzas_armadas",
                      "descripcion_rango_fuerzas_armadas_estatales", "rango_grupo_armado",
                      "descripcion_rango_grupo_armado", "mun_finali", "depto_finali", "grupo_salida",
                      "descripcion_grupo_salida", "espeficicacion_presunto_responsable1",
                      "observaciones_grupo_armado11", "num_vecre", "zon_id_lugar_del_hecho_2",
                      "muninicio_caso_2", "depto_caso_2", "region", "cabecera_municipal", "comuna", "barrio",
                      "area_rural", "corregimiento", "vereda", "codigo_centro_poblado", "centro_poblado",
                      "tipo_centro_poblado", "sitio", "territorio_colectivo", "resguardo", "modalidad",
                      "modalidad_descripcion", "forma_vinculacion", "tipo_vinculacion", "porte_listas",
                      "ingreso_vivienda_finca", "encapuchados", "perpetrador_identificado", "ingreso_escuela",
                      "presunto_reponsable", "descripcion_presunto_responsable1",
                      "espeficicacion_presunto_responsable_2", "observaciones_presunto_responsable1",
                      "numero_combatientes_grupo_armado1", "descripcion_combatientes_grupo_armado1",
                      "armas_grupo_armado1", "descripcion_tipo_armas_grupo_armado1",
                      "abandono_despojo_forzado_tierras", "amenaza_intimidacion", "ataque_contra_mision_medica",
                      "confinamiento_restriccion_movilidad", "desplazamiento_forzado", "extorsion",
                      "lesionados_civiles", "pillaje", "tortura", "violencia_basada_genero",
                      "otro_hecho_simultaneo", "grafitis_letreros", "vinculos_familiares", "mujeres_embarazadas",
                      "descripcion_del_caso", "usuario", "estado_2", "identificador_caso_2", "tipo_caso",
                      "caso_maestro", "numero_victimas_caso"]

# Aplicar la función de limpieza a las variables
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(
    lambda x: x.str.strip().str.upper())
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(lambda x: clean_func(x, na_values))
cnmh[variables_limpieza] = cnmh[variables_limpieza].replace(na_values)
nrow_cnmh = len(cnmh)
print("Registros despues de la limpieza :",nrow_cnmh)

# homologación de estructura, formato y contenido
# Datos sobre los hechos
# lugar de ocurrencia
# Definir variables_limpieza_dane
variables_limpieza_dane = ["departamento_ocurrencia", "municipio_ocurrencia"]

# Limpieza y normalización de las columnas de dane
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(lambda x: x.str.strip().str.upper())
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(lambda x: clean_func(x, na_values))
dane["codigo_dane_departamento"] = dane["codigo_dane_departamento"].str.strip()

# Crear un DataFrame con las equivalencias para el departamento
equivalencias_departamento = {
    "SIN INFORMACION": np.nan,
    "EXTERIOR": np.nan,
    "CUCUTA": "SAN JOSE DE CUCUTA",
    "ARMERO GUAYABAL": "ARMERO",
    "TOLU VIEJO": "SAN JOSE DE TOLUVIEJO",
    "CUASPUD": "CUASPUD CARLOSAMA",
    "BARRANCO MINAS": "BARRANCOMINAS",
    "MOMPOS": "SANTA CRUZ DE MOMPOX",
    "BELEN DE BAJIRA": "RIOSUCIO",
    "PIENDAMO": "PIENDAMO TUNIA",
    "SOTARA": "SOTARA PAISPAMBA",
    "FRONTERA VENEZUELA": np.nan,
    "FRONTERA PANAMA": np.nan,
    "FRONTERA BRASIL": np.nan,
    "GUICAN": "GUICAN DE LA SIERRA",
    "FRONTERA": np.nan,
    "FRONTERA ECUADOR": np.nan,
    "YACARATE": "YAVARATE",
    "PAPUNAUA": "PAPUNAHUA"
}
# Limpieza y normalización de las columnas en cnmh
cnmh["pais_ocurrencia"] = np.where(cnmh["depto_caso"] == "EXTERIOR", np.nan, "COLOMBIA")
cnmh[["depto_caso", "muninicio_caso"]] = cnmh[["depto_caso", "muninicio_caso"]].apply(lambda x: x.str.strip().str.upper())
cnmh["depto_caso"] = cnmh["depto_caso"].replace(equivalencias_departamento)
cnmh.rename(columns={'muninicio_caso': 'municipio_caso'}, inplace=True)
nrow_cnmh = len(cnmh)
print("Registros despues de la limpieza dane :",nrow_cnmh)
# Asegurarse de que los valores en "depto_caso" y "muninicio_caso" estén en "dane"
# #cnmh = cnmh[cnmh["depto_caso"].isin(dane["departamento_ocurrencia"]) &
# #            cnmh["muninicio_caso"].isin(dane["municipio_ocurrencia"])]
# #nrow_cnmh = len(cnmh)
# #print("Registros despues validacion dane depto muni:",nrow_cnmh)
# Realizar la unión (left join) con "dane"
cnmh = pd.merge(cnmh, dane, how='left', left_on=['depto_caso', 'municipio_caso'],
                right_on=['departamento_ocurrencia', 'municipio_ocurrencia'])
nrow_cnmh = len(cnmh)
print("Registros despues left dane depto muni:",nrow_cnmh)
cnmh_ndp = cnmh[cnmh["departamento_ocurrencia"].isna()]
# fecha de ocurrencia 
# Asegurarse de que las columnas "mesh," "diah," y "annoh" sean de tipo str
cnmh["mesh"] = cnmh["mesh"].astype(str)
cnmh["diah"] = cnmh["diah"].astype(str)
cnmh["annoh"] = cnmh["annoh"].astype(str)
# Limpieza y transformación de fechas
cnmh["mesh"] = np.where(cnmh["mesh"].isna(), "0", cnmh["mesh"])
cnmh["diah"] = np.where(cnmh["diah"].astype(float) < 10, "0" + cnmh["diah"], cnmh["diah"])
cnmh["mesh"] = np.where(cnmh["mesh"].astype(float) < 10, "0" + cnmh["mesh"], cnmh["mesh"])
cnmh["annoh"] = np.where(cnmh["annoh"].astype(float) < 1000, "1900", cnmh["annoh"])
cnmh["fecha_hecho_0"] = cnmh["diah"] + "-" + cnmh["mesh"] + "-" + cnmh["annoh"]
cnmh["fecha_hecho"] = pd.to_datetime(cnmh["fecha_hecho_0"], format="%d-%m-%Y", errors='coerce')
cnmh["fecha_desaparicion"] = cnmh["fecha_hecho"].dt.strftime("%Y%m%d")
cnmh["fecha_desaparicion_dtf"] = cnmh["fecha_hecho"]
cnmh["fecha_ocur_anio"] = np.where(cnmh["annoh"].astype(float) < 1900, np.nan, cnmh["annoh"])
cnmh["fecha_ocur_mes"] = np.where(cnmh["mesh"] == "00", np.nan, cnmh["mesh"])
cnmh["fecha_ocur_dia"] = np.where((cnmh["diah"] == "00") | (cnmh["diah"] == ""), np.nan, cnmh["diah"])
# Validaciones
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
dias = ["01", "02", "03", "04", "05", "06", "07", "08", "09"] + list(map(str, range(10, 32)))

cnmh_ocur = cnmh[cnmh["fecha_ocur_anio"].astype(str).isin(map(str, range(1900, 2023))) &
    cnmh["fecha_ocur_mes"].astype(str).isin(meses) &
    cnmh["fecha_ocur_dia"].astype(str).isin(dias) ]


cnmh_nocur = cnmh.merge(cnmh_ocur, on=['id_registro'], how='left', indicator=True)
cnmh_nocur = cnmh_nocur[cnmh_nocur['_merge'] == 'left_only']
cnmh_nocur = cnmh_nocur.drop(columns=['_merge'])
#presuntos responsables
sin_informacion_actores = {
    "NO DEFINIDO": np.nan,
    'NO IDENTIFICA': np.nan,
    'SIN INFORMACIÓN  CONFLICTO ARMADO': np.nan,
    'NO IDENTIFICA  CONFLICTO ARMADO': np.nan,
    'OTROS  VIOLENCIA GENERALIZADA': np.nan,
    'SIN INFORMACIÓN ': np.nan,
    'NO IDENTIFICA  RELACIÓN CERCANA Y SUFICIENTE': np.nan,
    'NO IDENTIFICA  VIOLENCIA GENERALIZADA': np.nan,
    'NO IDENTIFICA  RELACIÓN CERCANA Y SUFICIENTE ': np.nan,
    'SIN INFORMACIÓN  RELACIÓN CERCANA Y SUFICIENTE': np.nan,
    'NO IDENTIFICA  CONFLICTO ARMADO ': np.nan,
    'CONFLICTO ARMADO': np.nan,
    '0': np.nan,
    'SIN INFORMACIÓN  VIOLENCIA GENERALIZADA': np.nan,
    "NO IDENTIFICADO": np.nan
}
# Reemplazar valores en la columna 'espeficicacion_presunto_responsable'
cnmh['espeficicacion_presunto_responsable'] = cnmh['espeficicacion_presunto_responsable'].replace(sin_informacion_actores)

# Crear nuevas columnas para categorizar responsables
cnmh['pres_resp_agentes_estatales'] = np.where(
    (cnmh['grupo'].str.contains("FUERZA PÚBLICA") |
     cnmh['grupo'].str.contains("AGENTE DEL ESTADO") |
     cnmh['grupo'].str.contains("GENTE EXTRANJERO") |
     cnmh['presunto_reponsable'].str.contains("AGENTE DEL ESTADO")), 1, 0
)

cnmh['pres_resp_grupos_posdesmov'] = np.where(
    (cnmh['grupo'].str.contains("POSDESMOVILIZACION") |
     cnmh['presunto_reponsable'].str.contains("POSDESMOVILIZACION")), 1, 0
)

cnmh['pres_resp_paramilitares'] = np.where(
    (cnmh['grupo'].str.contains("AUTODEF") |
     cnmh['grupo'].str.contains("PARAMI") |
     cnmh['grupo'].str.contains("AUC") |
     cnmh['grupo'].str.contains("A.U.C") |
     cnmh['grupo'].str.contains("PARAMILITAR") |
     cnmh['presunto_reponsable'].str.contains("PARAMILITAR")), 1, 0
)

cnmh['pres_resp_guerr_eln'] = np.where(
    (cnmh['grupo'].str.contains("ELN") |
     (cnmh['grupo'].str.contains("GUERRILLA") &
      cnmh['descripcion_grupo'].str.contains("ELN"))), 1, 0
)

cnmh['pres_resp_guerr_farc'] = np.where(
    (cnmh['grupo'].str.contains("FARC") |
     (cnmh['grupo'].str.contains("GUERRILLA") &
      cnmh['descripcion_grupo'].str.contains("FARC"))), 1, 0
)

cnmh['pres_resp_guerr_otra'] = np.where(
    ((cnmh['grupo'].str.contains("GRUPOS GUERRILLEROS") &
      ~cnmh['grupo'].str.contains("FARC") &
      ~cnmh['grupo'].str.contains("ELN")) |
     (cnmh['grupo'].str.contains("GUERRILLA") &
      ~cnmh['descripcion_grupo'].str.contains("FARC") &
      ~cnmh['descripcion_grupo'].str.contains("ELN")) |
     (cnmh['grupo'].str.contains("EPL") |
      (cnmh['grupo'].str.contains("GUERRILLA") &
       cnmh['descripcion_grupo'].str.contains("EPL"))) |
     cnmh['presunto_reponsable'].str.contains("GUERRILLA")), 1, 0
)

cnmh['tmp'] =  cnmh['pres_resp_guerr_otra'] + cnmh['pres_resp_guerr_farc'] + cnmh['pres_resp_guerr_eln'] + cnmh['pres_resp_paramilitares'] + cnmh['pres_resp_grupos_posdesmov'] + cnmh['pres_resp_agentes_estatales'] 

cnmh['pres_resp_otro'] = np.where(
    (cnmh['tmp'] > 0 |
     cnmh['grupo'].str.contains("GRUPO ARMADO NO IDENTIFICADO") |
     cnmh['presunto_reponsable'].str.contains("GRUPO ARMADO NO") |
     cnmh['presunto_reponsable'].str.contains("BANDOLERISMO")), 1, 0
)

cnmh = cnmh.drop(columns=['tmp'])

# Resumen de las columnas de responsables
pres_resp_columns = cnmh.filter(like='pres_resp_')
pres_resp_summary = pres_resp_columns.sum()

# tipo de hecho
otros_hechos = ["OCULTAMIENTO DE CADAVER"]

cnmh['TH_DF'] = np.where(cnmh['otro_hecho_simultaneo'].str.contains("COMBATIENTE DADO POR DESAPARECIDO"), 1, 0)
cnmh['TH_SE'] = np.where(cnmh['otro_hecho_simultaneo'].str.contains("INTENTO DE ESCAPE"), 1, 0)
cnmh['TH_RU'] = 1
cnmh['TH_OTRO'] = np.where(cnmh['otro_hecho_simultaneo'].isin(otros_hechos), 1, 0)

# Contar los valores en las nuevas columnas
count_TH_DF = cnmh['TH_DF'].value_counts()
count_TH_SE = cnmh['TH_SE'].value_counts()
count_TH_RU = cnmh['TH_RU'].value_counts()
count_TH_OTRO = cnmh['TH_OTRO'].value_counts()

# relato
# Limpia la descripción del relato
cnmh['descripcion_relato'] = cnmh['descripcion_del_caso'].str.strip()
# Crea una nueva columna "situacion_actual_des" con valores NA
cnmh['situacion_actual_des'] = None

# Datos sobre las personas dadas por desaparecidas
# nombres y apellidos
cnmh['nombre_completo'] = cnmh['nombres_apellidos'].str.strip()  # Elimina espacios en blanco al principio y al final de la cadena
cnmh['nombre_completo'] = cnmh['nombre_completo'].replace('PERSONA SIN IDENTIFICAR', pd.NA)  # Reemplaza 'PERSONA SIN IDENTIFICAR' con NaN
cnmh['nombre_completo'] = cnmh['nombre_completo'].str.replace(r'\bNA\b', '', regex=True)  # Elimina la palabra 'NA' entre espacios en blanco

# Aplica la función a la columna "nombre_completo"
cnmh[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']] = cnmh['nombre_completo'].apply(lambda x: pd.Series(limpiar_nombres_apellidos(x)))

# Rellena los valores NaN con None
cnmh["primer_nombre"] = np.where(cnmh["primer_nombre"].isna(), None, cnmh["primer_nombre"])
cnmh["segundo_nombre"] = np.where(cnmh["segundo_nombre"].isna(), None, cnmh["segundo_nombre"])
cnmh["primer_apellido"] = np.where(cnmh["primer_apellido"].isna(), None, cnmh["primer_apellido"])
cnmh["segundo_apellido"] = np.where(cnmh["segundo_apellido"].isna(), None, cnmh["segundo_apellido"])

preposiciones = ["DE", "DEL", "DE LAS", "DE LA", "DE LOS", "VAN", "LA", "VIUDA DE", "VIUDA", "SAN", "DA"]
# Eliminar espacios en blanco al principio y al final de la columna tipo_documento
cnmh['tipo_documento'] = cnmh['tipo_documento'].str.strip()
# Modificar las columnas segundo_nombre, primer_apellido y segundo_apellido
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
cols_to_clean = ['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']
cnmh[cols_to_clean] = cnmh[cols_to_clean].apply(lambda x: np.where(x.str.len() == 1, np.nan, x))
# Reemplazar primer apellido por segundo apellido cuando el primer campo se encuentra vacío
cnmh['primer_apellido'] = np.where(cnmh['primer_apellido'].isna(), cnmh['segundo_apellido'], cnmh['primer_apellido'])


# Documento
# Eliminar espacios en blanco al principio y al final de la columna numero_documento
cnmh['documento'] = cnmh['numerodocumento'].str.strip()
# Eliminar símbolos y caracteres especiales
cnmh['documento'] = cnmh['documento'].str.replace('[^A-Z0-9]', '', regex=True)
# Eliminar cadenas de texto sin números y borrar registros de documentos de identificación iguales a '0'
cnmh['documento'] = np.where(cnmh['documento'] == "", np.nan, cnmh['documento'])
# Crear una columna auxiliar para indicar si el documento solo contiene caracteres de texto
cnmh['documento_solo_cadena_texto'] = np.where(cnmh['documento'].str.isnumeric() == False, 1, 0)
# Aplicar las reglas de validación de la Registraduría
cnmh['documento'] = np.where((cnmh['documento'].str.isnumeric() == False) , np.nan, cnmh['documento'])

cnmh['documento_CC_TI_no_numerico'] = np.where(
    (cnmh['documento'].str.isalpha()) &
    (cnmh['tipo_documento'].isin(['TARJETA DE IDENTIDAD',
                                  'CEDULA DE CIUDADANIA'])), 1, 0)

cnmh['documento'] = np.where(
    (cnmh['documento'].str.isalpha()) &
    (cnmh['tipo_documento'].isin(['TARJETA DE IDENTIDAD', 'CEDULA DE CIUDADANIA'])), np.nan, cnmh['documento'])

cnmh['documento_CC_TI_mayor1KM'] = np.where(
    (cnmh['documento'].str.len() >= 10) &
    (cnmh['documento'].str.isnumeric()) &
    (cnmh['documento'].astype(float) <= 1000000000) &
    (cnmh['tipo_documento'].isin(['TARJETA DE IDENTIDAD', 'CEDULA DE CIUDADANIA'])), 1, 0)

cnmh['documento_TI_10_11_caract'] = np.where(
    (~cnmh['documento'].str.len().isin([10, 11])) &
    (cnmh['tipo_documento'] == 'TARJETA DE IDENTIDAD'), 1, 0)

cnmh['documento_TI_11_caract_fecha_nac'] = np.where(
    (cnmh['documento'].str.len() == 11) &
    (~cnmh['documento'].str[:6].eq(cnmh['fechanacimiento'].dt.strftime('%y%m%d'))) &
    (cnmh['tipo_documento'] == 'TARJETA DE IDENTIDAD'), 1, 0)

cnmh['documento_CC_hombre_consistente'] = np.where(
    (~cnmh['documento'].astype(float).isin(range(1, 20000000))) &
    (~cnmh['documento'].astype(float).isin(range(70000000, 100000000))) &
    (cnmh['documento'].str.len().isin(range(4, 9))) &
    (cnmh['tipo_documento'] == 'CEDULA DE CIUDADANIA') &
    (cnmh['sexo'] == 'H'), 1, 0)

cnmh['documento_CC_mujer_consistente'] = np.where(
    (~cnmh['documento'].astype(float).isin(range(20000000, 70000000))) &
    (cnmh['documento'].str.len() == 8) &
    (cnmh['tipo_documento'] == 'CEDULA DE CIUDADANIA') &
    (cnmh['sexo'] == 'M'), 1, 0)

cnmh['documento_CC_mujer_consistente2'] = np.where(
    (~cnmh['documento'].str.len().isin([8, 10])) &
    (cnmh['tipo_documento'] == 'CEDULA DE CIUDADANIA') &
    (cnmh['sexo'] == 'M'), 1, 0)

cnmh['documento_CC_hombre_consistente2'] = np.where(
    (~cnmh['documento'].str.len().isin([4, 5, 6, 7, 8, 10])) &
    (cnmh['tipo_documento'] == 'CEDULA DE CIUDADANIA') &
    (cnmh['sexo'] == 'H'), 1, 0)

cnmh['documento_TI_mujer_consistente'] = np.where(
    (~cnmh['documento'].str[9].isin([1,3,5,7,9])) &
    (cnmh['documento'].str.len() == 11) &
    (cnmh['tipo_documento'] == 'TARJETA DE IDENTIDAD') &
    (cnmh['sexo'] == 'M'), 1, 0)

cnmh['documento_TI_hombre_consistente'] = np.where(
    (~cnmh['documento'].str[9].isin([0,2,4,6,8])) &
    (cnmh['documento'].str.len() == 11) &
    (cnmh['tipo_documento'] == 'TARJETA DE IDENTIDAD') &
    (cnmh['sexo'] == 'H'), 1, 0)

# Crear un resumen de los resultados
resumen_documento = cnmh[
    (cnmh.filter(like='documento_').sum(axis=1) > 0)
][['tipo_documento', 'numerodocumento', 'documento', 'sexo', 'fechanacimiento']]

#sexo
# Transformación de género
cnmh["sexo"] = cnmh["sexo"].apply(lambda x: "HOMBRE" if x == "H" else ("MUJER" if x == "M" else "NO INFORMA"))

# Pertenencia étnica
cnmh["iden_pertenenciaetnica"] = cnmh["etnia"].apply(lambda x: "NINGUNA" if x == "" else ("NARP" if x in ["AFROCOLOMBIANO", "PALENQUERO", "RAIZAL"] else ("INDIGENA" if x == "INDIGENA" else ("RROM" if x == "ROM" else None))))

# Fecha de nacimiento
cnmh["fecha_nacimiento_original"] = pd.to_datetime(cnmh["fechanacimiento"])
cnmh["anio_nacimiento"] = cnmh["fecha_nacimiento_original"].dt.strftime("%Y")
cnmh["mes_nacimiento"] = cnmh["fecha_nacimiento_original"].dt.strftime("%m")
cnmh["dia_nacimiento"] = cnmh["fecha_nacimiento_original"].dt.strftime("%d")
cnmh["fecha_nacimiento_dtf"] = cnmh["fecha_nacimiento_original"]
cnmh["fecha_nacimiento"] = cnmh["fecha_nacimiento_original"].dt.strftime("%Y%m%d")

# Verificar año de nacimiento
cnmh["anio_nacimiento"] = cnmh["anio_nacimiento"].apply(lambda x: None if (x is not None and int(x) < 1905) else x)
cnmh["anio_nacimiento"] = cnmh["anio_nacimiento"].apply(lambda x: None if (x is not None and int(x) > 2022) else x)

# Verificar meses y días de nacimiento
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
dias = ["01", "02", "03", "04", "05", "06", "07", "08", "09"] + [str(i) for i in range(10, 32)]

cnmh_naci = cnmh[cnmh["anio_nacimiento"].astype(str).isin(map(str, range(1905, 2022))) &
    cnmh["mes_nacimiento"].astype(str).isin(meses) &
    cnmh["dia_nacimiento"].astype(str).isin(dias) ]

cnmh_nnaci = cnmh.merge(cnmh_naci, on=['id_registro'], how='left', indicator=True)
cnmh_nnaci = cnmh_nnaci[cnmh_nnaci['_merge'] == 'left_only']
cnmh_nnaci = cnmh_nnaci.drop(columns=['_merge'])

# Calcula la edad
cnmh["edad"] = np.where((cnmh["fecha_ocur_anio"].isna() | cnmh["anio_nacimiento"].isna()), np.nan,
                        cnmh["fecha_ocur_anio"].astype(float) - cnmh["anio_nacimiento"].astype(float))
# Reemplaza edades mayores de 100 con NaN
cnmh["edad"] = np.where(cnmh["edad"] > 100, np.nan, cnmh["edad"])
# Verifica que la edad esté dentro del rango [1, 100] o sea NaN
cnmh_nedad = cnmh[(cnmh["edad"].between(1, 100, inclusive=True) == False | cnmh["edad"].isna())]

# Identificación de filas Unicas
# Eliminar duplicados
n_1 = len(cnmh)
cnmh = cnmh.drop_duplicates()
n_duplicados = n_1 - len(cnmh)

# Excluir víctimas indirectas
n_2 = len(cnmh)
cnmh = cnmh.dropna(subset=["nombre_completo", "documento", "fecha_ocur_anio", "departamento_ocurrencia"])
n_indirectas = n_2 - len(cnmh)
# Excluir personas jurídicas
n_3 = len(cnmh)
cnmh = cnmh.dropna(subset=["nombre_completo"])
n_juridicas = n_3 - len(cnmh)
# Crear tabla de datos únicamente con los campos requeridos
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

campos_faltantes = [campo for campo in campos_requeridos if campo not in cnmh.columns]
print(campos_faltantes)

cnmh = cnmh[campos_requeridos].copy()
cnmh = cnmh.drop_duplicates(subset=["codigo_unico_fuente"])
# Identificación y eliminación de registros No Identificados
cnmh_ident = cnmh.dropna(subset=["primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido",
                                  "documento", "fecha_ocur_anio", "departamento_ocurrencia"])

cnmh_no_ident = cnmh[~cnmh["id_registro"].isin(cnmh_ident["id_registro"])]

db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)

# Escribir el DataFrame cnmh_ident en la tabla orq_salida.CNMH_RU
cnmh_ident.to_sql(name='CNMH_RU_jdmc', con=engine, if_exists='replace', index=False)

# Escribir el DataFrame cnmh_no_ident en la tabla orq_salida.CNMH_RU_PNI
cnmh_no_ident.to_sql(name='CNMH_RU_PNI_jdmc', con=engine, if_exists='replace', index=False)

# Registro de resultados
fecha_fin = datetime.now()

log = {
    "fecha_inicio": str(fecha_inicio),
    "fecha_fin": str(fecha_fin),
    "tiempo_ejecucion": str(fecha_fin - fecha_inicio),
    "n_casos": n_casos,  # Debes definir n_casos antes de esta parte
    "n_personas": n_personas,  # Debes definir n_personas antes de esta parte
    "n_casos_sin_personas": n_casos_sin_personas,  # Debes definir n_casos_sin_personas antes de esta parte
    "filas_iniciales_cnmh": nrow_cnmh,  # Debes definir nrow_cnmh antes de esta parte
    "filas_final_cnmh": len(cnmh),
    "filas_cnmh_ident": len(cnmh_ident),
    "filas_cnmh_no_ident": len(cnmh_no_ident),
    "n_duplicados": n_duplicados,  # Debes definir n_duplicados antes de esta parte
    "n_indirectas": n_indirectas,  # Debes definir n_indirectas antes de esta parte
    "n_juridicas": n_juridicas,  # Debes definir n_juridicas antes de esta parte
}

with open("log/resultado_cnmh_ru.yaml", "w") as yaml_file:
    yaml.dump(log, yaml_file)


