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
import re
from datetime import datetime
from sqlalchemy import create_engine
import numpy as np
import yaml
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


def funcion_hash(row):
    return hashlib.sha1(str(row).encode()).hexdigest()


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
dane = pd.read_stata(
    "fuentes secundarias\\tablas complementarias\\DIVIPOLA_municipios_122021.dta")
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
dane_depts = dane[['codigo_dane_departamento',
                   'departamento_ocurrencia']].drop_duplicates()
# Configurar la conexión a la base de datos
# (asegúrate de proporcionar los detalles correctos)
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
n_casos = pd.read_sql(
    "select count(*) from [dbo].[V_CNMH_SE_C]", engine).iloc[0, 0]
n_personas = pd.read_sql(
    "select count(*) from [dbo].[V_CNMH_SE]", engine).iloc[0, 0]
# Obtener el número de casos sin personas
n_casos_sin_personas = pd.read_sql(
    "select count(*) from [dbo].[V_CNMH_SE_C] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_RU])", engine).iloc[0, 0]
# Obtener los casos sin personas
casos_sin_personas = pd.read_sql(
    "select * from [dbo].[V_CNMH_SE_C] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_SE_C])", engine)
# Obtener el número de personas sin casos
n_personas_sin_casos = pd.read_sql(
    "select count(*) from [dbo].[V_CNMH_SE] where IdCaso not in (select IdCaso from [dbo].[V_CNMH_SE])", engine).iloc[0, 0]
# Limpieza de nombres de columnas (clean_names no es necesario en pandas)
cnmh.columns = cnmh.columns.str.lower()
# Creación del ID único para cada registro
cnmh['id_registro'] = cnmh.apply(funcion_hash, axis=1)
cnmh['tabla_origen'] = "CNMH_SE"

# Lista de valores NA
na_values = {
    "SIN INFORMACION": np.nan,
    "ND": np.nan,
    "AI": np.nan,
    'NONE': None
}
# Lista de columnas que serán limpiadas
variables_limpieza = [
    "estado", "zon_id_lugar_del_hecho", "municipio_caso", "depto_caso",
    "nacionalidad", "tipo_documento", "nombres_apellidos",
    "sobre_nombre_alias", "sexo", "orientacion_sexual",
    "descripcion_edad", "etnia", "descripcion_etnia",
    "discapacidad", "ocupacion_victima", "descripcion_otra_ocupacion_victima",
    "calidad_victima", "cargo_rango_funcionario_publico",
    "cargo_empleado_sector_privado", "tipo_poblacion_vulnerable",
    "descripcion_otro_tipo_poblacion_vulnerable",
    "organizacion_civil", "militante_politico",
    "descripcion_otro_militante_politico",
    "situacion_actual_victima", "observaciones_situacion_actual_de_la_victima",
    "circustancia_muerte_en_cautiverio",
    "descripcion_otra_circustancia_muerte_en_cautiverio", "tipo_liberacion",
    "descripcion_otro_tipo_liberacion", "dias_cautiverio",
    "no_veces_secuestrado", "hechos_simultaneos_durante_periodo",
    "otro_hecho_simultaneos_durante_periodo",
    "grupo", "descripcion_grupo", "espeficicacion_presunto_responsable",
    "observaciones_grupo_armado1", "rango_fuerzas_armadas",
    "descripcion_rango_fuerzas_armadas_estatales", "rango_grupo_armado",
    "descripcion_rango_grupo_armado", "zon_id_lugar_del_hecho_2",
    "municipio_caso_2", "depto_caso_2", "region", "cabecera_municipal",
    "comuna", "barrio", "area_rural", "corregimiento", "vereda",
    "codigo_centro_poblado", "centro_poblado", "tipo_centro_poblado", "sitio",
    "territorio_colectivo", "resguardo", "modalidad",
    "descripcion_de_la_modalidad", "modalidad_de_secuestro", "tipo_secuestro",
    "finalidad_del_secuestro", "descripcion_otra_finalidad",
    "exigencia_para_la_liberacion", "descripcion_otra_exigencia",
    "porte_listas", "ingreso_vivienda_finca",
    "encapuchados", "perpetrador_identificado", "presunto_reponsable",
    "descripcion_presunto_responsable1",
    "espeficicacion_presunto_responsable_2",
    "observaciones_presunto_responsable", "abandono_despojo_forzado_tierras",
    "amenaza_intimidacion", "ataque_contra_mision_medica",
    "confinamiento_restriccion_movilidad", "desplazamiento_forzado",
    "extorsion", "lesionados_civiles", "pillaje", "tortura",
    "violencia_basada_genero", "otro_hecho_simultaneo", "total_civiles",
    "total_combatientes", "total_civiles_combatientes", "grafitis_letreros",
    "vinculos_familiares", "mujeres_embarazadas", "descripcion_del_caso",
    "usuario", "estado_2", "tipo_caso", "caso_maestro"]


# Limpieza y manipulación de datos
cnmh[variables_limpieza] = cnmh[variables_limpieza].apply(clean_text)
cnmh[variables_limpieza] = cnmh[variables_limpieza].replace(na_values)
# homologación de estructura, formato y contenido
# Datos sobre los hechos
# lugar de ocurrencia
# Definir las variables de limpieza DANE
variables_limpieza_dane = ["departamento_ocurrencia", "municipio_ocurrencia"]
# Limpieza y manipulación de datos en 'dane'
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(clean_text)
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].replace(
    na_values)

# Obtener valores únicos del departamento en 'dane'
unique_departamentos = dane['codigo_dane_departamento'].unique()
# Realizar la manipulación de 'cnmh'
cnmh['pais_ocurrencia'] = np.where(cnmh['depto_caso'] == 'EXTERIOR',
                                   np.nan, 'COLOMBIA')
# Mapear valores en 'depto_caso' y 'municipio_caso'
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

cnmh["depto_caso"] = cnmh["depto_caso"].replace(mapeo_deptos_mun)
cnmh["municipio_caso"] = cnmh["municipio_caso"].replace(mapeo_deptos_mun)

# Verificar valores en 'depto_caso' y 'municipio_caso' contra 'dane'
# Realizar la unión (left join) con 'dane' por departamento y municipio
cnmh = pd.merge(cnmh, dane, how='left', left_on=['depto_caso',
                                                 'municipio_caso'],
                right_on=['departamento_ocurrencia', 'municipio_ocurrencia'])
nrow_cnmh = len(cnmh)
print("Registros despues left dane depto muni:", nrow_cnmh)
cnmh_ndp = cnmh[cnmh["municipio_ocurrencia"].isna()]
# fecha de ocurrencia
cnmh['annoh'] = pd.to_numeric(cnmh['annoh'], errors='coerce')
cnmh['mesh'] = pd.to_numeric(cnmh['mesh'], errors='coerce')
cnmh['diah'] = pd.to_numeric(cnmh['diah'], errors='coerce')
homologacion.fecha.fechas_validas(cnmh, fecha_dia='diah',
                                  fecha_mes='mesh',
                                  fecha_anio='annoh',
                                  fecha='fecha_desaparicion_dtf',
                                  fechat='fecha_desaparicion')
# Definir meses y días válidos
cnmh['fecha_ocur_anio'] = cnmh['annoh']
cnmh['fecha_ocur_mes'] = cnmh['mesh']
cnmh['fecha_ocur_dia'] = cnmh['diah']
# Limpieza de fechas
# Verificación de fechas
meses = ["01", "02", "03", "04", "05", "06",
         "07", "08", "09", "10", "11", "12"]
dias = ["01", "02", "03", "04", "05", "06",
        "07", "08", "09"] + list(map(str, range(10, 32)))
cnmh_ocur = cnmh[cnmh["fecha_ocur_anio"].astype(str).isin(
    map(str, range(1900, 2023))) &
    cnmh["fecha_ocur_mes"].astype(str).isin(meses) &
    cnmh["fecha_ocur_dia"].astype(str).isin(dias)]
cnmh_nocur = cnmh.merge(cnmh_ocur, on=['id_registro'],
                        how='left', indicator=True)
cnmh_nocur = cnmh_nocur[cnmh_nocur['_merge'] == 'left_only']
cnmh_nocur = cnmh_nocur.drop(columns=['_merge'])
# presuntos responsables
# Lista de valores a considerar como "sin información"
# para espeficicacion_presunto_responsable
sin_informacion_actores = ['NO DEFINIDO', '', 'NO IDENTIFICA',
                           'SIN INFORMACIÓN CONFLICTO ARMADO',
                           'NO IDENTIFICA CONFLICTO ARMADO',
                           'OTROS VIOLENCIA GENERALIZADA',
                           'SIN INFORMACIÓN',
                           'NO IDENTIFICA RELACIÓN CERCANA Y SUFICIENTE',
                           'NO IDENTIFICA VIOLENCIA GENERALIZADA',
                           'NO IDENTIFICA RELACIÓN CERCANA Y SUFICIENTE ',
                           'SIN INFORMACIÓN RELACIÓN CERCANA Y SUFICIENTE',
                           'NO IDENTIFICA CONFLICTO ARMADO',
                           'CONFLICTO ARMADO',
                           '0', 'SIN INFORMACIÓN VIOLENCIA GENERALIZADA',
                           '', "NO IDENTIFICADO", 'DESCONOCIDO']
# Realizar transformaciones y limpieza
cnmh['espeficicacion_presunto_responsable'] = np.where(
    cnmh['espeficicacion_presunto_responsable'].isin(
        sin_informacion_actores),
    np.nan, cnmh['espeficicacion_presunto_responsable'])
# Lista de columnas relacionadas con los presuntos responsables
responsables = ["perpetrador_identificado", "presunto_reponsable",
                "descripcion_presunto_responsable1",
                "espeficicacion_presunto_responsable_2",
                "observaciones_presunto_responsable1"]
# Crear columnas para cada tipo de presunto responsable y asignar valores 0 o 1
cnmh['pres_resp_agentes_estatales'] = np.where(
    (cnmh['grupo'].str.contains(
        "FUERZA PÚBLICA|AGENTE DEL ESTADO|GENTE EXTRANJERO",
        flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("AGENTE DEL ESTADO",
                                              flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_grupos_posdesmov'] = np.where(
    (cnmh['grupo'].str.contains("POSDESMOVILIZACION", flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("POSDESMOVILIZACION",
                                              flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_paramilitares'] = np.where(
    (cnmh['grupo'].str.contains(
        "AUTODEF|PARAMI|AUC|A.U.C|PARAMILITAR", flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("PARAMILITAR",
                                              flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_guerr_eln'] = np.where(
    (cnmh['grupo'].str.contains("ELN|GUERRILLA", flags=re.IGNORECASE))
    & (cnmh['descripcion_grupo'].str.contains("ELN",
                                              flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_guerr_farc'] = np.where(
    (cnmh['grupo'].str.contains("FARC|GUERRILLA", flags=re.IGNORECASE)) &
    (cnmh['descripcion_grupo'].str.contains("FARC",
                                            flags=re.IGNORECASE)), 1, 0)
cnmh['pres_resp_guerr_otra'] = np.where(
    (cnmh['grupo'].str.contains("GRUPOS GUERRILLEROS", flags=re.IGNORECASE) &
     ~(cnmh['grupo'].str.contains("FARC|ELN", flags=re.IGNORECASE))) |
    (cnmh['grupo'].str.contains("GUERRILLA", flags=re.IGNORECASE) &
     ~(cnmh['grupo'].str.contains("FARC|ELN", flags=re.IGNORECASE))) |
    (cnmh['grupo'].str.contains("GUERRILLA", flags=re.IGNORECASE) &
     ~(cnmh['descripcion_grupo'].str.contains("FARC|ELN",
                                              flags=re.IGNORECASE))) |
    (cnmh['grupo'].str.contains("GUERRILLA", flags=re.IGNORECASE) &
     (cnmh['descripcion_grupo'].str.contains("EPL", flags=re.IGNORECASE))) |
    (cnmh['grupo'].str.contains("EPL", flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("GUERRILLA",
                                              flags=re.IGNORECASE)), 1, 0)
cnmh['tmp'] = cnmh[['pres_resp_agentes_estatales',
                    'pres_resp_grupos_posdesmov', 'pres_resp_paramilitares',
                    'pres_resp_guerr_eln', 'pres_resp_guerr_farc',
                    'pres_resp_guerr_otra']].sum(axis=1)
# Calcular columna 'pres_resp_otro' basada en
# la suma de las columnas anteriores
cnmh['pres_resp_otro'] = np.where(
    (cnmh['tmp'] > 0) |
    (cnmh['grupo'].str.contains("GRUPO ARMADO NO IDENTIFICADO",
                                flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("GRUPO ARMADO NO",
                                              flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("BANDOLERISMO",
                                              flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("AGENTE EXTRANJERO",
                                              flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("CRIMEN ORGANIZADO",
                                              flags=re.IGNORECASE)) |
    (cnmh['presunto_reponsable'].str.contains("OTRO CUAL",
                                              flags=re.IGNORECASE)), 1, 0)
# Mostrar la suma de presuntos responsables en cada categoría
sum_pres_resp = cnmh[['pres_resp_agentes_estatales',
                      'pres_resp_grupos_posdesmov', 'pres_resp_paramilitares',
                      'pres_resp_guerr_eln', 'pres_resp_guerr_farc',
                      'pres_resp_guerr_otra', 'pres_resp_otro']].sum()
# tipo de hecho
# Lista de otros hechos
otros_hechos = ["OCULTAMIENTO DE CADAVER",
                "OCULTAMIENTO DEL CADAVER", "VENTA DEL SECUESTRADO"]
# Realizar transformaciones y cálculos
cnmh['TH_DF'] = np.where(
    cnmh['otro_hecho_simultaneo'].str.contains("DESAPARICION FORZADA",
                                               flags=re.IGNORECASE), 1, 0)
cnmh['TH_SE'] = 1  # Columna TH_SE siempre se establece en 1
cnmh['TH_RU'] = np.where(
    cnmh['otro_hecho_simultaneo'].str.contains("DESMOVILIZADO",
                                               flags=re.IGNORECASE), 1, 0)
cnmh['TH_OTRO'] = np.where(
    cnmh['otro_hecho_simultaneo'].isin(otros_hechos), 1, 0)
# Mostrar la tabla de frecuencias de las columnas TH_DF, TH_SE, TH_RU, TH_OTRO
table_TH_DF = cnmh['TH_DF'].value_counts()
table_TH_SE = cnmh['TH_SE'].value_counts()
table_TH_RU = cnmh['TH_RU'].value_counts()
table_TH_OTRO = cnmh['TH_OTRO'].value_counts()
# relato
# Limpiar la descripción del caso
cnmh['descripcion_relato'] = cnmh['descripcion_del_caso'].str.strip()
# Mapear la situación actual de la víctima
cnmh['situacion_actual_des'] = cnmh['situacion_actual_victima'].replace({
    "APARECIO MUERTO": "Apareció Muerto",
    "SIGUE DESAPAR": "Continúa desaparecido",
    "VIVA": "Apareció Vivo",
    "MUERTA": "Apareció Muerto",
    "DESCONOCIDA": None,
    "APARECIO VIVO": "Apareció Vivo",
    "DESAPARECIDO FORZADO": "Continúa desaparecido"})

# Datos sobre las personas dadas por desaparecidas
# nombres y apellidos
# Limpiar y procesar el nombre completo
cnmh['nombre_completo'] = cnmh['nombres_apellidos'].str.strip()
cnmh['nombre_completo'] = np.where(
    cnmh['nombre_completo'] == "PERSONA SIN IDENTIFICAR", None,
    cnmh['nombre_completo'])
cnmh['nombre_completo'] = cnmh['nombre_completo'].str.replace(
    " NA ", "", regex=True)

# Aplica la función a la columna "nombre_completo"
cnmh[['primer_nombre',
      'segundo_nombre',
      'primer_apellido',
      'segundo_apellido']] = cnmh['nombre_completo'].apply(
          lambda x: pd.Series(
              homologacion.nombre_completo.limpiar_nombre_completo(x)))

homologacion.nombres.nombres_validos(cnmh, primer_nombre='primer_nombre',
                                     segundo_nombre='segundo_nombre',
                                     primer_apellido='primer_apellido',
                                     segundo_apellido='segundo_apellido',
                                     nombre_completo='nombre_completo')
# Aplicar la función para limpiar tipo_documento
cnmh['tipo_documento'] = cnmh['tipo_documento'].str.strip().str.upper()

# Documento
cnmh['documento'] = cnmh['numero_documento'].str.strip().str.upper()
homologacion.documento.documento_valida(cnmh, documento='documento')

cnmh['documento_solo_cadena_texto'] = np.where(
    cnmh['documento'].str.contains(r'[0-9]') == False, 0, 1)
# Convertir documento a NA si no contiene números o es igual a '0'
cnmh['documento'] = np.where(
    (cnmh['documento'].str.contains(r'[0-9]') == False) |
    (cnmh['documento'] == '0'), None, cnmh['documento'])
# implementacion de las reglas de la registraduria
# Crear columnas con las condiciones especificadas

cnmh['documento_CC_TI_no_numerico'] = np.where(
    cnmh['documento'].str.contains(r'[A-Z]') &
    (cnmh['tipo_documento'].isin(["TARJETA DE IDENTIDAD",
                                  "CEDULA DE CIUDADANIA"])), 1, 0)
cnmh['documento'] = np.where(
    cnmh['documento'].str.contains(r'[A-Z]') &
    (cnmh['tipo_documento'].isin(["TARJETA DE IDENTIDAD",
                                  "CEDULA DE CIUDADANIA"])),
    None, cnmh['documento'])
cnmh['documento_CC_TI_mayor1KM'] = np.where(
    (cnmh['documento'].str.len() >= 10) &
    (cnmh['documento'].astype(float) <= 1000000000) &
    (cnmh['tipo_documento'].isin(["TARJETA DE IDENTIDAD",
                                  "CEDULA DE CIUDADANIA"])), 1, 0)
cnmh['documento_TI_10_11_caract'] = np.where(
    ~((cnmh['documento'].str.len() == 10) |
      (cnmh['documento'].str.len() == 11)) &
    (cnmh['tipo_documento'] == "TARJETA DE IDENTIDAD"), 1, 0)
cnmh['fecha_nacimiento'] =  pd.to_datetime(
    cnmh['fecha_nacimiento'], format='%Y-%m-%d', errors='coerce')
cnmh['documento_TI_11_caract_fecha_nac'] = np.where(
    ~((cnmh['documento'].str.slice(0, 6) == cnmh['fecha_nacimiento'].dt.strftime('%y%m%d')) &
      (cnmh['documento'].str.len() == 11) &
      (cnmh['tipo_documento'] == "TARJETA DE IDENTIDAD")), 1, 0)

cnmh['documento_CC_hombre_consistente'] = np.where(
    ~(cnmh['documento'].astype(float).isin(range(1, 20000000)) |
      cnmh['documento'].astype(float).isin(range(70000000, 100000000))) &
    (cnmh['documento'].str.len().isin(range(4, 9))) &
    (cnmh['tipo_documento'] == "CEDULA DE CIUDADANIA") &
    (cnmh['sexo'] == "H"), 1, 0)

cnmh['documento_CC_mujer_consistente'] = np.where(
    ~(cnmh['documento'].astype(float).isin(range(20000000, 70000000))) &
    (cnmh['documento'].str.len() == 8) &
    (cnmh['tipo_documento'] == "CEDULA DE CIUDADANIA") &
    (cnmh['sexo'] == "M"), 1, 0)
cnmh['documento_CC_mujer_consistente2'] = np.where(
    ~(cnmh['documento'].str.len().isin([8, 10])) &
    (cnmh['tipo_documento'] == "CEDULA DE CIUDADANIA") &
    (cnmh['sexo'] == "M"), 1, 0)
cnmh['documento_CC_hombre_consistente2'] = np.where(
    ~(cnmh['documento'].str.len().isin([4, 5, 6, 7, 8, 10])) &
    (cnmh['tipo_documento'] == "CEDULA DE CIUDADANIA") &
    (cnmh['sexo'] == "H"), 1, 0)

cnmh['documento'].fillna('0', inplace=True)
cnmh['documento'] = cnmh['documento'].str.replace(r'\D', '0', regex=True)
cnmh['documento'] = pd.to_numeric(cnmh['documento'], errors='coerce')

ti_mujer_consistente = ["1", "3", "5", "7", "9"]
cnmh['documento_TI_mujer_consistente'] = np.where(
    (cnmh['documento'].notna()) &  # Excluye los valores NaN
    (~cnmh['documento'].astype(str).str[9:10].isin(ti_mujer_consistente)) & 
    (cnmh['documento'].astype(str).str.len() == 11) & 
    (cnmh['tipo_documento'] == "TARJETA DE IDENTIDAD") & 
    (cnmh['sexo'] == "M"),  1, 0)
ti_hombre_consistente = ["2", "4", "6", "8", "0"]
cnmh['documento_TI_hombre_consistente'] = np.where(
    ~(cnmh['documento'].astype(str).str[9:10].isin(ti_hombre_consistente)) &
    (cnmh['documento'].astype(str).str.len() == 11) &
    (cnmh['tipo_documento'] == "TARJETA DE IDENTIDAD") &
    (cnmh['sexo'] == "H"), 1, 0)

cnmh['documento'] = np.where(
    (cnmh['documento'].astype(float) == 0), "", cnmh['documento'])

# Crear un DataFrame resumen con las filas que cumplen al menos una condición
resumen_documento = cnmh[cnmh.filter(
    like='documento_').sum(axis=1) > 0][['documento_CC_TI_no_numerico',
                                         'documento', 'sexo',
                                         'fecha_nacimiento']]
# Guardar el DataFrame en un archivo CSV con formato adecuado
resumen_documento.to_csv("log/revision_documentos_cnmh_se.csv",
                         sep=";", index=False)
# Contar la cantidad de filas en resumen_documento
cantidad_filas = len(resumen_documento)
# Resumen de las columnas de documento_
# # sale error revisar
# #resumen_documento.filter(like='documento_').sum()
# sexo
cnmh['sexo'] = cnmh['sexo'].apply(
    lambda x: 'HOMBRE' if x == 'H' else (
        'MUJER' if x == 'M' else 'NO INFORMA'))
# Para la columna 'etnia'
cnmh.rename(columns={'etnia': 'iden_pertenenciaetnica'}, inplace=True)
homologacion.etnia.etnia_valida(cnmh, etnia='iden_pertenenciaetnica')

# fecha de nacimiento
# Copiar la columna 'fecha_nacimiento' a 'fecha_nacimiento_original'
cnmh['fecha_nacimiento_original'] = cnmh['fecha_nacimiento']
# Convertir 'fecha_nacimiento' a formato de año, mes y día separados
cnmh['anio_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'],
                                         format="%Y-%m-%d").dt.strftime("%Y")

cnmh['mes_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'],
                                        format='%Y-%m-%d').dt.strftime("%m")
cnmh['dia_nacimiento'] = pd.to_datetime(cnmh['fecha_nacimiento_original'],
                                        format='%Y-%m-%d').dt.strftime("%d")
cnmh['anio_nacimiento'] = pd.to_numeric(cnmh['anio_nacimiento'],
                                        errors='coerce')
cnmh['mes_nacimiento'] = pd.to_numeric(cnmh['mes_nacimiento'], errors='coerce')
cnmh['dia_nacimiento'] = pd.to_numeric(cnmh['dia_nacimiento'], errors='coerce')

homologacion.fecha.fechas_validas(cnmh, fecha_dia='dia_nacimiento',
                                  fecha_mes='mes_nacimiento',
                                  fecha_anio='anio_nacimiento',
                                  fecha='fecha_nacimiento_dtf',
                                  fechat='fecha_nacimiento')

# Calcula la edad y maneja los casos especiales
# conteo = cnmh['fecha_ocur_anio'].value_counts()
# conteo = cnmh['anio_nacimiento'].value_counts()

cnmh['fecha_ocur_anio'] = np.where(
    (cnmh['fecha_ocur_anio'].str.len() < 1), "0", cnmh['fecha_ocur_anio'])
cnmh['anio_nacimiento'] = np.where(
    (cnmh['anio_nacimiento'].str.len() < 1), "0", cnmh['anio_nacimiento'])

cnmh['edad'] = np.where(
    (cnmh['fecha_ocur_anio'].isna() | cnmh['anio_nacimiento'].isna()),
    np.nan,
    np.where(
        cnmh['fecha_ocur_anio'].astype(float) <= cnmh['anio_nacimiento'].astype(float),
        np.nan,
        cnmh['fecha_ocur_anio'].astype(float) -
        cnmh['anio_nacimiento'].astype(float)))

cnmh['fecha_ocur_anio'] = np.where(
    (cnmh['fecha_ocur_anio'].str.len() == 1), "", cnmh['fecha_ocur_anio'])
cnmh['anio_nacimiento'] = np.where(
    (cnmh['anio_nacimiento'].str.len() == 1), "", cnmh['anio_nacimiento'])

# Reemplaza edades fuera del rango 1-100 con NaN
cnmh['edad'] = np.where(
    (cnmh['edad'] > 100) | (cnmh['edad'] < 1), np.nan, cnmh['edad'])
# Asegura que la columna 'edad' esté dentro del rango 1-100
# (permitiendo valores NaN)
cols_to_clean = ['situacion_actual_des', 'descripcion_relato', 'edad',
                 'codigo_dane_departamento', 'departamento_ocurrencia',
                 'codigo_dane_municipio', 'municipio_ocurrencia']
for col in cols_to_clean:
    cnmh[col] = cnmh[col].fillna("")

# Identificación de filas unicas
# Contar filas originales
n_1 = len(cnmh)
# Eliminar duplicados
cnmh = cnmh.drop_duplicates()
# Contar duplicados eliminados
n_duplicados = n_1 - len(cnmh)
# Excluir víctimas indirectas
n_2 = len(cnmh)
cnmh = cnmh[~cnmh['iden_pertenenciaetnica'].isin(['VICTIMA INDIRECTA'])]
n_3 = len(cnmh)
cnmh = cnmh[~cnmh['tipo_documento'].isin(['PERSONA JURIDICA'])]
# Crear una lista con los campos requeridos para la comparación de nombres
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
                     'pres_resp_paramilitares',
                     'pres_resp_grupos_posdesmov',
                     'pres_resp_agentes_estatales', 'pres_resp_guerr_farc',
                     'pres_resp_guerr_eln', 'pres_resp_guerr_otra',
                     'pres_resp_otro', 'situacion_actual_des']
# Verificar si los campos requeridos están presentes en el DataFrame
campos_faltantes = [
    campo for campo in campos_requeridos if campo not in cnmh.columns]
# Seleccionar solo los campos requeridos
cnmh = cnmh[campos_requeridos]
# Eliminar duplicados basados en 'id_registro'
cnmh = cnmh.drop_duplicates(subset=['id_registro'], keep='first')
# Verificar que el número de registros sea igual al
# número de códigos únicos de fuente
cnmh_codigos_unicos = cnmh['codigo_unico_fuente'].nunique()
# Identificación y eliminacin de Registros No Identificados
# Filtrar registros con información de identificación
# cnmh_ident = cnmh[(~cnmh['primer_nombre'].isna() |
#                    ~cnmh['segundo_nombre'].isna()) &
#                   (~cnmh['primer_apellido'].isna() |
#                    ~cnmh['segundo_apellido'].isna()) &
#                   (~cnmh['documento'].isna() |
#                    ~cnmh['fecha_ocur_anio'].isna() |
#                    ~cnmh['fecha_ocur_anio'].isna() |
#                    ~cnmh['departamento_ocurrencia'].isna())]
cnmh_ident = cnmh[(~cnmh['primer_nombre'].isin(["", None]) |
                   ~cnmh['segundo_nombre'].isin(["", None])) &
                  (~cnmh['primer_apellido'].isin(["", None]) |
                   ~cnmh['segundo_apellido'].isin(["", None])) &
                  (~cnmh['documento'].isin(["", None]) |
                   ~cnmh['fecha_ocur_anio'].isin(["", None]) |
                   ~cnmh['fecha_ocur_anio'].isin(["", None]) |
                   ~cnmh['departamento_ocurrencia'].isin(["", None]))]

# Filtrar registros sin información de identificación
cnmh_no_ident = cnmh[~cnmh['id_registro'].isin(cnmh_ident['id_registro'])]
nrow_cnmh = len(cnmh)
nrow_cnmh_ident = len(cnmh_ident)
nrow_cnmh_no_ident = len(cnmh_no_ident)
# Guardar resultados en la base de datos de destino
db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)
# Escribir los DataFrames en las tablas correspondientes en la base de datos
cnmh_ident.to_sql('CNMH_SE', con=engine, if_exists='replace', index=False)
cnmh_no_ident.to_sql('CNMH_SE_PNI', con=engine, if_exists='replace',
                     index=False)
# Cerrar la conexión a la base de datos
# Registrar información de ejecución

fecha_fin = datetime.now()

log = {
    "fecha_inicio": str(fecha_inicio),
    "fecha_fin": str(fecha_fin),
    "tiempo_ejecucion": str(fecha_fin - fecha_inicio),
    "n_casos": n_casos,
    "n_personas": n_personas,
    "n_casos_sin_personas": n_casos_sin_personas,
    'filas_iniciales_cnmh': nrow_cnmh,
    'filas_final_cnmh': nrow_cnmh,
    'filas_cnmh_ident': nrow_cnmh_ident,
    'filas_cnmh_no_ident': nrow_cnmh_no_ident,
    'n_duplicados': n_duplicados,
}

with open('log/resultado_cnmh_se.yaml', 'w') as file:
    yaml.dump(log, file)
