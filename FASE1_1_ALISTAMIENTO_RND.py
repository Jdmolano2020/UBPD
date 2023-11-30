import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_PARAMILITARES
import FASE1_HOMOLOGACION_CAMPO_FUERZA_PUBLICA_Y_AGENTES_DEL_ESTADO
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_FARC
import FASE1_HOMOLOGACION_CAMPO_BANDAS_CRIMINALES
import FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_ELN
import FASE1_HOMOLOGACION_CAMPO_OTRAS_GUERRILLAS
import homologacion.limpieza
import homologacion.fecha
import homologacion.nombres
import homologacion.documento
import homologacion.etnia
import homologacion.nombre_completo


def clean_text(text):
    if text is None or text.isna().any():
        text = text.astype(str)
    text = text.apply(homologacion.limpieza.normalize_text)
    return text


# parametros programa stata
parametro_ruta = ""
parametro_cantidad = ""
# Establecer la ruta de trabajo
ruta =  "C:/Users/HP/Documents/UBPD/HerramientaAprendizaje/Fuentes/OrquestadorUniverso"  # Cambia esto según tu directorio

# Verificar si `1` es una cadena vacía y ajustar el directorio de trabajo
# en consecuencia
if parametro_ruta == "":
    os.chdir(ruta)
else:
    os.chdir(parametro_ruta)
# Borrar el archivo "fuentes secundarias\V_ICMP.dta"
archivo_a_borrar = os.path.join("fuentes secundarias",
                                "V_INML_RND.dta")
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
query = "EXECUTE CONSULTA_INML_DES"
df = pd.read_sql_query(query, engine)
# Aplicar filtro si `2` no es una cadena vacía parametro cantidad registros
if parametro_cantidad != "":
    limite = int(parametro_cantidad)
    df = df[df.index < limite]
# Guardar el DataFrame en un archivo
archivo_csv = os.path.join("fuentes secundarias",
                           "V_INML_RND.csv")
df.to_csv(archivo_csv, index=False)
# Cambiar directorio de trabajo
# #os.chdir(os.path.join(ruta, "fuentes secundarias"))
# Traducir la codificación Unicode
# #archivo_a_traducir = "V_ICMP.dta"
# #archivo_utf8 = archivo_a_traducir.replace(".dta", "_utf8.dta")
# #if os.path.exists(archivo_a_traducir):
# #    os.system(
# #     f'unicode translate "{archivo_a_traducir}" "{archivo_utf8}" transutf8')
# #    os.remove(archivo_a_traducir)
# Crear un identificador de registro
# #df = pd.read_csv(archivo_csv)
df.rename(columns={'DES_DARE_CLASIFICACION_DESAPARICION': 'CLASIFICACION_DESAPARICION',
                   'var21': 'DEPARTAMENTO_DE_NACIMIENTO',
                   'var26': 'CONDICION_VULNERABILIDAD',
                   'var37': 'HORA_DESAPARICION',
                   'DES_DARE_PRESUNCION_RESPONSABILIDAD': 'PRESUNCION_RESPONSABILIDAD',
                   'var42': 'DEPARTAMENTO_DESAPARICION',
                   'var44': 'BARRIO_VEREDA_DESAPARICION',
                   'DES_DARE_NUMERO_RADICADO': 'NUMERO_RADICADO',
                   'DES_DARE_FECHA_RADICADO': 'FECHA_RADICADO',
                   'DES_DARE_ESTADO_DESAPARICION': 'ESTADO_DESAPARICION',
                   'DES_DARE_USUARIO_REGISTRA': 'USUARIO_REGISTRA',
                   'DES_DARE_ENTIDAD_REGISTRA': 'ENTIDAD_REGISTRA',
                   'DES_DARE_FECHA_REPORTE': 'FECHA_REPORTE',
                   'DES_DARE_ESTATURA': 'ESTATURA',
                   'DES_DARE_INGRESA_IDENTIFICADO': 'INGRESA_IDENTIFICADO',
                   'DES_DARE_NOMBRES': 'NOMBRES',
                   'DES_DARE_PRIMER_APELLIDO': 'PRIMER_APELLIDO',
                   'DES_DARE_SEGUNDO_APELLIDO': 'SEGUNDO_APELLIDO',
                   'DES_DARE_SEXO': 'SEXO',
                   'DES_DARE_TIPO_DOCUMENTO': 'TIPO_DOCUMENTO',
                   'DES_DARE_NUMERO_DOCUMENTO': 'NUMERO_DOCUMENTO',
                   'DES_DARE_PAIS_EXPEDICION': 'PAIS_EXPEDICION',
                   'DES_DARE_DEPARTAMENTO_EXPEDICION': 'DEPARTAMENTO_EXPEDICION',
                   'DES_DARE_MUNICIPIO_EXPEDICION': 'MUNICIPIO_EXPEDICION',
                   'DES_DARE_PAIS_DE_NACIMIENTO': 'PAIS_DE_NACIMIENTO',
                   'DES_DARE_MUNICIPIO_DE_NACIMIENTO': 'MUNICIPIO_DE_NACIMIENTO',
                   'DES_DARE_EDAD_RANGOS': 'EDAD_RANGOS',
                   'DES_DARE_UNIDAD_EDAD': 'UNIDAD_EDAD',
                   'DES_DARE_ANCESTRO_RACIAL': 'ANCESTRO_RACIAL',
                   'DES_DARE_PERTENENCIA_ETNICA': 'PERTENENCIA_ETNICA',
                   'DES_DARE_OCUPACION': 'OCUPACION',
                   'DES_DARE_ESTADO_CIVIL': 'ESTADO_CIVIL',
                   'DES_DARE_ESCOLARIDAD': 'ESCOLARIDAD',
                   'DES_DARE_PROFESION': 'PROFESION',
                   'DES_DARE_PAIS_RESIDENCIA': 'PAIS_RESIDENCIA',
                   'DES_DARE_DEPARTAMENTO_RESIDENCIA': 'DEPARTAMENTO_RESIDENCIA',
                   'DES_DARE_MUNICIPIO_RESIDENCIA': 'MUNICIPIO_RESIDENCIA',
                   'DES_DARE_DIRECCION_DESAPARECIDO': 'DIRECCION_DESAPARECIDO',
                   'DES_DARE_TELEFONO_RESIDENCIA': 'TELEFONO_RESIDENCIA',
                   'DES_DARE_FECHA_DESAPARICION': 'FECHA_DESAPARICION',
                   'DES_DARE_DIRECCION_DESAPARICION': 'DIRECCION_DESAPARICION',
                   'DES_DARE_PAIS_DESAPARICION': 'PAIS_DESAPARICION',
                   'DES_DARE_DEPARTAMENTO_DESAPARICION': 'DEPARTAMENTO_DESAPARICION',
                   'DES_DARE_MUNICIPIO_DESAPARICION': 'MUNICIPIO_DESAPARICION',
                   'DES_DARE_LOCALIDAD_HECHO': 'LOCALIDAD_HECHO',
                   'DES_DARE_ZONA_HECHO': 'ZONA_HECHO',
                   'DES_DARE_FECHA_NACIMIENTO': 'FECHA_NACIMIENTO',
                   'DES_DARE_RESUMEN_HECHOS': 'RESUMEN_HECHOS',
                   'DES_DARE_FECHA_DE_CREACION': 'FECHA_DE_CREACION',
                   'DES_DARE_FECHA_DE_ACTUALIZACION': 'FECHA_DE_ACTUALIZACION',
                   }, inplace=True)

df.columns = df.columns.str.lower()


# Ordenar el DataFrame por las columnas especificadas
columnas_ordenadas = ['nombres', 'primer_apellido', 'segundo_apellido',
                      'tipo_documento', 'numero_documento', 'unidad_edad',
                      'edad_rangos', 'fecha_nacimiento', 'sexo',
                      'pertenencia_etnica', 'departamento_desaparicion',
                      'municipio_desaparicion', 'fecha_desaparicion',
                      'numero_radicado']

df = df.sort_values(by=columnas_ordenadas)
# Renombrar una columna
df.rename(columns={'numero_radicado': 'codigo_unico_fuente'}, inplace=True)
# Origen de los datos
df['tabla_origen'] = 'INML_DES'
# Origen
df['in_inml_des'] = 1

dane = pd.read_stata(
    "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")

variables_limpieza_dane = ["departamento", "municipio"]
# Aplicar transformaciones a las columnas de tipo 'str'
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(clean_text)

# Revisar fuente estos campos
# 'clasificacion_desaparicion',
# 'presuncion_responsabilidad',
columns_to_normalize = ['nombres', 'primer_apellido', 'segundo_apellido',
                        'departamento_desaparicion', 'municipio_desaparicion']
df[columns_to_normalize] = df[columns_to_normalize].apply(clean_text)
# Datos sobre los hechos
# 	Lugar de ocurrencia
# 		País ocurrencia
df.rename(columns={'pais_desaparicion': 'pais_ocurrencia'}, inplace=True)

df = pd.merge(df, dane, how='left',
              left_on=['departamento_desaparicion',
                       'municipio_desaparicion'],
              right_on=['departamento', 'municipio'])

dane_corregir = pd.read_csv(
    "fuentes secundarias/tablas complementarias/CorrecionMunicipioRnd.csv", sep = ";")
df = pd.merge(df, dane_corregir, how='left',
              left_on=['departamento_desaparicion',
                       'municipio_desaparicion'],
              right_on=['departamento_desaparicion', 'municipio_desaparicion'])

df['municipio_desaparicion'] = np.where(
    df['codigo_dane_municipio_n'].notna(),
    df['municipio_n'], df['municipio_desaparicion'])

df['codigo_dane_municipio'] = np.where(
    df['codigo_dane_municipio_n'].notna(),
    df['codigo_dane_municipio_n'], df['codigo_dane_municipio'])

df['codigo_dane_departamento'] = np.where(
    df['codigo_dane_municipio_n'].notna(),
    df['codigo_dane_departamento_n'], df['codigo_dane_departamento'])

df_ndp = df[df["codigo_dane_municipio"].isna()]
nrow_df = len(df)
print("Registros despues left dane depto muni:", nrow_df)

# Fecha de ocurrencia: Año, mes y día de ocurrencia del hecho
df['ymd_hecho'] = df['fecha_desaparicion'].astype(str)
df['fecha_ocur_anio'] = df['ymd_hecho'].str[0:4]
df['fecha_ocur_mes'] = df['ymd_hecho'].str[5:7]
df['fecha_ocur_dia'] = df['ymd_hecho'].str[8:10]

df['fecha_ocur_anio'] = pd.to_numeric(df['fecha_ocur_anio'], errors='coerce')
df['fecha_ocur_mes'] = pd.to_numeric(df['fecha_ocur_mes'], errors='coerce')
df['fecha_ocur_dia'] = pd.to_numeric(df['fecha_ocur_dia'], errors='coerce')
homologacion.fecha.fechas_validas(df, fecha_dia='fecha_ocur_dia',
                                  fecha_mes='fecha_ocur_mes',
                                  fecha_anio='fecha_ocur_anio',
                                  fecha='fecha_desaparicion_dtf',
                                  fechat='fecha_desaparicion')
# Tipo de responsable
df.rename(columns={'presuncion_responsabilidad': 'presunto_responsable'},
          inplace=True)
# Paramilitares
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_PARAMILITARES.homologar_paramilitares(df)
# Bandas criminales y grupos armados posdesmovilización
FASE1_HOMOLOGACION_CAMPO_BANDAS_CRIMINALES.homologar_bandas_criminales(df)
# Fuerza pública y agentes del estado
FASE1_HOMOLOGACION_CAMPO_FUERZA_PUBLICA_Y_AGENTES_DEL_ESTADO.homologar_fuerzapublica(df)
# FARC
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_FARC.homologar_farc(df)
# ELN
FASE1_HOMOLOGACION_CAMPO_ESTRUCTURA_ELN.homologar_eln(df)
# Otra guerrilla y grupo guerrillero no determinado
FASE1_HOMOLOGACION_CAMPO_OTRAS_GUERRILLAS.homologar_otras_guerrillas(df)
# Otro actor- PENDIENTE
df['pres_resp_otro'] = 0
df.loc[(df['presunto_responsable'] != "") &
       (df['presunto_responsable'] == "X") &
       (df['presunto_responsable'].str.contains("PENDIENTE") == False) &
       (df['presunto_responsable'].str.contains("INFORMACION") == False) &
       (df['presunto_responsable'].str.contains("ESTABLECER") == False) &
       (df['presunto_responsable'].str.contains("DETERMINAR") == False) &
       (df['presunto_responsable'].str.contains("PRECISAR") == False) &
       (df['presunto_responsable'].str.contains("REFIERE") == False) &
       (df['presunto_responsable'].str.contains("IDENTIFICADA") == False) &
       (df['pres_resp_paramilitares'].isna() | pd.to_numeric(df['pres_resp_paramilitares']) == 0) &
       (df['pres_resp_grupos_posdesmov'].isna() | pd.to_numeric(df['pres_resp_grupos_posdesmov']) == 0 ) &
       (df['pres_resp_agentes_estatales'].isna() | pd.to_numeric(df['pres_resp_agentes_estatales']) == 0) &
       (df['pres_resp_guerr_farc'].isna() | pd.to_numeric(df['pres_resp_guerr_farc']) == 0) &
       (df['pres_resp_guerr_eln'].isna() | pd.to_numeric(df['pres_resp_guerr_eln']) == 0) &
       (df['pres_resp_guerr_otra'].isna() | pd.to_numeric(df['pres_resp_guerr_otra']) == 0), 'pres_resp_otro'] = 1

# Tipo de hecho
# Crear la variable TH_DF
df['TH_DF'] = 0
df.loc[(df['clasificacion_desaparicion'].str.contains("DESAPARICION")) &
       (df['clasificacion_desaparicion'].str.contains("FORZADA")), 'TH_DF'] = 1
# Crear la variable TH_SE
df['TH_SE'] = 0
df.loc[df['clasificacion_desaparicion'].str.contains("SECUESTRO"), 'TH_SE'] = 1
# Crear la variable TH_RU
df['TH_RU'] = 0
df.loc[df['clasificacion_desaparicion'].str.contains("RECLUTAMIENTO"),
       'TH_SE'] = 1

df['TH_OTRO'] = 0
# Convertir el texto en la columna "descripcion_relato" a mayúsculas
df.rename(columns={'resumen_hechos': 'descripcion_relato'}, inplace=True)
df['descripcion_relato'] = df['descripcion_relato'].str.upper()
# Datos sobre las personas dadas por desaparecidos
# Nombres y apellidos
df.rename(columns={'nombres': 'nombre_completo'}, inplace=True)

df[['primer_nombre',
    'segundo_nombre',
    'primer_apellido',
    'segundo_apellido']] = df['nombre_completo'].apply(
          lambda x: pd.Series(
              homologacion.nombre_completo.limpiar_nombre_completo(x)))

# Corrección del uso de artículos y preposiciones en los nombres
# Eliminar nombres y apellidos que solo tienen una letra inicial
homologacion.nombres.nombres_validos(df, primer_nombre='primer_nombre',
                                     segundo_nombre='segundo_nombre',
                                     primer_apellido='primer_apellido',
                                     segundo_apellido='segundo_apellido',
                                     nombre_completo='nombre_completo')

# Documento de identificación
df.rename(columns={'numero_documento': 'documento'}, inplace=True)
homologacion.documento.documento_valida(df, documento='documento')
# Pertenencia_etnica [NARP; INDIGENA; RROM; MESTIZO]
# Renombrar la columna
df.rename(columns={'ancestro_racial': 'iden_pertenenciaetnica'}, inplace=True)
homologacion.etnia.etnia_valida(df, etnia='iden_pertenenciaetnica')
# Validar rango de fecha de nacimiento

df['ymd_nacimiento'] = df['fecha_nacimiento'].astype(str)
df['anio_nacimiento'] = df['ymd_nacimiento'].str[0:4]
df['mes_nacimiento'] = df['ymd_nacimiento'].str[5:7]
df['dia_nacimiento'] = df['ymd_nacimiento'].str[8:10]

df['anio_nacimiento'] = pd.to_numeric(df['anio_nacimiento'], errors='coerce')
df['mes_nacimiento'] = pd.to_numeric(df['mes_nacimiento'], errors='coerce')
df['dia_nacimiento'] = pd.to_numeric(df['dia_nacimiento'], errors='coerce')

homologacion.fecha.fechas_validas(df, fecha_dia='dia_nacimiento',
                                  fecha_mes='mes_nacimiento',
                                  fecha_anio='anio_nacimiento',
                                  fechat='fecha_nacimiento',
                                  fecha='fecha_nacimiento_dft')

# Validar rango de edad
df['edad_rangos'] = df['edad_rangos'].str.strip()
df.loc[(df['edad_rangos'] == "0-0") |
       (df['edad_rangos'] == "0"), 'edad_rangos'] = "0"

# df['edad_rangos'] = df['edad_rangos'].str.strip()
# df['edad_rangos'] = df['edad_rangos'].replace({"0-0": "", "0": ""})

# Divide la columna edad_rangos en dos columnas usando el guión como separador
df[['edad_1', 'edad_2']] = df['edad_rangos'].str.split('-', expand=True)
df['edad_1'] = df['edad_1'].fillna('0')
df['edad_2'] = df['edad_2'].fillna('0')
df.loc[(df['edad_1'] == ""), 'edad_1'] = "0"
df.loc[(df['edad_2'] == ""), 'edad_2'] = "0"

# Reemplaza edad_1 con edad_2 si la diferencia es menor a 11
df['edad_1'] = df.apply(
    lambda row: row['edad_2']
    if abs(float(row['edad_2']) - float(row['edad_1'])) < 11
    else row['edad_1'], axis=1)
# Reemplaza edad_1 con cadena vacía si la diferencia es mayor o igual a 10
df['edad_1'] = df.apply(
    lambda row: ""
    if abs(float(row['edad_2']) - float(row['edad_1'])) >= 10
    and abs(float(row['edad_2']) - float(row['edad_1'])) != float('nan')
    else row['edad_1'], axis=1)
# Convierte la columna 'edad_1' a tipo numérico
df['edad_1'] = pd.to_numeric(df['edad_1'], errors='coerce')
# Renombra la columna 'edad_1' a 'edad'
df.rename(columns={'edad_1': 'edad'}, inplace=True)

df.rename(columns={'estado_desaparicion': 'situacion_actual_des'},
          inplace=True)
df['situacion_actual_des'] = df['situacion_actual_des'].replace(
    {"APARECIO MUERTO": "Apareció Muerto",
     "APARECIO VIVO": "Apareció Vivo",
     "DESAPARECIDO": "Continúa desaparecido",
     "SIN RECODIFICAR": "Sin información",
     "ANULADO": "Anulado"}, inplace=True)
# Identificación de registros que no refieren a personas individualizables
# (datos almacenados en campos de identificación que refieren
#  a otras entidades)
# Crear una nueva columna 'non_miss' que cuenta la cantidad
# de columnas no nulas para cada fila
df['non_miss'] = df[['primer_nombre', 'segundo_nombre',
                     'primer_apellido',
                     'segundo_apellido']].count(axis=1)

# Crear una nueva columna 'rni'
# que indica si la fila debe ser eliminada (1) o no (0)
df['rni'] = 0
# Marcar filas con menos de 2 columnas no nulas
# (debes ajustar el valor 2 según tus criterios)
df.loc[df['non_miss'] < 2, 'rni'] = 1
# migrado 541
df.loc[(df['primer_nombre'] == "") |
       (df['primer_apellido'] == ""), 'rni'] = 1

# Marcar filas con nombres muy cortos que puedan ser siglas o abreviaturas
df.loc[(df['primer_nombre'].str.len() < 3) &
       (df['segundo_nombre'].str.len() < 3) &
       (df['primer_apellido'].str.len() < 3) &
       (df['segundo_apellido'].str.len() < 3), 'rni'] = 1
# Crear una lista de palabras clave y marcar filas que contienen esas
# palabras en las columnas de nombres y apellidos
keywords = ["GUERRIL", "FRENTE", "BLOQ", "ORGANIZ", "ACTOR", "ARMADO",
            "ARMADA", "INTEGRANTE", "SOLDADO", "BATALLON", "BRIGADA",
            "COLUMNA", "COMANDANTE", "TENIENTE", "CAPITAN", "DRAGONEANTE",
            "EJERCITO", "SARGENTO", "DIJIN", "SIJIN", "INTENDENTE",
            "GENERAL", "MILITAR", "BRIGADIER", "FP"]
for keyword in keywords:
    df.loc[(df['primer_nombre'].str.contains(keyword)) |
           (df['segundo_nombre'].str.contains(keyword)) |
           (df['primer_apellido'].str.contains(keyword)) |
           (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Marcar filas que contienen "CORONEL" en el primer nombre
df.loc[df['primer_nombre'].str.contains("CORONEL"), 'rni'] = 1
# Marcar filas que contienen "POLICIA"
# en el primer nombre o "POLICIA" como el único nombre
df.loc[(df['primer_nombre'].str.contains("POLICIA")) |
       ((df['primer_nombre'] == "POLICIA") &
        (df['segundo_nombre'].isna() | df['segundo_nombre'] == "") &
        (df['primer_apellido'].isna() | df['primer_apellido'] == "") &
        (df['segundo_apellido'].isna() | df['segundo_apellido'] == "")),
       'rni'] = 1
# Marcar filas que contienen "FUERZA PUBLICA" en cualquiera de las
keywords_f = ["FUERZA", "PUBLICA"]
for keyword in keywords_f:
    df.loc[(df['primer_nombre'].str.contains(keyword)) |
           (df['segundo_nombre'].str.contains(keyword)) |
           (df['primer_apellido'].str.contains(keyword)) |
           (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1

# cuatro columnas de nombres y apellidos
df.loc[
       (df[['primer_nombre',
            'segundo_nombre',
            'primer_apellido',
            'segundo_apellido']].apply(
                lambda x: x.str.contains(
                    "FUERZA PUBLICA")).any(axis=1)), 'rni'] = 1
# Crear una lista de palabras clave para hechos investigados por oficio
# y marcar filas que contienen esas palabras en las
# columnas de nombres y apellidos
inv_oficiosas = ["DIREC", "OFICI", "RADICA", "ASIG", "COMPULS", "COPIA",
                 "SECCION", "ADMIN", "PUBLIC", "ESTADO", "DEFENSOR",
                 "JUZGADO", "CIRCUITO"]
for keyword in inv_oficiosas:
    df.loc[(df['primer_nombre'].str.contains(keyword)) |
           (df['segundo_nombre'].str.contains(keyword)) |
           (df['primer_apellido'].str.contains(keyword)) |
           (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Crear una lista de palabras clave para términos que indican que no
# ha sido posible individualizar a la persona y marcar filas que contienen
# esas palabras en las columnas de nombres y apellidos
no_ident = ["IDENTIFICAD", "INFORMAC", "PERSONA", "DESCONOC", "CNI",
            "MASCULINO", "FEMENINO", "DEFINIR", "ESTABLECER"]
for keyword in no_ident:
    df.loc[(df['primer_nombre'].str.contains(keyword)) |
           (df['segundo_nombre'].str.contains(keyword)) |
           (df['primer_apellido'].str.contains(keyword)) |
           (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Crear una lista de palabras clave para términos relacionados con
# comunidades y marcar filas que contienen esas palabras
# en las columnas de nombres y apellidos
comunidad = ["COMUNIDAD", "ASOCIACION", "ASIACION", "ASENT", "CORREG",
             "VERED", "CONSEJO", "CONSORC", "COMISI", "COMIT", "CABECERA",
             "MUNICIPIO", "REGIMEN", "CONSTITUC"]
for keyword in comunidad:
    df.loc[(df['primer_nombre'].str.contains(keyword)) |
           (df['segundo_nombre'].str.contains(keyword)) |
           (df['primer_apellido'].str.contains(keyword)) |
           (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Marcar filas que contienen "ZONA" o "RURAL" como nombres o apellidos
df.loc[((df['primer_nombre'].str.contains("ZONA")) &
        (df['primer_nombre'].str.len() == 4)) |
       ((df['segundo_nombre'].str.contains("ZONA")) &
        (df['segundo_nombre'].str.len() == 4)) |
       ((df['primer_apellido'].str.contains("ZONA")) &
        (df['primer_apellido'].str.len() == 4)) |
       ((df['segundo_apellido'].str.contains("ZONA")) &
        (df['segundo_apellido'].str.len() == 4)), 'rni'] = 1

df.loc[((df['primer_nombre'].str.contains("RURAL")) &
        (df['primer_nombre'].str.len() == 5)) |
       ((df['segundo_nombre'].str.contains("RURAL")) &
        (df['segundo_nombre'].str.len() == 5)) |
       ((df['primer_apellido'].str.contains("RURAL")) &
        (df['primer_apellido'].str.len() == 5)) |
       ((df['segundo_apellido'].str.contains("RURAL")) &
        (df['segundo_apellido'].str.len() == 5)), 'rni'] = 1
# Marcar filas que contienen "ALIAS" al principio del primer nombre
df.loc[df['primer_nombre'].str.startswith("ALIAS "), 'rni'] = 1

otros = ["ACTA", "ALIAS ", "SIN ", "POR "]
for keyword in otros:
    df.loc[(df['primer_nombre'].str.contains(keyword)) |
           (df['segundo_nombre'].str.contains(keyword)) |
           (df['primer_apellido'].str.contains(keyword)) |
           (df['segundo_apellido'].str.contains(keyword)), 'rni'] = 1

# Marcar filas con valores comunes de no
# identificación como "NN", "N", "XX" y "X"
df.loc[((df['primer_nombre'].isin(["NN", "N", "XX", "X"])) |
        (df['segundo_nombre'].isin(["NN", "N", "XX", "X"])) |
        (df['primer_apellido'].isin(["NN", "N", "XX", "X"])) |
        (df['segundo_apellido'].isin(["NN", "N", "XX", "X"]))), 'rni'] = 1
# Marcar filas con todas las columnas de nombres y apellidos vacías
df.loc[((df['codigo_dane_departamento'] == "") &
       (df['fecha_ocur_anio'] == "") &
       (df['documento'] == "")), 'rni'] = 1

df.loc[(df['situacion_actual_des'] == "Anulado"), 'rni'] = 1

df.loc[(df['clasificacion_desaparicion'] == "DESASTRE NATURAL") |
       (df['clasificacion_desaparicion'] == "PRESUNTA TRATA DE PERSONAS") |
       (df['clasificacion_desaparicion'] == "PARA VERIFICACIÓN DE IDENTIDAD"),
       'rni'] = 1

# Guardar las filas marcadas como rni en un archivo
df_rni = df[df['rni'] == 1]

# #df_rni.to_stata("archivos depurados/BD_FGN_INACTIVOS_PNI.dta")
df_rni.to_sql('BD_INML_RND_PNI', con=engine, if_exists='replace', index=False)
# #df_rni.to_csv("archivos depurados/BD_ICMP_PNI.csv", index=False)
# Eliminar las filas marcadas como rni del DataFrame original
df = df[df['rni'] == 0]
nrow_df = len(df)
print("Registros despues eliminar RNI:", nrow_df)
df.drop(columns=['non_miss', 'rni'], inplace=True)

cols_to_clean = ['situacion_actual_des']
for col in cols_to_clean:
    df[col] = df[col].fillna("")
# 5. Identificación de registros únicos
# Seleccionar las columnas que deseas mantener

columnas = ['tabla_origen', 'codigo_unico_fuente', 'nombre_completo',
            'primer_nombre', 'segundo_nombre', 'primer_apellido',
            'segundo_apellido', 'documento', 'sexo', 'iden_pertenenciaetnica',
            'fecha_nacimiento', 'anio_nacimiento', 'mes_nacimiento',
            'dia_nacimiento', 'edad', 'fecha_desaparicion', 'fecha_ocur_anio',
            'fecha_ocur_mes', 'fecha_ocur_dia', 'pais_ocurrencia',
            'codigo_dane_departamento', 'departamento_ocurrencia',
            'codigo_dane_municipio', 'municipio_ocurrencia',
            'TH_DF',  'TH_SE', 'TH_RU', 'TH_OTRO',
            'pres_resp_paramilitares',
            'pres_resp_grupos_posdesmov', 'pres_resp_agentes_estatales',
            'pres_resp_guerr_farc',
            'pres_resp_guerr_eln', 'pres_resp_guerr_otra', 'pres_resp_otro',
            'situacion_actual_des', 'descripcion_relato']
df = df[columnas]
df['non_miss'] = df[['primer_nombre', 'segundo_nombre',
                     'primer_apellido', 'segundo_apellido',
                     'documento', 'sexo', 'iden_pertenenciaetnica',
                     'fecha_nacimiento', 'fecha_desaparicion', 'edad',
                     'codigo_dane_departamento', 'codigo_dane_municipio',
                     'TH_DF', 'TH_SE', 'TH_RU',
                     'pres_resp_paramilitares',
                     'pres_resp_grupos_posdesmov',
                     'pres_resp_agentes_estatales',
                     'pres_resp_guerr_farc',
                     'pres_resp_guerr_eln', 'pres_resp_guerr_otra',
                     'pres_resp_otro',
                     'situacion_actual_des',
                     'descripcion_relato']].count(axis=1)
# Ordenar el DataFrame por 'codigo_unico_fuente', 'documento' y 'nonmiss'
df.sort_values(by=['codigo_unico_fuente', 'documento', 'non_miss'],
               ascending=[True, True], inplace=True)
# Mantener solo el primer registro para cada 'codigo_unico_fuente'
df.drop_duplicates(subset=['codigo_unico_fuente'], keep='first', inplace=True)
nrow_df = len(df)
print("Registros despues eliminar duplicados por codigo_unico_fuente:",
      nrow_df)
df.to_sql('BD_INML_RND', con=engine, if_exists='replace', index=False)

# #df.to_stata("archivos depurados/BD_ICMP.dta", index=False)