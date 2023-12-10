import os
import json
import time
import yaml
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
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


with open('config.json') as config_file:
    config = json.load(config_file)

start_time = time.time()
DIRECTORY_PATH = config['DIRECTORY_PATH']
DB_SERVER = config['DB_SERVER']
DB_INSTANCE = config['DB_INSTANCE']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_DATABASE = "UNIVERSO_PDD"
DB_SCHEMA = "dbo"
DB_TABLE = "INMLCF_CAD_DATOS_DE_REGISTRO"

archivo_a_borrar = os.path.join(DIRECTORY_PATH, "fuentes secundarias",
                                "V_INML_CAD.csv")

if DIRECTORY_PATH:
    if os.path.exists(archivo_a_borrar):
        os.remove(archivo_a_borrar)

encoding = "ISO-8859-1"
# La codificación ISO-8859-1

# Configurar la codificación Unicode
encoding = "ISO-8859-1"
# 1. Conexión al repositorio de información (Omitir esta sección en Python)
# 2. Cargue de datos y creación de id_registro (Omitir esta sección en Python)
# Establecer la conexión ODBC

db_url = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}\\{DB_INSTANCE}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(db_url)
# JEP-CEV: Resultados integración de información (CA_DESAPARICION)
# Cargue de datos
query = f'SELECT * FROM {DB_DATABASE}.{DB_SCHEMA}.{DB_TABLE}'

df_inml = pd.read_sql_query(query, engine)
nrow_df_ini = len(df_inml)
print("Registros despues cargue fuente: ", nrow_df_ini)
# Aplicar filtro si `2` no es una cadena vacía parametro cantidad registros
# if parametro_cantidad != "":
#   limite = int(parametro_cantidad)
#   df = df[df.index < limite]
# Guardar el DataFrame en un archivo

archivo_csv = os.path.join(DIRECTORY_PATH, "fuentes secundarias",
                           "V_INML_CAD.csv")
df_inml.to_csv(archivo_csv, index=False)
# 11 min
df_inml_copy = df_inml.copy()
###############################
# df_inml=df_inml_copy.copy()

df_inml.rename(
    columns={'var20': 'CAD_DARE_DPTO_EXPEDICION',
             'var30': 'CAD_DARE_VULNERABILIDAD',
             'var37': 'CAD_DARE_DPTO_NACIMIENTO',
             'CAD_DARE_NUMERO_RADICADO': 'NUMERO_RADICADO',
             'CAD_DARE_REGIONAL': 'REGIONAL',
             'CAD_DARE_SECCIONAL': 'SECCIONAL',
             'CAD_DARE_SITIO_ATENCION': 'SITIO_ATENCION',
             'CAD_DARE_DEPARTAMENTO_ATENCION': 'DEPARTAMENTO_ATENCION',
             'CAD_DARE_MUNICIPIO_ATENCION': 'MUNICIPIO_ATENCION',
             'CAD_DARE_TIPO_CUERPO': 'TIPO_CUERPO',
             'CAD_DARE_ESTADO_CUERPO': 'ESTADO_CUERPO',
             'CAD_DARE_FECHA_RADICADO': 'FECHA_RADICADO',
             'CAD_DARE_TIPO_CASO': 'TIPO_CASO',
             'CAD_DARE_ESTADO_RECEPCION': 'ESTADO_RECEPCION',
             'CAD_DARE_ESTADO_IDENTIFICACION': 'ESTADO_IDENTIFICACION',
             'CAD_DARE_ESTADO_ENTREGA': 'ESTADO_ENTREGA',
             'CAD_DARE_ESTADO_ACTIVO': 'ESTADO_ACTIVO',
             'CAD_DARE_CERTIFICADO_DEFUNCION': 'CERTIFICADO_DEFUNCION',
             'CAD_DARE_INGRESA_IDENTIFICADO': 'INGRESA_IDENTIFICADO',
             'CAD_DARE_TIPO_DOCUMENTO': 'TIPO_DOCUMENTO',
             'CAD_DARE_NUMERO_DOCUMENTO': 'NUMERO_DOCUMENTO',
             'CAD_DARE_PAIS_DE_EXPEDICION': 'PAIS_DE_EXPEDICION',
             'CAD_DARE_DPTO_EXPEDICION': 'DPTO_EXPEDICION',
             'CAD_DARE_MUNICIPIO_EXPEDICION': 'MUNICIPIO_EXPEDICION',
             'CAD_DARE_NOMBRES': 'NOMBRES',
             'CAD_DARE_PRIMER_APELLIDO': 'PRIMER_APELLIDO',
             'CAD_DARE_SEGUNDO_APELLIDO': 'SEGUNDO_APELLIDO',
             'CAD_DARE_SEXO': 'SEXO',
             'CAD_DARE_RANGO_EDAD': 'RANGO_EDAD',
             'CAD_DARE_UNIDAD_EDAD': 'UNIDAD_EDAD',
             'CAD_DARE_ANCESTRO_RACIAL': 'ANCESTRO_RACIAL',
             'CAD_DARE_PERTENENCIA_ETNICA': 'PERTENENCIA_ETNICA',
             'CAD_DARE_VULNERABILIDAD': 'VULNERABILIDAD',
             'CAD_DARE_OCUPACION': 'OCUPACION',
             'CAD_DARE_ESTADO_CIVIL': 'ESTADO_CIVIL',
             'CAD_DARE_ESCOLARIDAD': 'ESCOLARIDAD',
             'CAD_DARE_PROFESION': 'PROFESION',
             'CAD_DARE_PAIS_DE_NACIMIENTO': 'PAIS_DE_NACIMIENTO',
             'CAD_DARE_MUNICIPIO_DE_NACIMIENTO': 'MUNICIPIO_DE_NACIMIENTO',
             'CAD_DARE_DPTO_NACIMIENTO': 'DPTO_NACIMIENTO',
             'CAD_DARE_NUNC': 'NUNC',
             'CAD_DARE_FECHA_INSPECCION': 'FECHA_INSPECCION',
             'CAD_DARE_DEPARTAMENTO_INSPECCION': 'DEPARTAMENTO_INSPECCION',
             'CAD_DARE_MUNICIPIO_INSPECCION': 'MUNICIPIO_INSPECCION',
             'CAD_DARE_ACTA_INSPECCION': 'ACTA_INSPECCION',
             'CAD_DARE_RADICADO_FOSA': 'RADICADO_FOSA',
             'CAD_DARE_NUMERO_FOSA': 'NUMERO_FOSA',
             'CAD_DARE_ACTA_FOSA': 'ACTA_FOSA',
             'CAD_DARE_FASE': 'FASE',
             'CAD_DARE_AUTORIDAD_SOLICITANTE': 'AUTORIDAD_SOLICITANTE',
             'CAD_DARE_UNIDAD_LABORATORIO': 'UNIDAD_LABORATORIO',
             'CAD_DARE_DIRECCION_INSPECCION': 'DIRECCION_INSPECCION',
             'CAD_DARE_SE_CONOCE_FECHA_HECHO': 'SE_CONOCE_FECHA_HECHO',
             'CAD_DARE_FECHA_HECHO': 'FECHA_HECHO',
             'CAD_DARE_SE_CONOCE_HORA_HECHO': 'SE_CONOCE_HORA_HECHO',
             'CAD_DARE_HORA_HECHO': 'HORA_HECHO',
             'CAD_DARE_ESCENARIO_HECHO': 'ESCENARIO_HECHO',
             'CAD_DARE_ACTIVIDAD_HECHO': 'ACTIVIDAD_HECHO',
             'CAD_DARE_CIRCUNSTANCIA_HECHO': 'CIRCUNSTANCIA_HECHO',
             'CAD_DARE_DIRECCION_HECHOS': 'DIRECCION_HECHOS',
             'CAD_DARE_PAIS_HECHOS': 'PAIS_HECHOS',
             'CAD_DARE_DEPARTAMENTO_HECHO': 'DEPARTAMENTO_HECHO',
             'CAD_DARE_MUNICIPIO_HECHO': 'MUNICIPIO_HECHO',
             'CAD_DARE_RESUMEN_HECHOS': 'RESUMEN_HECHOS',
             'CAD_DARE_FECHA_DE_CREACION': 'FECHA_DE_CREACION',
             'CAD_DARE_FECHA_DE_ACTUALIZACION': 'FECHA_DE_ACTUALIZACION'
             }, inplace=True)
df_inml.columns = df_inml.columns.str.lower()

# Ordenar el DataFrame por las columnas especificadas
columnas_ordenadas = ['nombres', 'primer_apellido', 'segundo_apellido',
                      'numero_documento', 'sexo',
                      'rango_edad', 'pertenencia_etnica',
                      'pais_hechos', 'departamento_hecho', 'municipio_hecho',
                      'fecha_hecho', 'numero_radicado']

df_inml = df_inml.sort_values(by=columnas_ordenadas)
# print(len(df)) 531620
# Crear identificador único de registro
df_inml['duplicates_reg'] = df_inml.duplicated()
df_inml = df_inml[~df_inml['duplicates_reg']]
nrow_df = len(df_inml)
print("Registros despues eliminar duplicados: ", nrow_df)

# Renombrar columna
df_inml.rename(columns={'numero_radicado': 'codigo_unico_fuente'},
               inplace=True)
# Origen de los datos
df_inml['tabla_origen'] = 'INML_CAD'
# Origen
df_inml['in_inml_cad'] = 1

###################

dane_file_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias",
                              "tablas complementarias",
                              "DIVIPOLA_municipios_122021.dta")
dane = pd.read_stata(dane_file_path)

variables_limpieza_dane = ["departamento", "municipio"]
# Aplicar transformaciones a las columnas de tipo 'str'
dane[variables_limpieza_dane] = dane[variables_limpieza_dane].apply(clean_text)

df_inml.rename(columns={'departamento_hecho': 'departamento_desaparicion',
                        'municipio_hecho': 'municipio_desaparicion'},
               inplace=True)


columns_to_normalize = ['nombres', 'primer_apellido', 'segundo_apellido',
                        'pais_hechos', 'departamento_desaparicion',
                        'municipio_desaparicion', 'estado_identificacion',
                        'estado_entrega', 'estado_activo']
df_inml[columns_to_normalize] = df_inml[columns_to_normalize].apply(clean_text)
# Datos sobre los hechos
# 	Lugar de ocurrencia
# 		País ocurrencia
df_inml.rename(columns={'pais_hechos': 'pais_ocurrencia'}, inplace=True)

df_inml = pd.merge(df_inml, dane, how='left',
                   left_on=['departamento_desaparicion',
                            'municipio_desaparicion'],
                   right_on=['departamento', 'municipio'])

dane_corregir = pd.read_csv(
    DIRECTORY_PATH +
    "fuentes secundarias/tablas complementarias/CorrecionMunicipioRnd.csv",
    sep=";")
df_inml = pd.merge(df_inml, dane_corregir, how='left',
                   left_on=['departamento_desaparicion',
                            'municipio_desaparicion'],
                   right_on=['departamento_desaparicion',
                             'municipio_desaparicion'])

df_inml['municipio_desaparicion'] = np.where(
    df_inml['codigo_dane_municipio_n'].notna(),
    df_inml['municipio_n'], df_inml['municipio_desaparicion'])

df_inml['codigo_dane_municipio'] = np.where(
    df_inml['codigo_dane_municipio_n'].notna(),
    df_inml['codigo_dane_municipio_n'], df_inml['codigo_dane_municipio'])

df_inml['codigo_dane_departamento'] = np.where(
    df_inml['codigo_dane_municipio_n'].notna(),
    df_inml['codigo_dane_departamento_n'], df_inml['codigo_dane_departamento'])

df_inml_ndp = df_inml[df_inml["codigo_dane_municipio"].isna()]
nrow_df = len(df_inml)
print("Registros despues left dane depto muni:", nrow_df)

# Fecha de ocurrencia: Año, mes y día de ocurrencia del hecho
df_inml['ymd_hecho'] = df_inml['fecha_hecho'].astype(str)
df_inml['fecha_ocur_anio'] = df_inml['ymd_hecho'].str[0:4]
df_inml['fecha_ocur_mes'] = df_inml['ymd_hecho'].str[5:7]
df_inml['fecha_ocur_dia'] = df_inml['ymd_hecho'].str[8:10]

df_inml['fecha_ocur_anio'] = df_inml[
    'fecha_ocur_anio'].str.replace('18', '19', n=1)
df_inml['fecha_ocur_anio'] = df_inml[
    'fecha_ocur_anio'].str.replace('179', '197', n=1)
df_inml['fecha_ocur_anio'] = df_inml[
    'fecha_ocur_anio'].str.replace('169', '196', n=1)
df_inml['fecha_ocur_anio'] = df_inml[
    'fecha_ocur_anio'].str.replace('159', '195', n=1)

df_inml['fecha_ocur_anio'] = pd.to_numeric(
    df_inml['fecha_ocur_anio'], errors='coerce')
df_inml['fecha_ocur_mes'] = pd.to_numeric(
    df_inml['fecha_ocur_mes'], errors='coerce')
df_inml['fecha_ocur_dia'] = pd.to_numeric(
    df_inml['fecha_ocur_dia'], errors='coerce')

homologacion.fecha.fechas_validas(df_inml, fecha_dia='fecha_ocur_dia',
                                  fecha_mes='fecha_ocur_mes',
                                  fecha_anio='fecha_ocur_anio',
                                  fecha='fecha_desaparicion_dtf',
                                  fechat='fecha_desaparicion')

# Convertir el texto en la columna "descripcion_relato" a mayúsculas
df_inml.rename(columns={'resumen_hechos': 'descripcion_relato'}, inplace=True)
df_inml['descripcion_relato'] = df_inml['descripcion_relato'].str.upper()

# Datos sobre las personas dadas por desaparecidos
# Nombres y apellidos
df_inml.rename(columns={'nombres': 'nombre_completo'}, inplace=True)

df_inml[['primer_nombre',
         'segundo_nombre',
         'primer_apellido',
         'segundo_apellido']] = df_inml['nombre_completo'].apply(
          lambda x: pd.Series(
              homologacion.nombre_completo.limpiar_nombre_completo(x)))

# Corrección del uso de artículos y preposiciones en los nombres
# Eliminar nombres y apellidos que solo tienen una letra inicial
homologacion.nombres.nombres_validos(df_inml, primer_nombre='primer_nombre',
                                     segundo_nombre='segundo_nombre',
                                     primer_apellido='primer_apellido',
                                     segundo_apellido='segundo_apellido',
                                     nombre_completo='nombre_completo')

# Documento de identificación
df_inml.rename(columns={'numero_documento': 'documento'}, inplace=True)
homologacion.documento.documento_valida(df_inml, documento='documento')

# Renombrar la columna
df_inml.rename(columns={'ancestro_racial': 'iden_pertenenciaetnica'},
               inplace=True)
homologacion.etnia.etnia_valida(df_inml, etnia='iden_pertenenciaetnica')

# Validar rango de edad
df_inml['rango_edad'] = df_inml['rango_edad'].str.strip()
df_inml.loc[(df_inml['rango_edad'] == "0-0") |
            (df_inml['rango_edad'] == "0"), 'rango_edad'] = "0"

# Divide la columna edad_rangos en dos columnas usando el guión como separador
df_inml[['edad_1', 'edad_2']] = df_inml['rango_edad'].str.split('-',
                                                                expand=True)
df_inml['edad_1'] = df_inml['edad_1'].fillna('0')
df_inml['edad_2'] = df_inml['edad_2'].fillna('0')
df_inml.loc[(df_inml['edad_1'] == ""), 'edad_1'] = "0"
df_inml.loc[(df_inml['edad_2'] == ""), 'edad_2'] = "0"

# Reemplaza edad_1 con edad_2 si la diferencia es menor a 11
df_inml['edad_1'] = df_inml.apply(
    lambda row: row['edad_2']
    if abs(float(row['edad_2']) - float(row['edad_1'])) < 11
    else row['edad_1'], axis=1)
# Reemplaza edad_1 con cadena vacía si la diferencia es mayor o igual a 10
df_inml['edad_1'] = df_inml.apply(
    lambda row: ""
    if abs(float(row['edad_2']) - float(row['edad_1'])) >= 10
    and abs(float(row['edad_2']) - float(row['edad_1'])) != float('nan')
    else row['edad_1'], axis=1)
# Convierte la columna 'edad_1' a tipo numérico
df_inml['edad_1'] = pd.to_numeric(df_inml['edad_1'], errors='coerce')
# Renombra la columna 'edad_1' a 'edad'
df_inml.rename(columns={'edad_1': 'edad'}, inplace=True)

df_inml['situacion_actual_des'] = np.where(
    ((df_inml['estado_identificacion'] == "PENDIENTE") &
     (df_inml['estado_entrega'] == "CONCLUIDO")) |
    (df_inml['estado_identificacion'] == "CONCLUIDO"), "Apareció Muerto", "")

df_inml['situacion_actual_des'] = np.where(
    (df_inml['estado_identificacion'] == "ANULADO"),
    "Anulado", df_inml['situacion_actual_des'])

# Identificación de registros que no refieren a personas individualizables
# (datos almacenados en campos de identificación que refieren
#  a otras entidades)
# Crear una nueva columna 'non_miss' que cuenta la cantidad
# de columnas no nulas para cada fila
nrow_df_fin = len(df_inml)
df_inml['non_miss'] = df_inml[['primer_nombre',
                               'segundo_nombre',
                               'primer_apellido',
                               'segundo_apellido']].count(axis=1)

# Crear una nueva columna 'rni'
# que indica si la fila debe ser eliminada (1) o no (0)
df_inml['rni'] = 0
# Marcar filas con menos de 2 columnas no nulas
# (debes ajustar el valor 2 según tus criterios)
df_inml.loc[df_inml['non_miss'] < 2, 'rni'] = 1
# migrado 541
df_inml.loc[(df_inml['primer_nombre'] == "") |
            (df_inml['primer_apellido'] == ""), 'rni'] = 1

# Marcar filas con nombres muy cortos que puedan ser siglas o abreviaturas
df_inml.loc[(df_inml['primer_nombre'].str.len() < 3) &
            (df_inml['segundo_nombre'].str.len() < 3) &
            (df_inml['primer_apellido'].str.len() < 3) &
            (df_inml['segundo_apellido'].str.len() < 3), 'rni'] = 1
# Crear una lista de palabras clave y marcar filas que contienen esas
# palabras en las columnas de nombres y apellidos
keywords = ["GUERRIL", "FRENTE", "BLOQ", "ORGANIZ", "ACTOR", "ARMADO",
            "ARMADA", "INTEGRANTE", "SOLDADO", "BATALLON", "BRIGADA",
            "COLUMNA", "COMANDANTE", "TENIENTE", "CAPITAN", "DRAGONEANTE",
            "EJERCITO", "SARGENTO", "DIJIN", "SIJIN", "INTENDENTE",
            "GENERAL", "MILITAR", "BRIGADIER"]
for keyword in keywords:
    df_inml.loc[(df_inml['primer_nombre'].str.contains(keyword)) |
                (df_inml['segundo_nombre'].str.contains(keyword)) |
                (df_inml['primer_apellido'].str.contains(keyword)) |
                (df_inml['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Marcar filas que contienen "CORONEL" en el primer nombre
df_inml.loc[df_inml['primer_nombre'].str.contains("CORONEL"), 'rni'] = 1
# Marcar filas que contienen "POLICIA"
# en el primer nombre o "POLICIA" como el único nombre
df_inml.loc[(df_inml['primer_nombre'].str.contains("POLICIA")) |
            (df_inml['primer_nombre'] == "POLICIA"), 'rni'] = 1
# Marcar filas que contienen "FUERZA PUBLICA" en cualquiera de las
keywords_f = ["FUERZA", "PUBLICA"]
for keyword in keywords_f:
    df_inml.loc[(df_inml['primer_nombre'].str.contains(keyword)) |
                (df_inml['segundo_nombre'].str.contains(keyword)) |
                (df_inml['primer_apellido'].str.contains(keyword)) |
                (df_inml['segundo_apellido'].str.contains(keyword)), 'rni'] = 1

# cuatro columnas de nombres y apellidos
df_inml.loc[
       (df_inml[['primer_nombre',
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
    df_inml.loc[(df_inml['primer_nombre'].str.contains(keyword)) |
                (df_inml['segundo_nombre'].str.contains(keyword)) |
                (df_inml['primer_apellido'].str.contains(keyword)) |
                (df_inml['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Crear una lista de palabras clave para términos que indican que no
# ha sido posible individualizar a la persona y marcar filas que contienen
# esas palabras en las columnas de nombres y apellidos

no_ident = ["IDENTIFICAD", "INFORMAC", "PERSONA", "DESCONOC", "CNI",
            "MASCULINO", "FEMENINO", "DEFINIR", "ESTABLECER"]
for keyword in no_ident:
    df_inml.loc[(df_inml['primer_nombre'].str.contains(keyword)) |
                (df_inml['segundo_nombre'].str.contains(keyword)) |
                (df_inml['primer_apellido'].str.contains(keyword)) |
                (df_inml['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Crear una lista de palabras clave para términos relacionados con
# comunidades y marcar filas que contienen esas palabras
# en las columnas de nombres y apellidos

comunidad = ["COMUNIDAD", "ASOCIACION", "ASIACION", "ASENT", "CORREG",
             "VERED", "CONSEJO", "CONSORC", "COMISI", "COMIT", "CABECERA",
             "MUNICIPIO", "REGIMEN", "CONSTITUC"]
for keyword in comunidad:
    df_inml.loc[(df_inml['primer_nombre'].str.contains(keyword)) |
                (df_inml['segundo_nombre'].str.contains(keyword)) |
                (df_inml['primer_apellido'].str.contains(keyword)) |
                (df_inml['segundo_apellido'].str.contains(keyword)), 'rni'] = 1
# Marcar filas que contienen "ZONA" o "RURAL" como nombres o apellidos
df_inml.loc[((df_inml['primer_nombre'].str.contains("ZONA")) &
             (df_inml['primer_nombre'].str.len() == 4)) |
            ((df_inml['segundo_nombre'].str.contains("ZONA")) &
             (df_inml['segundo_nombre'].str.len() == 4)) |
            ((df_inml['primer_apellido'].str.contains("ZONA")) &
             (df_inml['primer_apellido'].str.len() == 4)) |
            ((df_inml['segundo_apellido'].str.contains("ZONA")) &
             (df_inml['segundo_apellido'].str.len() == 4)), 'rni'] = 1

df_inml.loc[((df_inml['primer_nombre'].str.contains("RURAL")) &
             (df_inml['primer_nombre'].str.len() == 5)) |
            ((df_inml['segundo_nombre'].str.contains("RURAL")) &
             (df_inml['segundo_nombre'].str.len() == 5)) |
            ((df_inml['primer_apellido'].str.contains("RURAL")) &
             (df_inml['primer_apellido'].str.len() == 5)) |
            ((df_inml['segundo_apellido'].str.contains("RURAL")) &
             (df_inml['segundo_apellido'].str.len() == 5)), 'rni'] = 1
# Marcar filas que contienen "ALIAS" al principio del primer nombre
df_inml.loc[df_inml['primer_nombre'].str.startswith("ALIAS "), 'rni'] = 1

otros = ["ACTA", "ALIAS ", "SIN ", "POR "]
for keyword in otros:
    df_inml.loc[(df_inml['primer_nombre'].str.contains(keyword)) |
                (df_inml['segundo_nombre'].str.contains(keyword)) |
                (df_inml['primer_apellido'].str.contains(keyword)) |
                (df_inml['segundo_apellido'].str.contains(keyword)), 'rni'] = 1

# Marcar filas con valores comunes de no
# identificación como "NN", "N", "XX" y "X"
df_inml.loc[((df_inml['primer_nombre'].isin(["NN", "N", "XX", "X"])) |
             (df_inml['segundo_nombre'].isin(["NN", "N", "XX", "X"])) |
             (df_inml['primer_apellido'].isin(["NN", "N", "XX", "X"])) |
             (df_inml['segundo_apellido'].isin(["NN", "N", "XX", "X"]))),
            'rni'] = 1

df_inml.loc[(df_inml['primer_nombre'].str.contains("FETO")) |
            (df_inml['segundo_nombre'].str.contains("MORTINATO")) |
            (df_inml['primer_apellido'].str.contains("NACIDO")), 'rni'] = 1

df_inml.loc[((df_inml['codigo_dane_departamento'] == "") &
             (df_inml['fecha_ocur_anio'] == "") &
             (df_inml['documento'] == "")), 'rni'] = 1

df_inml.loc[(df_inml['situacion_actual_des'] == "Anulado"), 'rni'] = 1

# Guardar las filas marcadas como rni en un archivo
df_inml_rni = df_inml[df_inml['rni'] == 1]
nrow_df_no_ident = len(df_inml_rni)

nrow_df = len(df_inml)
print("Registros :", nrow_df)
# 531620
print("Registros NI:", nrow_df_no_ident)
# 142555

BD_INML_CAD_PNI_file_path = os.path.join(
    DIRECTORY_PATH, "archivos depurados", "BD_INML_CAD_PNI.csv")
df_inml_rni.to_csv(BD_INML_CAD_PNI_file_path, index=False)

DB_SCHEMA = "version5"
DB_TABLE = "BD_INML_CAD_PNI"
chunk_size = 1000
with engine.connect() as conn, conn.begin():
    df_inml_rni.to_sql(name=DB_TABLE, con=engine,
                       schema=DB_SCHEMA, if_exists='replace', index=False,
                       chunksize=chunk_size)

# Eliminar las filas marcadas como rni del DataFrame original
df_inml = df_inml[df_inml['rni'] == 0]
nrow_df = len(df_inml)
nrow_df_ident = nrow_df
print("Registros despues eliminar RNI:", nrow_df)
df_inml.drop(columns=['non_miss', 'rni'], inplace=True)

cols_to_clean = ['situacion_actual_des']
for col in cols_to_clean:
    df_inml[col] = df_inml[col].fillna("")
# 5. Identificación de registros únicos
# Seleccionar las columnas que deseas mantener
columnas = ['tabla_origen', 'codigo_unico_fuente', 'nombre_completo',
            'primer_nombre', 'segundo_nombre', 'primer_apellido',
            'segundo_apellido', 'documento', 'sexo', 'iden_pertenenciaetnica',
            'edad', 'fecha_desaparicion', 'fecha_desaparicion_dtf',
            'fecha_ocur_anio', 'fecha_ocur_mes', 'fecha_ocur_dia',
            'codigo_dane_departamento', 'departamento_desaparicion',
            'codigo_dane_municipio', 'municipio_desaparicion',
            'situacion_actual_des', 'descripcion_relato']
df_inml = df_inml[columnas]

df_inml['non_miss'] = df_inml[['primer_nombre',
                               'segundo_nombre',
                               'primer_apellido', 'segundo_apellido',
                               'documento', 'sexo', 'iden_pertenenciaetnica',
                               'fecha_desaparicion', 'edad',
                               'codigo_dane_departamento',
                               'codigo_dane_municipio',
                               'situacion_actual_des',
                               'descripcion_relato']].count(axis=1)
# Ordenar el DataFrame por 'codigo_unico_fuente', 'documento' y 'nonmiss'
df_inml.sort_values(by=['codigo_unico_fuente', 'documento', 'non_miss'],
                    ascending=[True, True, True], inplace=True)
# Mantener solo el primer registro para cada 'codigo_unico_fuente'
df_inml.drop_duplicates(subset=['codigo_unico_fuente'], keep='first',
                        inplace=True)
nrow_df_ident = len(df_inml)
n_duplicados = nrow_df_ini - nrow_df_ident
print("Registros despues eliminar duplicados por codigo_unico_fuente:",
      nrow_df_ident)

BD_INML_CAD_file_path = os.path.join(
    DIRECTORY_PATH, "archivos depurados", "BD_INML_CAD.csv")
df_inml.to_csv(BD_INML_CAD_file_path, index=False)

DB_SCHEMA = "version5"
DB_TABLE = "BD_INML_CAD"

with engine.connect() as conn, conn.begin():
    df_inml.to_sql(name=DB_TABLE, con=engine,
                   schema=DB_SCHEMA, if_exists='replace', index=False,
                   chunksize=chunk_size)

fecha_fin = time.time()

log = {
    "fecha_inicio": str(start_time),
    "fecha_fin": str(fecha_fin),
    "tiempo_ejecucion": str(fecha_fin - start_time),
    'filas_iniciales_df': nrow_df_ini,
    'filas_final_df': nrow_df_fin,
    'filas_df_ident': nrow_df_ident,
    'filas_df_no_ident': nrow_df_no_ident,
    'n_duplicados': n_duplicados,
}

log_file_path = os.path.join(
    DIRECTORY_PATH, "log", "resultado_df_inml_cad.yaml")
with open(log_file_path, 'w') as file:
    yaml.dump(log, file)
    # Registra el tiempo de finalización
end_time = time.time()

# Calcula el tiempo transcurrido
elapsed_time = end_time - start_time

print(f"Tiempo transcurrido: {elapsed_time/60} segundos")
