import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime
import yaml
import json
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


def clean_text(text):
    if text is None or text.isna().any():
        text = text.astype(str)
    text = text.apply(homologacion.limpieza.normalize_text)
    return text


# import config
with open('config.json') as config_file:
    config = json.load(config_file)

directory_path = config['DIRECTORY_PATH']
db_server = config['DB_SERVER']
db_username = config['DB_USERNAME']
db_password = config['DB_PASSWORD']

db_database = "ubpd_base"

# parametros programa stata
parametro_ruta = ""
parametro_cantidad = ""
# Establecer la ruta de trabajo
ruta = directory_path

# Verificar si `1` es una cadena vacía y ajustar el directorio de trabajo
# en consecuencia
if parametro_ruta == "":
    os.chdir(ruta)
else:
    os.chdir(parametro_ruta)
# Borrar el archivo "fuentes secundarias\V_ICMP.dta"
archivo_a_borrar = os.path.join("fuentes secundarias",
                                "V_ICMP.dta")
if os.path.exists(archivo_a_borrar):
    os.remove(archivo_a_borrar)
# Configurar la codificación Unicode
encoding = "ISO-8859-1"
# 1. Conexión al repositorio de información (Omitir esta sección en Python)
# 2. Cargue de datos y creación de id_registro (Omitir esta sección en Python)
# Establecer la conexión ODBC
fecha_inicio = datetime.now()
db_url = f'mssql+pyodbc://{db_username}:{db_password}@{db_server}/{db_database}?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(db_url)
# JEP-CEV: Resultados integración de información (CA_DESAPARICION)
# Cargue de datos
query = "EXECUTE [dbo].[CONSULTA_V_ICMP]"
df = pd.read_sql_query(query, engine)
nrow_df_ini = len(df)
print("Registros despues cargue fuente: ", nrow_df_ini)
# Aplicar filtro si `2` no es una cadena vacía parametro cantidad registros
if parametro_cantidad != "":
    limite = int(parametro_cantidad)
    df = df[df.index < limite]
# Guardar el DataFrame en un archivo
archivo_csv = os.path.join("fuentes secundarias",
                           "V_ICMP.csv")
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
df.columns = df.columns.str.lower()
df['duplicates_reg'] = df.duplicated()
df = df[~df['duplicates_reg']]

# Ordenar el DataFrame por las columnas especificadas
columnas_ordenadas = ['primer_nombre', 'segundo_nombre', 'primer_apellido',
                      'segundo_apellido', 'documento', 'sexo', 'edad_des_inf',
                      'edad_des_sup', 'anio_nacimiento', 'mes_nacimiento',
                      'dia_nacimiento', 'iden_pertenenciaetnica',
                      'codigo_dane_departamento', 'codigo_dane_municipio',
                      'fecha_ocur_dia', 'fecha_ocur_mes', 'fecha_ocur_anio',
                      'presunto_responsable', 'codigo_unico_fuente']

df = df.sort_values(by=columnas_ordenadas)
# Renombrar una columna
df.rename(columns={'fuente': 'tabla_origen'}, inplace=True)
# Origen de los datos
df['tabla_origen'] = 'ICMP'
# Origen
df['in_icmp'] = 1

df['codigo_unico_fuente'] = df['codigo_unico_fuente'].apply(
    lambda x: f'{x:08.0f}')
# Guardar el DataFrame final en un archivo
# #df.to_stata(archivo_utf8, write_index=False)
# Cambiar el nombre de las columnas a minúsculas
# #df.columns = df.columns.str.lower()
# #df.to_stata(archivo_utf8, index=False)
# 1.Selección de variable a homologar
# Normalización de los campos
columns_to_normalize = ['nombre_completo', 'primer_nombre', 'segundo_nombre',
                        'primer_apellido', 'segundo_apellido',
                        'pais_ocurrencia', 'sexo', 'descripcion_relato']
df[columns_to_normalize] = df[columns_to_normalize].apply(clean_text)

na_values = {
    'NO APLICA': None,
    'NULL': None,
    'ND': None,
    'NA': None,
    'SIN INFOR': None,
    'SIN DETERM': None,
    'POR DEFINIR': None,
    'NONE': None,
    'Indeterminado': None}

df[columns_to_normalize] = df[columns_to_normalize].replace(na_values)

df['pais_ocurrencia'].replace({"UNITED STATES": "ESTADOS UNIDOS"},
                              inplace=True)
# Realizar un merge con el archivo DIVIPOLA_departamentos_122021.dta
dane = pd.read_stata(
    "fuentes secundarias/tablas complementarias/DIVIPOLA_municipios_122021.dta")
# Renombrar columnas
dane = dane.rename(columns={
    'codigo_dane_departamento': 'codigo_dane_departamento',
    'departamento': 'departamento_ocurrencia',
    'codigo_dane_municipio': 'codigo_dane_municipio',
    'municipio': 'municipio_ocurrencia'
})
# Realizar la unión (left join) con "dane"
df = pd.merge(df, dane, how='left', left_on=['codigo_dane_departamento',
                                             'codigo_dane_municipio'],
              right_on=['codigo_dane_departamento', 'codigo_dane_municipio'])
nrow_df = len(df)
print("Registros despues left dane depto muni:", nrow_df)

# Fecha de ocurrencia
homologacion.fecha.fechas_validas(df, fecha_dia='fecha_ocur_dia',
                                  fecha_mes='fecha_ocur_mes',
                                  fecha_anio='fecha_ocur_anio',
                                  fecha='fecha_desaparicion_dtf',
                                  fechat='fecha_desaparicion')
# Guardar el DataFrame en un archivo
# #df.to_stata("archivos depurados/BD_FGN_INACTIVOS.dta", index=False)
# Convertir la columna "presunto_responsable" a cadena
df['presunto_responsable'] = df['presunto_responsable'].astype(str)
# Reemplazar las celdas que contienen un punto (".") con un valor vacío ("")
df['presunto_responsable'] = np.where(df['presunto_responsable'].isna(),
                                      "", df['presunto_responsable'])
# Tipo de responsable
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
# Aplicar las condiciones y asignar 1 a pres_resp_otro
# cuando todas las condiciones se cumplan.
df['pres_resp_otro'] = 0
df.loc[(df['presunto_responsable'] != "") &
       (df['presunto_responsable'] != "X") &
       (df['presunto_responsable'].str.contains("PENDIENTE") == False) &
       (df['presunto_responsable'].str.contains("INFORMACION") == False) &
       (df['presunto_responsable'].str.contains("DETERMINAR") == False) &
       (df['presunto_responsable'].str.contains("PRECISAR") == False) &
       (df['presunto_responsable'].str.contains("REFIERE") == False) &
       (df['presunto_responsable'].str.contains("IDENTIFICADA") == False) &
       (df['pres_resp_paramilitares'].isna()) &
       (df['pres_resp_grupos_posdesmov'].isna()) &
       (df['pres_resp_agentes_estatales'].isna()) &
       (df['pres_resp_guerr_farc'].isna()) &
       (df['pres_resp_guerr_eln'].isna()) &
       (df['pres_resp_guerr_otra'].isna()), 'pres_resp_otro'] = 1
# Tipos de hechos de interés: Desaparición Forzada, Secuestro y Reclutamiento
# Convertir la columna "tipo_de_hecho" a cadena
df['tipo_de_hecho'] = df['tipo_de_hecho'].astype(str)
# Crear variables binarias basadas en "tipo_de_hecho"
df['TH_DF'] = (df['tipo_de_hecho'].str.contains("DESAPARICION", case=False) &
               df['tipo_de_hecho'].str.contains("FORZADA",
                                                case=False)).astype(int)
df['TH_SE'] = (df['tipo_de_hecho'].str.contains("SECUESTRO",
                                                case=False)).astype(int)
df['TH_RU'] = (df['tipo_de_hecho'].str.contains("RECLUTAMIENTO",
                                                case=False)).astype(int)
df['TH_OTRO'] = ((df['TH_DF'] == 0) &
                 (df['TH_SE'] == 0) &
                 (df['TH_RU'] == 0)).astype(int)
# Convertir el texto en la columna "descripcion_relato" a mayúsculas
df['descripcion_relato'] = df['descripcion_relato'].str.upper()
# Datos sobre las personas dadas por desaparecidos
# Nombres y apellidos
# Corrección del uso de artículos y preposiciones en los nombres
# Eliminar nombres y apellidos que solo tienen una letra inicial
homologacion.nombres.nombres_validos(df, primer_nombre='primer_nombre',
                                     segundo_nombre='segundo_nombre',
                                     primer_apellido='primer_apellido',
                                     segundo_apellido='segundo_apellido',
                                     nombre_completo='nombre_completo')

# Documento de identificación
df['documento'].fillna('0', inplace=True)
homologacion.documento.documento_valida(df, documento='documento')
# Pertenencia_etnica [NARP; INDIGENA; RROM; MESTIZO]
# Renombrar la columna
homologacion.etnia.etnia_valida(df, etnia='iden_pertenenciaetnica')
# Validar rango de fecha de nacimiento


homologacion.fecha.fechas_validas(df, fecha_dia='dia_nacimiento',
                                  fecha_mes='mes_nacimiento',
                                  fecha_anio='anio_nacimiento',
                                  fechat='fecha_nacimiento',
                                  fecha='fecha_nacimiento_dft')

# Validar rango de edad
df['edad_des_inf'].fillna(value=0, inplace=True)
df['edad_des_sup'].fillna(value=0, inplace=True)
df.loc[(df['edad_des_inf'] != 0) &
       (df['edad_des_sup'] == 0), 'edad'] = df['edad_des_inf']
df.loc[(df['edad_des_inf'] == 0) &
       (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_sup']
df.loc[(df['edad_des_inf'] < df['edad_des_sup']) &
       (df['edad_des_inf'] != 0) &
       (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_inf']

df.loc[(df['edad_des_inf'] >= df['edad_des_sup']) &
       (df['edad_des_inf'] != 0) &
       (df['edad_des_sup'] != 0), 'edad'] = df['edad_des_sup']
df['edad'] = np.where(df['edad'] > 100, np.nan, df['edad'])
df['edad'] = np.where((df['edad_des_inf'] == 0) &
                      (df['edad_des_sup'] == 0), 0, df['edad'])
# Eliminar columnas de edad desaparición
# #df.drop(columns=['edad_des_inf', 'edad_des_sup'], inplace=True)
# Calcular edad_desaparicion_est y detectar inconsistencias
df['edad_desaparicion_est'] = (
    (df['fecha_desaparicion_dtf'].dt.year - df[
        'fecha_nacimiento_dft'].dt.year) -
    ((df['fecha_desaparicion_dtf'].dt.month - df[
        'fecha_nacimiento_dft'].dt.month) +
     (df['fecha_desaparicion_dtf'].dt.day - df[
        'fecha_nacimiento_dft'].dt.day)) / 12).round()
df['dif_edad'] = np.abs(df['edad_desaparicion_est'] - df['edad'])
p90 = df['dif_edad'].quantile(0.90)
df['inconsistencia_fechas'] = np.where(
    ((df['edad_desaparicion_est'] < 0) |
     (df['edad_desaparicion_est'] > 100)) &
    (df['edad_desaparicion_est'].notna()), True, False)
df['inconsistencia_fechas'] = np.where(
    (df['dif_edad'] > p90) &
    (df['dif_edad'].notna()), 2, df['inconsistencia_fechas'])
df['inconsistencia_fechas'] = np.where(
    (df['fecha_nacimiento_dft'] == df['fecha_desaparicion_dtf']) &
    (df['fecha_nacimiento_dft'].notna()) &
    (df['fecha_desaparicion_dtf'].notna()), 3, df['inconsistencia_fechas'])
# Limpiar valores en columnas relacionadas con fechas y edad


date_cols = ['fecha_nacimiento_dft', 'fecha_nacimiento', 'anio_nacimiento',
             'mes_nacimiento', 'dia_nacimiento', 'edad',
             'edad_desaparicion_est']
for col in date_cols:
    df[col] = df[col].where(df['inconsistencia_fechas'] == 0)

condicion = (df['edad_desaparicion_est'].notna()) & (df['edad'].isna())

# Realiza el reemplazo solo en las filas que cumplan con la condición
df.loc[condicion, 'edad'] = df.loc[condicion, 'edad_desaparicion_est']


# Eliminar columnas auxiliares y con inconsistencias
# #df.drop(columns=['edad_desaparicion_est', 'dif_edad',
# 'inconsistencia_fechas'], inplace=True)
# Limpiar valores en la columna 'situacion_actual_des'
df['situacion_actual_des'] = df['situacion_actual_des'].fillna("")
nrow_df_fin = len(df)

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
        (df['segundo_nombre'].isna()) &
        (df['primer_apellido'].isna()) &
        (df['segundo_apellido'].isna())), 'rni'] = 1
# Marcar filas que contienen "FUERZA PUBLICA" en cualquiera de las
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
# Marcar filas que contienen "ACTA"
# en cualquiera de las cuatro columnas de nombres y apellidos
df.loc[
       (df[['primer_nombre',
            'segundo_nombre',
            'primer_apellido',
            'segundo_apellido']].apply(
                lambda x: x.str.contains("ACTA ")).any(axis=1)), 'rni'] = 1
# Marcar filas que contienen "SIN" en cualquiera de las cuatro columnas
# de nombres y apellidos
df.loc[
       (df[['primer_nombre',
            'segundo_nombre',
            'primer_apellido',
            'segundo_apellido']].apply(
                lambda x: x.str.contains("SIN ")).any(axis=1)), 'rni'] = 1
# dfr=df[df['codigo_unico_fuente']=='75077119']
# Marcar filas que contienen "POR" en cualquiera de las cuatro
# columnas de nombres y apellidos
df.loc[
       (df[['primer_nombre',
            'segundo_nombre',
            'primer_apellido',
            'segundo_apellido']].apply(
                lambda x: x.str.contains("POR ")).any(axis=1)), 'rni'] = 1
# Marcar filas con valores comunes de no
# identificación como "NN", "N", "XX" y "X"
df.loc[((df['primer_nombre'].isin(["NN", "N", "XX", "X"])) |
        (df['segundo_nombre'].isin(["NN", "N", "XX", "X"])) |
        (df['primer_apellido'].isin(["NN", "N", "XX", "X"])) |
        (df['segundo_apellido'].isin(["NN", "N", "XX", "X"]))), 'rni'] = 1
# Marcar filas con todas las columnas de nombres y apellidos vacías
df.loc[(df['primer_nombre'].isna()) &
       (df['segundo_nombre'].isna()) &
       (df['primer_apellido'].isna()) &
       (df['segundo_apellido'].isna()) &
       (df['codigo_dane_departamento'].isna()) &
       (df['fecha_ocur_anio'].isna()) &
       (df['documento'].isna()) &
       (df['fecha_nacimiento_dft'].isna()), 'rni'] = 1
# Guardar las filas marcadas como rni en un archivo
df_rni = df[df['rni'] == 1]
nrow_df_no_ident = len(df_rni)
# #df_rni.to_stata("archivos depurados/BD_FGN_INACTIVOS_PNI.dta")
chunk_size = 1000
df_rni.to_sql('BD_ICMP_PNI', con=engine, if_exists='replace', index=False,
              chunksize=chunk_size)
# #df_rni.to_csv("archivos depurados/BD_ICMP_PNI.csv", index=False)
# Eliminar las filas marcadas como rni del DataFrame original
df = df[df['rni'] == 0]
nrow_df_ident = len(df)
print("Registros despues eliminar RNI:", nrow_df_ident)
df.drop(columns=['non_miss', 'rni'], inplace=True)

cols_to_clean = ['sexo', 'codigo_dane_departamento', 'departamento_ocurrencia',
                 'codigo_dane_municipio', 'municipio_ocurrencia',
                 'segundo_nombre', 'segundo_apellido', 'fecha_nacimiento',
                 'iden_pertenenciaetnica', 'situacion_actual_des',
                 'descripcion_relato', 'fecha_nacimiento_dft',
                 'anio_nacimiento', 'mes_nacimiento', 'dia_nacimiento', 'edad',
                 'edad_desaparicion_est']
for col in cols_to_clean:
    df[col] = df[col].fillna("")
# 5. Identificación de registros únicos
# Seleccionar las columnas que deseas mantener
columnas = ['tabla_origen', 'codigo_unico_fuente', 'nombre_completo',
            'primer_nombre', 'segundo_nombre', 'primer_apellido',
            'segundo_apellido', 'documento', 'sexo', 'iden_pertenenciaetnica',
            'fecha_nacimiento', 'anio_nacimiento', 'mes_nacimiento',
            'dia_nacimiento', 'edad', 'fecha_desaparicion', 'fecha_ocur_anio',
            'fecha_ocur_mes', 'fecha_ocur_dia',
            'codigo_dane_departamento', 'departamento_ocurrencia',
            'codigo_dane_municipio', 'municipio_ocurrencia',
            'TH_DF',  'TH_SE', 'TH_RU', 'TH_OTRO',
            'pres_resp_paramilitares',
            'pres_resp_grupos_posdesmov', 'pres_resp_agentes_estatales',
            'pres_resp_guerr_farc',
            'pres_resp_guerr_eln', 'pres_resp_guerr_otra', 'pres_resp_otro',
            'situacion_actual_des', 'descripcion_relato']
df = df[columnas]
# Ordenar el DataFrame por 'codigo_unico_fuente', 'documento' y 'nonmiss'
df.sort_values(by=['codigo_unico_fuente',
                   'documento'], ascending=[True, True], inplace=True)
# Mantener solo el primer registro para cada 'codigo_unico_fuente'
df.drop_duplicates(subset=['codigo_unico_fuente'], keep='first', inplace=True)
nrow_df = len(df)
n_duplicados = nrow_df_ident - nrow_df
df.to_sql('BD_ICMP', con=engine, if_exists='replace', index=False,
          chunksize=chunk_size)

# #df.to_stata("archivos depurados/BD_ICMP.dta", index=False)
fecha_fin = datetime.now()

log = {
    "fecha_inicio": str(fecha_inicio),
    "fecha_fin": str(fecha_fin),
    "tiempo_ejecucion": str(fecha_fin - fecha_inicio),
    'filas_iniciales_df': nrow_df_ini,
    'filas_final_df': nrow_df_fin,
    'filas_df_ident': nrow_df_ident,
    'filas_df_no_ident': nrow_df_no_ident,
    'n_duplicados': n_duplicados,
}

with open('log/resultado_df_icmp.yaml', 'w') as file:
    yaml.dump(log, file)
