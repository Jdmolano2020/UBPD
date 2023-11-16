por aqui paso JDMC
import os
from sqlalchemy import create_engine, text
import pandas as pd
import json
#from text_column import aplicar_reglas_validacion
import time
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
# Guarda el tiempo de inicio
start_time = time.time()

#import config
with open('config.json') as config_file:
    config = json.load(config_file)

DIRECTORY_PATH = config['DIRECTORY_PATH']
DB_SERVER = config['DB_SERVER']
DB_DATABASE = config['DB_DATABASE']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']

DB_SCHEMA = "orq_salida"
DB_TABLE = "RSB"

# Cambiar de directorio
archivo_a_borrar = os.path.join("fuentes secundarias",
                                "V_UBPD_RSB.csv")

    
if DIRECTORY_PATH:
    os.chdir(DIRECTORY_PATH)  # Cambia al directorio deseado si DIRECTORY_PATH no está vacío
    if os.path.exists(archivo_a_borrar):
        os.remove(archivo_a_borrar)

# La codificación ISO-8859-1 no es necesaria en Python ya que usa Unicode por defecto

# Conexión a la base de datos usando pyodbc
# Configurar la cadena de conexion
connection_string = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

# Conectar a la BBDD
engine = create_engine(connection_string)
# Cargar datos desde la base de datos
sql_query = f'SELECT * FROM {DB_SCHEMA}.{DB_TABLE}'
with engine.connect() as connection:
    result = connection.execute(text(sql_query))
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
# Convertir nombres de columnas a minúsculas (lower)
    df.columns = [col.lower() for col in df.columns]
# Guardar en archivo CSV
    csv_file_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias/V_UBPD_RSB.csv")
    df.to_csv(csv_file_path, index=False, encoding='utf-8')

# Cargar el archivo de datos en un DataFrame y convertir los nombres de las columnas a minúsculas
#df = pd.read_csv("fuentes secundarias/V_UBPD_RSB.csv")
#df.columns = df.columns.str.lower()

# Etiquetar observaciones duplicadas
df['duplicates_reg'] = df.duplicated(subset=None, keep='first')

# Mostrar una tabla de frecuencias de las observaciones duplicadas
table = df['duplicates_reg'].value_counts()
print(table)

# Eliminar la columna 'duplicates_reg'
df.drop(columns=['duplicates_reg'], inplace=True)

# 1. Ordenar el DataFrame
columns_to_sort = [
    "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido",
    "documento", "sexo", "edad_des_inf", "edad_des_sup", "anio_nacimiento", 
    "anio_nacimiento_ini", "anio_nacimiento_fin", "mes_nacimiento", 
    "dia_nacimiento", "iden_orientacionsexual", "iden_pertenenciaetnica",
    "pais_de_ocurrencia", "codigo_dane_departamento", "codigo_dane_municipio",
    "fecha_ocur_dia", "fecha_ocur_mes", "fecha_ocur_anio", "tipo_de_hecho",
    "presunto_responsable", "descripcion_relato", "codigo_unico_fuente"
]
df.columns = [col.replace(" ", "_") for col in df.columns]
df.sort_values(columns_to_sort, inplace=True)#quitar ordenamientos

df.rename(columns={"fuente": "tabla_origen"}, inplace=True)

# Crear una nueva columna "in_ubpd" y asignarle el valor constante 1
df['in_ubpd'] = 1
df['codigo_unico_fuente_'] = 'codigo_unico_fuente'

#Asigna valor posterior a formatear a 6 digitos en texto relleno a la izquierda con ceros
df['codigo_unico_fuente_'] = df['codigo_unico_fuente'].apply(lambda x: f"{int(x):06}")
df.drop(columns=['codigo_unico_fuente'], inplace=True)

df.rename(columns={'codigo_unico_fuente_': 'codigo_unico_fuente'}, inplace=True)

#Definición de ruta y exportacion de dataframe actual
csv_file_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias/V_UBPD_RSB.csv")
df.to_csv(csv_file_path, index=False, encoding='utf-8')

#1. Seleccionar variables que serán homologadas para la integración
# Exclusion de las que no se van a tener en cuenta
df.drop(columns=["tipo_de_otro_nombre", "otro_nombre", "iden_orientacionsexual", "lude_territoriocolectivo", "rein_nombre", "anio_nacimiento_ini", "anio_nacimiento_fin", "tipo_de_documento"], inplace=True)

#Variables a aplicar script depuracion
columns_to_clean = ["nombre_completo", "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido", "pais_de_ocurrencia", "presunto_responsable", "sexo", "tipo_de_hecho"]

#2. Normalización de los campos de texto
df[columns_to_clean] = df[columns_to_clean].apply(clean_text)
#df = aplicar_reglas_validacion(df, columns_to_clean)


#3. Homologación de estructura, formato y contenido
#Datos sobre los hechos	
	#Lugar de ocurrencia- País/Departamento/Muncipio
df.rename(columns={
    'pais_de_ocurrencia': 'pais_ocurrencia',
    'departamento_de_ocurrencia': 'departamento_ocurrencia',
    'municipio_de_ocurrencia': 'municipio_ocurrencia'
}, inplace=True)

# fecha de ocurrencia 
df['fecha_ocur_anio'] = pd.to_numeric(df['fecha_ocur_anio'], errors='coerce')
df['fecha_ocur_mes'] = pd.to_numeric(df['fecha_ocur_mes'], errors='coerce')
df['fecha_ocur_dia'] = pd.to_numeric(df['fecha_ocur_dia'], errors='coerce')
homologacion.fecha.fechas_validas (df,fecha_dia = 'fecha_ocur_dia', 
                                   fecha_mes = 'fecha_ocur_mes',
                                   fecha_anio = 'fecha_ocur_anio',
                                   fecha = 'fecha_desaparicion_dtf',
                                   fechat= 'fecha_desaparicion')

# nombres y apellidos
df['nombre_completo'] = df['nombre_completo'].replace('PERSONA SIN IDENTIFICAR', pd.NA)  # Reemplaza 'PERSONA SIN IDENTIFICAR' con NaN
df['nombre_completo'] = df['nombre_completo'].str.replace(r'\bNA\b', '', regex=True)  # Elimina la palabra 'NA' entre espacios en blanco
df['nombre_completo'] = df['nombre_completo'].fillna("")
# Aplica la función a la columna "nombre_completo"
df[['primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido']] = df['nombre_completo'].apply(lambda x: pd.Series(homologacion.nombre_completo.limpiar_nombre_completo(x)))

homologacion.nombres.nombres_validos (df , primer_nombre = 'primer_nombre',
                 segundo_nombre = 'segundo_nombre',
                 primer_apellido = 'primer_apellido',
                 segundo_apellido = 'segundo_apellido',
                 nombre_completo = 'nombre_completo')

# Documento
# Eliminar espacios en blanco al principio y al final de la columna numero_documento
df['documento'] = df['documento'].str.strip()
homologacion.documento.documento_valida (df, documento = 'documento')


# Pertenencia étnica
homologacion.etnia.etnia_valida (df, etnia = 'iden_pertenenciaetnica')


# fecha de nacimiento 
df['anio_nacimiento'] = pd.to_numeric(df['anio_nacimiento'], errors='coerce')
df['mes_nacimiento'] = pd.to_numeric(df['mes_nacimiento'], errors='coerce')
df['dia_nacimiento'] = pd.to_numeric(df['dia_nacimiento'], errors='coerce')

homologacion.fecha.fechas_validas (df,fecha_dia = 'dia_nacimiento', 
                                   fecha_mes = 'mes_nacimiento',
                                   fecha_anio = 'anio_nacimiento',
                                   fecha = 'fecha_nacimiento_dtf',
                                   fechat= 'fecha_nacimiento')


print(df.columns)


 
print(df.head(10))


# Guardar el tiempo de finalización
end_time = time.time()

# Calcular la duración total
total_duration = end_time - start_time
# duración total en segundos
print(f"El script tomó {total_duration:.2f} segundos en ejecutarse.")