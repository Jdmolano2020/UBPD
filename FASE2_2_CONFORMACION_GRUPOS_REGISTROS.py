import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


# parametros programa stata
parametro_ruta = ""
parametro_cantidad = ""
# Establecer la ruta de trabajo
ruta = "C:/Users/HP/Documents/UBPD/HerramientaAprendizaje/Fuentes/OrquestadorUniverso" 
# Cambia esto según tu directorio

# Verificar si `1` es una cadena vacía y ajustar el directorio de trabajo
# en consecuencia
if parametro_ruta == "":
    os.chdir(ruta)
else:
    os.chdir(parametro_ruta)

# Cargar el archivo Stata en un DataFrame de pandas
ruta_archivo_stata = "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/V_UNIVERSO_FASE2_1.dta"
df = pd.read_stata(ruta_archivo_stata)

# Paso 1: Mantener solo las observaciones donde
# clasificacion_final es igual a 1
df = df[df['clasificacion_final'] == 1]

# Paso 3: Crear grupos para cod1 y cod2
df['g1'] = df.groupby('cod1').grouper.group_info[0] + 1
df['g2'] = df.groupby('cod2').grouper.group_info[0] + 1

# Paso 4 y 5: Etiquetar duplicados en cod1 y cod2
df['duplicates_cod1'] = df.duplicated('cod1')
df['duplicates_cod2'] = df.duplicated('cod2')

# Paso 6: Mostrar resumen de duplicados
print(df[['duplicates_cod1', 'duplicates_cod2']].sum())

# Paso 7: Ordenar el DataFrame
df.sort_values(
    by=['duplicates_cod1', 'cod1', 'cod2'], ascending=[False, True, True],
    inplace=True)

# Paso 8: Crear group_id basado en cod1
df['group_id'] = (df['cod1'] != df['cod1'].shift()).cumsum()

# Paso 9: Ajustar group_id en consecuencia
j = df['group_id'].max()
for i in range(2, j + 1):
    print(f'{i}/{j}')
    df['group_id'].update(df.groupby('cod1')['group_id'].transform('max') + 1)

conteo = df['group_id'].value_counts()

# Bucle forvalues i=1(1)20
for i in range(1, 21):
    print(i)
    # Crear columna gr y ordenar por cod2 y cod1
    df['gr'] = df['group_id']
    df.sort_values(by=['cod2', 'cod1'], inplace=True)
    # Calcular el mínimo de group_id por cod2
    df['m'] = df.groupby('cod2')['group_id'].transform('min')
    # Actualizar group_id si gr es diferente de m
    df['group_id'].update(df['m'].where(df['gr'] != df['m']))
    # Eliminar columnas temporales
    df.drop(['gr', 'm'], axis=1, inplace=True)
    # Repetir el proceso para cod1 y cod2 invertidos
    df['gr'] = df['group_id']
    df.sort_values(by=['cod1', 'cod2'], inplace=True)
    df['m'] = df.groupby('cod1')['group_id'].transform('min')
    df['group_id'].update(df['m'].where(df['gr'] != df['m']))
    df.drop(['gr', 'm'], axis=1, inplace=True)

# Crear columna g y eliminar group_id
df['g'] = df.groupby('group_id').grouper.group_info[0] + 1
df.drop('group_id', axis=1, inplace=True)
7
# Ordenar DataFrame
df.sort_values(by=['g', 'cod1', 'cod2'], inplace=True)

# Crear columnas i y j
df['i'] = np.arange(1, len(df) + 1)
df['j'] = df.groupby('g').cumcount() + 1

# Convertir a formato largo (reshape)
df_long = df.melt(id_vars=['g', 'i', 'j'], value_vars=['cod1', 'cod2'], var_name='cod')

# Ordenar y mantener la primera observación por grupo y cod
df_long.sort_values(by=['g', 'cod'], inplace=True)
df_long = df_long.groupby(['g', 'cod']).first().reset_index()

# Eliminar columnas i y j
df_long.drop(['i', 'j'], axis=1, inplace=True)

# Bucle forvalues i=1(1)10
for i in range(1, 11):
    # Calcular mínimo de g por cod
    df_long['gr'] = df_long.groupby('cod')['g'].transform('min')
    # Calcular mínimo de gr por g
    df_long['g_'] = df_long.groupby('g')['gr'].transform('min')
    # Eliminar columnas temporales
    df_long.drop(['gr', 'g'], axis=1, inplace=True)
    # Mantener la primera observación por g_ y cod
    df_long = df_long.groupby(['g_', 'cod']).first().reset_index()
    # Renombrar columna g_ a g
    df_long.rename(columns={'g_': 'g'}, inplace=True)
# Crear columna id y por g, generar n
df_long['id'] = df_long.groupby('cod').cumcount() + 1
df_long['n'] = df_long.groupby('g').cumcount() + 1

# Convertir todas las columnas a minúsculas
df_long.columns = df_long.columns.str.lower()

# Guardar DataFrame en formato .dta
df_long.to_stata("ruta/del/archivo/V_UNIVERSO_FASE2_1_3I.dta", write_index=False)

# Renombrar columnas para la segunda parte del código
df.rename(columns={'cod': 'cod2', 'id': 'id2', 'n': 'n2'}, inplace=True)

# Guardar DataFrame en formato .dta
df.to_stata("ruta/del/archivo/V_UNIVERSO_FASE2_1_3II.dta", write_index=False)

# Cargar los dos archivos .dta
df1 = pd.read_stata("ruta/del/archivo/V_UNIVERSO_FASE2_1_3I.dta")
df2 = pd.read_stata("ruta/del/archivo/V_UNIVERSO_FASE2_1_3II.dta")

# Unir DataFrames por columna 'g'
result = pd.merge(df1, df2, on='g', how='inner')

# Ordenar y eliminar filas duplicadas
result.sort_values(by=['g', 'cod1', 'cod2'], inplace=True)
result.drop_duplicates(subset=['id', 'id2'], inplace=True)

# Mantener solo las columnas necesarias
result = result[['g', 'cod1', 'cod2', 'n', 'n2']]

# Renombrar columnas
result.rename(columns={'g': 'group_id', 'n': 'n1'}, inplace=True)

# Guardar el resultado final en formato .dta
result.to_stata("ruta/del/archivo/V_UNIVERSO_FASE2_2.dta", write_index=False)
