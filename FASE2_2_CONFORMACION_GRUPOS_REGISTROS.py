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
ruta_archivo_stata = "ruta/del/archivo/V_UNIVERSO_FASE2_1.dta"
df = pd.read_stata(ruta_archivo_stata)

# Conformar grupos de registros
df = df[df['clasificacion_final'] == 1]

# Crear grupos utilizando egen
df['g1'] = df.groupby('cod1').ngroup()
df['g2'] = df.groupby('cod2').ngroup()

# Etiquetar duplicados
df['duplicates_cod1'] = df.duplicated('cod1', keep=False)
df['duplicates_cod2'] = df.duplicated('cod2', keep=False)

# Mostrar resumen de duplicados
print(df[['duplicates_cod1', 'duplicates_cod2']].sum())

# Ordenar por duplicados
df.sort_values(by=['duplicates_cod1', 'cod1', 'cod2'], ascending=[False, True, True], inplace=True)
