import os
import json
import time
import pandas as pd


# Guarda el tiempo de inicio
start_time = time.time()


#import config
with open('config.json') as config_file:
    config = json.load(config_file)

DIRECTORY_PATH = config['DIRECTORY_PATH']

delete_file_path = os.path.join("fuentes secundarias",
                                "V_JEP_MACROCASO_003.csv")

if DIRECTORY_PATH:
    if os.path.exists(delete_file_path):
        os.remove(delete_file_path)

encoding = "ISO-8859-1"
# La codificación ISO-8859-1

#Macrocaso 03
excel_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias", "13092021_VíctimasCaso003_2002a2008.xlsx")

sheet_excel = "Dato a Dato por fuente 6402"
df_003 = pd.read_excel(excel_path, sheet_name=sheet_excel, header=0)

# Convertir nombres de columnas a minúsculas (lower)
df_003.columns = [col.lower() for col in df_003.columns]

# Etiquetar y eliminar observaciones duplicadas
df_003['duplicates_reg'] = df_003.duplicated()
df_003 = df_003[~df_003['duplicates_reg']]

# Crear la nueva variable 'tabla_origen' concatenando "MACROCASO_003_" y el valor de 'fuente'
df_003['tabla_origen'] = "MACROCASO_003" + "_" + df_003['fuente'].astype(str)

# Eliminar la columna 'fuente'
df_003.drop('fuente', axis=1, inplace=True)

# Crear la nueva variable 'in_macro003' e inicializarla con el valor 1
df_003['in_macro003'] = 1

# Crear la nueva columna 'codigo_unico_fuente' concatenando las columnas id_dedup e id_fuente
df_003['codigo_unico_fuente'] = df_003['id_dedup'].astype(str) + " - " + df_003['id_fuente']

# Ordenar el DataFrame por la columna 'codigo_unico_fuente'
df_003.sort_values('codigo_unico_fuente', inplace=True)

# Eliminar las columnas id_dedup e id_fuente
df_003.drop(['id_dedup', 'id_fuente'], axis=1, inplace=True)

# Reordenar el DataFrame primero por 'tabla' y luego por 'codigo_unico_fuente'
df_003.sort_values(['tabla', 'codigo_unico_fuente'], inplace=True)