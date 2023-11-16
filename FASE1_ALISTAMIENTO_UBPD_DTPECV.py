import os
import re
import json
import numpy as np
import pandas as pd
import homologacion.limpieza

def clean_text(text):
    if text is None or text.isna().any():
        text = text.astype(str)      
    text = text.apply(homologacion.limpieza.normalize_text)
    return text

#import config
with open('config.json') as config_file:
    config = json.load(config_file)

DIRECTORY_PATH = config['DIRECTORY_PATH']

excel_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias", "Entregas dignas UBPD Respuesta Hasta Encontrarlos.xlsx")

sheet_excel = "Hoja1"
df_excel = pd.read_excel(excel_path, sheet_name=sheet_excel, header=0)

columns_to_normalize = ["nombre_apellidos"]
                        
#. Normalización de los campos de texto
#Eliminación de acento, "NO APLICA", "NULL"
df_excel[columns_to_normalize] = df_excel[columns_to_normalize].apply(clean_text)


na_equal = {
    'ND': "",
    'NA': "",
    'NR': ""
}

equal_conditions = {value: lambda x, val=value: str(x) == val for value in na_equal.keys()}

# Aplicar la transformación de igualdad
for col in columns_to_normalize:
    for value, func in equal_conditions.items():
        df_excel[col] = df_excel[col].apply(lambda x: '' if func(x) else x)
        
na_content = {
    'NO APLICA': lambda x: ('NO ' in str(x)) and ('APLICA' in str(x)),
    'NULL': lambda x: 'NULL' in str(x),
    'SIN INFOR': lambda x: ('SIN' in str(x)) and ('INFOR' in str(x)),
    'NO SABE': lambda x: ('NO' in str(x)) and ('SABE' in str(x)),
    'DESCONOCID': lambda x: 'DESCONOCID' in str(x),
    'POR ESTABLECER': lambda x: ('POR' in str(x)) and ('ESTABLECER' in str(x)),
    'SIN DEFINIR': lambda x: ('SIN' in str(x)) and ('DEFINIR' in str(x)),
    'SIN ESTABLECER': lambda x: ('SIN' in str(x)) and ('ESTABLECER' in str(x)),
    'POR DEFINIR': lambda x: ('POR' in str(x)) and ('DEFINIR' in str(x)),
}

# Aplicar transformación
for col in df_excel.columns:
    for condition, func in na_content.items():
        df_excel[col] = df_excel[col].apply(lambda x: '' if func(x) else x)

df_excel.rename(columns={'nombre_apellidos': 'nombre_completo'}, inplace=True)
        
#Documento
#Eliminar símbolos y caracteres especiales
df_excel.rename(columns={'docuemento': 'documento'}, inplace=True)
df_excel['documento_'] = df_excel['documento'].str.upper()

# Iterar sobre los caracteres no permitidos
for i in range(209):
    if not (32 <= i <= 32 or 48 <= i <= 57 or 65 <= i <= 90 or 209 <= i <= 209):
        caracter = chr(i)
        df_excel['documento_'] = df_excel['documento_'].apply(lambda x: re.sub(re.escape(caracter), '', str(x)))

# Crear una nueva columna 'documento_dep' copiando 'documento_'
df_excel['documento_dep'] = df_excel['documento_']

# Eliminar dígitos del 0 al 9 de 'documento_dep'
df_excel['documento_dep'] = df_excel['documento_dep'].replace(to_replace='\d', value='', regex=True)

# Asignar valor nulo a 'documento_' cuando 'documento_dep' sea igual a 'documento_'
df_excel.loc[(df_excel['documento_dep'] == df_excel['documento_']) & (df_excel['documento_'] != ''), 'documento_'] = None

for i in range(256):
    if i != 32 and not (48 <= i <= 57):
        char_i = chr(i)
        pattern = re.escape(char_i)
        condition = (df_excel['documento_'].str.contains(pattern, na=False) & 
                     (df_excel['documento_'] != df_excel['documento_dep']) & 
                     (df_excel['documento_'] != '') & 
                     (df_excel['documento_dep'] != ''))
        df_excel.loc[condition, 'documento_'] = df_excel.loc[condition, 'documento_'].apply(lambda x: x.replace(char_i, ''))
        
        

# Reemplazar registros de documentos de identificación iguales a '0' con NaN
df_excel['documento_'] = np.where((df_excel['documento_'] == '0') | (df_excel['documento_'].astype(str).str.strip() == ''), np.nan, df_excel['documento_'])

# Eliminar filas donde 'documento_' es NaN
df_excel = df_excel.dropna(subset=['documento_'])

# Eliminar espacios adicionales y renombrar la columna

df_excel['documento_'] = df_excel['documento_'].astype(str)# según tus necesidades
#quita espacios
df_excel['documento_'] = df_excel['documento_'].str.replace(" ", "")

# Eliminar columnas
df_excel.drop(columns=['documento', 'lugar_entrega', 'documento_dep'], inplace=True)
df_excel.rename(columns={'documento_': 'documento'}, inplace=True)


# Ordenar por 'nombre_completo'
df_excel.sort_values('documento', inplace=True)

# Restablecer índices después de las eliminaciones y el ordenamiento
df_excel = df_excel.reset_index(drop=True)

# Conservar la primera fila por grupo de documento
mask_duplicates = df_excel['documento'].duplicated(keep='first')
df_excel = df_excel[~mask_duplicates].reset_index(drop=True)

# Renombrar la columna nombre_completo a nombre_completo_tabla_entrega
df_excel.rename(columns={'nombre_completo': 'nombre_completo_tabla_entrega'}, inplace=True)

csv_path = os.path.join(DIRECTORY_PATH, "fuentes secundarias", "V_UBPD_RSB.csv")
df_csv = pd.read_csv(csv_path)

# Realizar la unión (left join) con archivo csv
df_excel = pd.merge(df_excel, df_csv[['nombre_completo', 'codigo_unico_fuente']],
                        how='left', left_on='nombre_completo_tabla_entrega', right_on='nombre_completo')

# Conservar la primera fila por grupo de documento después de la fusión
df_excel = df_excel.sort_values('nombre_completo').drop_duplicates('documento').reset_index(drop=True)

# Eliminar la columna temporal 'merge'
df_excel.drop(columns='merge', inplace=True, errors='ignore')
   
# Guardar el DataFrame en un archivo
export_file_path = os.path.join(DIRECTORY_PATH, "archivos depurados", "BD_UBPD_DTPECV.csv")
df_excel.to_csv(export_file_path, index=False)

# Filtrar personas con identificación y guardar el resultado
df_identificados = df_excel[df_excel['documento'] != ""]
export_file_path = os.path.join(DIRECTORY_PATH, "archivos depurados", "BD_UBPD_DTPECV_identificados.csv")
df_identificados.to_csv(export_file_path, index=False)