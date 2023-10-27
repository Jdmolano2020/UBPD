import unicodedata
import re
import pandas as pd
import numpy as np

def clean_text(text, na_values):
    if text is None:
        text = ''
        return text
    # Reemplazar nulos con NA
    if text in na_values:
        text = None
        return text
    # Eliminar acentos
    text = ''.join(c for c in unicodedata.normalize(
        'NFD', text) if unicodedata.category(c) != 'Mn')
    # Convertir a mayúsculas
    text = text.upper()
    # Eliminar espacios al inicio y al final
    text = text.strip()
    
    # Eliminar caracteres no ASCII
    text = ''.join([c if ord(c) < 128 else ' ' for c in text])
    # Mantener solo caracteres alfanuméricos y espacios
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    # Eliminar espacios duplicados
    text = ' '.join(text.strip().split())
    return text

def fechas_validas(df : pd, fecha_dia, fecha_mes, fecha_anio, fecha):  
    df[fecha_anio] = np.where(df[fecha_anio] < 1900, np.nan, df[fecha_anio])
    df[fecha_mes] = np.where((df[fecha_mes] < 1) | (df[fecha_mes] > 12) & (df[fecha_mes] != np.nan), np.nan, df[fecha_mes])
    df[fecha_dia] = np.where((df[fecha_dia] < 1) | (df[fecha_dia] > 31) & (df[fecha_dia] != np.nan), np.nan, df[fecha_dia])
    # Crear columna fecha en formato datetime
    df['fecha_dia'] = df[fecha_dia].astype(str)
    df['fecha_dia'].fillna(value="0", inplace=True)
    df['fecha_dia'] = df['fecha_dia'].replace(np.nan, '0')
    df['fecha_dia'] = df['fecha_dia'].str.replace("nan", "0", regex=True)
    df['fecha_dia'] = df['fecha_dia'].str.replace(".0", "", regex=True).str.zfill(2) 
    
    df['fecha_mes'] = df[fecha_mes].astype(str)
    df['fecha_mes'].fillna(value="0", inplace=True)
    df['fecha_mes'] = df['fecha_mes'].replace(np.nan, '0')
    df['fecha_mes'] = df['fecha_mes'].str.replace("nan", "0", regex=True)
    df['fecha_mes'] = df['fecha_mes'].str.replace(".0", "", regex=True).str.zfill(2) 
    
    df['fecha_anio'] = df[fecha_anio].astype(str).str.slice(0, 4)
    df['fecha_anio'] = df['fecha_anio'].str.replace("nan", "0000", regex=True)
        
    df['fecha_ymd'] = df['fecha_anio'] + df['fecha_mes'] + df['fecha_dia']
    
    df['fecha_ymd_dtf'] = pd.to_datetime(df['fecha_ymd'], format='%Y%m%d', errors='coerce')
    df['fecha_anio'] = np.where((df['fecha_ymd_dtf'].isna()),None, df['fecha_anio'])
    df['fecha_mes'] = np.where(df['fecha_ymd_dtf'].isna(),None, df['fecha_mes'])
    df['fecha_dia'] = np.where(df['fecha_ymd_dtf'].isna(),None, df['fecha_dia'])
    df.loc[df['fecha_ymd_dtf'].isna(), 'fecha_ymd_dtf'] = None
    
    df.drop(columns=[fecha_anio, fecha_mes, fecha_dia, 'fecha_ymd'], inplace=True)
    df.rename(columns={'fecha_anio': fecha_anio, 
                       'fecha_mes': fecha_mes, 
                       'fecha_dia': fecha_dia,
                       'fecha_ymd_dtf':fecha}, inplace=True)

def fechas_nombres(df : pd, primer_nombre, segundo_nombre, primer_apellido, segundo_apellido, nombre_completo):
    # Eliminar nombres y apellidos que solo tienen una letra inicial
    preposiciones = ["DEL", "DE", "DE LAS", "DE LOS"]
    df['i'] = (df[segundo_nombre].isin(preposiciones))
    df.loc[df['i'], segundo_nombre] = df[segundo_nombre] + " " + df[primer_apellido]
    df.loc[df['i'], primer_apellido] = df[segundo_apellido]
    df.loc[df['i'], segundo_apellido] = ""
    df.drop(columns=['i'], inplace=True)

    # Reemplazar valores en primer_apellido
    df['i'] = (df[primer_apellido].isin(preposiciones))
    df.loc[df['i'], primer_apellido] = df[primer_apellido] + " " + df[segundo_apellido]
    df.loc[df['i'], segundo_apellido] = ""
    df.drop(columns=['i'], inplace=True)
    # Reemplazar primer apellido por segundo apellido cuando el primer campo está vacío
    df['i'] = (df[primer_apellido] == "") & (df[segundo_apellido] != "")
    df.loc[df['i'], primer_apellido] = df[segundo_apellido]
    df.loc[df['i'], segundo_apellido] = ""
    df.drop(columns=['i'], inplace=True)
    # Eliminar nombres y apellidos cuando solo se registra la letra inicial
    cols_to_clean = [primer_nombre, primer_apellido, segundo_nombre, segundo_apellido]
    for col in cols_to_clean:
        df.loc[df[col].str.len() == 1, col] = ""
    # Nombre completo
    cols_nombre = [ segundo_nombre, primer_apellido, segundo_apellido]
    # Inicializa la columna nombre_completo con el valor de primer_nombre
    df['nombre_completo_'] = df[primer_nombre]

    for col in cols_nombre:
        df['nombre_completo_'] = df['nombre_completo_'] + " " + df[col].fillna("")  # Concatenar nombres y apellidos no vacíos
        
    df['nombre_completo_'] = df['nombre_completo_'].str.strip()  # Eliminar espacios en blanco al principio y al final
    df['nombre_completo_'] = df['nombre_completo_'].str.replace('  ', ' ', regex=True)  # Reemplazar espacios dobles por espacios simples
    # Eliminar columna nombre_completo original
    df.drop(columns=[nombre_completo], inplace=True)
    # Renombrar columna
    df.rename(columns={'nombre_completo_': nombre_completo}, inplace=True)

def documento_valida (df : pd, documento):
    # Documento de identificación
    df[documento] = df[documento].str.upper()

    for i in range(256):
        if i not in [32, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 209]:
            df[documento] = df[documento].str.replace(chr(i), '', regex=True)
    df['documento_dep'] = df[documento]

    for i in range(48, 58):
        df['documento_dep'] = df['documento_dep'].str.replace(chr(i), '', regex=True)

    df[documento] = df.apply(lambda row: '' if row['documento_dep'] == row[documento] and row[documento] != '' else row[documento], axis=1)

    for i in range(256):
        if i not in [32, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57]:
            df[documento] = df.apply(lambda row: row[documento].replace(chr(i), '') if ' ' in row[documento] and row['documento_dep'] != row[documento] and row[documento] != '' and row['documento_dep'] != '' else row[documento], axis=1)

    df[documento] = df[documento].str.strip()
    df[documento] = df[documento].str.replace('   ', ' ', regex=True)
    df[documento] = df[documento].str.replace('  ', ' ', regex=True)
    df.drop(columns=['documento_dep'], inplace=True)
    df[documento] = df.apply(lambda row: '' if len(row[documento]) < 4 else row[documento], axis=1)

def etnia_valida (df : pd, etnia):
    df.rename(columns={etnia: 'iden_pertenenciaetnica_'}, inplace=True)
    # Reemplazar valores en la columna etnia
    df[etnia] = np.where(df['iden_pertenenciaetnica_'] == 'Afrocolombiano', 'NARP', df['iden_pertenenciaetnica_'])
    df[etnia] = np.where(df['iden_pertenenciaetnica_'].str.contains('Indígena|Nasa'), 'INDIGENA', df[etnia])
    df[etnia] = np.where(df['iden_pertenenciaetnica_'].str.contains('Ninguno'), 'NINGUNA', df[etnia])
    # Eliminar la columna original
    df.drop(columns=['iden_pertenenciaetnica_'], inplace=True)

na_values = ["SIN INFORMACION", "NA", "ND","AI"]
strig_prueba="Juand Daniw123235234'12234235?¡1__...: Pingüino: Málaga es una ciudad fantástica y en Logroño me pica el... moño"
nuevo = clean_text(strig_prueba, na_values)
print(strig_prueba)
print(nuevo)

strig_prueba=None
nuevo = clean_text(strig_prueba, na_values)
print(strig_prueba)
print(nuevo)

strig_prueba="SIN INFORMACION"
nuevo = clean_text(strig_prueba, na_values)
print(strig_prueba)
print(nuevo)

strig_prueba= "NA"
nuevo = clean_text(strig_prueba, na_values)
print(strig_prueba)
print(nuevo)

def homologar_fuerzapublica (df : pd ):
# Crear la variable 'pres_resp_agentes_estatales'
    df['pres_resp_agentes_estatales'] = np.where(
        (
        df['presunto_responsable'].str.contains("FUERZA PUBLICA") |
        
        (df['presunto_responsable'].str.contains("ESTADO") & 
         ~df['presunto_responsable'].str.contains("MAYOR") & 
         ~df['presunto_responsable'].str.contains("CONJUNTO") & 
         ~df['presunto_responsable'].str.contains("FARC")) |
        
        (df['presunto_responsable'].str.contains("AGENTE ESTA")) |
        
        ((df['presunto_responsable'].str.contains("CTI") &
          (df['presunto_responsable'].str.len() == 3)) |
         df['presunto_responsable'].str.contains(" CTI") |
         df['presunto_responsable'].str.contains("CTI ")) |
        
        (df['presunto_responsable'].str.contains("CUERPO") | 
         df['presunto_responsable'].str.contains("TECNICO") | 
         df['presunto_responsable'].str.contains("INVESTIGA")) |
        
        df['presunto_responsable'].str.contains("FISCALIA") |
        
        df['presunto_responsable'].str.contains("FGN") |
        
        df['presunto_responsable'].str.contains("POLICIA") |
        
        df['presunto_responsable'].str.contains("CARABINEROS") |
        
        df['presunto_responsable'].str.contains("SIJIN") |
        
        df['presunto_responsable'].str.contains("DIJIN") |
        
        (df['presunto_responsable'] == "F2") |
        
        (df['presunto_responsable'] == "DAS") |
        
        (df['presunto_responsable'].str.contains("EJERCITO") &
         ~df['presunto_responsable'].str.contains("LIBERACION") &
         ~df['presunto_responsable'].str.contains("REVOLUCIONA") & 
         ~df['presunto_responsable'].str.contains("PUEBLO")) |
        
        (df['presunto_responsable'].str.contains("DEPARTAMENTO") &
         df['presunto_responsable'].str.contains("SEGURIDAD")) |
        
        (df['presunto_responsable'].str.contains("EJERCITO") &
         ~df['presunto_responsable'].str.contains("LIBERACION") &
         df['presunto_responsable'].str.contains("NACIONAL") &
         ~df['presunto_responsable'].str.contains("REVOLUCI")) |
        
        df['presunto_responsable'].str.contains("BATALLON") |
        
        df['presunto_responsable'].str.contains("BINCI") |
        
        (df['presunto_responsable'].str.contains("CHARRY") &
         ~df['presunto_responsable'].str.contains("SOLANO")) |
        
        (df['presunto_responsable'] == "B2") |
        
        df['presunto_responsable'].str.contains("BRIGADA") |
        
        (df['presunto_responsable'].str.contains("MILITAR") &
         ~df['presunto_responsable'].str.contains("PARA")) |
        
        (df['presunto_responsable'].str.contains("FUERZA") &
         df['presunto_responsable'].str.contains("AEREA") &
         df['presunto_responsable'].str.contains("COLOMBIANA")) |
        
        (df['presunto_responsable'].str.contains("ARMADA") &
         df['presunto_responsable'].str.contains("NACIONAL")) |
        
        df['presunto_responsable'].str.contains("MARINA") |
        
        df['presunto_responsable'].str.contains("FAC") |
        
        (df['presunto_responsable'].str.contains("FUERZA") &
         df['presunto_responsable'].str.contains("AEREA")) |
        
        (df['presunto_responsable'].str.contains("FUERZA") &
         df['presunto_responsable'].str.contains("TAREA") &
         df['presunto_responsable'].str.contains("CONJUNTA")) |
        
        df['presunto_responsable'].str.contains("GAULA") |
        
        (df['presunto_responsable'].str.contains("DIVISION") &
         (df['presunto_responsable'].str.contains("PRIMERA") |
          df['presunto_responsable'].str.contains("SEGUNDA") | 
          df['presunto_responsable'].str.contains("TERCERA") | 
          df['presunto_responsable'].str.contains("CUARTA") | 
          df['presunto_responsable'].str.contains("QUINTA") | 
          df['presunto_responsable'].str.contains("SEXTA") | 
          df['presunto_responsable'].str.contains("SEPTIMA")))
        ), 1, 0)
