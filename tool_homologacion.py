import unicodedata
import re
import pandas as pd
import numpy as np

def normalize_text(text):
    # Eliminar acentos
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
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

def clean_text(text):
    if text is None or text.isna().any():
        return text      
    text = text.apply(normalize_text)
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
