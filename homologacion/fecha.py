import pandas as pd
import numpy as np

def fechas_validas(df : pd, fecha_dia, fecha_mes, fecha_anio, fecha, fechat):  
    df[fecha_anio] = np.where(df[fecha_anio] < 1900, np.nan, df[fecha_anio])
    df[fecha_mes] = np.where((df[fecha_mes] < 1) | (df[fecha_mes] > 12) & (df[fecha_mes] != np.nan), np.nan, df[fecha_mes])
    df[fecha_dia] = np.where((df[fecha_dia] < 1) | (df[fecha_dia] > 31) & (df[fecha_dia] != np.nan), np.nan, df[fecha_dia])
    # Crear columna fecha en formato datetime
    df['fecha_dia'] = df[fecha_dia].astype(str)
    df['fecha_dia'].fillna(value="0", inplace=True)
    df['fecha_dia'] = df['fecha_dia'].replace(np.nan, '0')
    df['fecha_dia'] = df['fecha_dia'].str.replace("nan", "0", regex=True) 
    df['fecha_dia'] = df['fecha_dia'].str.zfill(4)
    df['fecha_dia'] = df['fecha_dia'].str.slice(0, 2)
    
    df['fecha_mes'] = df[fecha_mes].astype(str)
    df['fecha_mes'].fillna(value="0", inplace=True)
    df['fecha_mes'] = df['fecha_mes'].replace(np.nan, '0')
    df['fecha_mes'] = df['fecha_mes'].str.replace("nan", "0", regex=True)
    df['fecha_mes'] = df['fecha_mes'].str.zfill(4)
    df['fecha_mes'] = df['fecha_mes'].str.slice(0, 2)
    
    df['fecha_anio'] = df[fecha_anio].astype(str).str.slice(0, 4)
    df['fecha_anio'] = df['fecha_anio'].str.replace("nan", "0000", regex=True)
        
    df['fecha_ymd'] = df['fecha_anio'] + df['fecha_mes'] + df['fecha_dia']    
    
    df['fecha_ymd_dtf'] = pd.to_datetime(df['fecha_ymd'], format='%Y%m%d', errors='coerce')
    df['fecha_anio'] = np.where((df['fecha_ymd_dtf'].isna()),"", df['fecha_anio'])
    df['fecha_mes'] = np.where(df['fecha_ymd_dtf'].isna(),"", df['fecha_mes'])
    df['fecha_dia'] = np.where(df['fecha_ymd_dtf'].isna(),"", df['fecha_dia'])
    df['fecha_ymd'] = np.where(df['fecha_ymd_dtf'].isna(),"", df['fecha_ymd'])
    df.loc[df['fecha_ymd_dtf'].isna(), 'fecha_ymd_dtf'] = None
    
    df.drop(columns=[fecha_anio, fecha_mes, fecha_dia], inplace=True)
    df.rename(columns={'fecha_anio': fecha_anio, 
                       'fecha_mes': fecha_mes, 
                       'fecha_dia': fecha_dia,
                       'fecha_ymd': fechat,
                       'fecha_ymd_dtf':fecha}, inplace=True)