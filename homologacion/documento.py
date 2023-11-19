import pandas as pd
import numpy as np


def documento_valida(df: pd, documento):
    # Documento de identificación
    df[documento] = df[documento].str.upper()
    # Eliminar caracteres no deseados
    df[documento] = df[documento].str.replace(r'[^0-9]', '', regex=True)
    # Eliminar espacios en blanco duplicados
    df[documento] = df[documento].str.replace(r'\s+', ' ', regex=True)
    # Filtrar solo documentos con al menos 4 caracteres
    df[documento] = df[documento].apply(lambda x: '' if len(x) < 4 else x)
    # Convertir a tipo numérico y manejar valores no convertibles como NaN
    df['documento_'] = pd.to_numeric(df[documento], errors='coerce')
    # Limpiar NaN (reemplazar NaN con cadena vacía)
    df['documento_'] = df['documento_'].fillna('')
    # Si documento es cero limpiar
    df[documento] = np.where((df['documento_'] == 0), "", df[documento])
    df[documento] = df[documento].fillna('')
    df.drop(columns=['documento_'], inplace=True)
