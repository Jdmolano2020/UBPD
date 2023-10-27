import pandas as pd
import numpy as np

def etnia_valida (df : pd, etnia):
    df.rename(columns={etnia: 'iden_pertenenciaetnica_'}, inplace=True)
    # Reemplazar valores en la columna etnia
    df[etnia] = np.where(df['iden_pertenenciaetnica_'] == 'Afrocolombiano', 'NARP', df['iden_pertenenciaetnica_'])
    df[etnia] = np.where(df['iden_pertenenciaetnica_'].str.contains('Indígena|Nasa'), 'INDIGENA', df[etnia])
    df[etnia] = np.where(df['iden_pertenenciaetnica_'].str.contains('Ninguno'), 'NINGUNA', df[etnia])
    # Eliminar la columna original
    df.drop(columns=['iden_pertenenciaetnica_'], inplace=True)


