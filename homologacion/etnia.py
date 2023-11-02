import pandas as pd
import numpy as np

def etnia_valida (df : pd, etnia):
    
    df.rename(columns={etnia: 'iden_pertenenciaetnica_'}, inplace=True)
    # Reemplazar valores en la columna etnia
    df[etnia] = np.where((df['iden_pertenenciaetnica_'] =='Afrocolombiano'), 'NARP', df['iden_pertenenciaetnica_'])
    df[etnia].replace({"AFROCOLOMBIANO": "NARP",
                        "AFROCOLOMBIANOA": "NARP",
                        "AFROCOLOMBIANA": "NARP",
                        "PALENQUERO": "NARP",
                        "RAIZAL": "NARP",
                        "NINGUNA": "MESTIZO"}, inplace=True)
    df[etnia] = np.where((df['iden_pertenenciaetnica_'].str.contains('Indígena|Nasa')), 'INDIGENA', df[etnia])
    df[etnia] = np.where((df['iden_pertenenciaetnica_'].str.contains('Ninguno')), 'NINGUNA', df[etnia])
    
    na_values = {
        'NO APLICA': None,
        'NULL': None,
        'ND': None,
        'NA': None,
        'SIN INFOR': None,
        'SIN DETERM': None,
        'POR DEFINIR': None,
        'NONE': None,
        'Indeterminado': None
    }

    df[etnia] = df[etnia].replace(na_values)
    df[etnia] = df[etnia].fillna("")
    # Eliminar la columna original
    df.drop(columns=['iden_pertenenciaetnica_'], inplace=True)

