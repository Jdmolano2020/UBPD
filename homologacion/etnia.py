import pandas as pd
import numpy as np


def etnia_valida(df: pd, etnia):

    df_copy = df.copy()
    subset = df_copy[etnia].copy()
    # Reemplazar valores en la columna etnia

    subset.replace({"Afrocolombiano": "NARP",
                    "AFROCOLOMBIANO": "NARP",
                    "AFROCOLOMBIANOA": "NARP",
                    "AFROCOLOMBIANA": "NARP",
                    "PALENQUERO": "NARP",
                    "RAIZAL": "NARP",
                    "MULATO": "NARP",
                    "NEGRO": "NARP",
                    "MESTIZO": "NINGUNA",
                    "BLANCO": "NINGUNA",
                    "MESTIZO": "NINGUNA",
                    "Indígena": 'INDIGENA',
                    "Nasa": 'INDIGENA',
                    "Ninguno": "NINGUNA",
                    "ROM": "RROM"
                    }, inplace=True)

    na_values = {
        'NO APLICA': None,
        'NULL': None,
        'ND': None,
        'NA': None,
        'NAN': None,
        'SIN INFOR': None,
        'SIN DETERM': None,
        'POR DEFINIR': None,
        'NONE': None,
        'Indeterminado': None}
    subset = subset.replace(na_values)
    subset = np.where(subset == "", "NINGUNA", subset)

    df.loc[df_copy.index, etnia] = subset    # Eliminar la columna original
    # df.drop(columns=['iden_pertenenciaetnica_'], inplace=True)
    df[etnia].fillna("")
