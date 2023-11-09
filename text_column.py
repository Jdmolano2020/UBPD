import pandas as pd
import re

def limpiar_texto(texto):
    texto = str(texto).upper().strip()
    texto = re.sub(r"[ÁÉÍÓÚÜ]", lambda x: {"Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ü": "U"}[x.group()], texto)
    texto = re.sub(r'\s{2,}', ' ', texto)
    return texto

def eliminar_caracteres_especiales(texto):
    return re.sub(r'[^a-zA-Z0-9 ]', '', texto)

def aplicar_reglas_validacion(df, columnas):
    for columna in columnas:
        df[columna] = df[columna].apply(limpiar_texto)
        df[columna] = df[columna].apply(eliminar_caracteres_especiales)

        undesired_values = ["NO APLICA", "NULL", "ND", "NA", "NR", "SIN INFORMACION", "NO SABE", "DESCONOCIDO"]
        condition = df[columna].isin(undesired_values) | ((df[columna].str.contains("POR|SIN")) & (df[columna].str.contains("DEFINIR|ESTABLECER")))
        df.loc[condition, columna] = ""

    return df
