import pandas as pd

def nombres_validos (df : pd, primer_nombre, segundo_nombre, primer_apellido, segundo_apellido, nombre_completo):
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
        df[col] = df[col].fillna("")
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
