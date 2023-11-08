import re
# Define una funciÃ³n para limpiar nombres y apellidos
def limpiar_nombre_completo(nombre_completo):
    if nombre_completo is None:
        return None, None, None, None
    
    if nombre_completo in ["PERSONA SIN IDENTIFICAR", "NA"] :
        return None, None, None, None
    
    # Divide el nombre completo en tokens
    tokens = re.split(r'\s+', nombre_completo.strip())
    
    primer_nombre, segundo_nombre, primer_apellido, segundo_apellido = None, None, None, None
    
    # Elimina preposiciones
    preposiciones = ["DE", "DEL", "DE LAS", "DE LA", "DE LOS", "VAN", "LA", "VIUDA DE", "VIUDA", "SAN", "DA"]
    tokens = [token for token in tokens if token not in preposiciones]
    
    n_iter = 0  # Variable para rastrear el número de iteraciones
    while len(tokens) > 4:
        k = 0
        while k < len(tokens):
            if k == 1:
                # Concatena el elemento actual con el siguiente elemento y almacena el resultado en la posición actual
                tokens[k] = tokens[k] + " " + tokens[k + 1]
                # Elimina el siguiente elemento
                tokens.pop(k + 1)
            k += 1
        n_iter += 1
        if n_iter > 10:
            break
    
    if len(tokens) == 4:
        primer_nombre, segundo_nombre, primer_apellido, segundo_apellido = tokens
    elif len(tokens) == 3:
        primer_nombre, primer_apellido, segundo_apellido = tokens
    elif len(tokens) == 2:
        primer_nombre, primer_apellido = tokens
    
    return primer_nombre, segundo_nombre, primer_apellido, segundo_apellido