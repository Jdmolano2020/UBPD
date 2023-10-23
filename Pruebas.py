import unicodedata
import re


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

