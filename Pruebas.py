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

def homologar_fuerzapublica (df : pd ):
# Crear la variable 'pres_resp_agentes_estatales'
    df['pres_resp_agentes_estatales'] = np.where(
        (
        df['presunto_responsable'].str.contains("FUERZA PUBLICA") |
        
        (df['presunto_responsable'].str.contains("ESTADO") & 
         ~df['presunto_responsable'].str.contains("MAYOR") & 
         ~df['presunto_responsable'].str.contains("CONJUNTO") & 
         ~df['presunto_responsable'].str.contains("FARC")) |
        
        (df['presunto_responsable'].str.contains("AGENTE ESTA")) |
        
        ((df['presunto_responsable'].str.contains("CTI") &
          (df['presunto_responsable'].str.len() == 3)) |
         df['presunto_responsable'].str.contains(" CTI") |
         df['presunto_responsable'].str.contains("CTI ")) |
        
        (df['presunto_responsable'].str.contains("CUERPO") | 
         df['presunto_responsable'].str.contains("TECNICO") | 
         df['presunto_responsable'].str.contains("INVESTIGA")) |
        
        df['presunto_responsable'].str.contains("FISCALIA") |
        
        df['presunto_responsable'].str.contains("FGN") |
        
        df['presunto_responsable'].str.contains("POLICIA") |
        
        df['presunto_responsable'].str.contains("CARABINEROS") |
        
        df['presunto_responsable'].str.contains("SIJIN") |
        
        df['presunto_responsable'].str.contains("DIJIN") |
        
        (df['presunto_responsable'] == "F2") |
        
        (df['presunto_responsable'] == "DAS") |
        
        (df['presunto_responsable'].str.contains("EJERCITO") &
         ~df['presunto_responsable'].str.contains("LIBERACION") &
         ~df['presunto_responsable'].str.contains("REVOLUCIONA") & 
         ~df['presunto_responsable'].str.contains("PUEBLO")) |
        
        (df['presunto_responsable'].str.contains("DEPARTAMENTO") &
         df['presunto_responsable'].str.contains("SEGURIDAD")) |
        
        (df['presunto_responsable'].str.contains("EJERCITO") &
         ~df['presunto_responsable'].str.contains("LIBERACION") &
         df['presunto_responsable'].str.contains("NACIONAL") &
         ~df['presunto_responsable'].str.contains("REVOLUCI")) |
        
        df['presunto_responsable'].str.contains("BATALLON") |
        
        df['presunto_responsable'].str.contains("BINCI") |
        
        (df['presunto_responsable'].str.contains("CHARRY") &
         ~df['presunto_responsable'].str.contains("SOLANO")) |
        
        (df['presunto_responsable'] == "B2") |
        
        df['presunto_responsable'].str.contains("BRIGADA") |
        
        (df['presunto_responsable'].str.contains("MILITAR") &
         ~df['presunto_responsable'].str.contains("PARA")) |
        
        (df['presunto_responsable'].str.contains("FUERZA") &
         df['presunto_responsable'].str.contains("AEREA") &
         df['presunto_responsable'].str.contains("COLOMBIANA")) |
        
        (df['presunto_responsable'].str.contains("ARMADA") &
         df['presunto_responsable'].str.contains("NACIONAL")) |
        
        df['presunto_responsable'].str.contains("MARINA") |
        
        df['presunto_responsable'].str.contains("FAC") |
        
        (df['presunto_responsable'].str.contains("FUERZA") &
         df['presunto_responsable'].str.contains("AEREA")) |
        
        (df['presunto_responsable'].str.contains("FUERZA") &
         df['presunto_responsable'].str.contains("TAREA") &
         df['presunto_responsable'].str.contains("CONJUNTA")) |
        
        df['presunto_responsable'].str.contains("GAULA") |
        
        (df['presunto_responsable'].str.contains("DIVISION") &
         (df['presunto_responsable'].str.contains("PRIMERA") |
          df['presunto_responsable'].str.contains("SEGUNDA") | 
          df['presunto_responsable'].str.contains("TERCERA") | 
          df['presunto_responsable'].str.contains("CUARTA") | 
          df['presunto_responsable'].str.contains("QUINTA") | 
          df['presunto_responsable'].str.contains("SEXTA") | 
          df['presunto_responsable'].str.contains("SEPTIMA")))
        ), 1, 0)
