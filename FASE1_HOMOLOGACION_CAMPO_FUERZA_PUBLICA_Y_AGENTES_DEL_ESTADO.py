import pandas as pd
import numpy as np

def homologar_fuerzapublica (df : pd ):
# Crear la variable 'pres_resp_agentes_estatales'
    df['pres_resp_agentes_estatales'] = np.where(
    (
        (df['presunto_responsable'].str.contains("FUERZA", na=False) & df['presunto_responsable'].str.contains("PUBLICA", na=False)) |
        (df['presunto_responsable'].str.contains("ESTADO", na=False) & ~df['presunto_responsable'].str.contains("MAYOR", na=False) & ~df['presunto_responsable'].str.contains("CONJUNTO", na=False) & ~df['presunto_responsable'].str.contains("FARC", na=False)) |
		(df['presunto_responsable'].str.contains("AGENTE", na=False) & df['presunto_responsable'].str.contains("ESTA", na=False)) |        
		((df['presunto_responsable'].str.contains("CTI", na=False) & df['presunto_responsable'].str.len()==3) | df['presunto_responsable'].str.contains(" CTI", na=False) | df['presunto_responsable'].str.contains("CTI ", na=False)) |        
		(df['presunto_responsable'].str.contains("CUERPO", na=False) | df['presunto_responsable'].str.contains("TECNICO", na=False) | df['presunto_responsable'].str.contains("INVESTIGA", na=False)) |		
        df['presunto_responsable'].str.contains("FISCALIA", na=False) |        
		df['presunto_responsable'].str.contains("FGN", na=False) |        
		df['presunto_responsable'].str.contains("POLICIA", na=False) |        
        df['presunto_responsable'].str.contains("CARABINEROS", na=False) |
        df['presunto_responsable'].str.contains("SIJIN", na=False) |
        df['presunto_responsable'].str.contains("DIJIN", na=False) |        
		(df['presunto_responsable'] == "F2") |
		((df['presunto_responsable'] == "DAS")) |         
		(df['presunto_responsable'].str.contains("EJERCITO", na=False) & ~df['presunto_responsable'].str.contains("LIBERACION", na=False) & ~df['presunto_responsable'].str.contains("EVOLUCIONA", na=False) & ~df['presunto_responsable'].str.contains("PUEBLO", na=False)) |         
		(df['presunto_responsable'].str.contains("DEPARTAMENTO", na=False) & df['presunto_responsable'].str.contains("SEGURIDAD", na=False)) |
		(df['presunto_responsable'].str.contains("EJERCITO", na=False) & ~df['presunto_responsable'].str.contains("LIBERACION", na=False) & df['presunto_responsable'].str.contains("NACIONAL", na=False) & ~df['presunto_responsable'].str.contains("REVOLUCI", na=False)) |        
		df['presunto_responsable'].str.contains("BATALLON", na=False) |
        df['presunto_responsable'].str.contains("BINCI", na=False) |                
		(df['presunto_responsable'].str.contains("CHARRY", na=False) & ~df['presunto_responsable'].str.contains("SOLANO", na=False)) |        
		(df['presunto_responsable'] == "B2") |        
		df['presunto_responsable'].str.contains("BRIGADA", na=False) |        
		(df['presunto_responsable'].str.contains("MILITAR", na=False) & ~df['presunto_responsable'].str.contains("PARA", na=False)) |        
		(df['presunto_responsable'].str.contains("FUERZA", na=False) & df['presunto_responsable'].str.contains("AEREA", na=False) & df['presunto_responsable'].str.contains("COLOMBIANA", na=False)) |        
		(df['presunto_responsable'].str.contains("ARMADA", na=False) & df['presunto_responsable'].str.contains("NACIONAL", na=False)) |        
		(df['presunto_responsable'].str.contains("MARINA", na=False)) |        
		df['presunto_responsable'].str.contains("FAC", na=False) |        
		(df['presunto_responsable'].str.contains("FUERZA", na=False) & df['presunto_responsable'].str.contains("AEREA", na=False)) |        
		(df['presunto_responsable'].str.contains("FUERZA", na=False) & df['presunto_responsable'].str.contains("TAREA", na=False) & df['presunto_responsable'].str.contains("CONJUNTA", na=False)) |        
		df['presunto_responsable'].str.contains("GAULA", na=False) |
        
		(df['presunto_responsable'].str.contains("DIVISION", na=False) & (df['presunto_responsable'].str.contains("PRIMERA", na=False) | df['presunto_responsable'].str.contains("SEGUNDA", na=False) | df['presunto_responsable'].str.contains("TERCERA", na=False) | df['presunto_responsable'].str.contains("CUARTA", na=False) | df['presunto_responsable'].str.contains("QUINTA", na=False) | df['presunto_responsable'].str.contains("SEXTA", na=False) | df['presunto_responsable'].str.contains("SEPTIMA", na=False)))
        ), 1, 0)