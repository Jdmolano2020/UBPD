import pandas as pd
import numpy as np

def homologar_otras_guerrillas (df : pd ):
    
    df['pres_resp_guerr_otra'] = np.where((
        (df['pres_resp_guerr_eln'].isna() | pd.to_numeric(df['pres_resp_guerr_eln']) == 0) & 
        (df['pres_resp_guerr_farc'].isna() | pd.to_numeric(df['pres_resp_guerr_farc']) == 0) & 
        (df['pres_resp_paramilitares'].isna() | pd.to_numeric(df['pres_resp_paramilitares']) == 0) & 
        (df['pres_resp_grupos_posdesmov'].isna() | pd.to_numeric(df['pres_resp_grupos_posdesmov']) == 0) & 
        (df['pres_resp_agentes_estatales'].isna() | pd.to_numeric(df['pres_resp_agentes_estatales']) == 0) &
		(df['presunto_responsable'].str.contains("GUERRILL") | 
		df['presunto_responsable'].str.contains("ERP") | 
		df['presunto_responsable'].str.contains("EPR") |
		(df['presunto_responsable'].str.contains("EJERCITO") & df['presunto_responsable'].str.contains("REVOLUCIONARIO") & df['presunto_responsable'].str.contains("PUEBLO")) |
		df['presunto_responsable'].str.contains("ERG") | 
		(df['presunto_responsable'].str.contains("MOVIMIENTO") & df['presunto_responsable'].str.contains("OBRERO") & df['presunto_responsable'].str.contains("ESTUDIANTIL") & df['presunto_responsable'].str.contains("CAMPESINO")) |
		df['presunto_responsable'].str.contains("MOEC") | 
		(df['presunto_responsable'].str.contains("EJERCITO") & df['presunto_responsable'].str.contains("REVOLUCIONARIO") & df['presunto_responsable'].str.contains("GUEVARISTA")) |
		df['presunto_responsable'].str.contains( "QUINTIN LAME") |
		df['presunto_responsable'].str.contains("EPL") |
		(df['presunto_responsable'].str.contains("EJERCITO") & df['presunto_responsable'].str.contains("POPULAR") & df['presunto_responsable'].str.contains("LIBERACION")) | 
		(df['presunto_responsable'].str.contains("M") & df['presunto_responsable'].str.contains("19")) |
		(df['presunto_responsable'].str.contains("PARTIDO") & df['presunto_responsable'].str.contains("REVOLUCIONARIO") & df['presunto_responsable'].str.contains("TRABAJADORES")) |
		(df['presunto_responsable'].str.contains("PRT")))
    ),1,0)

