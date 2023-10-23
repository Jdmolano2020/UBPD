import pandas as pd
import numpy as np

def homologar_otras_guerrillas (df : pd ):
    
    df['pres_resp_guerr_otra'] = np.where((
        df['pres_resp_guerr_eln'].isna() & df['pres_resp_guerr_farc'].isna() & df['pres_resp_paramilitares'].isna()  & df['pres_resp_grupos_posdesmov'].isna() & df['pres_resp_agentes_estatales'].isna() &
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

