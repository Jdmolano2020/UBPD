import pandas as pd
import numpy as np

def homologar_bandas_criminales (df : pd ):
    
    df['pres_resp_grupos_posdesmov'] = np.where((
        df['presunto_responsable'].str.contains("BACRIM") |  
        (df['presunto_responsable'].str.contains( "POS")  & df['presunto_responsable'].str.contains( "DESMOVILIZA")) |  
        df['presunto_responsable'].str.contains( "BANDA") |
        df['presunto_responsable'].str.contains( "RASTROJO") |  
        (df['presunto_responsable'].str.contains( "AGUILA") |  df['presunto_responsable'].str.contains( "NEGRA") ) |
        df['presunto_responsable'].str.contains( "AGC")  | 
        df['presunto_responsable'].str.contains( "URABENO") | 
        df['presunto_responsable'].str.contains( "USUGA") |
        (df['presunto_responsable'].str.contains( "CLAN") & df['presunto_responsable'].str.contains( "GOLFO")) |  
        df['presunto_responsable'].str.contains( "CAPARRO") |
        df['presunto_responsable'].str.contains( "PUNTILLERO") |
        (df['presunto_responsable'].str.contains( "OFICINA") & df['presunto_responsable'].str.contains( "ENVIGADO")) |
        (df['presunto_responsable'].str.contains( "LA") & df['presunto_responsable'].str.contains( "EMPRESA")) |
        (df['presunto_responsable'].str.contains( "LA") & df['presunto_responsable'].str.contains( "CONSTRU")) |
        df['presunto_responsable'].str.contains( "CAQUETEÑO") |
        df['presunto_responsable'].str.contains( "PACHENCA") |
        df['presunto_responsable'].str.contains( "PELUSO") |
        (df['presunto_responsable'].str.contains( "CLAN") & df['presunto_responsable'].str.contains( "NORTE")) |
        (df['presunto_responsable'].str.contains( "CLAN") & df['presunto_responsable'].str.contains( "ORIENTE"))
        ),1,0)
