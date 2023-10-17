import pandas as pd
import numpy as np

def homologar_paramilitares (df : pd ):

    df['pres_resp_paramilitares'] = np.where(
        (
		df['presunto_responsable'].str.contains("PARAMILITAR") | 
		(df['presunto_responsable'].str.contains("AUTO") & df['presunto_responsable'].str.contains("DEFENSA")) | 
		df['presunto_responsable'].str.contains("AUC") | 
		df['presunto_responsable'].str.contains( "ACC") |  
		df['presunto_responsable'].str.contains( "ACCU") | 
		df['presunto_responsable'].str.contains( "ACMV") | 
		df['presunto_responsable'].str.contains( "ACMM") | 
		df['presunto_responsable'].str.contains( "BCB") |
		((df['presunto_responsable'].str.contains( "BLOQUE") | df['presunto_responsable'].str.contains( "FRENTE")) & 
		((df['presunto_responsable'].str.contains( "ALEX") & df['presunto_responsable'].str.contains( "HURTADO")) |
		(df['presunto_responsable'].str.contains( "BAJO") & df['presunto_responsable'].str.contains( "CAUCA")) |
		df['presunto_responsable'].str.contains( "BANANERO") |
		(df['presunto_responsable'].str.contains( "CACIQUE") & df['presunto_responsable'].str.contains( "NUTIBARA")) | 
		df['presunto_responsable'].str.contains( "CALIMA") |
		(df['presunto_responsable'].str.contains( "CANAL") & df['presunto_responsable'].str.contains( "DIQUE")) | 
		df['presunto_responsable'].str.contains( "CATATUMBO") |
		(df['presunto_responsable'].str.contains( "CENTAUROS") | df['presunto_responsable'].str.contains( "CENTAROS ")) | 
		(df['presunto_responsable'].str.contains( "CENTRAL") & df['presunto_responsable'].str.contains( "BOLIVAR")) |
		(df['presunto_responsable'].str.contains( "CORDOBA") & (df['presunto_responsable'].str.contains( "JOSE") | df['presunto_responsable'].str.contains( "MARIA") | df['presunto_responsable'].str.contains( "28"))) | 
		(df['presunto_responsable'].str.contains( "CONTRA") & df['presunto_responsable'].str.contains( "WAYU")) |
		df['presunto_responsable'].str.contains( "CUNDINAMARCA") | 
		(df['presunto_responsable'].str.contains( "ELMER") & df['presunto_responsable'].str.contains( "CARDENA")) |
		(df['presunto_responsable'].str.contains( "HEROE") | df['presunto_responsable'].str.contains( "CHOCO")) |
		(df['presunto_responsable'].str.contains( "HEROE") | df['presunto_responsable'].str.contains( "GRANADA")) |
		(df['presunto_responsable'].str.contains( "HEROE") | df['presunto_responsable'].str.contains( "GUALIVA")) |
		(df['presunto_responsable'].str.contains( "HEROE") & df['presunto_responsable'].str.contains( "TOLOVA")) | 
		(df['presunto_responsable'].str.contains( "HEROE") & df['presunto_responsable'].str.contains( "SANTA") & df['presunto_responsable'].str.contains( "ROSA")) | 
		(df['presunto_responsable'].str.contains( "JOSE") & df['presunto_responsable'].str.contains( "CELESTINO") & df['presunto_responsable'].str.contains( "MANTILLA")) |
		(df['presunto_responsable'].str.contains( "JOSE") & df['presunto_responsable'].str.contains( "PABLO") & df['presunto_responsable'].str.contains( "DIAZ")) |
		(df['presunto_responsable'].str.contains( "LIBERTADORES") & df['presunto_responsable'].str.contains( "SUR")) |
		(df['presunto_responsable'].str.contains( "MAGDALENA") & df['presunto_responsable'].str.contains( "MEDIO")) |
		df['presunto_responsable'].str.contains( "METRO") | 
		df['presunto_responsable'].str.contains( "MINERO") |
		df['presunto_responsable'].str.contains( "MOJANA") |
		(df['presunto_responsable'].str.contains( "MONTES") & df['presunto_responsable'].str.contains( "MARIA")) | 
		(df['presunto_responsable'].str.contains( "NORDESTE") & df['presunto_responsable'].str.contains( "ANTIOQUENO")) | 
		(df['presunto_responsable'].str.contains( "NOROCCIDENTE") & df['presunto_responsable'].str.contains( "ANTIOQUENO")) | 
		df['presunto_responsable'].str.contains( "NORTE") | 
		df['presunto_responsable'].str.contains( "PACIFICO") |
		(df['presunto_responsable'].str.contains( "RAMON") & df['presunto_responsable'].str.contains( "ISAZA"))  |
		(df['presunto_responsable'].str.contains( "SANTA") & df['presunto_responsable'].str.contains( "ROSA") & df['presunto_responsable'].str.contains( "SUR")) | 
		(df['presunto_responsable'].str.contains( "SUR") & df['presunto_responsable'].str.contains( "PUTUMAYO")) | 
		(df['presunto_responsable'].str.contains( "SUROESTE") & df['presunto_responsable'].str.contains( "ANTIOQUENO")) | 
		df['presunto_responsable'].str.contains( "TOLIMA") |
		(df['presunto_responsable'].str.contains( "VENCEDORES") & df['presunto_responsable'].str.contains( "ARAUCA")) |
		(df['presunto_responsable'].str.contains( "ALFREDO") & df['presunto_responsable'].str.contains( "SOCARRAS")) | 
		df['presunto_responsable'].str.contains( "ANDAQUIES") | 
		df['presunto_responsable'].str.contains( "BAJIRA") | 
		df['presunto_responsable'].str.contains( "CAPITAL") | 
		(df['presunto_responsable'].str.contains( "CACIQUE") & df['presunto_responsable'].str.contains( "GUANENTA")) | 
		(df['presunto_responsable'].str.contains( "CACIQUE") & df['presunto_responsable'].str.contains( "PIPINTA")) |
		(df['presunto_responsable'].str.contains( "PIPINTA")) |
		(df['presunto_responsable'].str.contains( "COMPANERO") & df['presunto_responsable'].str.contains( "CARRILLO")) | 
		(df['presunto_responsable'].str.contains( "CONQUISTADORES") & df['presunto_responsable'].str.contains( "YARI")) | 
		df['presunto_responsable'].str.contains( "GABARRA") |
		(df['presunto_responsable'].str.contains( "GOLFO") & df['presunto_responsable'].str.contains( "MORROSQUILLO")) | 
		df['presunto_responsable'].str.contains( "GUAVIARE") |
		(df['presunto_responsable'].str.contains( "HEROE") & df['presunto_responsable'].str.contains( "FLORENCIA")) | 
		(df['presunto_responsable'].str.contains( "HEROE") & df['presunto_responsable'].str.contains( "GUATICA")) |
		(df['presunto_responsable'].str.contains( "HEROE") & df['presunto_responsable'].str.contains( "LLANO")) |
		(df['presunto_responsable'].str.contains( "HEROE") & df['presunto_responsable'].str.contains( "PRODIGIO")) | 
		(df['presunto_responsable'].str.contains( "HEROE") & df['presunto_responsable'].str.contains( "GUAVIARE")) | 
		(df['presunto_responsable'].str.contains( "HEROE") & df['presunto_responsable'].str.contains( "TOLOVA")) | 
		(df['presunto_responsable'].str.contains( "HECTOR") & df['presunto_responsable'].str.contains( "JULIO") & df['presunto_responsable'].str.contains( "PEINADO") & df['presunto_responsable'].str.contains( "BECERRA")) | 
		(df['presunto_responsable'].str.contains( "JOSE") & df['presunto_responsable'].str.contains( "LUIS") & df['presunto_responsable'].str.contains( "ZULUAGA")) | 
		(df['presunto_responsable'].str.contains( "MARTIR") & df['presunto_responsable'].str.contains( "GUATICA")) | 
		df['presunto_responsable'].str.contains( "MOJANA") |
		df['presunto_responsable'].str.contains( "ORTEGA") |
		(df['presunto_responsable'].str.contains( "OMAR") & df['presunto_responsable'].str.contains( "ISAZA")) | 
		(df['presunto_responsable'].str.contains( "PROCER") & df['presunto_responsable'].str.contains( "CAGUAN")) | 
		(df['presunto_responsable'].str.contains( "RESISTENCIA") & df['presunto_responsable'].str.contains( "TAYRONA"))  | 
		df['presunto_responsable'].str.contains( "QUIMBAYA") |
		df['presunto_responsable'].str.contains( "VICHADA"))) |
		(df['presunto_responsable'].str.contains( "CACIQUE") & df['presunto_responsable'].str.contains( "LLANOS")) |
		(df['presunto_responsable'].str.contains( "HEROES") & df['presunto_responsable'].str.contains( "LLANOS")) |
		(df['presunto_responsable'].str.contains( "MARTIN") & df['presunto_responsable'].str.contains( "NUTIBARA")) |
		(df['presunto_responsable'].str.contains( "CASA") & df['presunto_responsable'].str.contains( "CASTANO")) |
		(df['presunto_responsable'].str.contains("MUERTE") & df['presunto_responsable'].str.contains("SECUESTRADORES")) | 
		df['presunto_responsable'].str.contains("MAS")  | 
		df['presunto_responsable'].str.contains( "MASETO") |
		df['presunto_responsable'].str.contains( "TANGUERO") |
		df['presunto_responsable'].str.contains( "CARRANCERO") |
		df['presunto_responsable'].str.contains( "BUITRAG")  | 
		df['presunto_responsable'].str.contains( "CONVIVIR") 
        ), 1, 0)