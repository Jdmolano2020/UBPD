import pandas as pd
import numpy as np

def homologar_eln (df : pd ):

    df['pres_resp_guerr_eln'] = np.where((
  		df['presunto_responsable'].str.contains("ELN") | 
  		(df['presunto_responsable'].str.contains("EJERCITO") & df['presunto_responsable'].str.contains("LIBERACION") & df['presunto_responsable'].str.contains("NACIONAL")) |
  		df['presunto_responsable'].str.contains("COCE") |
  		(df['presunto_responsable'].str.contains("COMANDO") & df['presunto_responsable'].str.contains("CENTRAL")) |
  		(df['presunto_responsable'].str.contains("FRENTE") &
  		((df['presunto_responsable'].str.contains("ADONAY") & df['presunto_responsable'].str.contains("ARDILA")) |
  		(df['presunto_responsable'].str.contains("BERNARDO") & df['presunto_responsable'].str.contains("LOPEZ")) |
  		(df['presunto_responsable'].str.contains("BOCHE")) |
  		(df['presunto_responsable'].str.contains("BOLCHEVIQUES") & df['presunto_responsable'].str.contains("LIBANO")) |
  		(df['presunto_responsable'].str.contains("CACIQUE") & df['presunto_responsable'].str.contains("CALARCA")) |
  		(df['presunto_responsable'].str.contains("CAMILO") & df['presunto_responsable'].str.contains("TORRES")) |
  		(df['presunto_responsable'].str.contains("CAPITAN") & df['presunto_responsable'].str.contains("PARMENIO")) |
  		(df['presunto_responsable'].str.contains("CAPITAN") & df['presunto_responsable'].str.contains("FRANCISCO")) |
  		(df['presunto_responsable'].str.contains("CAPITAN") & df['presunto_responsable'].str.contains("MAURICIO")) |
  		(df['presunto_responsable'].str.contains("CAPITAN") & df['presunto_responsable'].str.contains("PARMENIO")) |
  		(df['presunto_responsable'].str.contains("CARLOS") & df['presunto_responsable'].str.contains("CACUA")) |
  		(df['presunto_responsable'].str.contains("HEROES") & df['presunto_responsable'].str.contains("CATATUMBO")) |
  		(df['presunto_responsable'].str.contains("COMPANIA") & df['presunto_responsable'].str.contains("CATATUMBO")) |
  		(df['presunto_responsable'].str.contains("CARLOS") & df['presunto_responsable'].str.contains("ALIRIO") & df['presunto_responsable'].str.contains("BUITRAGO")) |
  		(df['presunto_responsable'].str.contains("CARLOS") & df['presunto_responsable'].str.contains("ALBERTO") & df['presunto_responsable'].str.contains("TROCHE")) |
  		(df['presunto_responsable'].str.contains("CLAUDIA") & df['presunto_responsable'].str.contains("ISABEL") & df['presunto_responsable'].str.contains("ESCOBAR") & df['presunto_responsable'].str.contains("JEREZ")) |
  		(df['presunto_responsable'].str.contains("COMPANERO") & df['presunto_responsable'].str.contains("TOMAS")) |
  		(df['presunto_responsable'].str.contains("COMUNERO") & df['presunto_responsable'].str.contains("SUR")) |
  		(df['presunto_responsable'].str.contains("DARIO") & df['presunto_responsable'].str.contains("RAMIREZ") & df['presunto_responsable'].str.contains("CASTRO")) |
  		(df['presunto_responsable'].str.contains("DOMINGO") & df['presunto_responsable'].str.contains("LAIN") & df['presunto_responsable'].str.contains("SANZ")) |
  		(df['presunto_responsable'].str.contains("EDGAR") & df['presunto_responsable'].str.contains("GRIMALDO")) |
  		(df['presunto_responsable'].str.contains("EFRAIN") & df['presunto_responsable'].str.contains("PABON")) |
  		(df['presunto_responsable'].str.contains("FRANCISCO") & df['presunto_responsable'].str.contains("JAVIER") & df['presunto_responsable'].str.contains("CASTANO")) |
  		(df['presunto_responsable'].str.contains("GERMAN") & df['presunto_responsable'].str.contains("VELASCO") & df['presunto_responsable'].str.contains("VILLAMIZAR")) |
  		(df['presunto_responsable'].str.contains("GUERREROS") & df['presunto_responsable'].str.contains("SINDAGUA")) |
  		(df['presunto_responsable'].str.contains("JAIME") & df['presunto_responsable'].str.contains("BATEMAN") & df['presunto_responsable'].str.contains("CAYON")) |
  		(df['presunto_responsable'].str.contains("JAIME") & df['presunto_responsable'].str.contains("TONO")) |
  		(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("GALAN")) |
  		(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("DAVID") & df['presunto_responsable'].str.contains("SUAREZ")) |
  		(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("MANUEL") & df['presunto_responsable'].str.contains("MARTINEZ") & df['presunto_responsable'].str.contains("QUIROZ")) | 
  		(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("SEPULVEDA")) |
  		(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ELIECER") & df['presunto_responsable'].str.contains("GAITAN")) |
  		(df['presunto_responsable'].str.contains("KALEB") & df['presunto_responsable'].str.contains("GOMEZ") & df['presunto_responsable'].str.contains("PADRON")) |
  		(df['presunto_responsable'].str.contains("LUIS") & df['presunto_responsable'].str.contains("ENRIQUE") & df['presunto_responsable'].str.contains("LEON") & df['presunto_responsable'].str.contains("GUERRA")) | 
  		(df['presunto_responsable'].str.contains("LUIS") & df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("SOLANO") & df['presunto_responsable'].str.contains("SEPULVEDA")) | 
  		(df['presunto_responsable'].str.contains("LUCIANO") & df['presunto_responsable'].str.contains("ARIZA")) |
  		(df['presunto_responsable'].str.contains("LUIS") & df['presunto_responsable'].str.contains("ARIZA")) |
  		(df['presunto_responsable'].str.contains("LUCHO") & df['presunto_responsable'].str.contains("QUINTERO")) | 
  		(df['presunto_responsable'].str.contains("MARTIR") & df['presunto_responsable'].str.contains("ANORI")) |
  		(df['presunto_responsable'].str.contains("MARTIR") & df['presunto_responsable'].str.contains("SANTA") & df['presunto_responsable'].str.contains("ROSA")) |
  		(df['presunto_responsable'].str.contains("MANUEL") & df['presunto_responsable'].str.contains("GUSTAVO") & df['presunto_responsable'].str.contains("CHACON") & df['presunto_responsable'].str.contains("SARMIENTO")) |
  		(df['presunto_responsable'].str.contains("MANUEL") & df['presunto_responsable'].str.contains("HERNANDEZ")) |
  		(df['presunto_responsable'].str.contains("MANUEL") & df['presunto_responsable'].str.contains("VASQUEZ")) |
  		(df['presunto_responsable'].str.contains("OMAIRA") & df['presunto_responsable'].str.contains("MONTOYA") & df['presunto_responsable'].str.contains("HENAO")) |
  		(df['presunto_responsable'].str.contains("OMAR") & df['presunto_responsable'].str.contains("GOMEZ")) |
  		(df['presunto_responsable'].str.contains("RESISTENCIA") & df['presunto_responsable'].str.contains("CIMARRON")))) |
  		((df['presunto_responsable'].str.contains("FRENTE") & df['presunto_responsable'].str.contains("GUERRA")) &
  		(df['presunto_responsable'].str.contains("ORIENTAL") | 
  		df['presunto_responsable'].str.contains("SUROCCIDENTAL") | 
  		df['presunto_responsable'].str.contains("OCCIDENTAL") |
  		df['presunto_responsable'].str.contains("CENTRAL") |
  		df['presunto_responsable'].str.contains("NORTE") |
  		df['presunto_responsable'].str.contains("NORORIENTAL"))) |
  		((df['presunto_responsable'].str.contains("COMPANIA") | df['presunto_responsable'].str.contains("COMISION")) &
  		((df['presunto_responsable'].str.contains("CACIQUE") & df['presunto_responsable'].str.contains("CALARCA")) |
  		(df['presunto_responsable'].str.contains("COMANDANTE") & df['presunto_responsable'].str.contains("DIEGO")) |
  		(df['presunto_responsable'].str.contains("CAMILO") & df['presunto_responsable'].str.contains("CIENFUEGOS")) |
  		(df['presunto_responsable'].str.contains("FRANCISCO") & df['presunto_responsable'].str.contains("BOSSIO")) |
  		(df['presunto_responsable'].str.contains("HEROE") & df['presunto_responsable'].str.contains("TARAZA")) |
  		(df['presunto_responsable'].str.contains("JAIME") & df['presunto_responsable'].str.contains("TONO") & df['presunto_responsable'].str.contains("OBANDO")) |
  		(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("GALAN")) |
  		(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ALFREDO") & df['presunto_responsable'].str.contains("ARRIGUI")) |
  		(df['presunto_responsable'].str.contains("MARIA") & df['presunto_responsable'].str.contains("EUGENIA") & df['presunto_responsable'].str.contains("VEGA")) |
  		(df['presunto_responsable'].str.contains("MARTIR") & df['presunto_responsable'].str.contains("TARAZA")) |
  		(df['presunto_responsable'].str.contains("MARTHA") & df['presunto_responsable'].str.contains("ELENA") & df['presunto_responsable'].str.contains("BARON")) |
  		(df['presunto_responsable'].str.contains("NESTOR") & df['presunto_responsable'].str.contains("TULIO") & df['presunto_responsable'].str.contains("DURAN")) |
  		(df['presunto_responsable'].str.contains("OMAIRA") & df['presunto_responsable'].str.contains("MONTOYA") & df['presunto_responsable'].str.contains("HENAO")) |
  		df['presunto_responsable'].str.contains("SIMACOTA") |
  		df['presunto_responsable'].str.contains("CIMARRON") |
  		(df['presunto_responsable'].str.contains("TITO") & df['presunto_responsable'].str.contains("MARIN"))))
          ),1,0)