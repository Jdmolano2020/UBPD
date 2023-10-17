import pandas as pd
import numpy as np

def homologar_farc (df : pd ):

    df['pres_resp_agentes_estatales'] = np.where(
        (df['presunto_responsable'].str.contains("FARC") | 
    	(df['presunto_responsable'].str.contains("FUERZA") & df['presunto_responsable'].str.contains("ARMAD") & df['presunto_responsable'].str.contains("REVOLUCION")) | 
    	df['presunto_responsable'].str.contains("FRAC") |
    	df['presunto_responsable'].str.contains("FACR") | 
    	df['presunto_responsable'].str.contains("FARAC") |
    	(df['presunto_responsable'].str.contains("FAR") & df['presunto_responsable'].str.contains("GUERRILLA")) |
    	(df['presunto_responsable'].str.contains("ESTADO") & df['presunto_responsable'].str.contains("MAYOR") & df['presunto_responsable'].str.contains("CONJUNTO")) |
    	(df['presunto_responsable'].str.contains("FRENTE") & 
    	(df['presunto_responsable'].str.contains("1") | df['presunto_responsable'].str.contains("2") | df['presunto_responsable'].str.contains("3") | df['presunto_responsable'].str.contains("4") |
    	df['presunto_responsable'].str.contains("5") | df['presunto_responsable'].str.contains("6") | df['presunto_responsable'].str.contains("7") | df['presunto_responsable'].str.contains("8") |
    	df['presunto_responsable'].str.contains("9") | df['presunto_responsable'].str.contains("10") | df['presunto_responsable'].str.contains("11") | df['presunto_responsable'].str.contains("12") |
    	df['presunto_responsable'].str.contains("13") | df['presunto_responsable'].str.contains("14") | df['presunto_responsable'].str.contains("15") | df['presunto_responsable'].str.contains("16") |
    	df['presunto_responsable'].str.contains("17") | df['presunto_responsable'].str.contains("18") | df['presunto_responsable'].str.contains("19") | df['presunto_responsable'].str.contains("20") |
    	df['presunto_responsable'].str.contains("21") | df['presunto_responsable'].str.contains("22") | df['presunto_responsable'].str.contains("23") | df['presunto_responsable'].str.contains("24") |
    	df['presunto_responsable'].str.contains("25") | df['presunto_responsable'].str.contains("26") | df['presunto_responsable'].str.contains("27") | df['presunto_responsable'].str.contains("28") |
    	df['presunto_responsable'].str.contains("29") | df['presunto_responsable'].str.contains("30") | df['presunto_responsable'].str.contains("31") | df['presunto_responsable'].str.contains("32") |
    	df['presunto_responsable'].str.contains("33") | df['presunto_responsable'].str.contains("34") | df['presunto_responsable'].str.contains("35") | df['presunto_responsable'].str.contains("36") |
    	df['presunto_responsable'].str.contains("37") | df['presunto_responsable'].str.contains("38") | df['presunto_responsable'].str.contains("39") | df['presunto_responsable'].str.contains("40") |
    	df['presunto_responsable'].str.contains("41") | df['presunto_responsable'].str.contains("42") | df['presunto_responsable'].str.contains("43") | df['presunto_responsable'].str.contains("44") |
    	df['presunto_responsable'].str.contains("45") | df['presunto_responsable'].str.contains("46") | df['presunto_responsable'].str.contains("47") | df['presunto_responsable'].str.contains("48") |
    	df['presunto_responsable'].str.contains("49") | df['presunto_responsable'].str.contains("50") | df['presunto_responsable'].str.contains("51") | df['presunto_responsable'].str.contains("52") |	
    	df['presunto_responsable'].str.contains("53") | df['presunto_responsable'].str.contains("54") | df['presunto_responsable'].str.contains("55") | df['presunto_responsable'].str.contains("56") |
    	df['presunto_responsable'].str.contains("57") | df['presunto_responsable'].str.contains("58") | df['presunto_responsable'].str.contains("59") | df['presunto_responsable'].str.contains("60") |
    	df['presunto_responsable'].str.contains("61") | df['presunto_responsable'].str.contains("62") | df['presunto_responsable'].str.contains("63") | df['presunto_responsable'].str.contains("64") |
    	df['presunto_responsable'].str.contains("65") | df['presunto_responsable'].str.contains("66")))
        ), 1, 0)

    df['pres_resp_agentes_estatales'] = np.where((
        (df['presunto_responsable'].str.contains("ARMANDO") & df['presunto_responsable'].str.contains("RIOS")) |
    	(df['presunto_responsable'].str.contains("AUREL") & df['presunto_responsable'].str.contains("RODRIGUEZ")) |
    	(df['presunto_responsable'].str.contains("FRENTE") & df['presunto_responsable'].str.contains("MADRE")) |
    	(df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("SUCRE")) |
    	(df['presunto_responsable'].str.contains("TEOFILO")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTEQUERA")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("GALAN")) |
    	(df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("NARINO")) |
    	(df['presunto_responsable'].str.contains("HERNANDO") & df['presunto_responsable'].str.contains("GONZALEZ") & df['presunto_responsable'].str.contains("ACOSTA")) |
    	(df['presunto_responsable'].str.contains("JACOBO") & df['presunto_responsable'].str.contains("PRIAS") & df['presunto_responsable'].str.contains("ALAPE")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("GONZALO") & df['presunto_responsable'].str.contains("FRANCO")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("MARIA") & df['presunto_responsable'].str.contains("CORDOBA")) |
    	(df['presunto_responsable'].str.contains("ATANASIO") & df['presunto_responsable'].str.contains("GIRARDOT")) |
    	(df['presunto_responsable'].str.contains("GUADALUPE") & df['presunto_responsable'].str.contains("SALCEDO")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("ANZOATEGUI")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("GALAN")) | 
    	(df['presunto_responsable'].str.contains("CACICA") & df['presunto_responsable'].str.contains("GAITANA")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("IGNACIO") & df['presunto_responsable'].str.contains("MORA")) | 
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("PAEZ")) | 
    	(df['presunto_responsable'].str.contains("ANGELINO") & df['presunto_responsable'].str.contains("GODOY")) |
    	(df['presunto_responsable'].str.contains("CACIQUE") & df['presunto_responsable'].str.contains("COYARA")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("PRUDENCIO") & df['presunto_responsable'].str.contains("PADILLA")) |
    	(df['presunto_responsable'].str.contains("POLICARPA") & df['presunto_responsable'].str.contains("SALAVARRIETA")) |
    	(df['presunto_responsable'].str.contains("HEROES") & df['presunto_responsable'].str.contains("MARTIRES") & df['presunto_responsable'].str.contains("SANTA") & df['presunto_responsable'].str.contains("ROSA")) |
    	(df['presunto_responsable'].str.contains("ARMANDO") & df['presunto_responsable'].str.contains("RIOS")) |
    	(df['presunto_responsable'].str.contains("HERMOGENES") & df['presunto_responsable'].str.contains("MAZA")) |
    	(df['presunto_responsable'].str.contains("ISAIAS") & df['presunto_responsable'].str.contains("PARDO")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("MARIA") & df['presunto_responsable'].str.contains("CARBONELL")) |
    	(df['presunto_responsable'].str.contains("ALFONSO") & df['presunto_responsable'].str.contains("ARTEAGA")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("PAEZ")) |
    	(df['presunto_responsable'].str.contains("PEDRO") & df['presunto_responsable'].str.contains("NEL") & df['presunto_responsable'].str.contains("JIMENEZ") & df['presunto_responsable'].str.contains("OBANDO")) |
    	(df['presunto_responsable'].str.contains("MARISCAL") & df['presunto_responsable'].str.contains("SUCRE")) |
    	(df['presunto_responsable'].str.contains("ALBERTO") & df['presunto_responsable'].str.contains("MARTINEZ")) |
    	(df['presunto_responsable'].str.contains("BENKOS") & df['presunto_responsable'].str.contains("BIOHO")) |
    	(df['presunto_responsable'].str.contains("JAIR") & df['presunto_responsable'].str.contains("ALDANA") & df['presunto_responsable'].str.contains("BAQUERO")) |
    	(df['presunto_responsable'].str.contains("MARTIN") & df['presunto_responsable'].str.contains("CABALLERO")) |
    	(df['presunto_responsable'].str.contains("FRENTE") & df['presunto_responsable'].str.contains("CARIBE")) |
    	(df['presunto_responsable'].str.contains("BLOQUE") & df['presunto_responsable'].str.contains("CARIBE"))), 1, 0)

    df['pres_resp_agentes_estatales'] = np.where((
    	(df['presunto_responsable'].str.contains("CIRO") & df['presunto_responsable'].str.contains("TRUJILLO") & df['presunto_responsable'].str.contains("CASTANO")) |
    	(df['presunto_responsable'].str.contains("RICAURTE") & df['presunto_responsable'].str.contains("JIMENEZ")) |
    	(df['presunto_responsable'].str.contains("JOAQUIN") & df['presunto_responsable'].str.contains("BALLEN")) |
    	(df['presunto_responsable'].str.contains("JACOBO") & df['presunto_responsable'].str.contains("ARENAS")) |
    	(df['presunto_responsable'].str.contains("CACIQUE") & df['presunto_responsable'].str.contains("UPAR")) |
    	(df['presunto_responsable'].str.contains("MANUEL") & df['presunto_responsable'].str.contains("CEPEDA") & df['presunto_responsable'].str.contains("VARGAS")) |
    	(df['presunto_responsable'].str.contains("JOSELO") & df['presunto_responsable'].str.contains("LOZADA")) |
    	(df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("RICAURTE")) |
    	(df['presunto_responsable'].str.contains("ATANASIO") & df['presunto_responsable'].str.contains("GIRARDOT")) |
    	(df['presunto_responsable'].str.contains("RODRIGO") & df['presunto_responsable'].str.contains("GAITAN")) |
    	(df['presunto_responsable'].str.contains("LEONARDO") & df['presunto_responsable'].str.contains("POSADA") & df['presunto_responsable'].str.contains("PEDRAZA")) |
    	(df['presunto_responsable'].str.contains("PEDRO") & df['presunto_responsable'].str.contains("MARTINEZ")) |
    	(df['presunto_responsable'].str.contains("ANTONIO") & df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("SUCRE")) |
    	(df['presunto_responsable'].str.contains("HECTOR") & df['presunto_responsable'].str.contains("RAMIREZ")) |
    	(df['presunto_responsable'].str.contains("CACIQUE") & df['presunto_responsable'].str.contains("CALARCA")) |
    	(df['presunto_responsable'].str.contains("JAIME") & df['presunto_responsable'].str.contains("PARDO") & df['presunto_responsable'].str.contains("LEAL")) |
    	(df['presunto_responsable'].str.contains("JUAN") & df['presunto_responsable'].str.contains("CRUZ") & df['presunto_responsable'].str.contains("VARELA")) |		
    	(df['presunto_responsable'].str.contains("MIGUEL") & df['presunto_responsable'].str.contains("ANGEL") & df['presunto_responsable'].str.contains("BONILLA")) |
    	(df['presunto_responsable'].str.contains("TEOFILO") & df['presunto_responsable'].str.contains("FORERO")) |
    	(df['presunto_responsable'].str.contains("COMBATIENTES") & df['presunto_responsable'].str.contains("CUSIANA")) |
    	(df['presunto_responsable'].str.contains("EFRAIN") & df['presunto_responsable'].str.contains("BALLESTEROS")) |
    	(df['presunto_responsable'].str.contains("MARTIRES") & df['presunto_responsable'].str.contains("CANAS")) |
    	(df['presunto_responsable'].str.contains("RESISTENCIA") & df['presunto_responsable'].str.contains("GUAJIRA")) |
    	(df['presunto_responsable'].str.contains("JAIME") & df['presunto_responsable'].str.contains("PARDO") & df['presunto_responsable'].str.contains("LEAL")) |
    	(df['presunto_responsable'].str.contains("HEROE") & df['presunto_responsable'].str.contains("YARI")) |
    	(df['presunto_responsable'].str.contains("COMPANIA") & df['presunto_responsable'].str.contains("YARI")) |
    	(df['presunto_responsable'].str.contains("COLUMNA") & df['presunto_responsable'].str.contains("YARI")) |
    	(df['presunto_responsable'].str.contains("RODOLFO") & df['presunto_responsable'].str.contains("TANAS") ) |
    	(df['presunto_responsable'].str.contains("POLICARPA") & df['presunto_responsable'].str.contains("SALAVARRIETA"))), 1, 0)

    df['pres_resp_agentes_estatales'] = np.where((
    	(df['presunto_responsable'].str.contains("JOSELO") & df['presunto_responsable'].str.contains("LOSADA") ) |
    	(df['presunto_responsable'].str.contains("MARIO") & df['presunto_responsable'].str.contains("VELEZ") ) |
    	(df['presunto_responsable'].str.contains("MANUELA") & df['presunto_responsable'].str.contains("BELTRAN") ) |
    	(df['presunto_responsable'].str.contains("JACOBO") & df['presunto_responsable'].str.contains("ARENAS") ) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("ANTEQUERA") ) |
    	(df['presunto_responsable'].str.contains("MANUEL") & df['presunto_responsable'].str.contains("CEPEDA") & df['presunto_responsable'].str.contains("VARGAS")) |
    	(df['presunto_responsable'].str.contains("EDUARDO") & df['presunto_responsable'].str.contains("MAHECHA")) |
    	(df['presunto_responsable'].str.contains("MANUEL") & df['presunto_responsable'].str.contains("CEPEDA") & df['presunto_responsable'].str.contains("VARGAS")) |		
    	(df['presunto_responsable'].str.contains("ABELARDO") & df['presunto_responsable'].str.contains("ROMERO")) |
    	(df['presunto_responsable'].str.contains("REINALDO") & df['presunto_responsable'].str.contains("CUELLAR")) |
    	(df['presunto_responsable'].str.contains("ESTEBAN") & df['presunto_responsable'].str.contains("MARTINEZ")) |
    	(df['presunto_responsable'].str.contains("VLADIMIR") & df['presunto_responsable'].str.contains("STEVEN")) |
    	(df['presunto_responsable'].str.contains("FELIPE") & df['presunto_responsable'].str.contains("RINCON")) |
    	(df['presunto_responsable'].str.contains("URIAS") & df['presunto_responsable'].str.contains("RONDON")) |
    	(df['presunto_responsable'].str.contains("ACACIO") & df['presunto_responsable'].str.contains("MEDINA")) |
    	(df['presunto_responsable'].str.contains("ARTURO") & df['presunto_responsable'].str.contains("RUIZ")) |
    	(df['presunto_responsable'].str.contains("DARIO") & df['presunto_responsable'].str.contains("BONILLA")) |
    	(df['presunto_responsable'].str.contains("ISMAEL") & df['presunto_responsable'].str.contains("AYALA")) |
    	(df['presunto_responsable'].str.contains("JUDITH") & df['presunto_responsable'].str.contains("RONDON")) |
    	(df['presunto_responsable'].str.contains("JUAN") & df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("RONDON")) |
    	(df['presunto_responsable'].str.contains("JOSE") & df['presunto_responsable'].str.contains("RONDON")) |
    	(df['presunto_responsable'].str.contains("QUINO") & df['presunto_responsable'].str.contains("MENDEZ")) |
    	(df['presunto_responsable'].str.contains("MARCO") & df['presunto_responsable'].str.contains("AURELIO") & df['presunto_responsable'].str.contains("BUENDIA")) |
    	(df['presunto_responsable'].str.contains("XIOMARA") & df['presunto_responsable'].str.contains("MARIN")) |
    	(df['presunto_responsable'].str.contains("OCTAVIO") & df['presunto_responsable'].str.contains("SUAREZ") & df['presunto_responsable'].str.contains("BRICENO")) |
    	(df['presunto_responsable'].str.contains("EDWIN") & df['presunto_responsable'].str.contains("SUAREZ")) |
    	(df['presunto_responsable'].str.contains("YERMINSON") & df['presunto_responsable'].str.contains("RUIZ")) |
    	(df['presunto_responsable'].str.contains("MARTIN") & df['presunto_responsable'].str.contains("MARTINEZ")) |
    	(df['presunto_responsable'].str.contains("VICTOR") & df['presunto_responsable'].str.contains("ALIRIO") & df['presunto_responsable'].str.contains("SAAVEDRA"))),1, 0)

    df['pres_resp_agentes_estatales'] = np.where((
    	(df['presunto_responsable'].str.contains("REINEL") & df['presunto_responsable'].str.contains("MENDEZ")) |
    	(df['presunto_responsable'].str.contains("LUIS") & df['presunto_responsable'].str.contains("PARDO")) |
    	(df['presunto_responsable'].str.contains("ALFONSO") & df['presunto_responsable'].str.contains("CASTELLANOS")) |
    	(df['presunto_responsable'].str.contains("EFRAIN") & df['presunto_responsable'].str.contains("GUZMAN")) |
    	(df['presunto_responsable'].str.contains("GERARDO") & df['presunto_responsable'].str.contains("GUEVARA")) |
    	(df['presunto_responsable'].str.contains("RAUL") & df['presunto_responsable'].str.contains("EDUARDO") & df['presunto_responsable'].str.contains("MAHECHA")) |
    	(df['presunto_responsable'].str.contains("SALVADOR") & df['presunto_responsable'].str.contains("DIAZ")) |
    	(df['presunto_responsable'].str.contains("RESISTENCIA") & df['presunto_responsable'].str.contains("BARI")) |
    	(df['presunto_responsable'].str.contains("GILDARDO") & df['presunto_responsable'].str.contains("RODRIGUEZ")) |
    	(df['presunto_responsable'].str.contains("GILDARDO") & df['presunto_responsable'].str.contains("RODRIGUEZ")) |
    	(df['presunto_responsable'].str.contains("UNIDAD") & df['presunto_responsable'].str.contains("IVAN") & df['presunto_responsable'].str.contains("RIOS")) |
    	(df['presunto_responsable'].str.contains("ALONSO") & df['presunto_responsable'].str.contains("CORTES")) |
    	(df['presunto_responsable'].str.contains("MILLER") & df['presunto_responsable'].str.contains("PERDOMO")) |
    	(df['presunto_responsable'].str.contains("GABRIEL") & df['presunto_responsable'].str.contains("GALVIS")) |
    	(df['presunto_responsable'].str.contains("JACOBO") & df['presunto_responsable'].str.contains("ARENAS")) |
    	(df['presunto_responsable'].str.contains("DANIEL") & df['presunto_responsable'].str.contains("ALDANA")) |
    	(df['presunto_responsable'].str.contains("TEOFILO") & df['presunto_responsable'].str.contains("FORERO")) |
    	(df['presunto_responsable'].str.contains("YESID") & df['presunto_responsable'].str.contains("ORTIZ")) |
    	(df['presunto_responsable'].str.contains("ALIRIO") & df['presunto_responsable'].str.contains("TORRES")) |
    	(df['presunto_responsable'].str.contains("MARISCAL") & df['presunto_responsable'].str.contains("SUCRE")) |
    	(df['presunto_responsable'].str.contains("VICTOR") & df['presunto_responsable'].str.contains("SAAVEDRA")) |
    	(df['presunto_responsable'].str.contains("CACIQUE") & df['presunto_responsable'].str.contains("TIMANCO")) |
    	(df['presunto_responsable'].str.contains("RIGOBERTO") & df['presunto_responsable'].str.contains("LOZADA")) |
    	(df['presunto_responsable'].str.contains("MARQUETALIA")) |
    	(df['presunto_responsable'].str.contains("JACOBO") & df['presunto_responsable'].str.contains("PRIAS") & df['presunto_responsable'].str.contains("ALAPE")) |
    	(df['presunto_responsable'].str.contains("TULIO") & df['presunto_responsable'].str.contains("VARON")) |
    	(df['presunto_responsable'].str.contains("MILER SALCEDO") & df['presunto_responsable'].str.contains("VARON")) |
    	(df['presunto_responsable'].str.contains("COMISION") & df['presunto_responsable'].str.contains("MANUELITA") & df['presunto_responsable'].str.contains("SAENZ")) |
    	(df['presunto_responsable'].str.contains("COMISION") & df['presunto_responsable'].str.contains("RENE") & df['presunto_responsable'].str.contains("GONZALEZ")) |
    	(df['presunto_responsable'].str.contains("ESCUELA") & df['presunto_responsable'].str.contains("HERNAN") & df['presunto_responsable'].str.contains("MURILLO") & df['presunto_responsable'].str.contains("TORO")) |
    	(df['presunto_responsable'].str.contains("ESCUELA") & df['presunto_responsable'].str.contains("HERNAN") & df['presunto_responsable'].str.contains("MURILLO")) |
    	(df['presunto_responsable'].str.contains("AURELIO") & df['presunto_responsable'].str.contains("RODRIGUEZ")) |
    	(df['presunto_responsable'].str.contains("JACINTO") & df['presunto_responsable'].str.contains("MATALLANA")) |
    	(df['presunto_responsable'].str.contains("ALFREDO") & df['presunto_responsable'].str.contains("GONZALEZ")) |
    	(df['presunto_responsable'].str.contains("BLOQUE") & df['presunto_responsable'].str.contains("COMANDO") & df['presunto_responsable'].str.contains("CONJUNTO") & df['presunto_responsable'].str.contains("OCCIDENTE")) |
    	(df['presunto_responsable'].str.contains("BLOQUE") & df['presunto_responsable'].str.contains("ORIENTAL")) |
    	df['presunto_responsable'].str.contains("SECRETARIADO")) , 1, 0)
