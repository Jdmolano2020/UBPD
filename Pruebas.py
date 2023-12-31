import unicodedata
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
# import homologacion.nombre_completo


#df_CNMH_SE = pd.read_stata(
#    "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/BD_CNMH_SE.dta")

#df_CNMH_RU = pd.read_stata(
#    "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/BD_CNMH_RU.dta")

# df_CEV_JEP = pd.read_stata(
#     "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/BD_CEV_JEP.dta")

#df_CNMH_DF = pd.read_stata(
 #   "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/BD_CNMH_DF.dta")
# df_ICMP = pd.read_stata(
#     "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/BD_ICMP.dta")
# df_ICMP = pd.read_stata(
#     "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/BD_FGN_INACTIVOS.dta")
# df_ef = pd.read_stata(
#     "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/Equivalencia_fonetica.dta")

df_soundex = pd.read_csv(
    "C:/Users/HP/Documents/FIA/Demo/pruebaBlack/UBPD/datos/BDPD_SOUNDEX.csv")

server = 'LAPTOP-V6LUQTIO\SQLEXPRESS/'
database = 'userubpd'
trusted_connection = True
conn_str = f'mssql+pyodbc://{server}/{database}?trusted_connection={trusted_connection}'
engine = create_engine(conn_str)
df_soundex.to_sql('BDPD_SOUNDEX', con=engine, if_exists='replace', index=False) 

db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)# Escribir los DataFrames en las tablas correspondientes en la base de datos

df_soundex.to_sql('BDPD_SOUNDEX', con=engine, if_exists='replace', index=False) 
# df_ef[['FON1', 'FON2', 'FON3', 'FON4', 'FON5', 'FON6', 'FON7', 'FON8', 'FON9', 'FON10', 'FON11', 'FON12', 'FON13', 'FON14']] = df_ef['fonetico'].str.split(expand=True)
# df_ef.to_sql('Equivalencia_fonetica', con=engine, if_exists='replace', index=False)
# df_ICMP.to_sql('FGN_INACTIVOS_U', con=engine, if_exists='replace', index=False)
# df_ICMP.to_sql('ICMP_U', con=engine, if_exists='replace', index=False)
# df_CEV_JEP.to_sql('CEV_JEP_U', con=engine, if_exists='replace', index=False)

#df_CNMH_DF.to_sql('CNMH_DF_U', con=engine, if_exists='replace', index=False)

# df_CNMH_SE.to_sql('CNMH_SE_U', con=engine, if_exists='replace', index=False)

#df_CNMH_RU.to_sql('CNMH_RU_U', con=engine, if_exists='replace', index=False)


nombre_completo = ["BLANCA LUCIA DEL NIÑO JESUS SEPULVEDA GARCIA",
                   "JOSE RODRIGO MENOR DE EDA GUTIERREZ LACHES",
                   "FREDERICK ANTHONY SEBASTIAN ANGARITA MAYA",
                   "LUZ ADIELA DEL SOCORRO TABARES VELASQUEZ",
                   "FABIO NELSON O ALEXIS GAR GRACIANO DAVID",
                   "MELKICEDEK INDETERMINADO HERRERA HERRERA",
                   "EDWIN ANDRES O GUZMAN JUA HERNANDEZ RAYO",
                   "EDIMER ALONSO O ROMERO JH QUINTERO ROZO",
                   "JOSIMAR MERARDO GONZALEZRUBIO SARMIENTO",
                   "JULIAN ALEJANDRO DE ECOZAN CORTES LOPEZ",
                   "ARQUIMEDES DE JESUS VALDERRAMA QUINTANA",
                   "ESNEIDER FRANCISCO SEPULVEDA RODRIGUEZ",
                   "MAURICIO ALEJANDRO OLIVEROS CAN?AVERAL",
                   "",None]

#nombres com mas de 5 tokens
for nombres in nombre_completo:
    primer_nombre, segundo_nombre, primer_apellido, segundo_apellido =homologacion.nombre_completo.limpiar_nombre_completo(nombres)
    print("nombres:", nombres,
          "primer_nombre:", primer_nombre,
          "segundo_nombre:", segundo_nombre,
          "primer_apellido:", primer_apellido,
          "segundo_apellido:",segundo_apellido)