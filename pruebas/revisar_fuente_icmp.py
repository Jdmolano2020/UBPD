import pandas as pd
from pandas.api.types import CategoricalDtype
from sqlalchemy import create_engine

db_url = "mssql+pyodbc://userubpd:J3mc2005.@LAPTOP-V6LUQTIO\SQLEXPRESS/ubpd_base?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)

query = "SELECT * FROM BD_ICMP"
df1 = pd.read_sql_query(query, engine)

query = "EXECUTE [dbo].[VALIDA_V_ICMP]"
df2 = pd.read_sql_query(query, engine)

for col in df1.columns:
    df1[col] = df1[col].astype(str)

for col in df2.columns:
    df2[col] = df2[col].astype(str)
    
# Asegurarse de que los DataFrames tengan las mismas columnas
df3 = df1[['codigo_unico_fuente','nombre_completo', 'primer_nombre']]
df4 = df2[['codigo_unico_fuente','nombre_completo', 'primer_nombre']]

df3['nombre_completo'] = df3['nombre_completo'].astype(str)
df3['primer_nombre'] = df3['primer_nombre'].astype(str)

df4['nombre_completo'] = df4['nombre_completo'].astype(str)
df4['primer_nombre'] = df4['primer_nombre'].astype(str)
# Asegurarse de que tengan el mismo índice (por ejemplo, el índice basado en la fila)
df3.reset_index(drop=True, inplace=True)
df4.reset_index(drop=True, inplace=True)

# Asegurarse de que las etiquetas de columna sean idénticas
df3.columns = df4.columns

# Realizar la comparación
# df_diff = df4.compare(df3)
df_diff = pd.concat([df3,df4]).drop_duplicates(keep=False)

#df_diff = df1[['nombre_completo', 'primer_nombre']].compare(df2[['nombre_completo', 'primer_nombre']])
#df_diff = df1.compare(df2)
