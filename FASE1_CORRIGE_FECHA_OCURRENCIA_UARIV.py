import pandas as pd
import numpy as np


# Crea un diccionario con los reemplazos
def corrige_fecha_ocurrencia(df: pd):
    # Mapeo de fechas
    date_mapping = {
        "01/01/0001": "01/01/2001",
        "10/04/0997": "10/04/1997",
        "02/01/0200": "02/02/2000",
        "02/02/0012": "02/02/2012",
        "01/09/1009": "01/09/2009",
        "25/05/0204": "25/05/2004",
        "09/01/0199": "09/01/1999",
        "13/11/0204": "13/11/2004",
        "02/12/1191": "02/12/1991",
        "04/07/0200": "04/07/2000",
        "20/09/0904": "20/09/1994",
        "15/07/0200": "15/07/2000",
        "18/05/0199": "18/05/1999",
        "05/08/0199": "05/08/1999",
        "20/12/0202": "20/12/2002",
        "01/12/0993": "01/12/1993",
        "02/01/0201": "02/01/2001",
        "19/05/0200": "19/05/2000",
        "16/10/0018": "16/10/2018",
        "24/01/0202": "24/01/2002",
        "26/11/0200": "26/11/2000",
        "25/01/0005": "25/01/2005",
        "11/12/1081": "11/12/1981",
        "16/11/0992": "16/11/1992",
        "02/01/1190": "02/01/1990",
        "01/01/0994": "01/01/1994",
        "01/01/0992": "01/01/1992",
        "20/07/0700": "20/07/2007",
        "12/05/0001": "12/05/2001",
        "10/06/0200": "10/06/2000",
        "07/03/1002": "07/03/2002",
        "11/02/0003": "11/02/2003",
        "13/10/0194": "13/10/1994",
        "30/12/0007": "30/12/2007",
        "09/08/2201": np.nan,
        "10/02/2996": "10/02/1996"
    }

# Aplica el mapeo a la columna VILB_FECHAOCURRENCIA
    df['VILB_FECHAOCURRENCIA'] = df['VILB_FECHAOCURRENCIA'].map(
            date_mapping).fillna(df['VILB_FECHAOCURRENCIA'])
