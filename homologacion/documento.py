import pandas as pd

def documento_valida (df : pd, documento):
    # Documento de identificación
    df[documento] = df[documento].str.upper()

    for i in range(256):
        if i not in [32, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 209]:
            df[documento] = df[documento].str.replace(chr(i), '', regex=True)
    df['documento_dep'] = df[documento]

    for i in range(48, 58):
        df['documento_dep'] = df['documento_dep'].str.replace(chr(i), '', regex=True)

    df[documento] = df.apply(lambda row: '' if row['documento_dep'] == row[documento] and row[documento] != '' else row[documento], axis=1)

    for i in range(256):
        if i not in [32, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57]:
            df[documento] = df.apply(lambda row: row[documento].replace(chr(i), '') if ' ' in row[documento] and row['documento_dep'] != row[documento] and row[documento] != '' and row['documento_dep'] != '' else row[documento], axis=1)

    df[documento] = df[documento].str.strip()
    df[documento] = df[documento].str.replace('   ', ' ', regex=True)
    df[documento] = df[documento].str.replace('  ', ' ', regex=True)
    df.drop(columns=['documento_dep'], inplace=True)
    df[documento] = df.apply(lambda row: '' if len(row[documento]) < 4 else row[documento], axis=1)

