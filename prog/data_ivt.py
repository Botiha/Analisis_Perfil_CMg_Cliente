import pandas as pd
from subprocess import call
from prog.crear_vb_xlsb_to_csv import crea_archivo_vbscript
from pathlib import Path
import tempfile

def lee_medidas(año, mes, carpeta, sh='1'):
    excel = carpeta / f'Medidas_horarias_{año}{mes}.xlsb'
    med = carpeta / f'Medidas_{año}-{mes}.csv'
    vb_excel = 'ExcelToCsv.vbs'
    if (med).exists():
        df = filtra_medidas(med)
    else:
        vbs = crea_archivo_vbscript()
        with tempfile.NamedTemporaryFile(
                suffix=".vbs", delete=False) as temp_file:
            temp_filename = temp_file.name
            temp_file.write(vbs.encode())

        call(['cscript.exe', temp_filename, str(excel), str(med), sh])
        temp_file.close()
        df = filtra_medidas(med)

    lst = df.columns.tolist()
    lst[4] = 'descripcion'
    df.columns = lst

    df_out = pd.melt(
        df,
        id_vars=['nombre_barra', 'propietario', 'Tipo_Medida', 'clave',
                 'Valores', 'descripcion'],
        value_vars=df.columns[6:],
        var_name='fecha'
    )

    df_out['fecha'] = df_out['fecha'].astype(int)
    df_out = hour_col_to_date_col(df_out, f'20{año}-{mes}-01')
    df_out = df_out.sort_values(by=['propietario', 'fecha'])
    df_out = df_out.drop(columns=['Valores'])
    print(f'Medidas 20{año}-{mes} procesadas')
    return df_out

def filtra_medidas(csv_file):
    df = pd.read_csv(csv_file, encoding='latin', header=1)
    df = df.loc[df['Valores'].str.contains('Físico_')]
    return df

def hour_col_to_date_col(df, start_date):
    df_fin = df.copy()
    start_date = pd.to_datetime(start_date)
    horas = df_fin.fecha
    horas = pd.to_timedelta(horas.astype(int) - 1, unit='h')
    horas += start_date
    df_fin.fecha = horas
    return df_fin

def reordena_cols(df):
    df_fin = df.T
    df_fin.reset_index(inplace=True)
    cols = ['nombre_barra', 'propietario', 'Tipo_Medida', 'clave', 'descripcion']
    df_fin[cols] = df_fin.nombre.str.split(';', expand = True)
    df_fin.drop(columns=['nombre'], inplace=True)
    # reordenando columnas
    lst1 = [x for x in range(1, 13) if x in df_fin.columns]
    cols2 = cols + lst1
    df_fin = df_fin[cols2]
    return df_fin

def busca_cliente(df_data, cliente):
    df = df_data.T
    df.reset_index(inplace=True)
    cols = ['nombre_barra', 'propietario', 'Tipo_Medida', 'clave',
            'descripcion']
    df[cols] = df.nombre.str.split(';', expand=True)
    df.drop(columns=['nombre'], inplace=True)
    df = df.loc[df['descripcion'].str.contains(cliente)]
    df['nombre'] = df['nombre_barra'] + ';' + df['propietario'] + ';' + \
                   df['Tipo_Medida'] + ';' + df['clave'] + ';' + \
                   df['descripcion']

    df.drop(columns=cols, inplace=True)
    df.drop(columns='level_0', inplace=True)
    df.set_index('nombre', inplace=True)
    return df.T
