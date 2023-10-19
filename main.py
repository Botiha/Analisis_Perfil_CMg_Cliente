import pandas as pd
from prog.data_ivt import busca_cliente
from prog.data_cmg import CMg
from pathlib import Path

#%%
path_ivt = Path(r'C:\_BD_Clientes\2023\01_Chequeo_Medidas\IVT')
path_cmg = Path(r'C:\_Costos_Marginales')

cliente = 'IMERYS'
barra = 'QUIANI'
ivt = ['ivt_2022.parquet', 'ivt_2023.parquet']
#%%
df_ivt = pd.DataFrame()
for i in ivt:
    print(f'Procesando IVT: {i}...')
    df = pd.read_parquet(path_ivt / i)
    df = busca_cliente(df, cliente)
    df_ivt = pd.concat([df_ivt, df])

df_ivt.to_excel(path_ivt / 'IMERYS_perfil.xlsx', index=False)

#%%
barra = 'EJERCITO'
df_cmg = pd.DataFrame()
for a in [2018, 2019, 2020, 2021, 2022, 2023]:
    print(f'Procesando CMg: {a}...')
    df = CMg(a, barra)
    df = df.get_data()
    df_cmg = pd.concat([df_cmg, df])

df_cmg.to_excel(path_cmg / 'EJERCITO_cmg.xlsx', index=False)

#%%

df