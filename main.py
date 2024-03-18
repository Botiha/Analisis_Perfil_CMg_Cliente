#%%
import pandas as pd
from prog.data_ivt import ClientesIVT, CMgIVT
from pathlib import Path

path = Path(r'C:\_BD_Clientes\IVT')
meses = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

#%%   ***** BUSCA CLIENTE ****
cliente = 'beluxa'.upper()
agno, mes = '23', '12'
cl_ivt = ClientesIVT(path, agno, mes)
df2 = cl_ivt.busca_cliente(cliente)
print(df2.columns)

#%%
df2['CABRERO_______013;ENEL_GENERACION;L_D;ECOMASCABRE1;ECOMAS S.A.']
#%%
cliente = 'ors'
agno = '23'
df_fin = pd.DataFrame()
for mes in meses:
    print(f'procesando {mes}')
    cl_ivt = ClientesIVT(path, agno, mes)
    df2 = cl_ivt.busca_cliente(cliente)
    df_fin = pd.concat([df_fin, df2])
print(f'{"-"*5}\tProceso Terminado\t{"-"*5}')

#%%
df_fin
#%%
df_fin.to_excel(path / f'{cliente}_Olmue_{agno}.xlsx')
#%%   ***** COSTO MARGINAL ****
barra = ['MAULE']
agno, mes = '23', '01'
cl_cmg = CMgIVT(path, agno, mes)
df = cl_cmg.busca_barra(barra)
#%%
df.columns
#%%
barras = [
    'ANTOFAGASTA___013', 'EJERCITO______013',
    'M.DEVELASCO___013', 'PERALES_______015',
    'S.JOAQUIN_____015', 'S.PEDRO_______013',
    'STA.ELVIRA____013', 'CABRERO_______013'
]


agno = '23'
df_fin = pd.DataFrame()
for mes in meses:
    print(f'procesando {mes}')
    cl_cmg = CMgIVT(path, agno, mes)
    df2 = cl_cmg.busca_barra(barras)
    df_fin = pd.concat([df_fin, df2])
print(f'{"-"*5}\tProceso Terminado\t{"-"*5}')
#%%
df_fin
#%%
df_fin.to_excel(path / f'CMg_ClientesSur_{agno}.xlsx')
#%%  BUSCA CLIENTES CGEC

clientes = [
    'AGRICOLA LA QUEBRADA', 'AGROINDUSTRIAL ANAPROC S.A.',
    'AGRORESERVAS DE CHILE', 'AGRORESERVAS DE CHILE SPA',
    'COCA-COLA DE CHILE S.A', 'COCA-COLA DE CHILE S.A.',
    'COMERCIALIZADORA VEGA MONUMENTAL',
    'DES INM PADRE WERNER' ,'HOTEL HAMPTON',
    'INMOB VEGA MONUMENTAL', 'INVERSIONES AMANECER',
    'KAPINKE', 'MINERA_LOS_PELAMBRES',
    'MOLINO FUENTES S.A.', 'SANTIAGO AIRPORT HOTEL',
    'SCHÜSSLER', 'UNIVERSIDAD DE LA FRONTERA',
    'FORESTAL AITUE LTDA', 'SOUTH ORGANICS FRUIT',
    'COCA COLA DE CHILE',
    'ESTABLECIMIENTO_PENITENCIARIO',
    'ESTABLECIMIENTO PENITENCIARIO',
    'OMAMET',
    'SOC AGRICOLA EL OLIBAL',
    'SOCAGRICOLA_EL_OLIBAL',
    'HOTEL HAMPTON', 'HOTEL_HAMPTON',
    'MARIA_LORETO_FERNANDEZ_LEON',
    'MARIA LORETO FERNANDEZ LEON',
    'EX_ABASOLO_VALLEJOS',
    'ABASOLO VALLEJOS'
]

clientes = [
    'COMERCIALIZADORA VEGA MONUMENTAL',
    'INMOB VEGA MONUMENTAL',
    'ECOMAS',
    'CLINICA BIO-BIO',
    'MARINA DEL SOL',
    'PLAZA LA SERENA',
    'MALL_ANTOFAGASTA',
    'PLAZA_TREBOL',
    'PLAZA DEL TREBOL',
]

agno = '23'
meses = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
df_fin = pd.DataFrame()
for mes in meses:
    print(f'procesando {mes}')
    cl_ivt = ClientesIVT(path, agno, mes)
    df_aux = cl_ivt.busca_cliente(clientes)
    df_fin = pd.concat([df_fin, df_aux])
print(f'{"-"*5}\tProceso Terminado\t{"-"*5}')
#%%
df_fin.to_excel(path / f'CONSUMO_Clientes_VISITA_SUR_{agno}.xlsx')



#%%


