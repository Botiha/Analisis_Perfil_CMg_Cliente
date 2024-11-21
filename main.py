# %%
import pandas as pd
from prog.data_ivt import ClientesIVT, CMgIVT
from pathlib import Path

path = Path(r"C:\_BD_Clientes\IVT")
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

# %%   ***** BUSCA CLIENTE ****
cliente = "conal".upper()
agno, mes = "24", "01"
cl_ivt = ClientesIVT(path, agno, mes)
df2 = cl_ivt.busca_cliente(cliente)
print(df2.columns)

# %%
df2
# %%
df2["CABRERO_______013;ENEL_GENERACION;L_D;ECOMASCABRE1;ECOMAS S.A."]
# %% Busqueda de cliente por 12 merses completo
cliente = "".upper()
agno = "23"  # sólo 2 números, año 24, o año 23
meses = ["01", "02", "03"]
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
df_fin = pd.DataFrame()
for mes in meses:
    print(f"procesando {mes}")
    cl_ivt = ClientesIVT(path, agno, mes)
    df2 = cl_ivt.busca_cliente(cliente)
    df_fin = pd.concat([df_fin, df2])
print(f'{"-"*5}\tProceso Terminado\t{"-"*5}')

# %% Grabar resultado
df_fin.to_excel(path / f"{cliente}_{agno}.xlsx")

# %% Deja sólo clientes con L, L_D, y L_S
df_fin = df_fin.drop(df_fin.filter(like=";T;").columns, axis=1)
df_fin = df_fin.drop(df_fin.filter(like=";G;").columns, axis=1)
df_fin = df_fin.drop(df_fin.filter(like=";R;").columns, axis=1)
df_fin = df_fin.drop(df_fin.filter(like=";N;").columns, axis=1)

df_fin.columns = [";".join(col.split(";")[-3:]) for col in df_fin.columns]
df_fin.columns = [";".join(reversed(col.split(";"))) for col in df_fin.columns]
df_fin.columns = [";".join(col.split(";")[-3:]) for col in df_fin.columns]
df_fin.columns = [";".join(reversed(col.split(";"))) for col in df_fin.columns]
df_fin = df_fin * -1

# %% visualizar resultado
df_fin.to_excel(path / f"2023_Clientes.xlsx")
# %%
df_fin
# %%
df = pd.melt(df_fin, ignore_index=False)
# %%
df = df.assign(**df["variable"].str.split(";", expand=True).add_prefix("n_"))
df.rename(
    columns={
        "value": "Consumo [kWh]",
        "n_0": "Tipo",
        "n_1": "PuntoMedida",
        "n_2": "Cliente",
    },
    inplace=True,
)
df.drop(columns="variable", inplace=True)
df = df.reset_index()
df["Fecha"] = df["index"].dt.date
df["Hora"] = df["index"].dt.time
df = df.drop("index", axis=1)
# %%
df

# %%
df.to_parquet(path / f"2023_Clientes.parquet")
print("paf")
# %%   ***** COSTO MARGINAL ****
barra = [""]
agno, mes = "23", "02"
cl_cmg = CMgIVT(path, agno, mes)
df = cl_cmg.busca_barra(barra)
# %%
df.columns
# %%
df.to_excel(path / f"CMg_ANTOFAGASTA___013_{agno}.xlsx")
# %%
barras = [
    "ANTOFAGASTA___013",
    "EJERCITO______013",
    "M.DEVELASCO___013",
    "PERALES_______015",
    "S.JOAQUIN_____015",
    "S.PEDRO_______013",
    "STA.ELVIRA____013",
    "CABRERO_______013",
]
barras = ["CHILLAN_______013"]

agno = "23"
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

df_fin = pd.DataFrame()
for mes in meses:
    print(f"procesando {mes}")
    cl_cmg = CMgIVT(path, agno, mes)
    df2 = cl_cmg.busca_barra(barras)
    df_fin = pd.concat([df_fin, df2])
print(f'{"-"*5}\tProceso Terminado\t{"-"*5}')
# %%
df_fin
# %%
df_fin.to_excel(path / f"CMg_CHILLAN13_{agno}.xlsx")
# %%  BUSCA CLIENTES CGEC

clientes = [
    "AGRICOLA LA QUEBRADA",
    "AGROINDUSTRIAL ANAPROC S.A.",
    "AGRORESERVAS DE CHILE",
    "AGRORESERVAS DE CHILE SPA",
    "COCA-COLA DE CHILE S.A",
    "COCA-COLA DE CHILE S.A.",
    "COMERCIALIZADORA VEGA MONUMENTAL",
    "DES INM PADRE WERNER",
    "HOTEL HAMPTON",
    "INMOB VEGA MONUMENTAL",
    "INVERSIONES AMANECER",
    "KAPINKE",
    "MINERA_LOS_PELAMBRES",
    "MOLINO FUENTES S.A.",
    "SANTIAGO AIRPORT HOTEL",
    "SCHÜSSLER",
    "UNIVERSIDAD DE LA FRONTERA",
    "FORESTAL AITUE LTDA",
    "SOUTH ORGANICS FRUIT",
    "COCA COLA DE CHILE",
    "ESTABLECIMIENTO_PENITENCIARIO",
    "ESTABLECIMIENTO PENITENCIARIO",
    "OMAMET",
    "SOC AGRICOLA EL OLIBAL",
    "SOCAGRICOLA_EL_OLIBAL",
    "HOTEL HAMPTON",
    "HOTEL_HAMPTON",
    "MARIA_LORETO_FERNANDEZ_LEON",
    "MARIA LORETO FERNANDEZ LEON",
    "EX_ABASOLO_VALLEJOS",
    "ABASOLO VALLEJOS",
]

clientes = [
    "COMERCIALIZADORA VEGA MONUMENTAL",
    "INMOB VEGA MONUMENTAL",
    "ECOMAS",
    "CLINICA BIO-BIO",
    "MARINA DEL SOL",
    "PLAZA LA SERENA",
    "MALL_ANTOFAGASTA",
    "PLAZA_TREBOL",
    "PLAZA DEL TREBOL",
]

agno = "23"
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
df_fin = pd.DataFrame()
for mes in meses:
    print(f"procesando {mes}")
    cl_ivt = ClientesIVT(path, agno, mes)
    df_aux = cl_ivt.busca_cliente(clientes)
    df_fin = pd.concat([df_fin, df_aux])
print(f'{"-"*5}\tProceso Terminado\t{"-"*5}')
# %%
df_fin.to_excel(path / f"CONSUMO_Clientes_VISITA_SUR_{agno}.xlsx")
# %%
path = Path(r"C:\_BD_Clientes\2024\01_Chequeo_Medidas\CMg")
df = pd.read_parquet(path / "cmg2402_def.parquet")
# %%
df
