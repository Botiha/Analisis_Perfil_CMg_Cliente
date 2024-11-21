# %%
from prog.data_ivt import ClientesIVT, CMgIVT
import pandas as pd
from pathlib import Path


class Clientes(ClientesIVT):
    def __init__(self, path_ivt, agno, mes):
        super().__init__(path_ivt, agno, mes)


def arregla_df(df):
    #  Arregla Dataframe para dejarlo en el powerBI
    print("Arreglando Datos de consumo...")
    df = df.drop(df.filter(like=";T;").columns, axis=1)
    df = df.drop(df.filter(like=";G;").columns, axis=1)
    df = df.drop(df.filter(like=";R;").columns, axis=1)
    df = df.drop(df.filter(like=";N;").columns, axis=1)

    df.columns = [
        ";".join([valores[0]] + valores[-2:])
        for val in df.columns
        for valores in [val.split(";")]
    ]
    df.columns = [";".join(reversed(col.split(";"))) for col in df.columns]
    df = df / -1000

    return df


def agrupa_temporal(df, temporal):
    #  Agrupa mensualmente, y hace melt para power bi
    print(f"Agrupando por {temporal}")
    if temporal == "mensual":
        df_temp = df.groupby([df.index.month, df.index.year]).sum()
    elif temporal == "horario":
        df_temp = df.groupby([df.index.year, df.index.month, df.index.hour]).sum()

    df_temp = pd.melt(df_temp, ignore_index=False)
    df_temp = df_temp.assign(
        **df_temp["variable"].str.split(";", expand=True).add_prefix("n_")
    )
    df_temp.rename(
        columns={
            "value": "Consumo [kWh]",
            "n_0": "Cliente",
            "n_1": "PuntoMedida",
            "n_2": "Barra",
        },
        inplace=True,
    )
    df_temp.drop(columns="variable", inplace=True)
    df_temp.reset_index(inplace=True)
    df_temp.rename(
        columns={"level_0": "Año", "level_1": "Mes", "level_2": "Hora"},
        inplace=True,
    )
    return df_temp


# %% Busqueda de cliente por 12 merses completo
path = Path(r"C:\_BD_Clientes\IVT")
cliente = "".upper()
agno = "23"  # sólo 2 números, año 24, o año 23
meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
df_fin = pd.DataFrame()
for mes in meses:
    print(f"procesando {mes}")
    cl_ivt = Clientes(path, agno, mes)
    df2 = cl_ivt.busca_cliente(cliente)
    df_fin = pd.concat([df_fin, df2])
print(f'{"-"*5}\tProceso Terminado\t{"-"*5}')

# %% Arregla Dataframe para dejarlo en el powerBI
df_fin = arregla_df(df_fin)
df_mensual = agrupa_temporal(df_fin, "mensual")
df_mensual.to_parquet(path / f"2023_Clientes_mensual.parquet")
# %%
df_horario = agrupa_temporal(df_fin, "horario")
# %%
df_horario
# %%

df_horario.to_parquet(path / f"2023_Clientes_horario.parquet")

# %% Extrae CMg
barras = [""]
agno = "23"
meses = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
folder = Path(r"C:\_Costos_Marginales")

df_fin = pd.DataFrame()
for mes in meses:
    print(f"procesando {mes}")
    df = pd.read_parquet(folder / f"CMg_23_{mes}.parquet")
    df = df.map(lambda x: x.replace("\n", "") if isinstance(x, str) else x)
    df_fin = pd.concat([df_fin, df])
print(f'{"-" * 5}\tProceso Terminado\t{"-" * 5}')
# %% Columnas = ['Fecha', 'Barra', 'CMg [mills/kWh]', 'USD', 'CMg[$/KWh]']
df_fin["Mes"] = df_fin.Fecha.dt.month
df_fin["Año"] = df_fin.Fecha.dt.year
df_fin["Hora"] = df_fin.Fecha.dt.hour
# %% Resumen Mensual
cmg_mensual = df_fin.groupby(["Mes", "Año", "Barra"], as_index=False).mean()
cmg_mensual.drop(columns=["Fecha", "Hora"], inplace=True)
cmg_mensual.to_parquet(path / "CMg_2023_mensual.parquet")

# %% Resumen Horario
cmg_hor = df_fin.groupby(["Año", "Mes", "Hora", "Barra"], as_index=False).mean()
cmg_hor.drop(columns=["Fecha"], inplace=True)
cmg_hor.to_parquet(path / "CMg_2023_Horario.parquet")
# %%
cmg_hor
# %%
df_temp = df_fin.groupby(
    [df_fin.index.year, df_fin.index.month, df_fin.index.hour]
).sum()
df_temp = pd.melt(df_temp, ignore_index=False)
# %%
df_temp = df_temp.assign(
    **df_temp["variable"].str.split(";", expand=True).add_prefix("n_")
)
# %%
df_temp.rename(
    columns={
        "value": "Consumo [kWh]",
        "n_0": "Cliente",
        "n_1": "PuntoMedida",
        "n_2": "Barra",
    },
    inplace=True,
)

# %%
df_temp.drop(columns="variable", inplace=True)
# %%
df_fin.to_parquet(path / "CMg_2023.parquet")
# %%
path = Path(r"C:\_Costos_Marginales")
df_fin = pd.read_parquet(path / "2022.parquet")

# %%
df_fin.columns
# %%
barras = ["EJERCITO______013", "LASENCINAS____015", "ANTOFAGASTA___013"]
df1 = df_fin.loc[df_fin["Barra"].isin(barras)]
# %%
df1.pivot_table(
    index=["Mes", "Día", "Hora"], values="CMg [mills/kWh]", columns="Barra"
).reset_index().to_excel(path / "CMg_2022_Ejercito_LasEncins.xlsx")
# %%
df1
