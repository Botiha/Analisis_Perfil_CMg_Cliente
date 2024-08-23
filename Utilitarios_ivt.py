# %%
import pandas as pd
import polars as pl
from prog.data_ivt import lee_medidas, filtra_medidas
from prog.data_cmg import CMg
from pathlib import Path
import time
import glob

# %% Cambia archivos XLSB de medidas a CSV
path_ivt = Path(r"C:\_BD_Clientes\IVT\Medidas")
ar_xlsb = glob.glob(str(path_ivt) + "\*.xlsb")
print(f"Archivos XLSB: {ar_xlsb}")
año = "24"


def Medidas_xslb_to_csv(path_carpeta, año, archivos_xlsb):
    for xlsb in archivos_xlsb:
        mes = xlsb.split(".")[0][-2:]
        print(f"Prcesando año: 20{año}, mes: {mes}")
        lee_medidas(año, mes, path_carpeta, "c")


def crea_parquets(path_carpeta, archivos_csvs, tipo):
    for csv in archivos_csvs:
        año = csv.split("-")[0][-2:]
        mes = csv.split(".")[0][-2:]
        print(f"Prcesando año: 20{año}, mes: {mes}")
        df = filtra_medidas(csv, tipo)

        tipo_ar = ""
        if tipo.lower() in "CMg".lower():
            tipo_ar = "Cmg"
        elif tipo.lower() in "Físico".lower():
            tipo_ar = "IVT"
        else:
            print(tipo_ar.lower())
            raise ValueError('Tipo de archivo debe ser "C"mg o "F"ísico')

        df.to_parquet(path_carpeta / f"{tipo_ar}_{año}_{mes}.parquet")


# %% Ejecuta el cambio
ti = time.time()
print("Cambiando archivos de medida XLSB a CSV")
Medidas_xslb_to_csv(path_ivt, año, ar_xlsb)
print("Archivo xlsb procesado")


# % Crea Perfiles IVT_año y CMg_año por clientes
ar_csvs = glob.glob(str(path_ivt) + "\*.csv")
print(f"archivos csv a procesar: {[x for x in ar_csvs]}")


# %Crea datos IVT  -->  'F'
crea_parquets(path_ivt, ar_csvs, "F")
print(f"Demora: {round(time.time() - ti)} seg")
# %%Crea datos IVT  -->  'C'
crea_parquets(path_ivt, ar_csvs, "C")

# %%
ivt = pd.read_parquet(path_ivt / "IVT_24_03.parquet")
print(ivt.head())

# %%
cmg = pd.read_parquet(path_ivt / "CMg_24_03.parquet")
print(cmg.head())

# %%
ti = time.time()
ar = path_ivt / ar_xlsb[0]
df = pl.read_excel(ar, engine="calamine", read_options={"header_row": 1})
df = df.to_pandas()
df = df.loc[df["Valores"].str.contains("F")]

df.to_parquet(path_ivt / f"p1.parquet")

print(f"Demora: {round(time.time() - ti)} seg")

# %%
df2 = pd.read_parquet(Path(r"C:\_Costos_Marginales\2018.parquet"))
# %%
carpeta = Path(r"C:\_Costos_Marginales")
df_out = pd.DataFrame()
for cmg in range(2019, 2023):
    print("Procesando " + str(cmg))
    df = pd.read_parquet(carpeta / f"{str(cmg)}.parquet")
    df = df.loc[df.Barra == "LA_PINTANA____013"]
    df_out = pd.concat([df_out, df])

# %%
df_out.to_excel(carpeta / "LA_PINTANA_2019_2022.xlsx")
# %%
df_out.Barra.unique()
