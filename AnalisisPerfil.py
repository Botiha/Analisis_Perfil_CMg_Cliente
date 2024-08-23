from prog.data_ivt import ClientesIVT, CMgIVT, LeeMedidas, filtra_medidas
from prog.data_cmg import CMg

import configparser
import pandas as pd
import polars as pl
from pathlib import Path
import glob
import time

cfg = configparser.ConfigParser(
    inline_comment_prefixes="#",
    converters={"list": lambda x: [i.strip() for i in x.split("," "")]},
)
cfg.read("conf.ini")
path = Path(cfg["Direcciones"]["data"])


def BuscaCliente():
    """
    Utilizado en Freeter, para buscar cliente en IVT
    :return: Imprime en el terminal si existe el cliente, si existe entrega
    información de barra, proveedor, etc
    """
    cliente = [x.upper() for x in cfg.getlist("Clientes", "cliente")]
    agno = cfg["Clientes"]["agno"]
    meses = cfg.getlist("Clientes", "meses")
    mes = meses[-1]

    cl_ivt = ClientesIVT(path, agno, mes)
    df = cl_ivt.busca_cliente(cliente)
    return [print(x) for x in df.columns]


def BuscaCMg():
    barra = [x.upper() for x in cfg.getlist("CostoMarginal", "barra")]
    agno = cfg["CostoMarginal"]["agno"]
    meses = cfg.getlist("CostoMarginal", "meses")
    mes = meses[-1]

    cl_cmg = CMgIVT(path, agno, mes)
    df = cl_cmg.busca_barra(barra)
    return [print(x) for x in df.columns]


def perfil():
    cliente = [x.upper() for x in cfg.getlist("Clientes", "cliente")]
    agno = cfg["Clientes"]["agno"]
    meses = cfg.getlist("Clientes", "meses")

    df_fin = pd.DataFrame()
    for mes in meses:
        print(f"procesando {mes}")
        cl_ivt = ClientesIVT(path, agno, mes)
        df2 = cl_ivt.busca_cliente(cliente[0])
        df_fin = pd.concat([df_fin, df2])
    df_fin.to_excel(path / cfg["Clientes"]["archivo_salida"])
    return


def CalculaCMg():
    barra = [x.upper() for x in cfg.getlist("CostoMarginal", "barra")]
    agno = cfg["CostoMarginal"]["agno"]
    meses = cfg.getlist("CostoMarginal", "meses")

    df_fin = pd.DataFrame()
    for mes in meses:
        print(f"procesando {mes}")
        cl_cmg = CMgIVT(path, agno, mes)
        df = cl_cmg.busca_barra(barra)
        df_fin = pd.concat([df_fin, df])
    df_fin.to_excel(path / cfg["CostoMarginal"]["archivo_salida"])
    return


def Medidas_xslb_to_csv(path_carpeta, año, archivos_xlsb, qmin=True):
    if qmin:
        separador = "_Data"
    else:
        separador = "."

    for xlsb in archivos_xlsb:
        mes = xlsb.split(separador)[0][-2:]
        print(f"Procesando año: 20{año}, mes: {mes}")
        LeeMedidas(año, mes, path_carpeta, qmin)()


def crea_parquets(path_carpeta, archivos_csvs, tipo, qmin=True):
    for csv in archivos_csvs:
        año = csv.split("-")[0][-2:]
        mes = csv.split(".")[0][-2:]
        print(f"\tPrcesando año: 20{año}, mes: {mes}")
        df = filtra_medidas(csv, tipo)

        tipo_ar = ""
        if tipo.lower() in "CMg".lower():
            tipo_ar = "CMg"
        elif tipo.lower() in "Físico".lower():
            tipo_ar = "IVT"
        else:
            print(tipo_ar.lower())
            raise ValueError('Tipo de archivo debe ser "C"mg o "F"ísico')

        df.to_parquet(path_carpeta / f"{tipo_ar}_{año}_{mes}.parquet")


def procesa_medidas():
    ti = time.time()
    path_ivt = Path(cfg["ConvirteData"]["path_medidas"])
    # detecta archivos HORARIOS y archivos 15Minutales
    ar_xlsb = glob.glob(str(path_ivt) + r"\Medidas_*.xlsb")
    ar_zip = glob.glob(str(path_ivt) + r"\*15min.zip")

    if len(ar_xlsb) > 0:
        procesa_medidas_horarias(path_ivt, ar_xlsb)
    if len(ar_zip) > 0:
        procesa_medidas_15min(path_ivt, ar_zip)

    print(f"\nTiempo tomado en proceso: {round(time.time() - ti)} seg")
    pass


def procesa_medidas_horarias(path_ivt, ar_xlsb):
    agno = ar_xlsb[0].split("_")[-1]
    agno = agno[:2]

    print(f"\nArchivos XLSB a procesar: {ar_xlsb}")
    print("\n*********** Cambiando a formato CSV ***********\n")

    Medidas_xslb_to_csv(path_ivt, agno, ar_xlsb, qmin=False)

    return


def procesa_medidas_15min(path_ivt, ar_zip):
    agno = ar_zip[0].split("Valorizado_")[-1]
    agno = agno[:2]

    print(f"\nArchivos ZIP-15Min a procesar: {ar_zip}")
    print("\n*********** Extrayendo CSV y pasando a Parquet ***********\n")

    Medidas_xslb_to_csv(path_ivt, agno, ar_zip)
    ar_pqts = glob.glob(str(path_ivt) + r"\Medidas_15min*.parquet")

    return


# %%
if __name__ == "__main__":
    # %%
    # cmg = pd.read_parquet(path_ivt / "CMg_24_06.parquet")
    # ivt = pd.read_parquet(path_ivt / "IVT_24_06.parquet")
    import time
    import pandas as pd
    import polars as pl

    ti = time.time()
    m15 = pd.read_parquet(path_ivt / "Medidas_15min_2407.parquet")
    print(time.time() - ti)

    # %%
    ti = time.time()
    m16 = pl.read_parquet(path_ivt / "Medidas_15min_2407.parquet")
    m16 = m16.to_pandas()
    print(time.time() - ti)
    # %%
