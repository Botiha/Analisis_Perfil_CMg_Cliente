# %%
import pandas as pd
import polars as pl
from subprocess import call
from prog.crear_vb_xlsb_to_csv import crea_archivo_vbscript
from pathlib import Path
import tempfile
import patoolib


def lee_medidas(año, mes, carpeta, tipo, sh="1"):
    # tipo puede ser f de físico o c de cmg
    excel = carpeta / f"Medidas_horarias_{año}{mes}.xlsb"
    med = carpeta / f"Medidas_{año}-{mes}.csv"
    vb_excel = "ExcelToCsv.vbs"
    tipo_fc = "Físico" if tipo == "f" else "Cmg"

    if (med).exists():
        df = filtra_medidas(med, tipo_fc)
    else:
        vbs = crea_archivo_vbscript()
        with tempfile.NamedTemporaryFile(suffix=".vbs", delete=False) as temp_file:
            temp_filename = temp_file.name
            temp_file.write(vbs.encode())

        call(["cscript.exe", temp_filename, str(excel), str(med), sh])
        temp_file.close()

        df = filtra_medidas(med, tipo_fc)

    lst = df.columns.tolist()
    lst[4] = "descripcion"
    df.columns = lst

    df_out = pd.melt(
        df,
        id_vars=[
            "nombre_barra",
            "propietario",
            "Tipo_Medida",
            "clave",
            "Valores",
            "descripcion",
        ],
        value_vars=df.columns[6:],
        var_name="fecha",
    )

    df_out["fecha"] = df_out["fecha"].astype(int)
    df_out = hour_col_to_date_col(df_out, f"20{año}-{mes}-01")
    df_out = df_out.sort_values(by=["propietario", "fecha"])
    df_out = df_out.drop(columns=["Valores"])
    print(f"Medidas 20{año}-{mes} procesadas")
    return df_out


def filtra_medidas(csv_file, tipo):
    # tipo es F o C: (F)ísico__kWh o  (C)mg_[$/kWh]
    df = pd.read_csv(csv_file, encoding="latin", header=1)
    df = df.loc[df["Valores"].str.contains(tipo)]
    if tipo == "C":
        cols_to_drop = ["propietario", "Tipo_Medida", "clave", "descripcion", "Valores"]
        df = df.drop_duplicates(subset=["nombre_barra"])
        df = df.drop(columns=cols_to_drop)
    return df


def hour_col_to_date_col(df, start_date):
    df_fin = df.copy()
    start_date = pd.to_datetime(start_date)
    horas = df_fin.fecha
    horas = pd.to_timedelta(horas.astype(int) - 1, unit="h")
    horas += start_date
    df_fin.fecha = horas
    return df_fin


class LeeMedidas:
    def __init__(self, anio, mes, carpeta, QMin=True):
        self.anio = anio
        self.mes = mes
        self.carpeta = Path(carpeta)
        self.QMin = QMin
        self.ar_zip = f"Balance_Valorizado_{anio}{mes}_Data_VALORIZADO_15min.zip"
        self.ar_csv = f"Balance_Valorizado_{anio}{mes}_Data_VALORIZADO_15min.csv"

    def __call__(self):
        return self.lee_medidas()

    def lee_medidas(self):
        if self.QMin:
            print("Procesando data 15minutal")
            return self.lee_medidas_qmin()
        else:
            print("Procesando data Horaria")
            return self.lee_medidas_med()

    def lee_medidas_med(self):
        excel = self.carpeta / f"Medidas_horarias_{self.anio}{self.mes}.xlsb"
        med = self.carpeta / f"Medidas_{self.anio}-{self.mes}.csv"

        vb_excel = "ExcelToCsv.vbs"
        if not (med).exists():
            vbs = crea_archivo_vbscript()
            with tempfile.NamedTemporaryFile(suffix=".vbs", delete=False) as temp_file:
                temp_filename = temp_file.name
                temp_file.write(vbs.encode())

            call(["cscript.exe", temp_filename, str(excel), str(med), "1"])
            temp_file.close()

        df = self.filtra_medidas(med)

        return df

    def lee_medidas_qmin(self):
        med = self.carpeta / f"Medidas_15min_{self.anio}-{self.mes}.parquet"

        if not (med).exists():
            # con polars es 3 a 4 veces más rápido
            patoolib.extract_archive(
                str(self.carpeta / self.ar_zip),
                outdir=str(self.carpeta),
                verbosity=1,
                interactive=False,
            )
            df = pl.read_csv(self.carpeta / self.ar_csv, ignore_errors=True)
            df = df.drop(
                ["nro_med", "pnudo", "nro_lt", "clave_LT", "MedidaHoraria2", "Zona"]
            )
            nombre_parquet = f"Medidas_15min_{self.anio}-{self.mes}.parquet"
            df.write_parquet(self.carpeta / med)
            csv = self.carpeta / self.ar_csv  # Borra CSV
            csv.unlink()

        df = self.filtra_qmin()

        df = self.pasar_min_a_hora(df)

        df2 = df.pivot_table(
            index=["hora_del_mes"], columns="clave", values="MedidaHoraria"
        )

        if "GLINARES" in df2.columns:
            df2 = df2.drop(["GLINARES"], axis=1)

        df2, med_p = self.entrega_medidas(df2)
        return df2, med_p

    def entrega_medidas(self, df_in):
        df2 = df_in.copy()
        med_p = df_in.copy()

        med_p.fillna(0, inplace=True)
        df2["Total"] = df2.sum(axis=1)

        for c in med_p.columns.tolist():
            med_p[c] = med_p[c] / df2["Total"]

        return df2, med_p

    def filtra_medidas(self, csv_file):
        df = pd.read_csv(csv_file, encoding="latin", header=1)

        # CMg
        cmg = self.filtra_cmg(df)
        cmg.to_parquet(self.carpeta / f"CMg_{self.anio}_{self.mes}.parquet")

        # IVT
        ivt = self.filtra_ivt(df)
        ivt.to_parquet(self.carpeta / f"IVT_{self.anio}_{self.mes}.parquet")

        return df

    def filtra_cmg(self, df_cmg):
        cols_to_drop = [
            "propietario",
            "Tipo_Medida",
            "clave",
            "descripcion",
            "Valores",
        ]
        df = df_cmg.loc[df_cmg["Valores"].str.contains("C")]
        df = df.drop_duplicates(subset=["nombre_barra"])
        df = df.drop(columns=cols_to_drop)
        return df

    def filtra_ivt(self, df_ivt):
        df = df_ivt.loc[df_ivt["Valores"].str.contains("F")]
        df = df.loc[df["Tipo_Medida"].isin(["L", "L_D"])]

        return df

    def filtra_qmin(self, borrar=True):

        filtrado = df.filter(pl.col("propietario").str.contains("CGE_C"))
        return filtrado.to_pandas()

    def pasar_min_a_hora(self, med):
        df = med.copy()
        f_inicial = pd.to_datetime(f"20{self.anio}-{self.mes}-01 00:00:00")
        intervalo = pd.Timedelta(minutes=15)
        df["Cuarto de Hora"] = f_inicial + (df["Cuarto de Hora"] - 1) * intervalo
        df["fecha"] = df["Cuarto de Hora"].dt.date
        df["hora"] = df["Cuarto de Hora"].dt.hour
        cols = [
            "nombre_barra",
            "clave",
            "fecha",
            "hora",
            "propietario",
            "descripcion",
            "tipo1",
        ]

        df = df.groupby(cols, as_index=False).agg({"MedidaHoraria": "sum"})

        df["fecha"] = pd.to_datetime(df["fecha"])
        df["hora_del_mes"] = (df["fecha"].dt.day - 1) * 24 + df["hora"]
        df["hora_del_mes"] = df["hora_del_mes"] + 1
        return df


class DataIVT:
    cols_data_IVT = [
        "nombre_barra",
        "propietario",
        "Tipo_Medida",
        "clave",
        "descripcion",
    ]

    def __init__(self, path, año, mes):
        self.path = path
        self.año = año
        self.mes = str(mes).zfill(2)

    def hour_col_to_date_col_new(self, cols_ivt=True):
        df_fin = self.df_data.copy()

        if cols_ivt:
            cols_ivt = self.cols_data_IVT + ["Valores"]
            inicio_int = 6
        else:
            cols_ivt = [self.cols_data_IVT[0]]
            inicio_int = 1

        fecha = [int(x) for x in df_fin.columns[inicio_int:]]
        start_date = pd.to_datetime(f"20{self.año}-{int(self.mes)}-01")
        horas = pd.to_timedelta([x - 1 for x in fecha], unit="h")
        horas += start_date

        df_fin.columns = cols_ivt + horas.to_list()
        return df_fin


class ClientesIVT(DataIVT):
    def __init__(self, path_ivt, año, mes):
        super().__init__(path_ivt, año, mes)
        self.ivt = self.path / f"IVT_{self.año}_{self.mes}.parquet"
        self.df_data = pd.read_parquet(self.ivt)

    def busca_cliente(self, cliente):
        df = self.hour_col_to_date_col_new()
        cols = self.cols_data_IVT
        if isinstance(cliente, str):
            df = df.loc[df["descripcion"].str.upper().str.contains(cliente)]
        elif isinstance(cliente, list):
            df = df.loc[df["descripcion"].str.upper().str.contains("|".join(cliente))]

        df["nombre"] = (
            df["nombre_barra"]
            + ";"
            + df["propietario"]
            + ";"
            + df["Tipo_Medida"]
            + ";"
            + df["clave"]
            + ";"
            + df["descripcion"]
        )

        df.drop(columns=cols, inplace=True)
        df.drop(columns="Valores", inplace=True)
        df.set_index("nombre", inplace=True)
        return df.T


class CMgIVT(DataIVT):
    def __init__(self, path_cmg, año, mes):
        super().__init__(path_cmg, año, mes)
        self.path_cmg = self.path / f"CMg_{self.año}_{self.mes}.parquet"
        self.df_data = pd.read_parquet(self.path_cmg)

    def busca_barra(self, barra):
        cols_ivt = False  # False para 15min
        df = self.hour_col_to_date_col_new(True)
        cols = self.cols_data_IVT.copy()

        if isinstance(barra, str):
            df = df.loc[df["nombre_barra"].str.contains(barra)]
        elif isinstance(barra, list):
            df = df.loc[
                df["nombre_barra"].str.contains("|".join([x.upper() for x in barra]))
            ]

        # df["Barra"] = df["nombre_barra"]
        cols.remove("nombre_barra")
        df.drop(columns=cols, inplace=True)
        df.drop(columns="Valores", inplace=True)
        df = df.drop_duplicates()
        if cols_ivt:
            df["Barra"] = df["nombre_barra"]
            df.drop(columns=cols, inplace=True)
            df.drop(columns="Valores", inplace=True)
            df = df.drop_duplicates()
            df = pd.DataFrame(
                columns=df.nombre_barra,
                index=df.iloc[:, 1:].T.index,
                data=df.iloc[:, 1:].T.values,
            )
        else:
            df = df.drop_duplicates()
            df = pd.DataFrame(
                columns=df.nombre_barra,
                index=df.iloc[:, 1:].T.index,
                data=df.iloc[:, 1:].T.values,
            )

        return df


# %%
if __name__ == "__main__":
    # %%
    ar = Path(r"C:\_BD_Clientes\IVT\CMg_24_01.parquet")
    df = pd.read_parquet(ar)

    # %%
    barras = [""]

    agno = "23"
    meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

    df_fin = pd.DataFrame()
    for mes in meses:
        print(f"procesando {mes}")
        cl_cmg = CMgIVT(path, agno, mes)
        df2 = cl_cmg.busca_barra(barras)
        df_fin = pd.concat([df_fin, df2])
    print(f'{"-" * 5}\tProceso Terminado\t{"-" * 5}')
    # %%
    df = pd.melt(df_fin, ignore_index=False)
    # %%
    df = df.reset_index()
    df["Fecha"] = df["index"].dt.date
    df["Hora"] = df["index"].dt.time
    df = df.drop("index", axis=1)
    # %%
    df.rename(
        columns={
            "nombre_barra": "Barra",
            "value": "CMg [CLP/kWh]",
        },
        inplace=True,
    )
    # %%
    df.to_parquet(path / "CMg_2023.parquet")
