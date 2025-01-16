import xlwings as xw
from prog.data_ivt import ClientesIVT, CMgIVT
from pathlib import Path

# from prog.data_ivt import ClientesIVT, CMgIVT


def traer_perfil():
    wb = xw.Book.caller()
    ivt = wb.sheets[0]

    path_parquets = Path(ivt["B1"].value)
    agno = str(int(ivt["B2"].value))
    meses = str(int(ivt["B3"].value))
    cliente = str(ivt["B4"].value).upper()

    cl_ivt = ClientesIVT(path_parquets, agno, meses)

    ivt["i1"].value = str(cl_ivt.ivt)
    df2 = cl_ivt.busca_cliente(cliente)

    data = []
    for col, perfiles in enumerate(df2.columns):
        perfil = ordena_columnas(perfiles)
        ivt["C8"].offset(0, col).value = perfil["barra"]
        ivt["C9"].offset(0, col).value = perfil["suministrador"]
        ivt["C10"].offset(0, col).value = perfil["tipo_medida"]
        ivt["C11"].offset(0, col).value = perfil["clave"]
        ivt["C12"].offset(0, col).value = perfil["cliente"]

    ivt["B13"].options(header=False).value = df2


def ordena_columnas(lista_nombres):
    data_el = lista_nombres.split(";")
    data = {
        "barra": data_el[0],
        "suministrador": data_el[1],
        "tipo_medida": data_el[2],
        "clave": data_el[3],
        "cliente": data_el[4],
    }
    return data


@xw.func
def hello(name):
    return f"Hello {name}!"


if __name__ == "__main__":
    xw.Book("xl_perfil_cmg.xlsm").set_mock_caller()
