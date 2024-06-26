import requests
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm.auto import tqdm
import pandas as pd

class RevisaPagCMg:
    def __init__(self, year):
        self.cen = "https://www.coordinador.cl/mercados/documentos" \
                   "/transferencias-economicas/"
        self.cmg = self.cen + "costos-marginales-de-energia/" \
            f"{year}-costos-marginales-de-energia/"

    @staticmethod
    def get_status(url):
        return requests.get(url)

    def get_html(self):
        paginas = [self.get_status(self.cmg),
                   self.get_status(self.cmg + '?page=2')]
        texto, link, fecha_pub, size = [], [], [], []
        for pag in paginas:
            # Check if the request was successful
            if pag.status_code == 200:
                soup = BeautifulSoup(pag.text, 'html.parser')
                table = soup.find_all('div', class_='col-md-11')
                data2 = table[1].find_all('a')
                for a in data2:
                    link.append(a['href'])
                    try:
                        fecha_tent = a.parent.text.split('\n')[2].split(': ')
                        if '/' in fecha_tent[0]:
                            fecha_pub.append(fecha_tent[0].strip())
                        else:
                            fecha_pub.append(fecha_tent[1].strip())
                        texto.append(a.parent.text.split('\n')[1].strip())
                    except:
                        pass

            else:
                return f"Failed to retrieve data: {response.status_code}"

        for t in link:
            try:
                tam = requests.get(
                    t,
                    stream=True,
                    headers={'Accept-Encoding': None})
                tam = int(tam.headers['Content-length']) / 1024 / 1024
                tam = str(round(tam, 2)) + ' MB'
            except:
                tam = ' MB'
            size.append(tam)

        return texto, fecha_pub, link, size

class DescargaLink:
    """
        Descarga archivo desde la página url
            url: url de donde se quiere bajar el archivo
            carpeta: ubicación de carpeta de destino
        descarga_file:
            toma la url y la carpeta y descarga el archivo
    """
    def get_mes(self, mes, pre_def):
        mes = int(mes)
        if pre_def == 'pre':
            proceso = 'Preliminar'
        elif pre_def == 'def':
            proceso = 'Definitivo'

        if mes == 1:
            return f"{proceso} Enero"
        elif mes == 2:
            return f"{proceso} Febrero"
        elif mes == 3:
            return f"{proceso} Marzo"
        elif mes == 4:
            return f"{proceso} Abril"
        elif mes == 5:
            return f"{proceso} Mayo"
        elif mes == 6:
            return f"{proceso} Junio"
        elif mes == 7:
            return f"{proceso} Julio"
        elif mes == 8:
            return f"{proceso} Agosto"
        elif mes == 9:
            return f"{proceso} Septiembre"
        elif mes == 10:
            return f"{proceso} Octubre"
        elif mes == 11:
            return f"{proceso} Noviembre"
        elif mes == 12:
            return f"{proceso} Diciembre"

    def download_with_progress(self):
        size = requests.head(self.url)
        response = requests.get(self.url, stream=True,
                                headers={'Accept-Encoding': None})
        total_size = int(size.headers['Content-length'])

        block_size = 2 * 1024 * 1024
        progress_bar = tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            position=0,
            leave=True
        )

        with open(self.full_file, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()

class DescargaCMg(DescargaLink):
    def __init__(self, links, mes, carpeta, proceso='def'):
        self.mes = self.get_mes(mes, proceso)
        self.url = links.loc[links['texto'].str.contains(
            self.mes)].link.values[0]
        self.carpeta = Path(carpeta)
        self.file = self.url.split('/')[-1]
        self.full_file = self.carpeta / self.file
        Path(self.carpeta).mkdir(parents=True, exist_ok=True)

    def arregla_cmg(self):
        xls_name = self.full_file.stem + '.xlsm'
        #print(self.file.split('cmg'))
        fname = self.file.split('cmg')[1][:4]
        parquet_name = f'CMg_{fname[:2]}_{int(fname[2:])}.parquet'

        df_input = pd.read_excel(self.carpeta / xls_name,
                                 sheet_name='CMG Barras')
        cols = [
            'Mes', 'Día', 'Hora', 'Barra',
            'CMg [mills/kWh]', 'USD', 'CMg[$/KWh]'
        ]
        df_fin = pd.DataFrame(columns=cols)
        df1 = df_input[cols]
        df_fin = pd.concat([df_fin, df1])

        if len(df_input.filter(like='.1', axis=1).columns) != 0:
            df_aux = df_input[['Mes.1', 'Día.1', 'Hora.1', 'Barra.1',
                               'CMg [mills/kWh].1', 'USD.1', 'CMg[$/KWh].1']]
            df_fin = self._arregla_y_junta_df(df_fin, df_aux)
        if len(df_input.filter(like='.2', axis=1).columns) != 0:
            df_aux = df_input[['Mes.2', 'Día.2', 'Hora.2', 'Barra.2',
                               'CMg [mills/kWh].2', 'USD.2', 'CMg[$/KWh].2']]
            df_fin = self._arregla_y_junta_df(df_fin, df_aux)
        if len(df_input.filter(like='.3', axis=1).columns) != 0:
            df_aux = df_input[['Mes.3', 'Día.3', 'Hora.3', 'Barra.3',
                               'CMg [mills/kWh].3', 'USD.3', 'CMg[$/KWh].3']]
            df_fin = self._arregla_y_junta_df(df_fin, df_aux)

        df_fin = df_fin.dropna()
        df_fin['Mes'] = df_fin['Mes'].astype(int)

        df_fin['Year'] = df_fin['Mes'].astype(str).str[:4]
        df_fin['Month'] = df_fin['Mes'].astype(str).str[-2:]
        df_fin['Day'] = df_fin['Día'].astype(int)
        df_fin['Hour'] = df_fin['Hora'].astype(int)

        #df_fin['Fecha'] = pd.to_datetime(
        #    df_fin[['Year', 'Month', 'Day', 'Hour']], format='%Y%m%d%H')
        df['Fecha'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour']],
                                     format='%Y%m%d%H')
        df = df[['Fecha', 'Barra', 'CMg [mills/kWh]', 'USD', 'CMg[$/KWh]']]

        #df_fin = df_fin[['Year', 'Month', 'Day', 'Hour', 'Barra',
        #                 'CMg [mills/kWh]', 'USD', 'CMg[$/KWh]']]
        df_fin.to_parquet(self.carpeta / parquet_name, index=False)
        return df_fin

    def _arregla_y_junta_df(self, df1, df_to_fix):
        cols = [
            'Mes', 'Día', 'Hora', 'Barra',
            'CMg [mills/kWh]', 'USD', 'CMg[$/KWh]'
        ]
        df_to_fix = df_to_fix.dropna()
        df_to_fix.columns = cols
        return pd.concat([df1, df_to_fix])

#%%
pag = RevisaPagCMg(2023)
data = pag.get_html()
print('Extrayendo info desde el Coordinador...')
df = pd.DataFrame(columns=['texto', 'fecha', 'link', 'size'],
                  data=zip(data[0], data[1], data[2], data[3]))

folder = Path(r'C:\_Costos_Marginales')

#%%
for i in [11]:
    desc =  DescargaCMg(df, i, folder)
    desc.download_with_progress()
    print(f'{desc.mes}...descargado')
    df2 = desc.arregla_cmg()
    print(f'{desc.file}...transformado y grabado como parquet')

#%%
#TODO: Algo sucede con los meses, cómo que toma el mes siguiente
for i in [1]:
    desc =  DescargaCMg(df, i, folder)
    #desc.download_with_progress()
    #print(f'{desc.mes}...descargado')
    print(f'{i}, procesando {desc.file}')
    df2 = desc.arregla_cmg()
    fname = desc.file.split('cmg')[1][:4]
    parquet_name = f'CMg_{fname[:2]}_{int(fname[2:])}.parquet'
    print(f'{parquet_name} grabado...')
#%%
for i in range(12):
    j = i + 1
    if i < 9:
        name = f'/2023/0{j}/cmg230{j}_def.zip'
    elif i < 11:
        name = f'/2023/{j}/cmg23{j}_def.zip'
    else:
        name = f'/2024/12/cmg23{j}_def.zip'

    print(f'https://www.coordinador.cl/wp-content/uploads{name}')
