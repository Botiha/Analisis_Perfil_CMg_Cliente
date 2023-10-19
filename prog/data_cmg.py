import pandas as pd
from pathlib import Path

class CMg:
    def __init__(self, agno, barra):
        self.agno = agno
        self.barra = barra
        self.path = Path(r'C:\_Costos_Marginales')

    def get_data(self, cmg='usd/mwh'):
        # cmg = CMg [mills/kWh]; CMg[$/KWh]
        if cmg.lower == 'usd/mwh':
            cmg = 'CMg [mills/kWh]'
        else:
            cmg = 'CMg[$/KWh]'

        df = pd.read_parquet(self.path / f'{self.agno}.parquet')
        b_aux = df.loc[df['Barra'].str.contains(self.barra)]
        b_aux = b_aux.pivot_table(
            columns='Barra',
            index=['Mes', 'Día', 'Hora'],
            values=cmg).reset_index()
        return b_aux

    def save_data(self, df, tipo='xlsx'):
        if tipo == 'xlsx':
            df.to_excel(self.path / f'{self.agno}_{self.barra}.xlsx')
        elif tipo == 'parquet':
            df.to_parquet(self.path / f'{self.agno}_{self.barra}.parquet')
        elif tipo == 'csv':
            df.to_csv(self.path / f'{self.agno}_{self.barra}.csv')
        else:
            raise ValueError('Tipo de archivo debe ser xlsx, parquet, csv')
        return
