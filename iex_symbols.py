from iex.iex_api import IEX

import pandas as pd

from utils import utils, data

def symbols(region, sandbox=True, renew=False):
    path = utils.file_path(__file__)

    folder = f'data/{utils.file_name(__file__)}/{"sandbox" if sandbox else "cloud"}'
    name = f'symbols-{region}'
    extension = 'csv'

    file = f'{path}/{folder}/{name}.{extension}'

    if not renew:
        df = data.df_read(file)
        if df is not None:
            df = df.set_index('symbol')

    if renew or df is None:
        iex = IEX(sandbox=sandbox, verbose=True)
        symbols = iex.request_symbols(region)
        df = pd.json_normalize(symbols)
        df = df[['symbol','name']]
        df = df.set_index('symbol')
        data.df_write(df, file)

    return df

if __name__ == "__main__":
    print(symbols('us'))
