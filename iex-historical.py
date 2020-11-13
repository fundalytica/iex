import sys
import argparse

import os
import pathlib

import requests
import json

import numpy as np
import pandas as pd

from colorama import Fore, Style

from utils import utils, stock

from iex import iex_token, iex_ranges, iex_trading_days

def color_print(text, color):
    print(f'{color}{text}{Style.RESET_ALL}')

class Local:
    def __init__(self, symbol, sandbox, csv=True):
        self.csv = csv

        file_path = pathlib.Path(__file__).parent.absolute()
        file_name = os.path.splitext(__file__)[0]

        self.path = f"{file_path}/{file_name}/{'sandbox' if sandbox else 'cloud'}/"
        self.path += symbol
        self.path = f'{self.path}.{"csv" if csv else "pkl"}'

        color_print(f'\n[ Local: Init ]', Fore.GREEN)
        color_print(f'> Symbol: {symbol}, Path: {self.path}', Fore.CYAN)

    def read(self):
        color_print('\n[ Local: Read Historical Data ]', Fore.GREEN)

        try:
            if(self.csv):
                df = pd.read_csv(self.path, index_col='date')
            else:
                df = pd.read_pickle(self.path)

            # sort on load
            df.sort_index(inplace=True)

            print(f'\n{df}')
        except IOError as e:
            color_print('> File Not Found', Fore.RED)
            return None
        except pd.errors.EmptyDataError as e:
            color_print('> Empty Data', Fore.RED)
            return None

        return df

    def write(self, df):
        color_print('\n[ Local: Write Historical Data ]', Fore.GREEN)

        # sort on save
        df.sort_index(inplace=True)

        if(self.csv):
            df.to_csv(self.path)
        else:
            df.to_pickle(self.path)
        
        color_print('> OK', Fore.MAGENTA)

class Integrity:
    def __init__(self, remote):
        self.remote = remote

    def missing(self, historical_data_df):
        color_print(f'\n[ Integrity: Missing Dates ]', Fore.GREEN)

        # start and end dates in historical data frame
        start = pd.to_datetime(historical_data_df.index[0])
        end = pd.to_datetime(historical_data_df.index[-1])
        days = (end - start).days + 1
        years = days / 365
        format = '%b %d, %Y'
        color_print(f'> {start.strftime(format)} to {end.strftime(format)}, {days:,} days = {years:.1f} years', Fore.CYAN)

        # all dates data frame (start to end)
        range = pd.date_range(start=start, end=end)
        all_dates_df = range.to_frame(index=False, name='date')

        # all dates df join with historical df
        all_dates_df = all_dates_df.set_index('date').join(historical_data_df)

        # keep only rows with close value of NaN
        missing_dates_df = all_dates_df.loc[pd.isna(all_dates_df.close)]

        # remove weekends
        missing_dates_df = missing_dates_df.loc[pd.to_datetime(missing_dates_df.index).dayofweek.isin([0,1,2,3,4])]

        # US trading calendar
        cal = stock.USTradingCalendar().holidays(start, end)
        # remove holidays
        missing_dates_df = missing_dates_df.loc[~missing_dates_df.index.isin(cal)]
        # remove non trading days
        missing_dates_df = missing_dates_df.loc[~missing_dates_df.index.isin(stock.USOtherNonTradingDates())]

        if(missing_dates_df.empty):
            color_print(f'> No Missing Dates', Fore.MAGENTA)
            return
        else:
            color_print(f'> Missing Dates', Fore.MAGENTA)
            print(f'\n{missing_dates_df}')
            return missing_dates_df

    def update(self, df):
        insertions = 0

        color_print('\n[ Integrity: Historical Data Update ]', Fore.GREEN)

        # further date range
        end = pd.to_datetime(df.index[-1])
        next = end + pd.offsets.Day(1)
        today = pd.to_datetime('today')
        # request further days
        further = (today - next).days + 1
        if further > 0:
            color_print(f'\nDate Range To Date (Inclusive)', Fore.YELLOW)
            dtf = '%b %d, %Y'
            print(f'({next.strftime(dtf)}) to ({today.strftime(dtf)}), {further} days')
            dr = pd.date_range(start=next, end=today)
            insertions += self.add(dr, df)

        if(insertions > 0):
            print(f'\n{df}')
            color_print(f'\n{insertions} Insertions', Fore.GREEN)

        return insertions

    def add(self, dates, df):
        insertions = 0

        for date in dates:
            close = self.remote.fetch_date(date)

            if close is not None:
                index = date.strftime('%Y-%m-%d')
                df.loc[index] = close
                insertions += 1
                color_print(f'\nAppend Row To Data Frame', Fore.GREEN)
                print(f'{index} {close}')

        df = df.sort_index(inplace=True)

        return insertions

class Remote:
    def __init__(self, symbol, sandbox):
        self.symbol = symbol
        self.subdomain = 'sandbox' if sandbox else 'cloud'
        self.token = iex_token(sandbox)
        self.weight = 2
        color_print(f'\n[ Remote: Init ]', Fore.GREEN)
        color_print(f'> Symbol: {symbol}, Sandbox: {sandbox}, Token: {self.token}', Fore.CYAN)

    def fetch_range(self, range):
        color_print(f'\n[ Remote: Fetch Historical Data - Range - Adjusted Close ]', Fore.GREEN)

        if(range not in iex_ranges()):
            color_print(f'Invalid Range, {range}', Fore.RED)
            return

        days = iex_trading_days(range)
        cost = days * self.weight
        color_print(f'> Range {range}, Days {days:,}, Weight {self.weight}, Message Cost ~{cost:,}', Fore.CYAN)

        # request confirmation
        confirm = utils.confirm('> Proceed with IEX request?')
        if not confirm:
            return
        
        # access to Historical Prices from more than 5 years ago is only included with paid subscriptions
        url = f'https://{self.subdomain}.iexapis.com/v1/stock/{self.symbol}/chart/{range}?token={self.token}&chartCloseOnly=true'
        color_print(f'> {url}', Fore.CYAN)

        response = requests.get(url)
        if(response.status_code != requests.codes.ok):
            color_print(response.status_code, Fore.RED)
            color_print(response.text, Fore.RED)
            return
        messages = int(response.headers["iexcloud-messages-used"])
        color_print(f'> Messages Used {messages:,}, Days {int(messages/self.weight):,}', Fore.MAGENTA)
        data = json.loads(response.text)
        df = pd.json_normalize(data)
        
        df = df.drop(columns=['volume', 'change', 'changePercent', 'changeOverTime'])
        df = df.set_index('date')

        if not df.empty:
            print(df)
            return df

    def fetch_date(self, date):
        color_print(f'\n[ Remote: Fetch Historical Data - Day - Adjusted Close ]', Fore.GREEN)

        cost = self.weight
        color_print(f'> Date {date_str}, Weight {self.weight}, Message Cost {cost}', Fore.CYAN)

        date_str = date.strftime('%Y%m%d')
        url = f'https://{self.subdomain}.iexapis.com/v1/stock/{self.symbol}/chart/date/{date_str}?token={self.token}&chartByDay=true'
        color_print(f'> {url}', Fore.CYAN)

        response = requests.get(url)
        if(response.status_code != requests.codes.ok):
            color_print(response.status_code, Fore.RED)
            color_print(response.text, Fore.RED)
            return
        messages = int(response.headers["iexcloud-messages-used"])
        color_print(f'> Messages Used {messages:,}, Days {int(messages/self.weight):,}', Fore.MAGENTA)
        data = json.loads(response.text)
        df = pd.json_normalize(data)

        if df.empty:
            color_print(f'> Date {date_str}', Fore.CYAN)
            color_print(f'> None', Fore.MAGENTA)
        else:
            close = df.loc[0]['close']
            color_print(f'> Close: {close}', Fore.MAGENTA)
            return close


def run():
    argparser = argparse.ArgumentParser(description='IEX Historical Market Data')
    argparser.add_argument("-s", "--symbol", help="stock symbol", required=True)
    argparser.add_argument("--sandbox", action='store_true', help="sanbox mode")
    args = argparser.parse_args()

    if not stock.valid_symbol(args.symbol):
        color_print(f'{symbol} is not a valid stock symbol', Fore.RED)
        exit()

    local = Local(args.symbol, args.sandbox)
    remote = Remote(args.symbol, args.sandbox)

    df = local.read()
    if df is None:
        df = remote.fetch_range('max')
        if df is not None:
            local.write(df)
    else:
        integrity = Integrity(remote)
        
        missing = integrity.missing(df)
        if missing is not None:
            # request confirmation
            confirm = utils.confirm('> Fetch missing days data?')
            if not confirm:
                return
            # integrity.add(missing.index, df)
            # local.write(df)

    # insertions = integrity.fill(df)
    #    if(insertions > 0):
    #        local.write(df)

    print('')

run()