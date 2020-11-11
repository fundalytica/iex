import sys
import argparse

import os
import pathlib

import requests
import json

from utils import utils, stock

import numpy as np
import pandas as pd

from colorama import Fore, Style

from iex import iex_token

def color_print(text, color):
    print(f'{color}{text}{Style.RESET_ALL}')

# class to handle local data
class Local:
    def __init__(self, symbol, sandbox, csv=True):
        self.csv = csv

        file_path = pathlib.Path(__file__).parent.absolute()
        file_name = os.path.splitext(__file__)[0]

        self.path = f"{file_path}/{file_name}/{'sandbox' if sandbox else 'cloud'}/"
        self.path += symbol
        self.path = f'{self.path}.{"csv" if csv else "pkl"}'

        color_print(f'\n[ Local - Symbol: {symbol}, Path: {self.path} ]', Fore.GREEN)

    def read(self):
        color_print('\n[ Read Historical Data ]', Fore.CYAN)

        try:
            if(self.csv):
                df = pd.read_csv(self.path, index_col='date')
            else:
                df = pd.read_pickle(self.path)

            print(f'\n{df}')
        except IOError as e:
            color_print('x', Fore.RED)
            return None

        return df

    def write(self, df):
        color_print('\n[ Write Historical Data ]', Fore.CYAN)

        if(self.csv):
            df.to_csv(self.path)
        else:
            df.to_pickle(self.path)

        color_print('✓', Fore.GREEN)

class Remote:
    def __init__(self, symbol, sandbox):
        self.symbol = symbol
        self.subdomain = 'sandbox' if sandbox else 'cloud'
        self.token = iex_token(sandbox)
        color_print(f'\n[ Remote - Symbol: {symbol}, Sandbox: {sandbox}, Token: {self.token} ]', Fore.GREEN)            

    def range(self, range):
        color_print(f'\n[ Fetch Historical Data (Adjusted Close) ]', Fore.CYAN)

        weight = 2
        # valid ranges: max (up to 15y), 5y, 2y, 1y, 6m, 3m, 1m, 5d, ytd
        # access to Historical Prices from more than 5 years ago is only included with paid subscriptions
        messages = { 'max': 15 * 365, '5y': 5 * 365, '2y': 2 * 365, '1y': 1 * 365, '6m': 6 * 30, '3m': 3 * 30, '1m': 30, '5d': 5}

        if(range not in messages):
            color_print(f'Invalid Range, {range}', Fore.RED)
            return

        color_print(f'Range {range}, Weight {weight}, Messages {messages[range] * weight}', Fore.YELLOW)

        url = f'https://{self.subdomain}.iexapis.com/v1/stock/{self.symbol}/chart/{range}?token={self.token}&chartCloseOnly=true'
        color_print(f'{url}', Fore.CYAN)

        response = requests.get(url)
        if(response.status_code != requests.codes.ok):
            color_print(response.status_code, Fore.RED)
            color_print(response.text, Fore.RED)
            return

        data = json.loads(response.text)
        df = pd.json_normalize(data)

        df = df.drop(columns=['volume', 'change', 'changePercent', 'changeOverTime'])
        df = df.set_index('date')

        if(not df.empty):
            color_print('\nHistorical Data', Fore.YELLOW)
            print(df)
            return df

    def date(self, date):
        color_print(f'\n[ Fetch Historical Data Day (Adjusted Close) ]', Fore.CYAN)

        weight = 2
        messages = weight

        date_str = date.strftime('%Y%m%d')
        url = f'https://{self.subdomain}.iexapis.com/v1/stock/{self.symbol}/chart/date/{date_str}?token={self.token}&chartByDay=true'
        color_print(f'{url}', Fore.CYAN)

        response = requests.get(url)
        if(response.status_code != requests.codes.ok):
            color_print(response.status_code, Fore.RED)
            color_print(response.text, Fore.RED)
            return

        data = json.loads(response.text)
        df = pd.json_normalize(data)

        if df.empty:
            color_print(f'\nDate {date_str}', Fore.YELLOW)
            print('None')
        else:
            color_print(f'\nDate {date_str}, Weight {weight}, Messages {messages}', Fore.YELLOW)
            close = df.loc[0]['close']
            print(f'Close: {close}')
            return close

class Integrity:
    def __init__(self, remote):
        self.remote = remote

    def fill(self, df):
        insertions = 0

        color_print('\n[ Historical Data Fill ]', Fore.CYAN)

        # request missing days
        missing = self.missing(df)
        if len(missing.index) > 0:
            insertions += self.add(missing.index, df)

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
            close = self.remote.date(date)

            if close is not None:
                index = date.strftime('%Y-%m-%d')
                df.loc[index] = close
                insertions += 1
                color_print(f'\nAppend Row To Data Frame', Fore.GREEN)
                print(f'{index} {close}')

        df = df.sort_index(inplace=True)

        return insertions

    def missing(self, historical_data_df):
        # date range
        start = pd.to_datetime(historical_data_df.index[0])
        end = pd.to_datetime(historical_data_df.index[-1])
        days = (end - start).days + 1
        years = days / 365
        dtf = '%b %d, %Y'
        color_print(f'\nDate Range (Inclusive)', Fore.YELLOW)
        print(f'({start.strftime(dtf)}) to ({end.strftime(dtf)}), {days} days = {round(years,2)} years')

        # all dates range data frame
        dr = pd.date_range(start=start, end=end)
        df = dr.to_frame(index=False, name='date')

        # joined data frame, all dates join historical dates on date
        df = df.set_index('date').join(historical_data_df)

        # keep only rows with close value of NaN
        df = df.loc[pd.isna(df.close)]

        # add dayofweek column for debugging
        # df = df.assign(day=pd.to_datetime(df.index).dayofweek)
        # print(df)

        # remove rows that are weekends
        df = df.loc[pd.to_datetime(df.index).dayofweek.isin([0,1,2,3,4])]

        # get US trading calendar
        cal = stock.USTradingCalendar().holidays(start, end)

        # remove rows that are non trading days because of holidays
        df = df.loc[~df.index.isin(cal)]
        # US Markets Closed December 5, 2018: National Day of Mourning for George H.W. Bush
        df = df[df.index != '2018-12-05']

        if(df.empty):
            color_print(f'\nNo Missing Dates ✓', Fore.GREEN)
        else:
            print(f'\n{df}')
            print(df.shape)
            color_print(f'\nMissing Dates 𝗑', Fore.RED)

        return df

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
        df = remote.range('max')

        if df is not None:
            local.write(df)
    #else:
    #    integrity = Integrity(remote)
    #    insertions = integrity.fill(df)
    #    if(insertions > 0):
    #        local.write(df)

    color_print('\n- - - - -', Fore.GREEN)

run()

# https://iexcloud.io/docs/api/#historical-prices

# Range max = All available data up to 15 years
# Adjusted close only
# 2 per symbol per time interval returned (Excluding 1d)
# use chartCloseOnly param
# Example: If you query for SPY 5 day, it will return 5 days of prices for SPY for a total of 10
