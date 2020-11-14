import sys
import argparse

import os
import pathlib

import requests
import json

import numpy as np
import pandas as pd

from colorama import Fore

from utils import utils, stock

from iex import IEX

class Local:
    def __init__(self, symbol, sandbox, csv=True):
        self.csv = csv

        file_path = pathlib.Path(__file__).parent.absolute()
        file_name = os.path.splitext(__file__)[0]

        self.path = f"{file_path}/{file_name}/{'sandbox' if sandbox else 'cloud'}/"
        self.path += symbol
        self.path = f'{self.path}.{"csv" if csv else "pkl"}'

        utils.cprint(f'\n[ Local: Init ]', Fore.GREEN)
        utils.cprint(f'> Symbol: {symbol}, Path: {self.path}', Fore.CYAN)

    def read(self):
        utils.cprint('\n[ Local: Read Historical Data ]', Fore.GREEN)

        try:
            if(self.csv):
                df = pd.read_csv(self.path, index_col='date')
            else:
                df = pd.read_pickle(self.path)

            # sort on load
            df.sort_index(inplace=True)

            print(f'\n{df}')
        except IOError as e:
            utils.cprint('> File Not Found', Fore.RED)
            return None
        except pd.errors.EmptyDataError as e:
            utils.cprint('> Empty Data', Fore.RED)
            return None

        return df

    def write(self, df):
        utils.cprint('\n[ Local: Write Historical Data ]', Fore.GREEN)

        # sort on save
        df.sort_index(inplace=True)

        if(self.csv):
            df.to_csv(self.path)
        else:
            df.to_pickle(self.path)
        
        utils.cprint('> OK', Fore.MAGENTA)

class Integrity:
    def __init__(self, local, remote, confirm):
        self.local = local
        self.remote = remote
        self.confirm = confirm

    def missing_dates(self, df):
        utils.cprint(f'\n[ Integrity: Missing Dates ]', Fore.GREEN)

        # start and end dates in historical data frame
        start = pd.to_datetime(df.index[0])
        end = pd.to_datetime(df.index[-1])
        days = (end - start).days + 1
        years = days / 365
        format = '%b %d, %Y'
        utils.cprint(f'> {start.strftime(format)} to {end.strftime(format)}, {days:,} days = {years:.1f} years', Fore.CYAN)

        # all dates data frame (start to end)
        range = pd.date_range(start=start, end=end)
        all_dates_df = range.to_frame(index=False, name='date')

        # all dates df join with historical df
        all_dates_df = all_dates_df.set_index('date').join(df)

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
            utils.cprint(f'> No Missing Dates', Fore.MAGENTA)
            return
        else:
            utils.cprint(f'> Missing Dates', Fore.MAGENTA)
            print(f'\n{missing_dates_df}')
            return missing_dates_df.index

    def additional_dates(self, df):
        utils.cprint(f'\n[ Integrity: Additional Dates ]', Fore.GREEN)

        today = pd.to_datetime('today')

        end = pd.to_datetime(df.index[-1])
        next = end + pd.offsets.Day(1)

        # further days
        days = (today - next).days + 1
        if days > 0:
            format = '%b %d, %Y'
            utils.cprint(f'> {next.strftime(format)} to {today.strftime(format)}, {days:,} days', Fore.CYAN)
            return pd.date_range(start=next, end=today)

    def insert(self, dates, df):
        if dates is None:
            return

        # request confirmation
        if self.confirm:
            if not utils.confirm(f'> Insert {dates.shape[0]:,} entries?'):
                return

        insertions = 0

        for date in dates:
            close = self.remote.fetch_date(date)
            if close is not None:
                index = date.strftime('%Y-%m-%d')
                df.loc[index] = close
                insertions += 1
                utils.cprint(f'\n[ Integrity: Insertion ]', Fore.GREEN)
                utils.cprint(f'> {index} {close}', Fore.MAGENTA)
                # save on every date because of messages used
                self.local.write(df)

        utils.cprint(f'\n[ Integrity: Insert Complete ]', Fore.GREEN)
        utils.cprint(f'> {insertions} Insertions', Fore.MAGENTA)

class Remote:
    def __init__(self, symbol, sandbox, confirm):
        self.symbol = symbol        
        self.iex = IEX(sandbox, confirm=confirm, verbose=True)
        self.adjusted = True # adjusted for splits not for dividends
        self.key = 'close' if self.adjusted else 'uClose'

        utils.cprint(f'\n[ Remote: Init ]', Fore.GREEN)
        utils.cprint(f'> Symbol: {symbol}, Sandbox: {sandbox}', Fore.CYAN)

    def fetch_range(self, range):
        close_string = 'Adjusted' if self.adjusted else 'Unadjusted'
        utils.cprint(f'\n[ Remote: Fetch Historical Data - Date Range - {close_string} Close ]', Fore.GREEN)

        # iex request
        data = self.iex.request_historical_range(self.symbol, range, self.adjusted)
        if data is not None:
            df = pd.json_normalize(data)

            df = df.drop(df.columns.difference(['date', self.key]), axis=1)

            df = df.set_index('date')

            if not df.empty:
                print(df)
                return df

    def fetch_date(self, date):
        close_string = 'Adjusted' if self.adjusted else 'Unadjusted'
        utils.cprint(f'\n[ Remote: Fetch Historical Data - Single Date - {close_string} Close ]', Fore.GREEN)
        
        date = date.strftime('%Y%m%d')

        # iex request
        data = self.iex.request_historical_date(self.symbol, date)
        if data is not None:
            df = pd.json_normalize(data)

            if df.empty:
                utils.cprint(f'> Date {date}', Fore.CYAN)
                utils.cprint(f'> None', Fore.MAGENTA)
            else:
                close = df.loc[0][self.key]
                utils.cprint(f'> Close: {close}', Fore.MAGENTA)
                return close

def run():
    argparser = argparse.ArgumentParser(description='IEX Historical Market Data')
    argparser.add_argument("-s", "--symbol", help="stock symbol", required=True)
    argparser.add_argument("--sandbox", action='store_true', help="sanbox mode")
    argparser.add_argument("--confirm", action='store_true', help="confirm mode")
    args = argparser.parse_args()

    if not stock.valid_symbol(args.symbol):
        utils.cprint(f'{symbol} is not a valid stock symbol', Fore.RED)
        exit()

    local = Local(args.symbol, args.sandbox)
    remote = Remote(args.symbol, args.sandbox, args.confirm)

    df = local.read()
    if df is None:
        df = remote.fetch_range('max')
        if df is not None:
            local.write(df)
    else:
        integrity = Integrity(local, remote, args.confirm)

        missing = integrity.missing_dates(df)
        integrity.insert(missing, df)

        additional = integrity.additional_dates(df)
        integrity.insert(additional, df)

    print('')

def test():
    iex = IEX(sandbox=False, confirm=False, verbose=True)
    print(pd.json_normalize(iex.request_historical_date('SPY', '20170201')))
    print(pd.json_normalize(iex.request_historical_date('TSLA', '20200303')))

# TODO set max to 5y do not pay for anything
try:
    run()
    # test()
except KeyboardInterrupt:
    print('\n')

# TODO fetch close if you can fetch dividends and splits, that would help skip the validity check

# TODO add backwards data as well as forward data

# TODO rely more on yfinance since it is free and has a library

# TODO: on additions, get the last df date, make a request to see if it is valid then there are no splits or dividends
# call the method: valid, Integrity: Valid (no splits or dividends since . . .)

# TODO: schedule updates
