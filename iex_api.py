import yaml
import requests
import json

from colorama import Fore

from utils import utils

class IEX:
    def __init__(self, sandbox=True, confirm=False, verbose=False):
        self.sandbox = sandbox
        self.confirm = confirm
        self.verbose = verbose

        self.token = self.get_token(sandbox)
        self.subdomain = 'sandbox' if sandbox else 'cloud'

    def get_token(self, sandbox):
        with open('/secret/iex.yml', 'r') as file:
            config = yaml.load(file, yaml.Loader)
            return config['token']['sandbox' if sandbox else 'live']

    def get_url(self, path, query=''):
        return f'https://{self.subdomain}.iexapis.com/v1{path}?token={self.token}{query}'

    def request_quote(self, symbol):
        url = self.get_url(f'/stock/{symbol}/quote')
        return self.handle_response(requests.get(url))

    def request_historical_range(self, symbol, range, adjusted=False):
        weight = 2 if adjusted else 10

        # check range validity
        if(range not in self.valid_ranges()):
            self.message(f'Invalid Range, {range}', Fore.RED)
            return

        days = self.trading_days_in_range(range)
        self.message(f'> Range {range}, Days {days:,}, Weight {weight}, Message Cost ~{(days * weight):,}', Fore.CYAN)

        # request confirmation
        if self.confirm:
            if not utils.confirm('> Proceed with IEX request?'):
                return

        url = self.get_url(f'/stock/{symbol}/chart/{range}')
        if adjusted:
            url += '&chartCloseOnly=true'
        self.message(f'> {url}', Fore.CYAN)

        return self.handle_historical_result(requests.get(url), weight)

    def request_historical_date(self, symbol, date):
        weight = 2 # weight is 2 not 10

        self.message(f'> Date {date}, Weight {weight}, Message Cost {weight}', Fore.CYAN)

        # both adjusted and unadjusted close prices, no need to use chartCloseOnly
        url = self.get_url(f'/stock/{symbol}/chart/date/{date}', '&chartByDay=true')
        self.message(f'> {url}', Fore.CYAN)

        return self.handle_historical_result(requests.get(url), weight)

    def request_symbols(self, region):
        weight = 100

        self.message(f'> Region {region}, Weight {weight}, Message Cost {weight}', Fore.CYAN)

        url = self.get_url(f'/ref-data/region/{region}/symbols')
        self.message(f'> {url}', Fore.CYAN)

        return self.handle_response(requests.get(url))

    def handle_response(self, response):
        if(response.status_code != requests.codes.ok):
            return { "code": response.status_code }

        # show message cost
        self.message(f'> Messages Used {int(response.headers["iexcloud-messages-used"]):,}', Fore.MAGENTA)

        return json.loads(response.text)

    def handle_historical_result(self, response, weight):
        # show if error
        if(response.status_code != requests.codes.ok):
            self.message(response.status_code, Fore.RED)
            self.message(response.text, Fore.RED)
            return

        # show message cost
        messages = int(response.headers["iexcloud-messages-used"])
        self.message(f'> Messages Used {messages:,}, Days {int(messages/weight):,}', Fore.MAGENTA)

        return json.loads(response.text)

    def valid_ranges(self):
        return ['max','5y','2y','1y','6m','3m','1m','5d']

    def trading_days_in_range(self, range, paid=False):
        annual_holidays = 9
        annual_trading_days = 52 * 5 - annual_holidays

        days = {
            'max':  annual_trading_days * (15 if paid else 5), # access to prices from more than 5 years ago is only included with paid subscriptions
            '5y':   annual_trading_days * 5,
            '2y':   annual_trading_days * 2,
            '1y':   annual_trading_days,
            '6m':   annual_trading_days / 2,
            '3m':   annual_trading_days / 4,
            '1m':   annual_trading_days / 12,
            '5d':   5
        }
        return days[range]

    def message(self, text, color):
        if self.verbose:
            utils.cprint(text, color)