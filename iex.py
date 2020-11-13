import yaml
import requests
import json

def iex_token(sandbox=False):
    with open('/secret/iex.yml', 'r') as file:
        config = yaml.load(file, yaml.Loader)
        return config['token']['sandbox' if sandbox else 'live']

def iex_quote(symbol, sandbox=False):
    token = iex_token(sandbox)

    subdomain = 'sandbox' if sandbox else 'cloud'
    url = f'https://{subdomain}.iexapis.com/v1/stock/{symbol}/quote?token={token}'

    response = requests.get(url)
    if(response.status_code != requests.codes.ok):
        return { "code": response.status_code }

    data = json.loads(response.text)
    data['iexcloud-messages-used'] = response.headers['iexcloud-messages-used']
    return data

def iex_ranges():
    return ['max','5y','2y','1y','6m','3m','1m','5d']

def iex_trading_days(iex_range):
    annual_holidays = 9
    annual_trading_days = 52 * 5 - annual_holidays

    days = {
        'max':  annual_trading_days * 15,
        '5y':   annual_trading_days * 5,
        '2y':   annual_trading_days * 2,
        '1y':   annual_trading_days,
        '6m':   annual_trading_days / 2,
        '3m':   annual_trading_days / 4,
        '1m':   annual_trading_days / 12,
        '5d':   5
    }

    return days[iex_range]