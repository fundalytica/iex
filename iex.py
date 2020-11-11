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