import sys

from datetime import datetime

from iex import iex_quote

from utils import utils, stock

def run():
    response = {}

    if not utils.valid_arg_count(2):
        response["error"] = "No Symbol"
        print(response)
        exit()

    symbol = sys.argv[1]

    if not stock.valid_symbol(symbol):
        response["error"] = "Invalid Symbol"
        print(response)
        exit()

    iex_response = iex_quote(symbol)

    # check if the market is open
    isUSMarketOpen = (iex_response["isUSMarketOpen"] == True)
    # latestTime can be date without a time or time without a date
    latestTime = iex_response["latestTime"]

    # iex date format, January 1, 2020 (Month as locale’s full name, Day of the month, Year with century)
    iex_date_fmt = "%B %d, %Y"
    # iex time format, January 1, 2020 (12 Hour, Minute, Second, AM/PM)
    iex_time_fmt = "%I:%M:%S %p"

    if isUSMarketOpen:
        # time only
        try:
            # raise exception if iex time does not match iex time format
            datetime.strptime(latestTime, iex_time_fmt)

            # latestTime contains only time and no date, get date from latestUpdate epoch timestamp
            latestUpdate = int(iex_response["latestUpdate"] / 1000)
            response["date"] = datetime.fromtimestamp(latestUpdate).strftime(iex_date_fmt)
            response["time"] = latestTime
        except ValueError as error:
            print(error)
            exit()

    if not isUSMarketOpen:
        # date only
        try:
            # raise exception if iex time does not match iex date format
            # day of month leading zero is optional when used with strptime (January 1 and January 01 both acceptable)
            datetime.strptime(latestTime, iex_date_fmt)

            # latesTime contains only date and no time
            response["date"] = latestTime
        except ValueError as error:
            print(error)
            exit()

    # create and print response object, include message cost
    response["symbol"] = symbol
    response["price"] = iex_response["latestPrice"]
    response["change"] = iex_response["changePercent"]
    response["market"] = "open" if isUSMarketOpen else "closed"
    response["messages"] = int(iex_response["iexcloud-messages-used"])
    print(response)

run()
