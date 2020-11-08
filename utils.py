import sys
import re

# generic utils

def valid_arg_count(count = 2):
    if len(sys.argv) != count:
        return False

    return True

# stock utils

def valid_symbol(symbol):
    STOCK_SYMBOL_PATTERN = "[A-Z]{1,4}((\.){1}[A|B]{1}){0,1}$"

    pattern = re.compile(STOCK_SYMBOL_PATTERN)
    match = pattern.match(symbol)

    if match is None:
        return False

    return True