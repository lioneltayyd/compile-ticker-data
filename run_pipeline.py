

import subprocess 

from autoprocess_ticker import collect_tickers
from configuration.config import dict_sectors, dict_equities, dict_commodities


# --------------------------------------------------------------
# Run.
# --------------------------------------------------------------

if __name__ == "__main__":
    commline_input = input(
        '''
        What ticker category do you wish to collect?
        S: Sectors.
        E: Equities. 
        Q: Quit.

        Please select: 
        '''
    )
    print(f'Answer: {commline_input}')

    if commline_input == 'S':
        collect_tickers.collect_sectors(dict_sectors)
    elif commline_input == 'E': 
        collect_tickers.collect_equities(dict_equities)
    else:
        print('Not running. Please provide the correct input.') 