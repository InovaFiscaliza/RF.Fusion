import sys,os

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
sys.path.append(CONFIG_PATH)

from rfpye.parser import parse_bin

# Import libraries for file processing
import time
import random

from geopy.geocoders import Nominatim  #  Processing of geographic data
from geopy.exc import GeocoderTimedOut

# Import modules for file processing
import config as k
from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM
import shared as sh
import os

import signal
import inspect
from datetime import datetime

def main():
    filename = '/mnt/reposfi/2025/AL/2706703/58/rfeye002106_250109_T113352.bin'
    try:
        bin_data = parse_bin(filename)
        print('Processamento completo')
    except FileNotFoundError:
        print('Arquivo nao encontrado.')
    except Exception as e:
        print(f"Error parsing file {filename}", "PARSE", e)
        
if __name__ == "__main__":
    main()