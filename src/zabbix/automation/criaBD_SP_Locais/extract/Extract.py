import time
import pandas as pd
from auxiliar.autenticador import SHAREPOINT_URL, SHAREPOINT_SITE, SHAREPOINT_LIST, USERNAME, PASSWORD
from auxiliar.dados_sharepoint import authenticate, get_sp_list, download_list_items


def extract_Locais():

    sp_site = authenticate(SHAREPOINT_URL, SHAREPOINT_SITE, USERNAME, PASSWORD)

    sp_list = get_sp_list(sp_site, SHAREPOINT_LIST)

    sp_items_all = download_list_items(sp_list)

    # df = pd.read_json(sp_items_all, orient ='index')
    dados_Locais = pd.DataFrame.from_records(sp_items_all)

    time.sleep(2)

    return dados_Locais