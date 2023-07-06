from auxiliar.autenticador import SHAREPOINT_URL, SHAREPOINT_SITE, SHAREPOINT_LIST_ENLACESFIX, SHAREPOINT_LIST_ESTSERV, USERNAME, PASSWORD
from auxiliar.dados_sharepoint import authenticate, get_sp_list, download_list_items
import pandas as pd

def extract_EstServ_Enlaces():

    es_site = authenticate(SHAREPOINT_URL, SHAREPOINT_SITE, USERNAME, PASSWORD)

    es_list = get_sp_list(es_site, SHAREPOINT_LIST_ESTSERV)

    es_items_all = download_list_items(es_list)

    # df = pd.read_json(sp_items_all, orient ='index')
    dados_EstServ = pd.DataFrame.from_records(es_items_all)

    en_site = authenticate(SHAREPOINT_URL, SHAREPOINT_SITE, USERNAME, PASSWORD)

    en_list = get_sp_list(en_site, SHAREPOINT_LIST_ENLACESFIX)

    en_items_all = download_list_items(en_list)

    # df = pd.read_json(sp_items_all, orient ='index')
    dados_EnlacesFix = pd.DataFrame.from_records(en_items_all)

    return dados_EstServ, dados_EnlacesFix
