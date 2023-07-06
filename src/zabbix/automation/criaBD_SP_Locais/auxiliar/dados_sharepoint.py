# --------------------------------------------------
# Script de criação/atualização de BD
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_PyZabbix_FISF3
# --------------------------------------------------

from shareplum.site import Version
from shareplum import Site, Office365
from auxiliar.autenticador import SHAREPOINT_SITE, SHAREPOINT_URL, USERNAME, PASSWORD
import sys

def authenticate(sp_url, sp_site, user_name, password):
    """
    Takes a SharePoint url, site url, username and password to access the SharePoint site.
    Returns a SharePoint Site instance if passing the authentication, returns None otherwise.
    """
    site = None
    try:
        authcookie = Office365(SHAREPOINT_URL, username=USERNAME, password=PASSWORD).GetCookies()
        site = Site(SHAREPOINT_SITE, version=Version.v365, authcookie=authcookie)
    except:
        # We should log the specific type of error occurred.
        print('Failed to connect to SP site: {}'.format(sys.exc_info()[1]))
    return site



def get_sp_list(sp_site, sp_list_name):
    """
    Takes a SharePoint Site instance and invoke the "List" method of the instance.
    Returns a SharePoint List instance.
    """
    sp_list = None
    try:
        sp_list = sp_site.List(sp_list_name)
    except:
        # We should log the specific type of error occurred.
        print('Failed to connect to SP list: {}'.format(sys.exc_info()[1]))
    return sp_list


def download_list_items(sp_list, view_name=None, fields=None, query=None, row_limit=0):
    """
    Takes a SharePoint List instance, view_name, fields, query, and row_limit.
    The rowlimit defaulted to 0 (unlimited)
    Returns a list of dictionaries if the call succeeds; return a None object otherwise.
    """
    sp_list_items = None
    try:
        sp_list_items = sp_list.GetListItems(view_name=view_name, fields=fields, query=query, row_limit=row_limit)
    except:
        # We should log the specific type of error occurred.
        print('Failed to download list items {}'.format(sys.exc_info()[1]))
        raise SystemExit('Failed to download list items {}'.format(sys.exc_info()[1]))
    return sp_list_items
