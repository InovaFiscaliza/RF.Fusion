#!/usr/bin/python3.9
"""_summary_
    Periodically update macro values in Zabbix corresponding to hostid and interfaceid

    Parameters:
        none
        
    Returns:
        (json) =  { 'Host Checked': (int),
                    'Host Updated': (int),
                    'Interface Checked': (int),
                    'Interface Updated': (int),
                    'Status': (int), 
                    'Message': (str)}

        Status may be 1=valid data or 0=error in the script
        All keys except "Message" are suppresed when Status=0
        Message describe the error or warning information    
"""
# module constants
URL_ZABBIX = "http://zabbixsfi.anatel.gov.br/"
TOKEN_ZABBIX = "<PASTE SUPER ADMIN TOKEN HERE>"

HOST_DATA = ["hostids", "host", "name", "status"]
INTER_DATA = ["interfaceid","hostid","type","ip","dns","main"]
GROUP_DATA = ["groupid","name"]

# module imports
from pyzabbix import ZabbixAPI

import pandas as pd

# connect to Zabbix API
zapi = ZabbixAPI(URL_ZABBIX)

zapi.session.verify = False # Disable SSL certificate verification

zapi.timeout = 5.1 #seconds

zapi.login(api_token=TOKEN_ZABBIX)

# load host data
zbx_dict = zapi.host.get(output=HOST_DATA)

df_host = pd.DataFrame(zbx_dict)

# load interface data
zbx_dict = zapi.hostinterface.get(output=INTER_DATA)

df_inter = pd.DataFrame(zbx_dict)

# merge host and interface data
df_full = pd.merge(df_host, df_inter, on='hostid')

# labda function to set the Zabbix hostid macro value
def set_host_id(host):
    # create a list of dictionaries with the macro name and value
    macro = [{'macro' : '{$hostid}', 'value' : host['hostid']}],
    
    #udate host macro value
# TODO: See if it is possible to update the host macro value in the same call
# TODO: Include counters for output
# TODO: Make this work. Extracted with little edition from notebook
    try:
        zbx_dict = zapi.host.update(hostid=host['hostid'], )
    except Exception as e:
        print(f"----> ERROR: {host['interfaceid']} - {host['DNS']}")
        print(e)
    
none = df_full.apply(set_host_id, axis=1)

# labda function to set the Zabbix interfaceid macro valueor later manual processing
# TODO: build from the following example extracted with little edition from notebook
def set_interface_id(host): 
    try:
        zbx_dict = zapi.hostinterface.update(interfaceid=host['interfaceid'], dns=host['DNS'])
    except Exception as e:
        print(f"----> ERROR: {host['interfaceid']} - {host['DNS']}")
        print(e)

# create a reduced dataframe including only hosts that have an assigned DNS
none = df_full.apply(set_interface_id, axis=1)

# produce output
# TODO: Make this work, AI generated code
print(f'{{"Host Checked":{len(df_full)},"Host Updated":{len(df_full)},"Interface Checked":{len(df_full)},"Interface Updated":{len(df_full)},"Status":1,"Message":"none"}}')