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
                    'status': (int), 
                    'message': (str)}

        Status may be 1=valid data or 0=error in the script
        All keys except "Message" are suppresed when Status=0
        Message describe the error or warning information    
"""
# module imports
from pyzabbix import ZabbixAPI
import pandas as pd
import json

import z_shared as zsh
import secrets as s
# module constants
URL_ZABBIX = "http://zabbixsfi.anatel.gov.br/" 
TOKEN_ZABBIX = "<zabbix token>"

HOST_DATA = ["hostids", "host", "name", "status"]
INTER_DATA = ["interfaceid","hostid","type","ip","dns","main"]
GROUP_DATA = ["groupid","name"]

wm = zsh.warning_msg()

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

stats = {'host_checked':len(df_full),'id_updated':0,'interface_updated':0,'errors':0,'status':1,'message':'none'}

# labda function to set the Zabbix hostid macro value
def set_host_id(host):

    try:
        zapi.usermacro.create(hostid=host['hostid'], macro="{$HOST_ID}", value=host.hostid)
        stats['id_updated'] += 1
    except Exception as e:
        if e.error['code'] == -32602:
            pass
        else:
            wm.compose_warning(e.args[0])
            stats['errors'] += 1
            stats['status'] = 0
            pass
        
    try:
        zapi.usermacro.create(hostid=host['hostid'], macro="{$INTERFACE_ID}", value=host.interfaceid)
        stats['interface_updated'] += 1
    except Exception as e:
        if e.error['code'] == -32602:
            pass
        else:
            wm.compose_warning(e.args[0])
            stats['errors'] += 1
            stats['status'] = 0
            pass
    
none = df_full.apply(set_host_id, axis=1)

# produce output
if stats['status'] == 0:
    stats['message'] = wm.warning_msg

print(json.dumps(stats))