#!/usr/bin/python3.9
"""_summary_
    Change between IP and DNS address for the Zabbix host if the host fails to respond to the current DNS or IP address.

    Parameters:
        <hostid> single string with hostid to be used in the Zabbix API to check the recent host activity
        <interfaceid> single string interfaceid to be used in the Zabbix API to change the IP/DNS address
        <retries> single string with the number of retries to be used in the Zabbix API to check the how many times the host failed to respond

    Returns:
        (json) =  { 'Failed attempts': (int),
                    'Change': (int),
                    'Current': (str),
                    'Status': (int), 
                    'Message': (str)}

        'Change' may be 0 for no change or 1 for change
        Status may be 1=valid data or 0=error in the script
        All keys except "Message" are suppresed when Status=0
        Message describe the error or warning information
    
"""