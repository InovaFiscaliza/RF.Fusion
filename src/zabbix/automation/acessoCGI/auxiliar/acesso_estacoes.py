# Importando os módulos do Clayton Ricardo, GR03
import logging

import urllib3
import sys
import re
import json
from auxiliar.arquivos_locais import MOSTRAR_PRINTS

from auxiliar.print_json import json_print

def define_nome(url, AUTH_DEFAULT):

    try:

        http = urllib3.PoolManager(retries=False, timeout=3)
        url = url + '/cgi-bin/unitname.cgi'
        headers = urllib3.make_headers(basic_auth=AUTH_DEFAULT)
        response = http.request('GET', url, headers=headers)

        assert str(response.status) == '200'

        if MOSTRAR_PRINTS == 1:
            print("URL: " + url + "\nResposta: " + str(response.status))

        logging.debug("URL: " + url + "\nResposta: " + str(response.status))

        return str(response.data)


    except:

        if MOSTRAR_PRINTS == 1:

            print("\n-------------------\nErro!\n")
            print("Ocorrido: " + str(sys.exc_info()[0]) + "\n-------------------\n")

        logging.debug("Ocorrido: " + str(sys.exc_info()[0]))

        return "False"


def gpsstatus(url, AUTH_DEFAULT):

    try:

        #statuscgi = json.loads( self.getCGI('status.cgi') )
        http = urllib3.PoolManager(retries=False)
        url = url + '/cgi-bin/status.cgi'
        headers = urllib3.make_headers(basic_auth=AUTH_DEFAULT)
        response = http.request('GET', url, headers=headers)

        assert str(response.status) == '200'

        resposta = json.loads(response.data)

        print(resposta)

        m = re.search(r'\d+ \d+ (\d+) \d+ (\d+) (\-?\d+)(\d{6}) (\-?\d+)(\d{6}) .*', resposta["GPS"])

        print(resposta["GPS"])

        #fix = m.group(1)
        #sat = m.group(2)

        lat = float( m.group(3) + '.' + m.group(4) )
        lon = float( m.group(5) + '.' + m.group(6) )

        lat = round(lat, 4)
        lon = round(lon, 4)

        free_mem = resposta["FreeMemory"]
        ip = resposta["IP"]

        return lat, lon, free_mem, ip

    except:

        return "False", "False", "False", "False"

def address(url, AUTH_DEFAULT):
    
    # addr = self.getCGI('ifconfig.cgi').decode('utf-8').strip()
    
    http = urllib3.PoolManager(retries=False)
    url = url + '/cgi-bin/ifconfig.cgi'
    headers = urllib3.make_headers(basic_auth=AUTH_DEFAULT)
    response = http.request('GET', url, headers=headers)
    
    addr = response.data.decode('utf-8').strip()
    
    try:
        m = re.search(r'.*HWaddr ([\w|:]+).*',addr)
        
        mac = m.group(1)

        m = re.search(r'.*addr:([\d|\.]+) +P-t-P.*',addr)

        vpn = m.group(1)
        
        
    except:
        
        return "False", "False", "False"

    tuns = len(re.findall(r'tun.', addr))
    
    return mac, vpn, tuns

def apps(url, AUTH_DEFAULT):
    
    try:
    
        #apps = json.loads( self.getCGI('apps_list.cgi') )
    
        http = urllib3.PoolManager()
        url = url + '/cgi-bin/apps_list.cgi'
        headers = urllib3.make_headers(basic_auth=AUTH_DEFAULT)
        response = http.request('GET', url, headers=headers)
    
        apps = json.loads(response.data)
        
        lista_apps_nome = []
        lista_apps_running = []
        lista_apps_version = []

        print(apps)

        apps_list = (apps['apps'])
        
        return apps_list
        
    except:
        
        return "False"
