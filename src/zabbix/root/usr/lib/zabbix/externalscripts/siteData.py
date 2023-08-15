#!/usr/bin/python3.9
"""_summary_
Atualiza informações de tags, grupos e inventário do host à partir das informações de coordenadas cadastradas no inventário

Mantém tabela de locais no servidor zabbix

Trigger:
    Mudança de localização sqrt(lat_diff^2 + long_diff^2) maior que determinado valor (e.g. 0.001)
    Calculado quando diff_lat ou diff_long > 0.0007
    
Entrada:
    Identificação do host (zabbix hostid)    

Saída:
    String JSON resultado da atualização (sucesso ou erros). 
    """

