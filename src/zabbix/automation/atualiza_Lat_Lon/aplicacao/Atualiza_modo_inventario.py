import time
import pandas as pd
import logging

from auxiliar.arquivos_locais import UPDATE_MODO_INVENTARIO, MODO_DEBUG, TIPO_INVENTARIO

def modo_inventario(logging, dados_Zabbix, zapi):

    if UPDATE_MODO_INVENTARIO == 1:

        if MODO_DEBUG == 1:
            print("Atualizando modo de inventario de todos os hosts!")

        logging.debug("Atualizando modo de inventario de todos os hosts!")

        for index_dados_Zabbix, row_dados_Zabbix in dados_Zabbix.iterrows():

            host_id = str(row_dados_Zabbix["host_id"])

            logging.debug("Iterando por " + host_id)

            try:

                result = zapi.host.update({
                    "hostid": host_id,
                    "inventory_mode": str(TIPO_INVENTARIO)

                })

                if MODO_DEBUG == 1:
                    print(result)

            except:

                if MODO_DEBUG == 1:
                    print("Erro ao atualizar inventory type - " + str(host_id))

            finally:

                time.sleep(1)

    if MODO_DEBUG == 1:
        print("Hosts atualizados para modo " + str(TIPO_INVENTARIO))
