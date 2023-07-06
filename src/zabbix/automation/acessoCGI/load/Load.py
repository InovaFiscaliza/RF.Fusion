from auxiliar.arquivos_locais import MOSTRAR_PRINTS
import pandas as pd

def load_df_cgis_BD(list_ids, list_nome, list_lat, list_lon, list_free_mem, list_ip, list_mac, list_vpn, list_tuns, list_apps):

    data = {"id_bd_cgis": list_ids, "nome": list_nome, "lat": list_lat, "lon": list_lon, "free_mem": list_free_mem,
            "ip": list_ip, "mac": list_mac, "vpn": list_vpn, "tuns": list_tuns, "apps": list_apps}

    df = pd.DataFrame(data)

    # passa tudo para string
    df = df.astype('string')

    # para evitar erros, tirar onde o host está nulo
    df = df.dropna(subset=["nome"])

    if MOSTRAR_PRINTS == 1:
        print("Dataframe que será salvo!")
        print(df.head())
        print("--------------------------------------\n\n")

    return df