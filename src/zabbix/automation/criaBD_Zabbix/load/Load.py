import pandas as pd

def load(id_bd, nome, host, host_disponivel_zabbix, id, ip, grupo_lista, ovpn, erro_recente, templates, dif_erro, quantidade_problemas, qtd_prolemas_total, lista_ultimo_ocorrido):

    data = {"id_bd_zabbix": id_bd, "nome": nome, "host": host, "disponivel_no_zabbix": host_disponivel_zabbix, "host_id": id, "host_ip": ip,
            "grupos": grupo_lista, "conexao_OVPN": ovpn,"erros_ultimas_24h": erro_recente,
            "templates_vinculados": templates,
            "ultimo_problema_ocorrido": dif_erro, "qtd_problemas_graves": quantidade_problemas, "qtd_prolemas_total": qtd_prolemas_total, "ultimo_ocorrido": lista_ultimo_ocorrido}

    df = pd.DataFrame(data)

    # passa tudo para string
    df = df.astype('string')

    colunas = ["id_bd_zabbix", "nome", "host", "disponivel_no_zabbix", "host_id", "host_ip", "grupos", "conexao_OVPN", "erros_ultimas_24h", "templates_vinculados", "ultimo_problema_ocorrido", "qtd_problemas_graves", "ultimo_ocorrido"]
    df = df[colunas]

    # para evitar erros, tirar onde o host está nulo
    df = df.dropna(subset=["host"])

    print(df.head())

    return df
