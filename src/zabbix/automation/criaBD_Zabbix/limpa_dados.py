def limpa_dados_zabbix(nome):

    # limpeza de nome caso seja rfeye
    if "ovpn" in nome:
    
        nome.replace("_ovpn", "")
    
    # outros casos
    else:

        if "_" in nome:
            nome = nome.replace("_", "/")

        if "-" in nome:
            nome = nome.replace("-", "/")

    return nome
