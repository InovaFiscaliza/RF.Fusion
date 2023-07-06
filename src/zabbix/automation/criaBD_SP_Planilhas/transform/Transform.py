from auxiliar.limpa_dados import limpa_dados_estserv, limpa_registros_SP

def transform_EstServ(logging, dados_EstServ):


    dados_EstServ['ID de rede'] = dados_EstServ['ID de rede'].fillna('ID de rede vazio')
    dados_EstServ['ID de rede'] = dados_EstServ['ID de rede'].astype(str)

    # adiciona coluna de ID
    ids = []

    for index, row in dados_EstServ.iterrows():

        input_hash = str(row["ID de rede"])

        resultado_id = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

        ids.append(resultado_id)

    dados_EstServ["ID_BD_EstServ"] = ids

    logging.debug(dados_EstServ.head())

    # passa coluna para string
    # dados_EstServ["ID de rede"] = dados_EstServ["ID de rede"].astype(str)
    dados_EstServ = dados_EstServ.astype(str)

    # atualiza strings para lowercase
    dados_EstServ["ID de rede"] = dados_EstServ["ID de rede"].apply(lambda palavra: palavra.lower())

    # limpa casos específicos
    dados_EstServ["ID de rede"] = dados_EstServ["ID de rede"].apply(limpa_dados_estserv)

    dados_EstServ.rename({'ID_BD_EstServ': 'id_bd_estserv',
                          'Local:UF': 'local_uf',
                          'Local:Município': 'local_municipio',
                          "Local": "local_nome",
                          "ID de rede": "id_de_rede",
                          "Detentor": "detentor",
                          "Tipo de Estação": "tipo_de_estacao",
                          "Marca": "marca",
                          "Modelo": "modelo",
                          "Patrimônio": "patrimonio",
                          "Nº Série": "n_serie",
                          "Situação do Equipamento": "situacao_equipamento",
                          "Versão FW/SW": "versao_fw_sw",
                          "Altura e configuração de antenas": "altura_e_configuracao_de_antenas",
                          "IP OpenVPN": "ip_openvpn",
                          "Data Chave OpenVPN": "data_chave_openvpn",
                          "ID OpenVPN": "id_openvpn",
                          "Observações": "observacoes",
                          "Pendência": "pendencia",
                          "Ações a serem adotadas": "acoes_a_serem_adotadas",
                          "Responsável na Anatel pela ação": "responsavel_na_anatel_pela_acao",
                          "Modificado": "modificado",
                          "Modificado por": "modificado_por",
                          "Versão": "versao",
                          "Instrumento Fiscaliza": "instrumento_fiscaliza",  # colunas novas
                          "Local:Referência_original": "local_ref_original",
                          "Local:Latitude": "local_lat",
                          "Local:Longitude": "local_lon",
                          "Local:Referência": "local_ref",
                          "Status de Aprovação": "status",
                          "IP OpenVPN (não editar)": "ip_ovpn"}, axis='columns', inplace=True)

    #colunas = ['Caminho da URL', 'Conjunto de Propriedades', 'Criado', 'ID', 'Id Exclusiva', 'Máscara de Permissões Efetivas', 'Nome', 'Nível', 'ScopeId', 'Tipo de Item', 'owshiddenversion', 'ID OpenVPN (não editar)', 'ip_ovpn']
    #dados_EstServ.drop(colunas, inplace=True, axis=1)

    colunas = ["local_uf", "local_municipio", "local_nome", "id_de_rede", "instrumento_fiscaliza", "detentor", "tipo_de_estacao", "marca", "modelo", "patrimonio", "n_serie", "situacao_equipamento", "versao_fw_sw", "altura_e_configuracao_de_antenas", "ip_openvpn", "data_chave_openvpn", "id_openvpn", "observacoes", "pendencia", "acoes_a_serem_adotadas", "responsavel_na_anatel_pela_acao", "modificado", "modificado_por", "versao", "local_ref_original", "local_lat", "local_lon", "local_ref", "status", "id_bd_estserv"]
    dados_EstServ = dados_EstServ[colunas]

    dados_EstServ = dados_EstServ.applymap(limpa_registros_SP)

    # reset de indice
    dados_EstServ = dados_EstServ.reset_index(drop=True)

    logging.debug(dados_EstServ.info())

    return dados_EstServ

def transform_EnlacesFix(logging, dados_EnlacesFix):

    # dados_EnlacesFix = dados_EnlacesFix.dropna(subset=["Designação do Circuito"])

    # adiciona coluna de ID
    ids = []

    for index, row in dados_EnlacesFix.iterrows():

        input_hash = str(row["Designação do Circuito"])

        resultado_id = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

        ids.append(resultado_id)

    dados_EnlacesFix["ID_BD_EnlacesFix"] = ids

    logging.debug(dados_EnlacesFix.head())

    # passa pra string
    # dados_EnlacesFix["Designação do Circuito"] = dados_EnlacesFix["Designação do Circuito"].astype(str)

    dados_EnlacesFix = dados_EnlacesFix.astype(str)

    dados_EnlacesFix.rename({'ID_BD_EnlacesFix': 'id_bd_enlacesfix',
                             'Local:UF': 'local_uf',
                             'Local:Município': 'local_municipio',
                             "Local": "local_nome",
                             "IP Público da Estação": "ip_publico_da_estacao",
                             "Designação do Circuito": "designacao_do_circuito",
                             "Designação do Roteador": "designacao_do_roteador",
                             "IP Público do Roteador": "ip_publico_do_roteador",
                             "IP Gateway": "ip_gateway",
                             "Máscara de Rede": "mascara_de_rede",
                             "Contrato": "contrato",
                             "Tecnologia de Acesso": "tecnologia_de_acesso",
                             "Situação do Enlace": "situacao_enlace",
                             "Responsável Anatel": "responsavel_anatel",
                             "Referência Suspensão": "referencia_suspensao",
                             "Data Suspensão": "data_suspensao",
                             "Referência Solicitação": "referencia_solicitacao",
                             "Data Solicitação": "data_solicitacao",
                             "Referência Homologação": "referencia_homologacao",
                             "Data Homologação": "data_homologacao",
                             "Referência TRD": "referencia_trd",
                             "Data TRD": "data_trd",
                             "Observações": "observacoes",
                             "Pendência": "pendencia",
                             "Ações a serem adotadas": "acoes_a_serem_adotadas",
                             "Responsável na Anatel pela ação": "responsavel_na_anatel_pela_acao",
                             "Modificado": "modificado",
                             "Modificado por": "modificado_por",
                             "Meses Pagos": "meses_pagos",
                             "Versão": "versao"},
                            axis='columns', inplace=True)

    # aplica lowercase em tudo
    dados_EnlacesFix["designacao_do_circuito"] = dados_EnlacesFix["designacao_do_circuito"].apply(lambda palavra: palavra.lower())

    #colunas = ['Caminho da URL', 'Conjunto de Propriedades', 'Criado', 'ID', 'Id Exclusiva', 'Máscara de Permissões Efetivas', 'Nome', 'Nível', 'ScopeId', 'Status de Aprovação', 'Tipo de Item', 'Title', 'owshiddenversion']
    #dados_EnlacesFix.drop(colunas, inplace=True, axis=1)

    colunas = ["local_uf", "local_municipio", "local_nome", "ip_publico_da_estacao", "designacao_do_circuito", "designacao_do_roteador", "ip_publico_do_roteador", "ip_gateway", "mascara_de_rede", "contrato", "situacao_enlace", "responsavel_anatel", "referencia_solicitacao", "data_solicitacao", "referencia_homologacao", "data_homologacao", "referencia_trd", "data_trd", "observacoes", "pendencia", "acoes_a_serem_adotadas", "modificado", "modificado_por", "meses_pagos", "versao", "tecnologia_de_acesso", "referencia_suspensao", "data_suspensao", "responsavel_na_anatel_pela_acao", "id_bd_enlacesfix"]
    dados_EnlacesFix  = dados_EnlacesFix[colunas]

    dados_EnlacesFix = dados_EnlacesFix.applymap(limpa_registros_SP)

    # reset de indice
    dados_EnlacesFix = dados_EnlacesFix.reset_index(drop=True)

    logging.debug(dados_EnlacesFix.info())

    return dados_EnlacesFix

