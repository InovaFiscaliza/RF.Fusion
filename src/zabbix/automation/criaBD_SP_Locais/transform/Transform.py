from auxiliar.arquivos_locais import MOSTRAR_PRINTS

def transform_locais(logging, dados_Locais):

    dados_Locais['CEP'] = dados_Locais['CEP'].fillna('CEP vazio')
    dados_Locais['CEP'] = dados_Locais['CEP'].astype(str)

    # adiciona coluna de ID
    ids = []

    for index, row in dados_Locais.iterrows():

        input_hash = str(row["CEP"])

        if MOSTRAR_PRINTS == 1:
            print("Iterando por -> " + str(row["Logradouro"]) + " " + str(row["CEP"]))

        resultado_id = abs(hash(input_hash)) % (10 ** 4)

        ids.append(resultado_id)

    dados_Locais["ID_BD_Locais"] = ids

    logging.debug(dados_Locais.head())

    # passa coluna para string
    # dados_Locais = dados_Locais.astype('string')

    dados_Locais['Latitude'] = dados_Locais['Latitude'].astype(str)

    dados_Locais['Longitude'] = dados_Locais['Longitude'].astype(str)

    dados_Locais['Latitude'] = dados_Locais['Latitude'].apply(lambda palavra: palavra.replace(",", "."))

    dados_Locais['Longitude'] = dados_Locais['Longitude'].apply(lambda palavra: palavra.replace(",", "."))

    dados_Locais.rename({'ID_BD_Locais': 'id_bd_locais',
                         'UF': 'uf',
                         'Município': 'municipio',
                         'Referência': 'referencia',
                         'Bairro': 'bairro',
                         'Logradouro': 'logradouro',
                         'Número': 'numero',
                         'Complemento': 'complemento',
                         'CEP': 'cep',
                         'Atendimento': 'atendimento',
                         'Responsável pelo Local': 'responsavel_local',
                         'Situação Local': 'situacao_local',
                         'Contrato/Cessão': 'contrato_cessao',
                         'Contrato/Cessão:Instrumento.': 'contrato_cessao_instrumento',
                         'Contrato/Cessão: Situação': 'contrato_cessao_situacao',
                         'Responsável Anatel': 'responsavel_anatel',
                         'Latitude': 'lat',
                         'Longitude': 'lon',
                         'Observações': 'observacoes',
                         'Pendência': 'pendencia',
                         'Ações a serem adotadas': 'acoes_a_serem_adotadas',
                         'Responsável na Anatel pela ação': 'responsavel_acao_na_anatel',
                         'Modificado': 'modificado',
                         'Modificado por': 'modificado_por',
                         'Versão': 'versao',
                         'Status de Aprovação': 'status_aprovacao'}, axis='columns', inplace=True)

    columns = ['Caminho da URL', 'Conjunto de Propriedades', 'Criado', 'ID', 'Id Exclusiva', 'Máscara de Permissões Efetivas', 'Nível', 'ScopeId', 'Tipo de Item', 'owshiddenversion', 'Title', 'Nome']
    dados_Locais.drop(columns, inplace=True, axis=1)

    dados_Locais = dados_Locais.reset_index(drop=True)

    logging.debug(dados_Locais.info())


    return dados_Locais