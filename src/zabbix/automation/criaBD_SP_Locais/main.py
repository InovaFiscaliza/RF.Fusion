# --------------------------------------------------
# Script de criação/atualização de BD
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_PyZabbix_FISF3
# --------------------------------------------------

import sys
import os
import time
import sqlite3
import logging
from datetime import datetime

from auxiliar.arquivos_locais import HOST_BD, MOSTRAR_PRINTS
from auxiliar.salva_horario import salva_horario

from extract import Extract
from transform import Transform

def main():

    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8',
                        format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando script!\n")

    dados_Locais = Extract.extract_Locais()

    #  Limpeza dos dados
    # ------------------------------------------------------------------------

    dados_Locais = Transform.transform_locais(logging, dados_Locais)

    # Acesso ao BD
    # LOAD
    # ------------------------------------------------------------------------

    logging.debug(dados_Locais.dtypes)

    logging.debug("\n\nCriando/alterando banco de dados!")

    # ele trabalha com o banco de dados na pasta de cima, que ai tem outras pastas
    # nesse diretório se alimentando desse banco de dados!
    conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
    c = conn.cursor()

    # criando tabela LOCAIS
    c.execute(
        'CREATE TABLE IF NOT EXISTS locais (id_bd_locais VARCHAR(4) PRIMARY KEY, uf VARCHAR(5), municipio VARCHAR(155), referencia VARCHAR(155), bairro VARCHAR(155), logradouro VARCHAR(355), numero VARCHAR(55), complemento VARCHAR(255), cep VARCHAR(25), atendimento VARCHAR(155), responsavel_local VARCHAR(255), situacao_local VARCHAR(255), contrato_cessao VARCHAR(155), contrato_cessao_instrumento VARCHAR(155), contrato_cessao_situacao VARCHAR(155), responsavel_anatel VARCHAR(255), lat VARCHAR(50), lon VARCHAR(50), observacoes VARCHAR(255), pendencia VARCHAR(5), acoes_a_serem_adotadas VARCHAR(255), responsavel_acao_na_anatel VARCHAR(255), modificado VARCHAR(155), modificado_por VARCHAR(155), versao VARCHAR(50), status_aprovacao VARCHAR(155))')

    conn.commit()
    time.sleep(1)

    # criando tabela log (caso não exista)
    c.execute(
        'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
    conn.commit()
    time.sleep(2)

    # bota na tabela de log que execucao foi um sucesso
    # update if row exists, else insert
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                 "VALUES ('criaBD_Locais', '" + agora + "', 'True');"

    c.execute(sql_string)

    conn.commit()
    time.sleep(1)

    try:
        if dados_Locais.empty:
            if MOSTRAR_PRINTS == 1:
                print("Dataframe de informações a serem salvas voltou vazio!")
            logging.debug("Dataframe de informações a serem salvas voltou vazio!")
        else:
            dados_Locais.to_sql('locais', conn, if_exists='replace', index=False)


    except:
        logging.warning("Erro na conexão com banco de dados!")
        logging.warning("Ocorreu: " + str(sys.exc_info()[0]))

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('criaBD_Locais', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)

    finally:

        logging.debug("Encerrando conexão com BD!")

        conn.close()


# ------------------------------------------------------------------------


if __name__ == '__main__':
    # contabiliza o tempo de execução!
    inicio = time.time()
    main()
    fim = time.time()

    salva_horario(inicio)

    duracao = (fim - inicio) / 60
    logging.debug("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))
