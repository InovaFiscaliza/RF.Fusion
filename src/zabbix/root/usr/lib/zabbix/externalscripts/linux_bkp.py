#!/usr/bin/python3.9
"""_summary_
Realiza comunicação com agente instalado no host via arquivos
Dispara processo de cópia
Registra arquivos copiados na lista de processamento de catalogação

Entrada:
    Identificação do host (zabbix hostid)
    Direção de acesso (IP ou DNS)
    Usuário de acesso
    Chaves de acesso

Saída:
    String JSON resultado indicando se houve acesso (acesso:sucesso/falha) e situação da fila de processos de backup para o host indicado. (Qtd arquivos/bytes de backup realizados, em andamento, aguardando 
"""