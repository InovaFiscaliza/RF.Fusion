# Construção de processos appCataloga

Referência descritiva para alterações de processos e seus colaboradores.
Consulte ao alterar worker, fila, domínio ou handler. Os contratos normativos
continuam na [arquitetura](../../src/appCataloga/.instructions/ARCHITECTURE.md)
e nas [regras locais](../../src/appCataloga/.instructions/INSTRUCTIONS.md).

## Escolher a categoria antes do fluxo

| Categoria | Unidade e sequência | Exemplo |
|---|---|---|
| Gateway | recebe, normaliza pedido, usa handlers e despacha/enfileira | `appCataloga.py` |
| Task worker | lê task, disputa claim, trabalha e finaliza | host-check, discovery, backlog, backup e processamento appAnalise |
| Manutenção | temporização/checkpoint, sem claim artificial de HOST_TASK/FILE_TASK | summary, garbage collector e host maintenance |
| Utilitário | operação pontual com finalidade própria | ferramentas em `utils/` |

Gateway não é task worker. O summary mantém outbox e checkpoint próprios,
não passa a finalizar a fila operacional por materializar suas informações.

## Alterar um task worker

1. Identifique tipo de task, handler público e estado afetado. Localize
   produtores e consumidores com
   `rg -n -F '<CONSTANTE_DO_TIPO>' src/appCataloga src/webfusion src/zabbix test`.
2. Preserve bootstrap antes de imports internos, configuração `config as k`,
   logger, `process_status` e instalação dos sinais. O runtime e a configuração
   pertencem a caminhos diferentes, resolvidos pelo bootstrap.
3. Leia a próxima task; fila vazia segue a espera existente. Faça claim
   condicional antes de executar o trabalho da task; disputa perdida não é
   falha. O processamento appAnalise verifica disponibilidade do serviço
   antes do claim para não reservar trabalho durante indisponibilidade.
4. Execute trabalho com a dependência concreta e delegue transporte/arquivos
   ao domínio e SQL ao handler. O entrypoint continua dono das transições
   de task. Backlog é DB-only; discovery abre uma sessão SFTP após claim.
5. Preserve o resultado específico do worker e meça o tempo no ponto
   existente. Discovery/backlog devolvem dicionários de resultado;
   host-check devolve status e mensagem. Os chamadores medem o tempo total.
6. Finalize sucesso ou erro uma vez pelo caminho pertinente, incluindo os
   eventos de log. Liberar recursos não é concluir task. Preserve atomicidade
   de fila/histórico, recuperação e prioridade de task do contrato.
7. Se houver recursos por iteração, libere-os no encerramento apropriado.
   Host-check não tem `_cleanup` como os workers com recursos; não introduza
   um helper vazio só para igualar a anatomia. O contrato declara uma anatomia
   canônica mais uniforme que algumas implementações atuais: registre a
   diferença antes de generalizar assinaturas ou refatorar.
8. Acompanhe invalidação de summary, testes e wiring do processo quando
   necessários. Alterar um helper não autoriza iniciar/reiniciar daemons.

**Alerta — mesma família não significa mesma assinatura:** antes de copiar
`_do_work`, finalização ou cleanup, confira as definições e o chamador:
`rg -n '^def _do_work|^def _finalize|^def _cleanup|_do_work\(' <worker-alvo>`.
Use o contrato arquitetural como regra de mudança; não transforme a variação
de um exemplar numa exceção normativa permanente.

**Alerta — categoria:** apenas cinco dos nove entrypoints têm `_claim_task`.
Antes de acrescentar claim a um processo, confirme se ele realmente consome
uma task operacional:
`rg -n '_claim_task|read_outbox_batch|selector|HOST_TASK|FILE_TASK' <processo-alvo>`.
A classificação e as contagens ficam nas evidências.

## Dados, domínio e integração

- `DBHandlerBase` centraliza conexão mysql-connector; os handlers BKP, RFM
  e Summary expõem as operações por domínio de dados. Chamadores usam métodos
  públicos, sem SQL nos entrypoints.
- BKP e RFM possuem `begin_transaction`, `commit` e `rollback`; ambos mantêm
  `in_transaction` e restauram autocommit. Preserve o escopo que torna as
  escritas relacionadas atômicas; rollback SQL não desfaz um arquivo movido.
- `request_host_summary_refresh` publica o escopo via infraestrutura comum
  da outbox. `SUMMARY_OUTBOX` está em `RFFUSION_SUMMARY`, não em BPDATA.
- O processo summary usa uma conexão de trabalho e outra proprietária do lock
  MariaDB. O caminho incremental só checkpointa depois de atualizar o modelo;
  o contrato e as métricas compartilhadas definem os consumidores.
- SSH/SFTP usa Paramiko em `host_handler`; appAnalise é serviço externo MATLAB
  acessado por socket e parser próprios. Não derivar seu funcionamento interno
  nem tratá-lo como uma chamada LLM pelo nome.
- Constantes de paths, fila e domínio ficam na configuração local. Leia
  símbolos precisos; não copie valores de credenciais para documentação.

## Wiring e validação

Cada processo possui launcher shell; criação/renomeação exige conferir também
start/stop/status coletivos e supervisão, conforme as regras locais:
`rg -n -F '<nome-do-processo>' src/appCataloga service test`.
Essa busca é inspeção; não executar launchers para validar documentação.

O ambiente declarado é Conda `appdata`, Python 3.10.8, com manifesto em
`install/appCataloga/linux/environment.yml`. São dependências declaradas,
entre outras, mysql-connector-python 8.1.0, Paramiko 3.3.1, pandas 2.0.3 e
rfpye 0.3.7. O manifesto não prova disponibilidade de todos os pacotes da
imagem em execução. Use o interpretador autorizado no ENVIRONMENT e selecione
testes que não importem WebFusion.

## Exemplares

| Caminho, relativo ao runtime appCataloga | O que ilustra | Como localizar o equivalente hoje |
|---|---|---|
| `appCataloga_discovery.py` | claim, SFTP e descoberta em lotes | entrypoint que chama `iter_metadata_files` |
| `appCataloga_host_check.py` | prioridades e trabalho sem HOST.IS_BUSY | entrypoint com `HOST_TASK_PRIORITY` de checagem |
| `appCataloga_backlog_management.py` | promoção/rollback DB-only | entrypoint que chama `update_backlog_by_filter` |
| `appCataloga_summary_database.py` | manutenção, outbox e lock dedicado | entrypoint com `read_outbox_batch` |

Runtime: `src/appCataloga/server_volume/usr/local/bin/appCataloga/`.

## Evidências

Buscas a partir da raiz em 2026-10-27; nenhum daemon foi iniciado.

| Afirmação | Confiança | Onde foi comprovada | Comando | Alcance | Verificado em |
|---|---|---|---|---|---|
| Nove entrypoints, cinco com claim | medido | três workers lidos integralmente, outros dois medidos; manutenção summary lida integralmente | `rg --files src/appCataloga -g 'appCataloga*.py' \| wc -l` → 9; `rg -l -g 'appCataloga*.py' '^def _claim_task\(' src/appCataloga \| wc -l` → 5 | família de entrypoints | 2026-10-27 |
| Claim é usado pelos cinco workers | medido | chamadas nos respectivos loops | `rg -l -g 'appCataloga*.py' 'if not _claim_task\(' src/appCataloga \| wc -l` → 5 | mesmos cinco arquivos | 2026-10-27 |
| Driver concentrado | medido | `db/dbHandlerBase.py:263` | `rg -l -g '*.py' '^[[:space:]]*(import mysql.connector\|from mysql.connector)' src/appCataloga/server_volume/usr/local/bin/appCataloga` → um arquivo | runtime appCataloga | 2026-10-27 |
| Variações de resultados e cleanup | inferido | leitura integral de discovery, host-check e backlog; definições e chamadas diferentes | conferir definições e chamadas no worker; testes dirigidos confirmam o contrato do alvo | três workers nomeados, sem declarar equivalência dos demais | 2026-10-27 |
| Transação e outbox | medido | `db/dbHandlerBKP.py:173,888`, `db/dbHandlerRFM.py:48`, `db/dbHandlerBase.py:20` | leitura dos métodos públicos e da constante de schema | handlers nomeados | 2026-10-27 |
| Preflight antes do claim | medido | `appCataloga_file_bin_process_appAnalise.py:641` | leitura do loop que chama `check_connection_with_log` antes de `_claim_task` | worker appAnalise | 2026-10-27 |

## Descartado

| Achado | Motivo | Alcance medido | O que mudaria a decisão | Data |
|---|---|---|---|---|
| Mesma assinatura em todos os workers | contradito pelas implementações atuais | três exemplares integrais | alinhamento explícito do contrato e código | 2026-10-27 |
| Claim em todo entrypoint | gateway/manutenção possuem outra categoria | cinco de nove possuem claim | nova categoria aprovada em contrato | 2026-10-27 |
