# Manutenção e refatoração do appCataloga

Leia as [regras gerais](../../../docs/agents/INSTRUCTIONS.md) e
[ARCHITECTURE.md](ARCHITECTURE.md). A arquitetura prevalece neste escopo;
este guia não redefine anatomia, loop, assinaturas ou eventos de log.

## Procedimento

1. Identificar se o arquivo é gateway, task worker, daemon de manutenção ou
   utilitário. Não impor o loop de fila a processos que não consomem tasks.
2. Conferir responsabilidades no mapa de módulos da arquitetura. Não criar
   extrações por redução de linhas nem deslocar complexidade para um handler.
3. Conferir chamadas e testes antes de remover funções ou wrappers.
4. Preservar o ciclo de vida operacional no entrypoint e o SQL nos handlers.
   Domínio pode persistir dados analíticos em RFDATA por métodos públicos.
5. Preservar atomicidade, locks, cooldowns, recuperação, retenção e horários.
6. Tipar os handlers concretamente e usar `config as k`, inclusive `STAGE_*`.
7. Validar contratos afetados no ambiente de [ENVIRONMENT.md](ENVIRONMENT.md),
   sem importar o WebFusion no container deste aplicativo.

## Cuidados específicos

- Não separar movimentos de arquivos e transações sem revisar falhas parciais:
  uma transação SQL não desfaz uma movimentação já concluída no filesystem.
- Helpers de domínio levantam exceções; o entrypoint captura uma vez e resolve
  a fila. Não usar `err.triggered` como guarda entre etapas.
- Não mudar contratos de retorno apenas para coletar tempo de execução fora
  do escopo pedido. `_do_work()` mantém `elapsed_sec`; fases usam a API canônica.
- Não unificar funções apenas por percentual de semelhança: conferir semântica,
  estado persistido e recuperação antes de introduzir parâmetros ou abstrações.
- Não retirar docstrings para cumprir uma meta de linhas. O contrato completo
  de entradas, saídas e exceções deve permanecer documentado.
- Ao renomear workers ou pacotes, revisar imports, launchers, supervisão e
  `shell/tool_start_all.sh`, `tool_stop_all.sh`, `tool_status_all.sh` juntos.

