---
name: rffusion-appcataloga-fluxo
description: >
  Conduz a implementação ou alteração de processos, task workers e handlers
  appCataloga no RF.Fusion. Use ao mudar discovery, backlog, backup,
  processamento appAnalise ou criar uma rotina de manutenção do catálogo.
  Não usar para HTTP WebFusion, deploy, execução operacional de filas,
  dicionário de dados, auditoria de segurança ou configuração isolada de testes.
---

# Implementar um processo appCataloga

## Entrada e enquadramento

Obtenha a mudança desejada, o processo/domínio e os efeitos esperados. Reutilize
o pedido e esclareça estados ou comportamento não definidos no contrato.

1. Leia o [AGENTS do appCataloga](../../../src/appCataloga/AGENTS.md) e seus
   contratos. Classifique gateway, task worker, manutenção ou utilitário pela
   unidade realmente consumida, conforme
   [construção appCataloga](../../references/appcataloga-construcao.md).
   Termine com a categoria, o dono dos estados e a cobertura arquitetural
   identificados; registre decisão antes de implementar caso não coberto.
2. Identifique produtores, consumidores, configuração e métodos públicos
   dos handlers. Mantenha domínio, infraestrutura de runtime e shared em sua
   direção de dependências. SQL e conexões pertencem ao handler concreto;
   entrypoints continuam donos de HOST_TASK/FILE_TASK. Termine com as
   superfícies necessárias da mudança delimitadas.
3. Nos task workers, preserve bootstrap, sinais e estado de execução;
   leia a task, trate fila vazia e faça claim condicional antes do trabalho.
   Disputa perdida segue a espera existente, não a finalização de erro.
   Preserve preflight e prioridade quando pertencem àquele worker. Gateway
   e manutenção usam seu ciclo próprio, sem receber claim artificial.
4. Execute trabalho por colaboradores do domínio e operações públicas do
   handler. Preserve o formato de retorno e a medição do chamador: tupla,
   dicionário e recursos de cleanup variam. Confira definições e chamadas
   antes de generalizar a anatomia canônica sobre implementações divergentes.
   Havendo conflito com contrato, explicite-o em vez de instituir exceção.
5. Finalize sucesso/erro pelo caminho do entrypoint, com estado e logs.
   Preserve a atomicidade exigida para fila/histórico e as conexões donas de
   locks; liberar recursos não finaliza task. Rollback SQL não desfaz efeitos
   em arquivos. Termine com falha, recuperação e liberação de recursos
   pertinentes cobertas, sem acrescentar helpers vazios apenas por simetria.
6. Se alterar dado derivado ou contrato compartilhado, acompanhe outbox,
   refresh e consumidores em
   [dados e integrações](../../references/dados-e-integracoes.md). Se criar ou
   renomear processo, rastreie launchers, start/stop/status coletivos,
   supervisão e testes por nome. Faça inspeção desses scripts; executá-los
   não é uma validação unitária.
7. Selecione testes conforme
   [ambientes e testes](../../references/ambientes-e-testes.md) e
   [test/AGENTS.md](../../../test/AGENTS.md). Use appdata apenas para imports
   exclusivos do appCataloga; uma suíte mista segue as restrições WebFusion.
   Substitua banco/rede e isole filesystem. Verifique estados, disputa de
   claim, idempotência e falhas aplicáveis à mudança no menor grupo necessário.
   SQL/locks reais exigem MariaDB isolado, não são comprovados por doubles.

## Critério de conclusão e saída

Entregue implementação, wiring e documentação relacionados, preservando
contratos externos e comportamento fora do pedido. Informe arquivos, impacto,
comandos realmente executados e limitações. Separe aprovação de teste de
validação apenas planejada. Iniciar daemon, consumir fila, reconciliar banco
ou operar serviço externo depende de autorização operacional própria.

## Conferência do padrão

A [referência de construção](../../references/appcataloga-construcao.md#exemplares)
mantém os exemplares, suas regras de localização e evidências: cinco task
workers entre nove entrypoints. Compare a mesma categoria e o mesmo recurso;
o nome `appCataloga*.py` sozinho não determina o lifecycle.

## Casos para avaliação comportamental

Prompts propostos, não executados nem comparados com baseline:

- “Mude a discovery para aceitar um filtro adicional.” Esperado: acompanhar
  normalização, produtores/consumidores e lifecycle, sem iniciar daemon.
- “Crie uma rotina periódica de manutenção no appCataloga.” Esperado:
  classificar manutenção e usar temporização/checkpoint pertinente sem
  inventar claim de task nem promover um padrão concorrente a contrato.
