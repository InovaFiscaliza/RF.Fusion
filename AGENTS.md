# AGENTS.md

## Contexto obrigatorio

Antes de responder, planejar, editar ou refatorar codigo neste repositorio, leia:

1. [ARCHITECTURE.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/.instructions/ARCHITECTURE.md)
2. [INSTRUCTIONS.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/.instructions/INSTRUCTIONS.md)

Esses arquivos sao o contexto obrigatorio do projeto.

## Ordem de autoridade

Se houver conflito, siga esta ordem:

1. `ARCHITECTURE.md`
2. `INSTRUCTIONS.md`
3. Codigo existente

## Regras inegociaveis

- Nao altere logica de negocio sem pedido explicito.
- Nao invente arquitetura.
- Nao mova funcoes para novos modulos sem permissao do `ARCHITECTURE.md`.
- Siga a anatomia dos workers e o loop canonico definidos em `ARCHITECTURE.md` quando a mudanca tocar o runtime do `appCataloga`.
- Siga as regras de refatoracao definidas em `INSTRUCTIONS.md`.
- Use constantes de `config.py` em vez de literais magicos.
- Use `err.capture(...)`, nunca `err.set(...)`.
- Helpers devem levantar excecoes; nao devem retornar valores sentinela para falha.
- Tipar handlers de banco com classes concretas.
- Preserve o comportamento atual do banco e o ciclo de vida das tasks.
- Comentarios de codigo devem ficar em ingles.
- Documentacoes Markdown (`*.md`) devem ficar em portugues (PT-BR).

## Formato obrigatorio de resposta para mudancas de codigo

Antes de mudar codigo, explique:

1. Qual regra de `ARCHITECTURE.md` ou `INSTRUCTIONS.md` se aplica.
2. Quais arquivos serao alterados.
3. Se a mudanca e arquitetural, apenas de refatoracao ou com impacto comportamental.

Depois da mudanca, informe:

1. O que mudou.
2. O que nao mudou.
3. Comandos de validacao ou testes a executar.
4. Qualquer risco ou ponto de revisao manual.
