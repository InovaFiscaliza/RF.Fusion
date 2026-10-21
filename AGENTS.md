# Instruções para agentes — RF.Fusion

## Leitura obrigatória

Leia as [regras gerais](docs/agents/INSTRUCTIONS.md) antes de planejar ou alterar
o repositório. Depois, leia as instruções do escopo envolvido:

| Escopo | Entrada |
|---|---|
| appCataloga, workers e handlers | [src/appCataloga/AGENTS.md](src/appCataloga/AGENTS.md) |
| WebFusion, APIs, interface e autenticação | [src/webfusion/AGENTS.md](src/webfusion/AGENTS.md) |
| Testes e ferramentas da suíte | [test/AGENTS.md](test/AGENTS.md) |
| Summary e contratos entre aplicações | [docs/architecture/SUMMARY.md](docs/architecture/SUMMARY.md) |
| Métricas e integração operacional com Zabbix | [docs/architecture/METRICAS_OPERACIONAIS.md](docs/architecture/METRICAS_OPERACIONAIS.md) |

Mudanças em `install/`, `service/`, banco ou testes devem consultar também as
instruções das aplicações afetadas e os READMEs dos componentes envolvidos.

## Autoridade e escopo

No escopo correspondente, a ordem é: contrato arquitetural (`ARCHITECTURE.md`
e contratos compartilhados), regras de manutenção (`INSTRUCTIONS.md`), código
existente. Não inferir arquitetura por arquivos isolados. Se um caso não estiver
coberto ou houver conflito entre contratos, registrar a decisão arquitetural
antes de implementar. Um plano ou backlog não autoriza sua execução.

Os documentos `ENVIRONMENT.md` definem os containers, interpretadores e caminhos
autorizados. A conveniência do ambiente local não altera essas restrições.

## Manutenção das próprias instruções

- Regras comuns ficam em `docs/agents/`; regras locais, no subprojeto.
- Contratos entre aplicações ficam em `docs/architecture/`.
- `AGENTS.md` deve encaminhar às fontes, sem copiar seus contratos.
- READMEs descrevem componentes. Planos que abrangem várias aplicações ficam em
  [docs/planejamento/](docs/planejamento/README.md), com situação explícita.
- Remover tarefas concluídas, diagnósticos vencidos e relatos de revisões dos
  documentos ativos. Manter apenas contratos vigentes e pendências reais;
  o histórico fica no Git.
- Usar links relativos ao repositório; caminhos absolutos ficam restritos a
  comandos e referências de ambiente que realmente dependam deles.
- Ao mover documentos, atualizar referências e regras de ignore. Não deixar
  cópias normativas concorrentes. Histórico de conteúdo removido pertence ao Git.
