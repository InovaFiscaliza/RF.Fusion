# Instruções para agentes — RF.Fusion

## Contexto do projeto

O RF.Fusion integra medições de espectro de estações remotas. O appCataloga
descobre, transfere e processa arquivos; MariaDB mantém filas, catálogo e
modelos derivados; WebFusion publica páginas e APIs. Novas funcionalidades
normalmente atravessam um processo/handler no appCataloga ou um módulo Flask
com rotas e serviços no WebFusion.

## Dependências técnicas do projeto

- **appCataloga**: Python 3.10.8/Conda appdata; mysql-connector, Paramiko,
  pandas e rfpye declarados no [manifesto](install/appCataloga/linux/environment.yml).
- **WebFusion**: Python 3.11, Flask, Waitress e PyMySQL declarados no
  [Containerfile](install/webserver/Containerfile); runtime separado do appCataloga.
- **Persistência**: MariaDB, com `BPDATA`, `RFDATA`, `RFFUSION_SUMMARY` e
  `WEBFUSION`; [bootstrap SQL](src/mariadb/scripts/).
- **Validação**: suíte `unittest` executada por `pytest`, em [test/](test/);
  selecionar ambiente pelos imports, conforme as instruções da suíte.
- **Integrações**: estações por SSH/SFTP, processamento appAnalise, Zabbix e
  identidade encaminhada pelo proxy; consultar o contrato local antes de
  alterar payloads compartilhados.

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

## Contexto operacional sob demanda

| Tarefa | Material |
|---|---|
| Acrescentar ou alterar página, rota ou API WebFusion | [rffusion-webfusion-fluxo](.agents/skills/rffusion-webfusion-fluxo/SKILL.md) |
| Conferir como WebFusion monta identidade, dados ou templates | [webfusion-construcao.md](.agents/references/webfusion-construcao.md) |
| Acrescentar ou alterar processo, worker ou handler appCataloga | [rffusion-appcataloga-fluxo](.agents/skills/rffusion-appcataloga-fluxo/SKILL.md) |
| Conferir categorias e ciclo de processos appCataloga | [appcataloga-construcao.md](.agents/references/appcataloga-construcao.md) |
| Alterar dados compartilhados, summary ou protocolo de integração | [dados-e-integracoes.md](.agents/references/dados-e-integracoes.md) e contratos normativos acima |
| Selecionar teste, runtime ou acesso a container | [ambientes-e-testes.md](.agents/references/ambientes-e-testes.md), subordinada aos ENVIRONMENTs e a [test/AGENTS.md](test/AGENTS.md) |
| Solicitar revisão de segurança | [owasp-playbook](.agents/skills/owasp-playbook/SKILL.md), com [ponte do projeto](.agents/security/mapa-cwe-guia.md) |
