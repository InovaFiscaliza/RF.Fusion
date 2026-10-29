# PostgreSQL paralelo do RF.Fusion

## Situação e escopo

Pacote de preparação para Debian 12, PostgreSQL 18/PGDG, PostGIS e Podman.
Não substitui MariaDB nem altera as aplicações. Os arquivos SQL representam
uma instalação nova, não uma migração de produção.

**A1 continua pendente:** a política de compatibilidade entre versões/hashes
do bootstrap e estado persistido não foi aprovada. Não usar em produção.
Reinício de cluster já inicializado e `--recreate` ficam bloqueados
explicitamente, preservando os volumes. Esse bloqueio é um gate de entrega,
não uma política de comparação escolhida implicitamente. A primeira instalação
pode ser preparada; o pacote não está aprovado para operação continuada.

Os testes locais são apenas doubles e análise estática. Não se afirma que o
build, SQL, locks, FDW ou instalação real já passaram. Os scripts de ensaio na
VM não devem ser executados no container appCataloga.

## Decisões locais

- Quatro bancos físicos e schemas homônimos: BPDATA, RFDATA,
  RFFUSION_SUMMARY e WEBFUSION; identificadores de negócio preservados.
- 40 tabelas, 451 colunas, 40 PKs, oito regras únicas, 52 índices comuns,
  quatro textuais, um espacial, 22 FKs e 23 geradores.
- FDW interno por socket, quatro foreign tables e uma view dinâmica de
  13 campos; sem cópia dos dados operacionais.
- Identidade local em [environment.sh](linux/environment.sh), sem modificar
  os helpers comuns. Configuração lida como dados, nunca executada.
- Texto UTF8/ICU não determinístico, `und-u-ks-level1`; não é igualdade literal
  com collations MariaDB. FULLTEXT usa GIN/tsvector `simple`.
- Unicidade longa usa guarda por tabela/host e comparação integral; não usa
  hash único ou unicidade de prefixo. Pode exigir retry integral em
  `40001`/`40P01`; não fornece automaticamente alvo para `ON CONFLICT`.
- JSON textual, não JSONB; teto nativo próximo de 1 GiB. TEXT/BLOB mantêm
  limite de 65535 bytes; tinyint permanece número assinado.
- Datas sem timezone, UTC e instante do comando. No-op preserva data;
  atribuir a mesma data antiga junto de outra mudança pode atualizar a data.
- PostGIS `geometry(Point)` 2D sem impor SRID positivo.
- Fontes e correspondências ficam em [object-map.json](sql/object-map.json).
  Rotinas legadas de SUMMARY, shadow tables e backfill Fixed não são seeds.
- Conversor e adaptações dos consumidores ficam para outro escopo. Uma futura
  carga deve preservar IDs e reajustar geradores, sem recriar o schema.

## Arquivos e preparação estática

- [Containerfile](linux/Containerfile): build com contexto somente deste pacote.
- [Deploy](linux/deploy-debian12-postgres.sh): preflight e criação na VM.
- [Bootstrap](linux/bootstrap.sh): carga inicial protegida.
- [Verificação](linux/verify.sh): catálogo e valores de referência, só leitura.
- [Testes locais](tests/test_install.py): biblioteca padrão, sem serviços.
- [Gerador do snapshot](tests/build_snapshot.py): transforma somente os quatro
  criadores canônicos e copia os cinco CSVs. É ferramenta de preparação, não
  conversor de produção. Rejeita mudança da baseline.

Na raiz do repositório, no appCataloga:

```bash
/opt/conda/envs/appdata/bin/python -B -m unittest discover -s install/postgres/tests -p 'test_*.py' -v
/opt/conda/envs/appdata/bin/python -B install/postgres/tests/build_snapshot.py --check
bash -c 'set -e; for s in install/postgres/linux/*.sh install/postgres/tests/*.sh; do bash -n "$s"; done'
```

O gerador incorpora todo o inventário e valores esperados, não apenas counts.
Os SQLs `verify*.sql` executam somente consultas/assertivas em transação
read-only. A igualdade de arquivos não prova a equivalência entre motores.

## Pré-requisitos da VM e entradas

Executar apenas na VM Linux autorizada, como o proprietário do Podman local,
sem trocar rootful/rootless nem selecionar sockets remotos. Conferir CPU,
RAM, swap, espaço e disponibilidade de Bookworm/PGDG para a arquitetura.
É necessário Bash, Podman, Python 3 (metadados de inspeção), iproute2,
coreutils e acesso aos repositórios oficiais durante o build.
Exigir ao menos 1 GiB livre no graphRoot do Podman para a instalação vazia;
isso não dimensiona armazenamento de produção. A imagem fixa postgres em
UID/GID 999; os dois secrets de senha são montados 0400 para esse usuário,
enquanto authorized_keys permanece restrito a root.

O repositório da VM é `/RFFusion-dev/RF.Fusion`; alternativamente, passar
`--repo CAMINHO` explicitamente. A configuração `.env/runtime.env` desse
repositório deve conter exatamente `RFFUSION_ENV=development` ou `production`.
Ensaios aceitam apenas development, em cópia isolada, sem bancos compartilhados.
Não alterar configuração dos serviços existentes.

| Recurso | Valor |
|---|---|
| Container/imagem | debian12-postgres |
| Hostname | postgres-dev ou postgres-prod |
| Rede/IP | podman / 10.88.0.35, sujeito a disponibilidade |
| Banco | 9083 no host → 5432 |
| SSH | 2226 no host → 2828, somente chave |
| 9084 | Não usada |
| Repositório | Bind em /RFFusion, somente leitura, sem relabel automático |
| PGDATA | rffusion-postgres-ENV-data → /var/lib/postgresql/data |
| Estado/host keys | rffusion-postgres-ENV-state → /srv/app_volume |

ENV é development ou production. Não reaproveitar volumes alheios, mudar
ownership de volume existente ou executar prune/reset. Não há transação
atômica envolvendo os quatro CREATE DATABASE.

Se SELinux enforcing impedir o compartilhamento, o deploy para e exige
preparação administrativa; não aplicar `:Z` cegamente nem desabilitar SELinux.

### Secrets e acesso

Preparar três objetos Podman com arquivos locais protegidos:

```bash
podman secret create rffusion-postgres-appcataloga /caminho/protegido/appcataloga-password
podman secret create rffusion-postgres-webfusion /caminho/protegido/webfusion-password
podman secret create rffusion-postgres-authorized-keys /caminho/protegido/authorized_keys
```

São caminhos de exemplo, não credenciais. As senhas devem ser não vazias,
em uma linha, distintas, sem CR/LF adicional no conteúdo. Nunca passar senha
literal em argv/histórico, imagem, arquivo versionado ou log.
SSH usa root com chave; PostgreSQL remoto não admite o superusuário.
O provisionamento usa `rffusion_appcataloga` e `rffusion_webfusion` separados,
donos NOLOGIN por banco e leitor FDW com SELECT apenas nas quatro tabelas.
appCataloga recebe escrita em BPDATA/RFDATA/SUMMARY; WebFusion lê esses
domínios e escreve seu próprio WEBFUSION. Não há ativação dos consumidores.

### Comandos exclusivos da VM, não executados nesta entrega

```bash
bash install/postgres/linux/deploy-debian12-postgres.sh --check
bash install/postgres/linux/deploy-debian12-postgres.sh
bash install/postgres/tests/integration.sh --container debian12-postgres --ack-disposable
```

`--check` não cria/para/substitui recursos. Build ocorre antes de qualquer
substituição; atualmente `--recreate` para no gate A1, antes de mutações.
Limites desejados: uma CPU, 2 GiB RAM, 3 GiB RAM+swap e 1024 PIDs.
Cada limite omitido por falta de controlador é avisado; conferir inspect,
não apenas argumentos. Não são promessas de capacidade de produção.

## Estados, retenção e recuperação

Dois volumes realmente novos e vazios permitem bootstrap, com intenção/etapa
durável e lock. Marca complete só após verificação completa. Falha preserva
estado parcial e encerra serviços; não há retomada automática nem limpeza.
Cluster desconhecido, estado sem cluster ou instalação parcial são erros.
Cluster existente completo também para no A1 enquanto a política não for
aprovada; não se presume aceitação/rejeição de versões ou hashes diferentes.

Dados e host keys permanecem nos volumes. Recuperar exige autorização e
backup compatível ou novos volumes vazios, mantendo os anteriores intocados.
Não apagar a marca de estado para forçar uma carga. Mudança de major exige
procedimento separado. Rotação de secrets não altera automaticamente senhas
existentes nem deve trocar fingerprint SSH.

## Ensaios pendentes e medições

O ensaio exige `--container NOME --ack-disposable`; sem alvo padrão ou fallback.
Confere nome exato, identidade development, volumes exclusivos e ausência de
dados operacionais antes de qualquer fixture. Falhas esperadas contam somente
com SQLSTATE esperado e integridade posterior. O gate A1 impede concluir os
cenários de reinício/recriação; o harness deve informar isso, não dizer aprovado.

Verificar os 5618 registros por PK/valor, não apenas hashes/counts, além dos
atributos das 451 colunas, índices/constraints e objetos técnicos. A prova de
collation, IDs, geometria e unicidade longa depende de PostgreSQL real.

Bootstrap registra `duration_ms` e resultado. O ensaio concorrente deve
registrar cenário, isolamento, hosts sintéticos, duração, retries e bloqueadores
observados por pg_stat_activity/pg_blocking_pids. Duração de uma operação não
é tempo de espera por lock. Não há meta TPS/latência e dados simulados não são
benchmark real. O relatório concorrente completo permanece pendente de ensaio.

### Limites da entrega

Os arquivos de criação, snapshot e verificadores estão preparados, mas o
aceite não está concluído. Além do A1, a cobertura do harness ainda precisa
ser completada para COPY/lotes, troca de host, DELETE/rollback concorrentes,
autenticação SQL/SSH pela VM, FDW após commit e ciclo de vida persistente.
O verificador de catálogo ainda não prova todas as expressões CHECK.
Não interpretar os testes locais aprovados como aprovação desses cenários.
