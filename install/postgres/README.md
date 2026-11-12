# Pod PostgreSQL e pgAdmin

O deploy cria o pod `rffusion-postgres-pod` com dois containers: PostgreSQL 17
Alpine com PostGIS e pgAdmin 4. A imagem PostGIS para PostgreSQL é
estendida para incluir SSH e identificação de ambiente do RF.Fusion; a imagem
do pgAdmin também é estendida para incluir SSH. O cluster PostgreSQL fica no
volume nomeado `rffusion-postgres-data`, no
armazenamento do Podman do usuário proprietário; em produção rootless, esse
armazenamento normalmente fica em `/home`. As chaves SSH e as configurações
do pgAdmin ficam em volumes nomeados separados.

O pod também possui o container de infraestrutura `rffusion-postgres-infra`,
com imagem `registry.k8s.io/pause:3.10` e comando `/pause`. Ele mantém a rede
compartilhada e concentra as portas publicadas. PostgreSQL e pgAdmin continuam
em `rffusion-postgres` e `rffusion-pgadmin`; a listagem completa contém esses
três containers. O nome, a imagem e o comando do infra são explícitos para
facilitar a identificação em `podman ps`.

O instalador único é [deploy-postgres.sh](linux/deploy-postgres.sh); as imagens
e seus entrypoints ficam em [postgres](linux/postgres/) e [pgadmin](linux/pgadmin/).

O pod usa a rede `podman` e o IP `10.88.0.35`:

| Porta da VM | Porta no pod | Serviço |
|---|---|---|
| 2226 | 22 | SSH do container PostgreSQL |
| 2227 | 2222 | SSH do container pgAdmin |
| 9083 | 5432 | PostgreSQL |
| 9084 | 8080 | Interface web do pgAdmin |

As portas são publicadas nas interfaces disponíveis do host, conforme o Podman.
O pgAdmin é HTTP; proteja o acesso na rede ou configure HTTPS antes de expô-lo.

## Deploy inicial

No host proprietário dos containers, configure [.env/runtime.env](../../.env/README.md).
É necessário Podman, rede `podman`, IP e portas disponíveis e o repositório no
host. O repositório é montado com `:z` para compartilhamento SELinux com os
demais containers. O script usa a raiz deste checkout; `RFFUSION_REPO_ROOT` pode apontar para
outro checkout montado no host. A partir da raiz do repositório:

```bash
install/postgres/linux/deploy-postgres.sh
```

O banco padrão é `RFFUSION`, com maiúsculas, criado na inicialização de um
volume vazio. `POSTGRES_DB` seleciona outro banco de forma explícita, tanto no
bootstrap quanto nos backups e nas recriações. A variável não renomeia nem cria
bancos em clusters já inicializados; use o nome existente nessas instalações.

Sem opções, o deploy recusa substituir um pod ou container existente. Ele espera o servidor
PostgreSQL responder por TCP e, se nenhum dos quatro schemas do projeto existir,
executa os scripts SQL e carrega os CSVs em uma única transação. Se qualquer
schema já existir, em maiúsculas ou na convenção anterior em minúsculas, a carga
é ignorada e requer revisão manual. Novas instalações preservam maiúsculas em
schemas, tabelas e colunas usando aspas duplas. A recriação do pod com os mesmos
volumes não altera os nomes de uma instalação existente. Falhas preservam
pod e volumes para diagnóstico; confira `podman logs rffusion-postgres`.
Instalações anteriores ainda precisam de atualização planejada para disponibilizar
PostGIS e migrar os dados espaciais; editar estes arquivos não altera o pod existente.
Em seguida, o deploy cria o container pgAdmin; confira também
`podman logs rffusion-pgadmin` e abra `http://IP_DA_VM:9084`.

Ambos os acessos SSH usam `root` e a senha inicial `changeme`; o PostgreSQL usa `postgres` e a
mesma senha inicial. O pgAdmin inicia com `admin@anatel.gov.br` e `changeme`.
Troque as senhas após a criação. Para definir outros valores no deploy, forneça
`SSH_PASSWORD`, `PGADMIN_SSH_PASSWORD`, `POSTGRES_PASSWORD`,
`PGADMIN_DEFAULT_EMAIL` e `PGADMIN_DEFAULT_PASSWORD` pelo ambiente, sem registrá-los no histórico.
A senha SSH alterada dentro de cada container é preservada ao reiniciá-lo, mas uma
recriação do container volta a usar o valor fornecido no deploy. A senha inicial
do PostgreSQL e o login inicial do pgAdmin só são aplicados a volumes vazios.

No pgAdmin, cadastre o servidor com host `127.0.0.1`, porta `5432`, banco
`RFFUSION`, usuário `postgres` e a senha do banco. Os containers compartilham a
rede do pod. O pgAdmin tem credenciais próprias, separadas das do banco.

Os limites desejados do PostgreSQL são 1 CPU, 2 GiB de memória, 3 GiB incluindo
swap e 1024 processos, aplicados conforme os controladores disponíveis. O pgAdmin
não recebe esses limites. Em cgroups v1 rootless, nenhum argumento de limite
é enviado, inclusive `--pids-limit`; os avisos indicam as restrições do host. A instalação não migra dados MariaDB e os consumidores
existentes ainda não foram adaptados para PostgreSQL.

## Recriação com os scripts atuais

No host proprietário do Podman, a partir da raiz do repositório, execute:

```bash
install/postgres/linux/deploy-postgres.sh --recreate --reset-schemas
```

`--recreate` faz backup, constrói as imagens antes de parar o pod e substitui
os containers, preservando o diretório de dados e os volumes nomeados. A imagem PostgreSQL usa
PostGIS; o build verifica os arquivos da extensão e o deploy verifica sua
disponibilidade no servidor antes de qualquer reset.

`--reset-schemas` recria `BPDATA`, `RFDATA`, `RFFUSION_SUMMARY` e `WEBFUSION`
com os SQLs atuais e recarrega os catálogos CSV. **Os dados de BPDATA, RFDATA e
WEBFUSION são removidos**; somente as duas tabelas de uso do Summary são
preservadas automaticamente. Os demais agregados começam vazios para reconcile.
Use apenas `--recreate` para atualizar o container conservando todos os schemas.

Os backups completos e a exportação das duas tabelas de uso ficam em
`.env/postgres-backups/recreate_<timestamp>_<sufixo>/`, fora do Git e com acesso
restrito. O arquivo `postgres-before.dump` tem formato custom do `pg_dump`;
`archive-list.txt` e `SHA256SUMS` permitem conferir o backup. A recriação com
reset gera um backup antes da substituição e outro antes da alteração dos schemas.
Se o backup inicial de um PostgreSQL em execução falhar, o pod existente não
é removido. Se restar apenas o infra ou o PostgreSQL estiver parado, o deploy
recria os serviços nos mesmos volumes e faz o backup após o banco ficar pronto,
antes de qualquer reset de schemas. O download da imagem infra também ocorre
antes da remoção do pod anterior.

A preservação de `WEBFUSION_USAGE_LOG_CHECKPOINT` e
`WEBFUSION_USAGE_METRIC_MONTHLY` usa cópias temporárias sob lock, na mesma
transação que remove e recria os quatro schemas. As duas tabelas são restauradas
com seus valores atuais, incluindo timestamps e offsets. Qualquer erro no DDL
ou na restauração desfaz o reset inteiro. Não há remoção de volumes. Schemas
legados em minúsculas e uma instalação com apenas uma das duas tabelas de uso
exigem revisão manual antes do reset.

O pod e o container PostgreSQL têm nomes distintos para compatibilidade com
o backend BoltDB do Podman. Se uma tentativa anterior deixou apenas o infra
no pod antigo `rffusion-postgres`, `--recreate` reconhece esse pod e o substitui
por `rffusion-postgres-pod`, preservando os volumes. Não use `--reset-schemas`
para recuperar essa falha.

Para corrigir a identificação e recuperar os serviços sem apagar schemas:

```bash
install/postgres/linux/deploy-postgres.sh --recreate
podman ps -a --pod --filter pod=rffusion-postgres-pod
```

Após a recriação, conferir:

```bash
podman exec -u postgres rffusion-postgres psql -X -U postgres -d RFFUSION -c 'SELECT public.postgis_full_version();'
podman exec -u postgres rffusion-postgres psql -X -U postgres -d RFFUSION -c "SELECT table_schema, count(*) FROM information_schema.tables WHERE table_schema IN ('BPDATA','RFDATA','RFFUSION_SUMMARY','WEBFUSION') AND table_type = 'BASE TABLE' GROUP BY table_schema ORDER BY table_schema;"
```

O Summary deve conter onze tabelas. A recriação não inicia workers, não executa
reconcile e não migra dados MariaDB nem consultas dos consumidores.

## Containers existentes e persistência

O volume `rffusion-postgres-data` conserva o cluster ao recriar o pod. O caminho
físico depende do armazenamento do Podman, não do diretório do repositório.
No host, execute como o usuário proprietário dos containers antes do deploy:

```bash
podman info --format '{{.Store.GraphRoot}}'
df -h /home
```

Depois de criar o volume, confira seu caminho e a capacidade do filesystem:

```bash
podman volume inspect rffusion-postgres-data --format '{{.Mountpoint}}'
df -h "$(podman volume inspect rffusion-postgres-data --format '{{.Mountpoint}}')"
```

Em produção rootless, confirme que o armazenamento está sob `/home` e tem espaço
para dados, índices e WAL. Não execute o deploy como root para esse stack.
O diretório `/home` do container do appCataloga não representa o filesystem do host.

Para usar um diretório explícito no host, informe `RFFUSION_POSTGRES_DATA_DIR`
com caminho absoluto, ou registre o caminho em uma única linha no arquivo local
`.env/postgres-data-dir`, ignorado pelo Git. A variável tem prioridade sobre o
arquivo. Em produção, `/var/lib/rffusion/postgres-data` permite usar o filesystem
de `/var` sem colocar dados no repositório. Prepare esse diretório no host com
propriedade do usuário e do grupo principal do Podman e modo 700 antes da primeira cópia; o container
ajusta a propriedade interna do cluster. No DEV que já usa `.env/postgres-data`, mantenha a seleção
em toda execução, inclusive na recriação:

```bash
RFFUSION_POSTGRES_DATA_DIR="$PWD/.env/postgres-data" POSTGRES_DB=postgres \
    install/postgres/linux/deploy-postgres.sh --recreate
```

Nesse exemplo, `postgres` é o banco usado pelo bootstrap antigo; selecione o banco
real da instalação. O banco de ensaio `rffusion_test_migracao` é independente.
Se o diretório legado do repositório contiver `PG_VERSION` e o caminho não for
informado, o deploy aborta antes de alterar containers. Ele não transfere esse
cluster de volta ao volume nomeado.

Com um diretório explícito vazio e um volume `rffusion-postgres-data` existente,
`--recreate` ainda pode copiar o volume com o servidor parado, preservando dono e
permissões. A cópia não sobrescreve dados e mantém o volume para retorno. Esse
fluxo é opcional; o padrão de produção usa diretamente o volume.

Se o diretório foi preparado por um administrador, tanto o usuário quanto o
grupo devem pertencer à conta rootless. Em um shell root, `id -u` e `id -g`
retornam os IDs do root. Use a identidade explícita do operador; por exemplo,
`chown 'lx.svc.fi.sensores.pd:' /var/lib/rffusion/postgres-data` atribui o dono e
seu grupo principal, sem recursão. Um grupo do host sem mapeamento no namespace
pode impedir o `chown` do entrypoint. A cópia inicial confere dono, grupo e modo
da raiz e rejeita divergências mesmo quando `cp` retornar sucesso.

O bind mount recebe `:z` e o cluster pertence ao usuário `postgres` do container,
com modo 700. Não edite seus arquivos com o servidor em execução. Os volumes
`rffusion-postgres-ssh`, `rffusion-pgadmin-data` e `rffusion-pgadmin-ssh` guardam
as chaves de host SSH e a configuração do pgAdmin. Preserve os volumes e valide
um backup antes de alterar uma instalação existente.
Um container criado antes deste pod pode ocupar o IP e a porta 9084; o deploy
não o remove. Sua substituição exige planejamento e verificação dos volumes.
A recriação também altera a senha SSH para o valor do novo deploy.

Se houver falha depois de criar o pod, examine os logs e o estado dos containers
e volumes antes de recuperar a instalação; o instalador não substitui recursos
existentes automaticamente. Os scripts SQL e suas limitações estão em
[src/postgres/scripts](../../src/postgres/scripts/README.md).
