# Pod PostgreSQL e pgAdmin

O deploy cria o pod `rffusion-postgres` com dois containers: PostgreSQL 17
Alpine e pgAdmin 4, a partir das imagens oficiais. A imagem do PostgreSQL é
estendida para incluir SSH e identificação de ambiente do RF.Fusion; a imagem
do pgAdmin também é estendida para incluir SSH. Os dados do banco e as
configurações do pgAdmin ficam em volumes nomeados separados.

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

O deploy recusa substituir um pod ou container existente. Ele espera o servidor
PostgreSQL responder por TCP e, se nenhum dos quatro schemas do projeto existir,
executa os scripts SQL e carrega os CSVs em uma única transação. Se qualquer
schema já existir, em maiúsculas ou na convenção anterior em minúsculas, a carga
é ignorada e requer revisão manual. Novas instalações preservam maiúsculas em
schemas, tabelas e colunas usando aspas duplas. A recriação do pod com os mesmos
volumes não altera os nomes de uma instalação existente. Falhas preservam
pod e volumes para diagnóstico; confira `podman logs rffusion-postgres`.
Em seguida, o deploy cria o container pgAdmin; confira também
`podman logs rffusion-pgadmin` e abra `http://IP_DA_VM:9084`.

Ambos os acessos SSH usam `root` e a senha inicial `changeme`; o PostgreSQL usa `postgres` e a
mesma senha inicial. O pgAdmin inicia com `admin@example.com` e `changeme`.
Troque as senhas após a criação. Para definir outros valores no deploy, forneça
`SSH_PASSWORD`, `PGADMIN_SSH_PASSWORD`, `POSTGRES_PASSWORD`,
`PGADMIN_DEFAULT_EMAIL` e `PGADMIN_DEFAULT_PASSWORD` pelo ambiente, sem registrá-los no histórico.
A senha SSH alterada dentro de cada container é preservada ao reiniciá-lo, mas uma
recriação do container volta a usar o valor fornecido no deploy. A senha inicial
do PostgreSQL e o login inicial do pgAdmin só são aplicados a volumes vazios.

No pgAdmin, cadastre o servidor com host `127.0.0.1`, porta `5432`, banco
`postgres`, usuário `postgres` e a senha do banco. Os containers compartilham a
rede do pod. O pgAdmin tem credenciais próprias, separadas das do banco.

Os limites desejados do PostgreSQL são 1 CPU, 2 GiB de memória, 3 GiB incluindo
swap e 1024 processos, aplicados conforme os controladores disponíveis. O pgAdmin
não recebe esses limites. A instalação não migra dados MariaDB e os consumidores
existentes ainda não foram adaptados para PostgreSQL.

## Containers existentes e persistência

Os volumes `rffusion-postgres-data`, `rffusion-postgres-ssh`,
`rffusion-pgadmin-data` e `rffusion-pgadmin-ssh` guardam os dados, as chaves de
host SSH separadas e a configuração do pgAdmin. Preserve e valide um backup
antes de alterar uma instalação existente.
Um container criado antes deste pod pode ocupar o IP e a porta 9084; o deploy
não o remove. Sua substituição exige planejamento e verificação dos volumes.
A recriação também altera a senha SSH para o valor do novo deploy.

Se houver falha depois de criar o pod, examine os logs e o estado dos containers
e volumes antes de recuperar a instalação; o instalador não substitui recursos
existentes automaticamente. Os scripts SQL e suas limitações estão em
[src/postgres/scripts](../../src/postgres/scripts/README.md).
