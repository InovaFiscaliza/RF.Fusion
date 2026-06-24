# Container MariaDB

Este diretorio contem o material de deploy do container MariaDB usado pelo
RF.Fusion.

Esse container fornece a camada relacional do projeto, incluindo os schemas:

- `BPDATA`
- `RFDATA`
- `RFFUSION_SUMMARY`

## Arquivos Principais

- [Containerfile](./linux/Containerfile)
- [docker-entrypoint.sh](./linux/docker-entrypoint.sh)
- [deploy-debian12-mariadb.sh](./linux/deploy-debian12-mariadb.sh)

## O Que O Container Faz

Depois do deploy, o ambiente entrega:

- imagem `debian12-mariadb`
- container `debian12-mariadb`
- IP `10.88.0.33` na rede `podman`
- MariaDB publicado na porta `9081` do host
- SSH publicado na porta `2224` do host

O bootstrap do banco acontece em duas etapas:

1. o `docker-entrypoint.sh` inicializa o runtime MariaDB e o acesso SSH
2. o script `deploy-debian12-mariadb.sh` carrega os schemas do RF.Fusion

## Inicializacao Do Banco

Depois que o container entra em execucao, o script de deploy aplica:

- [createProcessingDB.sql](../../src/mariadb/scripts/createProcessingDB.sql)
- [createMeasureDB.sql](../../src/mariadb/scripts/createMeasureDB.sql)
- [createFusionSummaryDB.sql](../../src/mariadb/scripts/createFusionSummaryDB.sql)

Esses scripts criam, respectivamente:

- `BPDATA`
- `RFDATA`
- `RFFUSION_SUMMARY`

## Pre-requisitos No Host

O deploy atual assume:

- repositorio do projeto em `/RFFusion-dev/RF.Fusion`
- `podman` instalado e funcional
- rede `podman` previamente existente

Observacao importante:

- o caminho `/RFFusion-dev/RF.Fusion` esta hardcoded em `deploy-debian12-mariadb.sh`

## Como Fazer O Deploy

Entre no diretorio de deploy:

```bash
cd /RFFusion/install/mariaDB/linux
```

Garanta permissao de execucao:

```bash
chmod +x *.sh
```

Execute o deploy:

```bash
./deploy-debian12-mariadb.sh
```

## O Que O Script De Deploy Faz

O script:

1. seleciona o contexto do Podman
2. recompila a imagem `debian12-mariadb`
3. remove o container anterior, se existir
4. cria um novo container com rede, portas e volume do repositorio
5. valida se o container entrou em estado `running`
6. executa os scripts SQL de inicializacao do RF.Fusion

## Volume Montado

O deploy monta:

- `/RFFusion-dev/RF.Fusion -> /RFFusion:Z`

Esse volume e necessario porque os scripts SQL sao executados a partir do
repositorio montado dentro do container.

## Acesso Ao Container

Depois do deploy, os acessos esperados sao:

- MariaDB: `127.0.0.1:9081`
- SSH: `ssh root@127.0.0.1 -p 2224`

## Observacoes Operacionais

- O script de deploy e a fonte de verdade para esse container.
- O deploy atual nao monta um volume externo persistente para `/var/lib/mysql`.
- Na pratica, recriar o container equivale a refazer o bootstrap do banco.
- Isso pode ser aceitavel em ambientes de rebuild, mas nao substitui uma
  estrategia formal de persistencia externa.
