# Container do appCataloga

Este diretorio contem o material de deploy do container Linux usado pelo
runtime do `appCataloga`.

O objetivo desse container e fornecer o ambiente de execucao do modulo
operacional do RF.Fusion, com:

- sistema base Debian 12
- ambiente Conda
- acesso SSH
- repositorio do projeto montado em `/RFFusion`
- repositorio compartilhado montado em `/mnt/reposfi`

Importante: esse container prepara o ambiente, mas nao sobe sozinho todo o
pipeline do `appCataloga` no entrypoint.

## Arquivos Principais

- [Containerfile](./linux/Containerfile)
- [docker-entrypoint.sh](./linux/docker-entrypoint.sh)
- [environment.yml](./linux/environment.yml)
- [deploy-debian12-python.sh](./linux/deploy-debian12-python.sh)

## O Que O Container Faz

Depois do deploy, o container entrega:

- imagem `debian12-python`
- container `debian12-python`
- IP `10.88.0.2` na rede `podman`
- SSH publicado na porta `2828`
- porta `5555` publicada para a aplicacao

O runtime real do `appCataloga` continua vindo do repositorio montado em
`/RFFusion`, especialmente de:

- [src/appCataloga/server_volume/usr/local/bin/appCataloga](../../src/appCataloga/server_volume/usr/local/bin/appCataloga)

## O Que O Container Nao Faz

O entrypoint do container nao inicializa automaticamente todos os workers do
`appCataloga`.

Ele apenas:

- prepara o ambiente
- configura SSH
- aplica a senha de root
- valida o `sshd`
- entrega o processo principal do container

Para operacao automatica do stack completo, veja:

- [service/README.md](../../service/README.md)

## Pre-requisitos No Host

O script atual de deploy assume:

- repositorio do projeto em `/RFFusion-dev/RF.Fusion`
- repositorio compartilhado montado em `/mnt/reposfi`
- `podman` instalado e funcional
- rede `podman` previamente existente

Observacao importante:

- o caminho `/RFFusion-dev/RF.Fusion` esta hardcoded em `deploy-debian12-python.sh`

## Como Fazer O Deploy

Entre no diretorio de deploy:

```bash
cd /RFFusion/install/appCataloga/linux
```

Garanta permissao de execucao:

```bash
chmod +x *.sh
```

Execute o deploy:

```bash
./deploy-debian12-python.sh
```

## O Que O Script De Deploy Faz

O script:

1. seleciona o contexto do Podman
2. valida os arquivos obrigatorios
3. gera a imagem `debian12-python`
4. remove o container anterior, se existir
5. cria o novo container com volumes e portas
6. verifica se o container entrou em estado `running`

## Volumes Montados

O deploy monta:

- `/RFFusion-dev/RF.Fusion -> /RFFusion:Z`
- `/mnt/reposfi -> /mnt/reposfi`

Observacao:

- `/mnt/reposfi` nao usa `:Z` porque esse filesystem nao suporta o ajuste esperado pelo script

## Acesso Ao Container

Depois do deploy, os acessos esperados sao:

- SSH: `ssh root@localhost -p 2828`
- Aplicacao: `http://localhost:5555/`
- Shell interativo: `podman exec -it debian12-python bash`

## Operacao Manual Dos Workers

Os scripts operacionais versionados ficam em:

- [src/appCataloga/server_volume/usr/local/bin/appCataloga/shell](../../src/appCataloga/server_volume/usr/local/bin/appCataloga/shell)

Os principais sao:

- `tool_start_all.sh`
- `tool_status_all.sh`
- `tool_stop_all.sh`

Exemplo:

```bash
cd /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell
./tool_start_all.sh
./tool_status_all.sh
./tool_stop_all.sh
```

## Observacoes Operacionais

- O script de deploy e a fonte de verdade para esse container.
- O container e um ambiente de runtime, nao um orquestrador completo do sistema.
- Se a estrutura de volumes, portas ou caminho do repositorio mudar, o script
  `deploy-debian12-python.sh` deve ser revisado junto com esta documentacao.
