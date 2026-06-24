# Container Web do RF.Fusion

Este diretorio contem o material de deploy do container `rffusion-web`,
responsavel por publicar a interface `webfusion`.

O objetivo desse container e expor a camada web do RF.Fusion em um unico
runtime, reunindo:

- `nginx`
- `webfusion`
- `sshd`

## Arquivos Principais

- [Containerfile](./Containerfile)
- [docker-entrypoint.sh](./docker-entrypoint.sh)
- [nginx-webfusion.conf](./nginx-webfusion.conf)
- [deploy-rffusionweb.sh](./deploy-rffusionweb.sh)

## Arquitetura Interna

O container sobe tres componentes:

1. `webfusion`
   - executado por `python3 /RF.Fusion/src/webfusion/app.py`
   - escuta internamente em `127.0.0.1:8000`

2. `nginx`
   - escuta na porta `80` do container
   - faz proxy para o `webfusion`
   - entrega downloads do repositorio via `X-Accel-Redirect`

3. `sshd`
   - mantido para acesso operacional ao container

Fluxo HTTP:

`cliente -> nginx:80 -> webfusion:127.0.0.1:8000`

Fluxo de download:

`cliente -> nginx -> /mnt/reposfi`

## O Que O Container Faz

Depois do deploy, o ambiente entrega:

- imagem `rffusion-web`
- container `rffusion-web`
- IP `10.88.0.34` na rede `podman`
- interface web publicada na porta `9082` do host
- SSH publicado na porta `2225` do host

## Pre-requisitos No Host

O deploy atual assume:

- repositorio do projeto em `/RFFusion-dev/RF.Fusion`
- repositorio `/mnt/reposfi` montado no host
- `podman` instalado e funcional
- rede `podman` previamente existente

Observacoes importantes:

- o caminho `/RFFusion-dev/RF.Fusion` esta hardcoded em `deploy-rffusionweb.sh`
- o volume `/mnt/reposfi` e montado em modo somente leitura

## Como Fazer O Deploy

Entre no diretorio de deploy:

```bash
cd /RFFusion/install/webserver
```

Garanta permissao de execucao:

```bash
chmod +x *.sh
```

Execute o deploy:

```bash
./deploy-rffusionweb.sh
```

## O Que O Script De Deploy Faz

O script:

1. valida os caminhos esperados no host
2. normaliza line endings de arquivos `.sh`
3. recompila a imagem `rffusion-web`
4. remove o container anterior, se existir
5. cria um novo container com rede, portas e volumes
6. valida se o container entrou em estado `running`
7. testa o endpoint `/health` e a porta SSH publicada

## Volumes Montados

O deploy monta:

- `/RFFusion-dev/RF.Fusion -> /RF.Fusion:Z`
- `/mnt/reposfi -> /mnt/reposfi:ro`

Observacao:

- o codigo da aplicacao fica montado em `/RF.Fusion`, sem o segundo `F`

## Acesso Ao Container

Depois do deploy, os acessos esperados sao:

- Web: `http://127.0.0.1:9082`
- Healthcheck: `http://127.0.0.1:9082/health`
- SSH: `ssh root@127.0.0.1 -p 2225`

## Observacoes Operacionais

- O script de deploy e a fonte de verdade para esse container.
- O container usa `--restart unless-stopped`.
- Se `webfusion` ou `nginx` encerrar, o container termina.
- Alteracoes em `Containerfile`, `docker-entrypoint.sh`,
  `nginx-webfusion.conf` ou no codigo de `src/webfusion/` exigem recriacao do
  container via novo deploy.
- Os logs da aplicacao e do runtime podem ser inspecionados com:

```bash
podman logs -f rffusion-web
```
