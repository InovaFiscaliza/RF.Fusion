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

`cliente -> nginx:80/rffusion/ -> webfusion:127.0.0.1:8000`

A raiz direta `/` nao publica conteudo e responde com HTTP 404, mesmo que a
imagem base contenha arquivos de boas-vindas fornecidos pelos pacotes do Nginx.

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
- o volume `/mnt/reposfi` é montado em modo somente leitura, com uma montagem
  adicional gravável apenas em `/mnt/reposfi/upload`
- o usuário do deploy e a conta do compartilhamento CIFS precisam permitir
  criação e escrita de arquivos nessa subpasta

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

Use o mesmo usuário que administra o container no Podman rootless. Não use
`sudo podman`: ele consulta os containers do root, não os do seu usuário.
Este script recompila a imagem e recria o container; não o execute apenas
para ajustar a montagem de uma instância em funcionamento.

```bash
./deploy-rffusionweb.sh
```

## O Que O Script De Deploy Faz

O script:

1. valida os caminhos, a montagem do repositório e a escrita em `upload` no host
2. normaliza line endings de arquivos `.sh`
3. recompila a imagem `rffusion-web`
4. remove o container anterior, se existir
5. cria um novo container com rede, portas e volumes
6. valida se o container entrou em estado `running` e testa escrita em `upload`
7. testa o endpoint `/api/server/runtime-health` e a porta SSH publicada

## Volumes Montados

O deploy monta:

- `/RFFusion-dev/RF.Fusion -> /RF.Fusion:Z`
- `/mnt/reposfi -> /mnt/reposfi:ro`
- `/mnt/reposfi/upload -> /mnt/reposfi/upload:rw`

A montagem de `upload` fica registrada na configuração do novo container e é
restaurada quando ele reinicia. Os testes de escrita criam arquivos temporários
com nomes exclusivos e os removem ao concluir. O nginx bloqueia a publicação
dessa pasta pelas rotas de download e permite até 513 MiB por requisição na API
de upload (512 MiB de arquivo mais o envelope multipart).

## Ajuste temporário sem recriar o container

Para restaurar a escrita no container já iniciado, execute na VM como o usuário
proprietário dos containers:

```bash
bash /RFFusion-dev/RF.Fusion/install/webserver/ensure-upload-mount.sh
```

Para reiniciar uma instância antiga e restaurar a montagem em seguida:

```bash
bash /RFFusion-dev/RF.Fusion/install/webserver/restart-webfusion.sh
```

Esse segundo comando reinicia o container existente, sem recriá-lo. O primeiro
somente verifica/restaura a montagem e testa escrita com um arquivo temporário.
O serviço `service/rffusion-start.sh` também chama o helper ao iniciar o stack.
Um `podman restart` direto, ou um reinício automático independente desse serviço,
não executa o helper; nesses casos, rode `ensure-upload-mount.sh` depois.
Para cobrir qualquer reinício sem helper, use a montagem persistente do próximo
deploy completo.

Para uma instância antiga, o procedimento abaixo foi validado no ambiente
rootless. Execute na VM, com o usuário proprietário do container. A montagem
vale apenas enquanto esse container continuar em execução; após reiniciá-lo,
reaplique o ajuste. O próximo deploy completo usa os volumes persistentes acima.

```bash
(
    set -eu
    web_pid=$(podman inspect --format '{{.State.Pid}}' rffusion-web)
    test "$web_pid" -gt 1

    podman unshare nsenter \
        --target "$web_pid" --mount --pid \
        --root="/proc/$web_pid/root" \
        --wd="/proc/$web_pid/root" \
        /bin/sh -eu -c '
            if ! mountpoint -q /mnt/reposfi/upload; then
                mount --bind /mnt/reposfi/upload /mnt/reposfi/upload
            fi
            mount -o remount,bind,rw /mnt/reposfi/upload
            findmnt -T /mnt/reposfi -o TARGET,VFS-OPTIONS
            findmnt -T /mnt/reposfi/upload -o TARGET,VFS-OPTIONS
        '

    podman exec rffusion-web /bin/sh -eu -c '
        cd /
        probe=$(mktemp /mnt/reposfi/upload/.teste-escrita.XXXXXX)
        printf "teste de escrita\n" > "$probe"
        rm -- "$probe"
        echo "Escrita confirmada no WebFusion."
    '
)
```

O resultado esperado é `/mnt/reposfi` com `ro` e `/mnt/reposfi/upload` com `rw`.
Esse ajuste trata somente a montagem, sem atualizar o código ou o nginx.

Observacao:

- o codigo da aplicacao fica montado em `/RF.Fusion`, sem o segundo `F`

## Acesso Ao Container

Depois do deploy, os acessos esperados sao:

- Web: `http://127.0.0.1:9082/rffusion/`
- Healthcheck: `http://127.0.0.1:9082/rffusion/api/server/runtime-health`
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
