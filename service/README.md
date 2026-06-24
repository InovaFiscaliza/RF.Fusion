# Servico De Inicializacao Do RF.Fusion

Este diretorio contem os arquivos usados para subir o stack de containers do
RF.Fusion automaticamente via `systemd`.

O servico nao faz deploy de containers. Ele apenas:

- garante a montagem de `/mnt/reposfi`
- inicia os containers ja existentes
- sobe os workers internos do `appCataloga`
- para o stack em ordem no desligamento

## Arquivos Principais

- [rffusion-start.sh](./rffusion-start.sh)
- [rffusion-stop.sh](./rffusion-stop.sh)
- [rffusion-containers.service](./rffusion-containers.service)
- [install-service.sh](./install-service.sh)

## Ordem De Inicializacao

O fluxo atual e:

1. validar ou montar `/mnt/reposfi`
2. iniciar `debian12-mariadb`
3. aguardar o MariaDB responder
4. iniciar `debian12-python`
5. executar `tool_start_all.sh` dentro do container do `appCataloga`
6. iniciar `rffusion-web`

## Pre-requisitos

Antes de instalar o servico no host:

1. os containers do projeto ja devem estar implantados
2. o arquivo `/root/.reposfi` deve existir com as credenciais CIFS
3. `cifs-utils` deve estar instalado no host
4. o repositorio deve estar disponivel no host

Formato esperado de `/root/.reposfi`:

```text
username=mnt.sfi.sensores.pd
password=<SENHA>
```

Permissao recomendada:

```bash
chmod 600 /root/.reposfi
```

## Como Instalar O Servico

Execute como `root`, diretamente no host:

```bash
bash /RFFusion-dev/RF.Fusion/service/install-service.sh
```

Se o repositorio estiver em outro caminho:

```bash
bash /caminho/para/RF.Fusion/service/install-service.sh /caminho/para/RF.Fusion
```

O script:

1. ajusta permissao de execucao dos scripts
2. substitui o placeholder `__SCRIPTS_DIR__` no arquivo `.service`
3. instala a unit em `/etc/systemd/system/`
4. executa `systemctl daemon-reload`
5. habilita o servico no boot

## Comandos Uteis

```bash
systemctl start rffusion-containers
systemctl stop rffusion-containers
systemctl status rffusion-containers
journalctl -u rffusion-containers -f
journalctl -u rffusion-containers -b
podman ps
```

## Caminhos Operacionais Importantes

O script de inicializacao usa o seguinte caminho dentro do container do
`appCataloga`:

```text
/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell/tool_start_all.sh
```

Esse detalhe e importante porque os scripts operacionais do `appCataloga`
ficam hoje em `shell/`.

## Troubleshooting

### Servico falhou no boot

```bash
systemctl status rffusion-containers
journalctl -u rffusion-containers -b --no-pager
```

### CIFS nao montou

```bash
ping reposfi
mount -t cifs -o credentials=/root/.reposfi,uid=987,gid=983,file_mode=0666,dir_mode=0777 \
    //reposfi/sfi$/SENSORES /mnt/reposfi
```

### Workers do appCataloga nao subiram

```bash
podman logs debian12-python
podman exec -it debian12-python bash
bash /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell/tool_start_all.sh
```

### MariaDB demorou para responder

```bash
podman logs debian12-mariadb
```

Se necessario, ajuste `MARIADB_READY_TIMEOUT` em [rffusion-start.sh](./rffusion-start.sh).
