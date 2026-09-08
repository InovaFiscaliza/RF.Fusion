# Servico De Inicializacao Do RF.Fusion

Este diretorio contem os arquivos usados para subir o stack de containers do
RF.Fusion automaticamente via `systemd`.

O servico nao faz deploy de containers. Ele apenas:

- garante a montagem de `/mnt/reposfi`
- inicia os containers ja existentes
- sobe os workers internos do `appCataloga`
- reconfigura a chave SSH restrita usada pelo painel de saude
- para o stack em ordem no desligamento

## Arquivos Principais

- [rffusion-start.sh](./rffusion-start.sh)
- [rffusion-stop.sh](./rffusion-stop.sh)
- [rffusion-runtime-health-bootstrap.sh](./rffusion-runtime-health-bootstrap.sh)
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
7. garantir a chave SSH de saude e o arquivo `known_hosts` no WebFusion

## Pre-requisitos

Antes de instalar o servico no host:

1. os containers do projeto ja devem estar implantados
2. o arquivo `/root/.reposfi` deve existir com as credenciais CIFS
3. `cifs-utils` deve estar instalado no host
4. o repositorio deve estar disponivel no host
5. informar o usuario que criou os containers rootless do Podman

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

Execute como `root`, diretamente no host, informando o usuario dono dos
containers. No ambiente atual, esse usuario e `lx.svc.fi.sensores.pd`:

```bash
bash /RFFusion-dev/RF.Fusion/service/install-service.sh \
    /RFFusion-dev/RF.Fusion lx.svc.fi.sensores.pd
```

Se o repositorio estiver em outro caminho:

```bash
bash /caminho/para/RF.Fusion/service/install-service.sh \
    /caminho/para/RF.Fusion usuario-do-podman
```

O script:

1. ajusta permissao de execucao dos scripts
2. substitui o placeholder `__SCRIPTS_DIR__` no arquivo `.service`
3. instala a unit em `/etc/systemd/system/`
4. executa `systemctl daemon-reload`
5. prepara `/RFFusion-dev/.secrets` com permissao `0700` para o usuario do Podman
6. habilita o `linger` do usuario do Podman para o runtime rootless existir no boot
7. habilita o servico no boot

A unit permanece sendo uma unit de sistema para aguardar rede e a montagem
CIFS, mas executa `podman` como o usuario informado. Isso garante que ela veja
os mesmos containers exibidos por `podman ps -a` nesse usuario.

## Saude SSH Automatica

Em cada inicializacao pelo `rffusion-containers`, o script
`rffusion-runtime-health-bootstrap.sh` executa depois que todos os containers
estao ativos. Ele cria ou reutiliza uma chave exclusiva em
`/RFFusion-dev/.secrets`, instala a chave publica com comando SSH restrito no
appCataloga e no MariaDB, atualiza o `known_hosts` e copia a chave privada e o
`known_hosts` para o WebFusion.

Isso recupera o painel de saude apos reiniciar a VM, sem recriar os containers
existentes. O diretorio pode ser alterado no host com
`RFFUSION_RUNTIME_HEALTH_SECRETS_DIR`.

O deploy do WebFusion tambem chama esse bootstrap depois de recriar somente o
container web. Assim, essa recriacao nao exige uma etapa manual adicional para
o painel de saude.

Para aplicar a configuracao imediatamente, com os containers ja ativos:

```bash
bash /caminho/para/RF.Fusion/service/rffusion-runtime-health-bootstrap.sh
```

O bootstrap nao expoe a chave privada ao appCataloga ou ao MariaDB. O WebFusion
recebe apenas uma copia local com permissao `0600`, e os dois containers remotos
aceitam essa chave somente para executar seus scripts de saude em modo leitura.

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
