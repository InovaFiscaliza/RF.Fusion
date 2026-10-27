# Identificação dos ambientes de execução

## Escopo

Este contrato define a identificação de produção e desenvolvimento dos três
containers: appCataloga, WebFusion e MariaDB. A configuração pertence ao host
do deploy e fica em `.env/runtime.env`, fora do Git. O procedimento de
preenchimento está no [README da configuração](../../.env/README.md).

## Fonte e validação

- A única chave é `RFFUSION_ENV`, com valor `production` ou `development`.
- Os deploys leem o arquivo do repositório que será montado no container,
  usando `install/common/environment.sh`. Não executam o arquivo como shell.
- Configuração ausente, inválida, duplicada ou com chaves desconhecidas
  interrompe o deploy antes de comandos Podman, builds ou substituições.
- Não inferir o ambiente por IP, caminho do repositório, branch ou hostname.
- Não definir produção ou desenvolvimento como padrão silencioso.

## Identidade dos containers

Os nomes dos containers usados pelo Podman e pelo serviço permanecem
`debian12-python`, `rffusion-web` e `debian12-mariadb`.
O deploy define hostname, variáveis e labels a partir do ambiente validado:

| Componente | Produção | Desenvolvimento |
|---|---|---|
| appCataloga | `appcataloga-prod` | `appcataloga-dev` |
| WebFusion | `webfusion-prod` | `webfusion-dev` |
| MariaDB | `mariadb-prod` | `mariadb-dev` |

As variáveis `RFFUSION_ENV` e `RFFUSION_COMPONENT` identificam o container.
Os componentes são `appcataloga`, `webfusion` e `mariadb`. As labels
`io.rffusion.environment` e `io.rffusion.component` reproduzem esses valores.

Os entrypoints executam `install/common/container-identity.sh` pelo repositório
montado, antes de iniciar serviços. O helper valida as variáveis, registra a
identidade no log de inicialização e em `/etc/rffusion-environment`, e configura
o prompt Bash: `[PRODUÇÃO]` vermelho ou `[DEV]` verde. Terminais sem cores
mantêm o texto. Shells SSH e `podman exec -it ... bash` usam a mesma identidade.

## Ciclo de vida e limites

O ambiente fica fixado na criação do container. Editar `.env/runtime.env`
não altera containers existentes; reiniciar também não muda suas variáveis,
labels ou hostname. A ativação segue o procedimento de deploy de cada
componente, com atenção à persistência do MariaDB.

A identificação não altera portas, IPs, conexões, permissões, schemas ou tarefas.
Credenciais e destinos de banco, Zabbix e compartilhamento continuam nos
mecanismos próprios de cada componente. Esta pasta não substitui esses arquivos.
