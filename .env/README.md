# Configuração local do ambiente

Esta pasta identifica se os containers do RF.Fusion pertencem à produção
ou ao desenvolvimento. Preencha a configuração separadamente em cada host,
no repositório usado pelos scripts de deploy.

## Como preencher

Na raiz do repositório, copie o modelo se ainda não existir configuração local:

```bash
cp -n .env/runtime.env.example .env/runtime.env
chmod 600 .env/runtime.env
```

Edite `.env/runtime.env` e substitua `CHANGE_ME` por **um** dos valores abaixo.

Em produção:

```dotenv
RFFUSION_ENV=production
```

Em desenvolvimento:

```dotenv
RFFUSION_ENV=development
```

Use a chave uma única vez, sem aspas, sem `export` e sem comentário na mesma
linha. Linhas vazias e comentários em linhas próprias são aceitos. Outras
chaves são rejeitadas. O modelo com `CHANGE_ME` não permite fazer deploy.

Somente este README e `runtime.env.example` são versionados. O arquivo
`runtime.env` e os demais arquivos locais desta pasta são ignorados pelo Git.
Ao copiar ou clonar o projeto para outra VM, preencha novamente o ambiente;
não reutilize automaticamente a configuração da VM de origem.

## Resultado nos três containers

| Componente | Nome no Podman | Hostname em produção | Hostname em desenvolvimento |
|---|---|---|---|
| appCataloga | `debian12-python` | `appcataloga-prod` | `appcataloga-dev` |
| WebFusion | `rffusion-web` | `webfusion-prod` | `webfusion-dev` |
| MariaDB | `debian12-mariadb` | `mariadb-prod` | `mariadb-dev` |
| PostgreSQL | `rffusion-postgres` | `postgres-prod` | `postgres-dev` |

Ao entrar por SSH ou abrir Bash com `podman exec -it`, o prompt mostra
`[PRODUÇÃO]` em vermelho ou `[DEV]` em verde, seguido do usuário, hostname e
diretório. Terminais sem cores mantêm a identificação por texto.
As labels do Podman e os logs de inicialização também identificam o ambiente.

## Validação antes do deploy

É possível validar o preenchimento sem criar ou reiniciar containers:

```bash
bash -c 'source install/common/environment.sh; rffusion_load_environment "$PWD" appcataloga'
bash -c 'source install/common/environment.sh; rffusion_load_environment "$PWD" webfusion'
bash -c 'source install/common/environment.sh; rffusion_load_environment "$PWD" mariadb'
bash -c 'source install/common/environment.sh; rffusion_load_environment "$PWD" postgres'
```

Os deploys usam essa validação antes de executar comandos Podman ou
modificar containers. Eles leem o arquivo a partir de `repoRoot`, definido
nos respectivos scripts, que deve apontar para este repositório no host.

## Ativação e containers existentes

O preenchimento da pasta prepara os próximos deploys. **Ele não modifica
containers existentes.** As variáveis, labels e hostname são definidos quando
o container é criado. Editar o arquivo ou reiniciar um container antigo não
atualiza essa identidade.

Os procedimentos de deploy estão nos READMEs de
[appCataloga](../install/appCataloga/README.md),
[WebFusion](../install/webserver/README.MD) e
[MariaDB](../install/mariaDB/README.md) e
[PostgreSQL](../install/postgres/README.md).

**Não execute o deploy do MariaDB apenas para aplicar esta identificação em
produção.** O deploy atual remove o container anterior e não reutiliza
explicitamente seu volume de dados. A ativação no MariaDB existente precisa
ser planejada junto da preservação e recuperação do banco, antes da recriação.

## Como conferir depois da ativação

No host, como o usuário proprietário dos containers:

```bash
podman ps --format 'table {{.Names}}\t{{.Labels}}'
podman inspect --format '{{.Config.Hostname}}' debian12-python
podman exec debian12-python cat /etc/rffusion-environment
podman exec rffusion-web cat /etc/rffusion-environment
podman exec debian12-mariadb cat /etc/rffusion-environment
podman exec -it debian12-python bash
```

As labels são `io.rffusion.environment` e `io.rffusion.component`.

Esta configuração identifica o ambiente; conexões com banco, Zabbix e
compartilhamentos continuam configuradas nos arquivos próprios de cada
componente. Não coloque credenciais neste modelo. O contrato comum está em
[AMBIENTES.md](../docs/architecture/AMBIENTES.md).
