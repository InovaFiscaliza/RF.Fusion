# RFFusion — Serviço de Inicialização Automática

Arquivos para configurar o stack de containers como serviço systemd na VM Red Hat host (`172.16.18.11`), de forma que tudo suba automaticamente após um reboot.

## Arquitetura

```
VM Red Hat (172.16.18.11)
├── /mnt/reposfi          ← CIFS share (//reposfi/sfi$/SENSORES)
└── Podman (rootful)
    ├── debian12-mariadb  ← MariaDB  (porta 9081)
    ├── debian12-python   ← appCataloga (porta 2828)
    └── rffusion-web      ← webfusion  (porta 9082)
```

A ordem de inicialização é: **CIFS mount → MariaDB → appCataloga → workers internos → webfusion**

## Arquivos

| Arquivo | Descrição |
|---|---|
| `rffusion-start.sh` | Inicia o stack em ordem. Chamado pelo systemd no boot. |
| `rffusion-stop.sh` | Para o stack em ordem inversa. Chamado pelo systemd no shutdown. |
| `rffusion-containers.service` | Unit systemd (template). Contém o placeholder `__SCRIPTS_DIR__`. |
| `install-service.sh` | Script de instalação. Substitui o placeholder e registra o serviço. |

## Pré-requisitos

Executar **uma única vez** no host antes de instalar o serviço:

1. **Containers já implantados** — os scripts `install/*/deploy-*.sh` devem ter sido executados. O serviço apenas faz `podman start`; não cria nem reconstrói containers.

2. **Credenciais CIFS presentes** — o arquivo `/root/.reposfi` deve existir no host com o seguinte formato:
   ```
   username=mnt.sfi.sensores.pd
   password=<SENHA>
   ```
   Permissão correta: `chmod 600 /root/.reposfi`

   > ⚠️ Nunca recrie este arquivo via script — isso sobrescreve a senha armazenada.

3. **`cifs-utils` instalado** no host:
   ```bash
   dnf install -y cifs-utils
   ```

## Instalação

Executar como **root** diretamente no host VM (não dentro de um container):

```bash
bash /RFFusion-dev/RF.Fusion/service/install-service.sh
```

Se o repositório estiver em outro caminho no host, passe como argumento:

```bash
bash /caminho/para/RF.Fusion/service/install-service.sh /caminho/para/RF.Fusion
```

O script realiza:
- `chmod +x` nos scripts de start/stop
- Substitui `__SCRIPTS_DIR__` pelo caminho real no `.service`
- Copia o unit para `/etc/systemd/system/`
- Executa `systemctl daemon-reload` e `systemctl enable`

Para iniciar imediatamente sem precisar reiniciar a VM:

```bash
systemctl start rffusion-containers
```

## Comandos úteis

| Ação | Comando |
|---|---|
| Iniciar stack | `systemctl start rffusion-containers` |
| Parar stack | `systemctl stop rffusion-containers` |
| Status do serviço | `systemctl status rffusion-containers` |
| Logs em tempo real | `journalctl -u rffusion-containers -f` |
| Logs completos do boot | `journalctl -u rffusion-containers -b` |
| Desabilitar autostart | `systemctl disable rffusion-containers` |
| Verificar containers | `podman ps` |

## Comportamento no boot

1. Systemd aguarda `network-online.target` e `remote-fs.target` (rede e mounts remotos prontos)
2. `rffusion-start.sh` verifica se `/mnt/reposfi` está montado; monta via CIFS se necessário
3. Se a montagem falhar → serviço para com erro (visível em `systemctl status`)
4. MariaDB é iniciado; script aguarda até `mysqladmin ping` responder (timeout: 120s)
5. appCataloga container é iniciado; após estabilizar, `tool_start_all.sh` é executado dentro do container para subir todos os workers internos
6. webfusion é iniciado por último

## Troubleshooting

**Serviço falhou no boot:**
```bash
systemctl status rffusion-containers
journalctl -u rffusion-containers -b --no-pager
```

**CIFS não monta:**
```bash
# Verificar conectividade com o servidor de arquivos
ping reposfi

# Testar montagem manualmente
mount -t cifs -o credentials=/root/.reposfi,uid=987,gid=983,file_mode=0666,dir_mode=0777 \
    //reposfi/sfi$/SENSORES /mnt/reposfi
```

**Workers do appCataloga não subiram:**
```bash
podman logs debian12-python
podman exec -it debian12-python bash
# dentro do container:
bash /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_start_all.sh
```

**MariaDB demorou mais que 120s:**
```bash
podman logs debian12-mariadb
# Aumentar MARIADB_READY_TIMEOUT em rffusion-start.sh se necessário
```
