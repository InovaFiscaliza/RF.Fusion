# APIs compartilhadas do WebFusion

Este diretório reúne o mapa compartilhado, o contrato de consulta do appAnalise
e o cliente Python da API externa do Zabbix. Os blueprints HTTP são registrados
em [app.py](../app.py); a organização por pacote não altera as URLs públicas.

| Pacote | Responsabilidade | Interface |
|---|---|---|
| [map_api](map_api/README.md) | Mapa comum à interface web e ao appAnalise | GET `/rffusion/api/map/...` |
| [appAnalise_api](appAnalise_api/README.md) | Catálogos, arquivos, espectros, contagem e estatísticas | `/rffusion/api/appanalise/...` |
| [zabbix_api](zabbix_api/README.md) | Cliente JSON-RPC de hosts, templates, macros e problemas | Chamadas Python; não registra rotas HTTP |

## Endereçamento

Escolha a base conforme a rede de acesso:

| Acesso | Base da aplicação |
|---|---|
| F5 | `https://fiscalizacao.anatel.gov.br/rffusion` |
| VPN/rede interna por IP | `http://172.16.18.11:9082/rffusion` |
| VPN/rede interna por hostname | `http://rhfisnspdex02.anatel.gov.br:9082/rffusion` |

Acrescente `/api/map` ou `/api/appanalise`. Não existe segmento `/v1` no contrato
atual. O NGINX remove `/rffusion` antes de encaminhar ao Flask; os blueprints
usam `/api/...`, sem repetir o prefixo público.

Exemplos GET para navegador ou cliente HTTP:

```text
http://rhfisnspdex02.anatel.gov.br:9082/rffusion/api/map/stations
http://rhfisnspdex02.anatel.gov.br:9082/rffusion/api/map/stations?include_details=true
http://rhfisnspdex02.anatel.gov.br:9082/rffusion/api/appanalise/equipments?stateCode=AM
http://rhfisnspdex02.anatel.gov.br:9082/rffusion/api/appanalise/files/count?equipmentId=135&siteId=85
```

A disponibilidade da URL pública depende das políticas do F5. Estes pacotes
HTTP não implementam uma autenticação própria do cliente MATLAB. A conexão
com o Zabbix tem configuração técnica separada, documentada no seu pacote.

## Responsabilidades

- `map_api` é a única montagem de pontos e detalhes. A página inicial chama
  seu serviço diretamente; o MATLAB usa o endpoint compartilhado.
- `appAnalise_api` aceita operações nomeadas e filtros. SQL e conexões ao
  MariaDB ficam no servidor; não há endpoint para executar SQL arbitrário.
- `zabbix_api` centraliza JSON-RPC e configuração de conexão. Os módulos
  `configuration`, `tasks` e `alarms` mantêm suas regras de funcionalidade.

As APIs específicas da interface permanecem em `modules/`, como `/api/spectrum`,
`/api/server`, `/api/host`, `/api/tasks`, `/api/users` e `/api/configuration`.
A estrutura não exige mover toda rota `/api` para este diretório.

## Contratos e compatibilidade

As respostas têm formatos próprios por API:

| API | Formato principal |
|---|---|
| appAnalise tabular | `columns` e `rows`; datas ISO, nulos JSON |
| Contagem appAnalise | `{"count": 123}` |
| Arquivos appAnalise | Tabela mais `pagination`; até `pageSize + 1` linhas |
| Mapa | `points`, opcionalmente `site_details`; datas dos detalhes em formato HTTP |
| Cliente Zabbix | Objetos Python normalizados, ou `ZabbixApiError` |

A API de mapa não tem a mesma política de erros da API appAnalise: o mapa
pode responder 200 com arrays vazios em uma falha de consulta. Consulte os
READMEs específicos antes de interpretar resultados vazios.

As antigas rotas `/rffusion/api/appanalise/map`, `/map/sites` e `/map/stations`
sob o prefixo appAnalise foram removidas, sem aliases. Use `/api/map/stations`.
O download de binários permanece no NGINX em `/downloads/...`, fora de
`/rffusion`; não é uma operação de transferência destes pacotes.

## Validação

Execute no container webserver, com `/RF.Fusion` como raiz e
`/usr/local/bin/python` como interpretador. Exemplos por contrato:

```bash
cd /RF.Fusion
/usr/local/bin/python -m unittest test.tests.webfusion.test_appanalise_api -q
/usr/local/bin/python -m unittest test.tests.webfusion.test_map_api test.tests.webfusion.test_map_service -q
/usr/local/bin/python -m unittest test.tests.zabbix.test_zabbix_api -q
/usr/local/bin/python -m unittest test.tests.webfusion.test_app_headers -q
```

Mantenha a execução por pacote quando houver stubs de módulos globais nos
unitários. Para conferir consumidores do Zabbix, execute também as suítes
`test_configuration_service`, `test_alarms_service` e `test_task_service` em
`test.tests.webfusion`, conforme a funcionalidade alterada.

Não há necessidade de consultar produção para executar esses testes.
A verificação ponta a ponta inclui acesso pelo NGINX e consumo pelo cliente real.
Mudanças de código requerem recarregar o processo WebFusion pelo procedimento
do ambiente. Alterar somente os READMEs não exige reinício.

Consulte também o [README geral do WebFusion](../README.MD).
