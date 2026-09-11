# APIs do WebFusion

As interfaces compartilhadas estão organizadas por responsabilidade:

| Pacote | Responsabilidade | URLs públicas |
|---|---|---|
| [map_api](map_api/README.md) | Mapa comum ao navegador e ao appAnalise | `/rffusion/api/map/...` |
| [appAnalise_api](appAnalise_api/README.md) | Consultas de catálogo e arquivos para o appAnalise | `/rffusion/api/appanalise/...` |
| [zabbix_api](zabbix_api/README.md) | Cliente da API externa do Zabbix | Não registra rotas HTTP próprias |

`app.py` registra os blueprints. A página inicial também chama o serviço do
mapa diretamente, sem uma requisição HTTP interna. Módulos de configuração e
alarmes usam o cliente do Zabbix por `api.zabbix_api`.

Não crie cópias das consultas ou da montagem do mapa em APIs de consumidores.
Use `map_api`, inclusive quando o consumidor for o MATLAB. As demais APIs de
funcionalidade já existentes em `modules/` permanecem junto de seus módulos;
esta reorganização abrange os três pacotes acima.

O caminho do segredo Zabbix continua sendo
`src/zabbix/.secret/zabbix_api.env`; a mudança do pacote não desloca segredos.
Não houve mudança nas permissões, no banco ou na configuração do nginx.

## Validação

No container webserver, com `/usr/local/bin/python`:

```bash
cd /RF.Fusion
/usr/local/bin/python -m unittest \
  test.tests.webfusion.test_appanalise_api \
  test.tests.webfusion.test_map_api \
  test.tests.webfusion.test_map_service \
  test.tests.webfusion.test_app_headers \
  test.tests.webfusion.test_configuration_service \
  test.tests.webfusion.test_alarms_service \
  test.tests.zabbix.test_zabbix_api
```

A aplicação precisa ser recarregada para usar os novos imports. A rota
duplicada `/rffusion/api/appanalise/map` foi removida; use o mapa compartilhado.
