# API compartilhada do mapa

Uma única implementação consulta os summaries, monta os pontos e calcula os
marcadores para o WebFusion e para clientes externos, incluindo o appAnalise.

## Endereço e organização

| Acesso | Base da API |
|---|---|
| F5 | `https://fiscalizacao.anatel.gov.br/rffusion/api/map` |
| VPN/rede interna, IP | `http://172.16.18.11:9082/rffusion/api/map` |
| VPN/rede interna, hostname | `http://rhfisnspdex02.anatel.gov.br:9082/rffusion/api/map` |

O blueprint usa `/api/map`; `/rffusion` é o prefixo acrescentado pelo proxy.
Não há segmento `/v1`. O serviço consulta `MAP_SITE_SUMMARY` e
`MAP_SITE_STATION_SUMMARY`, em `RFFUSION_SUMMARY`, pelas fábricas de conexão
existentes. O dataset é reconstruído a cada chamada, sem cache de processo
neste pacote. A página inicial usa o mesmo serviço para incluir os pontos no HTML.

## Rotas

| GET | Resposta |
|---|---|
| `/rffusion/api/map/stations` | `{"points": [...]}` |
| `/rffusion/api/map/stations?include_details=true` | `{"points": [...], "site_details": [...]}` |
| `/rffusion/api/map/stations/77` | Detalhe do site 77 |

`include_details=true` aproveita os detalhes já montados na mesma leitura.
Não executa uma consulta por site. O conjunto completo usa duas consultas ao
summary: sites e estações. Os arrays `points` e `site_details` correspondem aos
mesmos sites e seguem a mesma ordem.

Filtros opcionais, disponíveis nas duas rotas:

```text
/rffusion/api/map/stations?include_details=true&start_date=2026-09-01&end_date=2026-09-11
```

Datas usam `AAAA-MM-DD`, com dias inicial e final inclusivos. Mantém-se o
comportamento anterior do mapa: datas inválidas são ignoradas e intervalos
invertidos são ordenados. Pontos sem observações no período são removidos.

## Exemplos

No navegador, abra uma das URLs:

```text
http://rhfisnspdex02.anatel.gov.br:9082/rffusion/api/map/stations
http://rhfisnspdex02.anatel.gov.br:9082/rffusion/api/map/stations?include_details=true
http://rhfisnspdex02.anatel.gov.br:9082/rffusion/api/map/stations/85
```

Em MATLAB, a leitura do mapa e de seus detalhes usa uma só requisição:

```matlab
baseUrl = 'http://rhfisnspdex02.anatel.gov.br:9082/rffusion';
options = weboptions('ContentType', 'json', 'Timeout', 25);
payload = webread([baseUrl '/api/map/stations'], ...
    'include_details', 'true', options);
points = payload.points;
siteDetails = payload.site_details;
```

O exemplo retorna o JSON decodificado. A adaptação para os structs do appAnalise
continua responsável por arrays vazios, valores nulos, strings e conversão de
datas. Não há autenticação implementada neste exemplo.

## Contrato

Pontos contêm identidade e localização do site (`site_id`, `county_id`,
`district_id`, `site_label`, nomes geográficos, coordenadas, altitude e medidas
GNSS), `stations`, `station_names`, `marker_state`, `has_online_station`,
`has_online_host` e `has_known_host`.

Cada item de `site_details` contém a identidade do site, os mesmos flags e a
lista completa de estações. As estações detalhadas acrescentam `first_seen_at`,
`last_seen_at` e `spectrum_count` aos campos das estações dos pontos.

Cada estação dos pontos contém `equipment_id`, `equipment_name`, `host_id`,
`host_name`, `is_offline`, `is_current_location` e `map_state`. Os detalhes
acrescentam as datas e a contagem. Campos opcionais podem ser JSON `null`;
`stations` e `station_names` são arrays, inclusive quando vazios.

A prioridade de marcador, da maior para a menor relevância, é:

1. `online_current`;
2. `online_previous`;
3. `offline_current`;
4. `offline_previous`;
5. `no_host`.

O mapa utiliza as regras existentes do WebFusion: havendo estações, os estados
e flags são recalculados a partir delas. Elas são ordenadas por prioridade do
marcador e nome. Pontos e detalhes compartilham esse cálculo.

Datas dos detalhes mantêm a serialização padrão existente do Flask, em texto
HTTP como `Fri, 11 Sep 2026 00:00:00 GMT`. O consumidor MATLAB deve tratar esse
formato explicitamente; ele difere do ISO usado pelas tabelas de appAnalise.
Essa formatação não acrescenta uma conversão de fuso aos dados originais.

Em falha de consulta, a rota conserva a degradação existente: HTTP 200 com
arrays vazios para o mapa ou detalhe vazio para um site. Portanto, array vazio
não distingue ausência de dados de indisponibilidade do banco; os erros são
registrados no servidor. A política de acesso existente também foi mantida.

## Organização e migração

- `routes.py`: blueprint HTTP, preservando as URLs existentes do mapa.
- `service.py`: consultas ao summary, filtros e montagem compartilhada.
- `config.py`: prefixo HTTP do mapa.

As rotas de mapa dentro de `appAnalise_api` foram removidas, assim como seus
SQLs e sua montagem duplicada. Clientes passam a chamar estas rotas comuns;
não existem redirecionamentos nem aliases para os caminhos removidos.

## Validação

Execute no container webserver, com o projeto em `/RF.Fusion`:

```bash
cd /RF.Fusion
/usr/local/bin/python -m unittest test.tests.webfusion.test_map_api test.tests.webfusion.test_map_service -q
```

As suítes verificam rotas, filtros temporais, montagem dos pontos, estados,
detalhes e ausência de consultas paralelas para o appAnalise. Os testes não
validam a conectividade VPN/F5 do cliente final.

Voltar ao [índice das APIs](../README.md) ou ao [WebFusion](../../README.MD).
