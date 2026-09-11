# API compartilhada do mapa

Uma única implementação consulta os summaries, monta os pontos e calcula os
marcadores para o WebFusion e para clientes externos, incluindo o appAnalise.

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

## Contrato

Pontos contêm identidade e localização do site (`site_id`, `county_id`,
`district_id`, `site_label`, nomes geográficos, coordenadas, altitude e medidas
GNSS), `stations`, `station_names`, `marker_state`, `has_online_station`,
`has_online_host` e `has_known_host`.

Cada item de `site_details` contém a identidade do site, os mesmos flags e a
lista completa de estações. As estações detalhadas acrescentam `first_seen_at`,
`last_seen_at` e `spectrum_count` aos campos das estações dos pontos.

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
