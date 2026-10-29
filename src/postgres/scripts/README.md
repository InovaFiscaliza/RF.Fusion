# Scripts PostgreSQL

Os scripts criam os modelos de `src/mariadb/scripts` para uma instalação nova
em PostgreSQL 17. A decisão de usar quatro schemas dentro de um único banco e os
limites da instalação paralela estão no [contrato](../../../docs/architecture/POSTGRESQL.md).
Os nomes SQL sem aspas são armazenados em minúsculas. O deploy do container
[PostgreSQL](../../../install/postgres/README.md) executa o bootstrap apenas
quando nenhum dos quatro schemas existe.

## Criação inicial

Conecte-se ao container PostgreSQL com o repositório montado em `/RFFusion`,
como descrito no deploy, e execute os comandos abaixo em um banco `postgres`
vazio, com um usuário autorizado a criar schemas. O `psql` precisa ler os CSVs
locais do repositório. `--single-transaction` desfaz cada script individual em
caso de erro, mas scripts anteriores já concluídos permanecem no banco.

```bash
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction -f /RFFusion/src/postgres/scripts/createProcessingDB.sql
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction -f /RFFusion/src/postgres/scripts/createMeasureDB.sql
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction -f /RFFusion/src/postgres/scripts/createFusionSummaryDB.sql
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction -f /RFFusion/src/postgres/scripts/createWebFusionDB.sql
```

Os scripts não atualizam instalações existentes nem migram dados. Os cinco CSVs
são cópias dos arquivos do MariaDB; a carga ajusta as sequências após inserir IDs.
O backfill [backfillFixedSiteType.sql](backfillFixedSiteType.sql) só deve ser
executado quando os sites sem classificação forem comprovadamente fixos e o tipo
1 for `Fixed`. O relatório [postgres_runtime_health.sh](postgres_runtime_health.sh)
verifica processo e consulta local; não está integrado ao WebFusion.

## Diferenças e limites

- Os tipos `datetime`, inteiros com largura de exibição, flags, JSON, BLOB e
  identidades foram adaptados para tipos PostgreSQL; timestamps não têm fuso.
- `GEO_POINT` usa o tipo nativo `point` e índice GiST. Não é geometria PostGIS:
  suas funções, representação e semântica espacial diferem das do MariaDB.
- Os índices de texto usam `to_tsvector('simple', ...)`; buscas e normalização
  não reproduzem automaticamente o FULLTEXT MariaDB. Os índices de prefixo usam
  `left` e exigem consultas correspondentes para serem aproveitados.
- Para comportar caminhos longos, as identidades de arquivo usam índices únicos
  SHA-256 de caminho e nome. Uma colisão, embora improvável, rejeitaria registros
  distintos. Comparações usam as regras do PostgreSQL, não as collations MariaDB;
  verifique duplicatas, caixa e acentos antes de migrar dados.
- Datas `DT_UPDATED_AT` usam triggers; índices e funções foram adaptados para
  PostgreSQL. A view de erros calcula SHA-256 sobre UTF-8.
- A migração legada de métricas de uso do script MariaDB não é executada: o
  bootstrap é para schemas novos. Dados antigos precisam de migração planejada.
- A versão legada `environment.yml` não é necessária para executar esses SQLs.
  Não se deve usar o driver MySQL para conectar ao PostgreSQL. As aplicações e
  workers existentes não foram convertidos, e não mantêm estes modelos ainda.
