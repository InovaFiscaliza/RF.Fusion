# Scripts PostgreSQL

Os scripts criam os bancos operacionais, analíticos e de usuários para uma
instalação nova em PostgreSQL 17. O Summary usa o
[modelo canônico revisado](../../../docs/architecture/SUMMARY_POSTGRESQL.md),
sem copiar as projeções físicas do MariaDB. A decisão de usar quatro schemas dentro de um único banco e os
limites da instalação paralela estão no [contrato](../../../docs/architecture/POSTGRESQL.md).
Schemas, tabelas, views e colunas usam aspas duplas para preservar maiúsculas,
por exemplo `SELECT "ID_HOST" FROM "BPDATA"."HOST"`. O banco de conexão continua
sendo `postgres`. Funções auxiliares, índices e triggers mantêm os nomes técnicos
existentes. O deploy do container
[PostgreSQL](../../../install/postgres/README.md) executa o bootstrap apenas
quando nenhum dos quatro schemas existe.

## Criação inicial

Conecte-se ao container PostgreSQL com o repositório montado em `/RFFusion`,
como descrito no deploy, e execute os comandos abaixo em um banco `postgres`
vazio, com um usuário autorizado a criar schemas e a extensão PostGIS. O `psql`
precisa ler os CSVs locais do repositório. `--single-transaction` desfaz cada script individual em
caso de erro, mas scripts anteriores já concluídos permanecem no banco.

```bash
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction -f /RFFusion/src/postgres/scripts/createProcessingDB.sql
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction -f /RFFusion/src/postgres/scripts/createMeasureDB.sql
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction -f /RFFusion/src/postgres/scripts/createFusionSummaryDB.sql
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction -f /RFFusion/src/postgres/scripts/createWebFusionDB.sql
```

Os scripts não atualizam instalações existentes nem migram dados. Recriar apenas
o pod preserva os volumes e, portanto, os nomes em minúsculas já existentes.
A alteração de caixa em uma instalação existente exige migração própria ou uma
nova instalação em banco vazio, com preservação prévia dos dados necessários.
Os cinco CSVs
são cópias dos arquivos do MariaDB; a carga ajusta as sequências após inserir IDs.
O bootstrap também cadastra `DIM_SITE_TYPE.ID_TYPE = 1` como `Fixed`, exigido
pelo fluxo convencional de `insert_site`, e `ID_TYPE = 2` como `Mobile`.
A sequência avança para permitir novos tipos a partir de 3. Essa carga estava
ausente também no DDL MariaDB de referência, que só criava a tabela.

Instalações PostgreSQL criadas antes desse cadastro podem receber apenas o
[reparo dos tipos de site](ensureSiteTypes.sql), no container PostgreSQL e com
um usuário autorizado a escrever no catálogo e ajustar sua sequência:

```bash
psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 -f /RFFusion/src/postgres/scripts/ensureSiteTypes.sql
```

O reparo é repetível, preserva os demais tipos e não modifica sites. Rejeita
o catálogo se os IDs 1 ou 2 já tiverem significado diferente de `Fixed` ou `Mobile`,
respectivamente, sem sobrescrevê-los. Aceita rótulos legados com aspas simples
externas (`'Fixed'` e `'Mobile'`); novas linhas armazenam `Fixed` e `Mobile`.
Nesse caso, revisar os tipos e suas referências antes de corrigir os dados.
Não executar novamente o script de criação em uma instalação existente.

O backfill [backfillFixedSiteType.sql](backfillFixedSiteType.sql) só deve ser
executado quando os sites sem classificação forem comprovadamente fixos e o tipo
1 for `Fixed`. O relatório [postgres_runtime_health.sh](postgres_runtime_health.sh)
verifica processo e consulta local; não está integrado ao WebFusion.

## Diferenças e limites

- Os tipos `datetime`, inteiros com largura de exibição, flags, JSON, BLOB e
  identidades foram adaptados para tipos PostgreSQL; timestamps não têm fuso.
- `GEO_POINT` usa `public.geometry(Point)` do PostGIS e índice GiST. O tipo não
  fixa SRID para permitir preservar o valor da origem; antes do restore, verificar
  os SRIDs e a ordem das coordenadas. Converta dados por WKB e SRID separados;
  não copie diretamente bytes internos do MariaDB para o PostGIS.
- Os índices de texto usam `to_tsvector('simple', ...)`; buscas e normalização
  não reproduzem automaticamente o FULLTEXT MariaDB. Os índices de prefixo usam
  `left` e exigem consultas correspondentes para serem aproveitados.
- Para comportar caminhos longos, as identidades de arquivo usam índices únicos
  SHA-256 de caminho e nome: `FILE_TASK` e `FILE_TASK_HISTORY` em BPDATA e
  `DIM_SPECTRUM_FILE` (volume, caminho e nome) em RFDATA. Cada schema define sua
  própria `file_identity_sha256(text)`, sem dependência entre schemas. A função fixa
  UTF-8 e declara `IMMUTABLE`, pois a codificação do banco é fixa e o resultado
  depende apenas do texto. Isso permite criar os índices sem usar diretamente
  `convert_to`, classificada como `STABLE` pelo PostgreSQL. Uma colisão, embora improvável, rejeitaria registros
  distintos. Comparações usam as regras do PostgreSQL, não as collations MariaDB;
  verifique duplicatas, caixa e acentos antes de migrar dados.
- Em RFDATA, os rótulos de `DIM_SPECTRUM_DETECTOR`, `DIM_SPECTRUM_TRACE_TYPE`,
  `DIM_SPECTRUM_UNIT`, `DIM_SPECTRUM_PROCEDURE` e o `NA_EQUIPMENT` de
  `DIM_SPECTRUM_EQUIPMENT` são únicos, para que o get-or-create do handler não
  crie duplicatas sob concorrência. O MariaDB não tinha essas restrições: o
  restore deve consolidar rótulos e arquivos repetidos, reapontando as chaves
  estrangeiras, antes da carga. `FACT_SPECTRUM` não recebe índice
  único; o handler serializa suas escritas pelo site.
- Datas `DT_UPDATED_AT` usam triggers; índices e funções foram adaptados para
  PostgreSQL. O agrupamento de erros pertence ao motor de Summary, sem view
  duplicada de eventos brutos.
- A migração legada de métricas de uso do script MariaDB não é executada: o
  bootstrap é para schemas novos. Dados antigos precisam de migração planejada.
- A versão legada `environment.yml` não é necessária para executar esses SQLs.
  Não se deve usar o driver MySQL para conectar ao PostgreSQL. O motor/worker
  Summary usam a API nova; a integração completa das aplicações e dos demais
  workers continua pendente.

## Reconstrução do Summary

`createFusionSummaryDB.sql` cria onze tabelas, sem views de compatibilidade:
seis agregados, três tabelas de controle e duas de uso. Não restaure projeções
antigas. Preserve e restaure conjuntamente apenas
`WEBFUSION_USAGE_LOG_CHECKPOINT` e `WEBFUSION_USAGE_METRIC_MONTHLY`; depois
reconstrua os agregados por full reconcile. O DDL não apaga instalações
existentes e não executa a transferência de dados.

Sinais de discovery/SSH/GNSS estão em `BPDATA.HOST` e pertencem ao procedimento
operacional do BPDATA. Campos novos começam sem evidência de avaliação;
não reconstruir evidências por inferência durante reconcile.
`RFFUSION_SUMMARY.HOST_EQUIPMENT_LINK` concentra o cruzamento automático dos
cadastros de hosts e equipamentos. Seus vínculos são reconstruídos por refresh,
sem tabela de overrides nem flag de vínculo manual na instalação PostgreSQL.
O DDL atualizado vale para instalações novas; não remove tabelas ou colunas de
instalações existentes. Decisões manuais anteriores deixam de participar do
matching quando o runtime atualizado for utilizado.
