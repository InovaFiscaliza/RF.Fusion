# Migração de dados MariaDB → PostgreSQL

Ferramenta para transferir um dump lógico do MariaDB (`mysqldump`, compactado
com gzip) para uma instalação PostgreSQL nova, criada pelos
[scripts de bootstrap](../scripts/README.md). Segue o
[contrato PostgreSQL](../../../docs/architecture/POSTGRESQL.md) e a fase 2.1 do
[planejamento](../../../docs/planejamento/PLANEJAMENTO_POSTGRESQL.md). Os nomes de
tabelas e colunas são preservados; a taxonomia é uma etapa posterior e separada.

| Arquivo | Responsabilidade |
|---|---|
| [mariadb_dump_to_postgres.py](mariadb_dump_to_postgres.py) | Leitura do dump, conversão de valores, inspeção e carga |
| [config.py](config.py) | Escopo da migração, unicidades exclusivas do PostgreSQL e limites de execução |

## Ambiente

Executar no container do appCataloga com `/opt/conda/envs/appdata/bin/python`
(Psycopg 3.3.8), que monta o repositório e `/mnt/reposfi`. A ferramenta não
precisa de um MariaDB nem do `psql`: lê o `.sql.gz` em fluxo, sem descompactá-lo
em disco, e aplica o DDL traduzindo `\copy` para `COPY FROM STDIN`.

O destino é informado exclusivamente pela variável `RFFUSION_MIGRATION_PG_CONNINFO`,
sem reaproveitar a configuração das aplicações. O banco dessa conexão é usado
para criar o destino quando solicitado. A credencial não deve aparecer em
comandos, logs ou arquivos versionados.

## Escopo

| Origem no dump | Destino |
|---|---|
| `BPDATA`, `RFDATA`, `WEBFUSION` | Todas as tabelas, com os mesmos IDs |
| `RFFUSION_SUMMARY` | Somente `WEBFUSION_USAGE_LOG_CHECKPOINT` e `WEBFUSION_USAGE_METRIC_MONTHLY` |
| Demais tabelas do Summary | Ignoradas; os agregados canônicos são reconstruídos por full reconcile |
| `mysql`, `sys`, `appdb` e outros bancos | Ignorados, sem leitura dos valores |

As colunas são associadas por nome, conforme o `CREATE TABLE` do dump. Colunas
do dump ausentes no PostgreSQL ou com tipo incompatível interrompem a migração.
Colunas novas do PostgreSQL, como os sinais de `BPDATA.HOST`, recebem o default
do DDL e aparecem no relatório.

## Uso

1. Verificar o dump sem gravar nada:

   ```bash
   /opt/conda/envs/appdata/bin/python src/postgres/migration/mariadb_dump_to_postgres.py \
       inspect /mnt/reposfi/db/<dump>.sql.gz --report /tmp/inspect.json
   ```

   Saída 0 indica dump convertível; 2 indica problemas bloqueantes no relatório
   (valores inválidos ou duplicatas rejeitadas pelo PostgreSQL); 1 indica falha.

2. Carregar em um banco sem os quatro schemas do RF.Fusion:

   ```bash
   /opt/conda/envs/appdata/bin/python src/postgres/migration/mariadb_dump_to_postgres.py \
       load /mnt/reposfi/db/<dump>.sql.gz --database rffusion_test_migracao \
       --create-database --max-database-gb <limite> \
       --disk-guard-path / --min-free-gb <reserva> --report /tmp/load.json
   ```

   Em produção, selecione `--database RFFUSION` explicitamente. O deploy inicial
   pode já ter criado seus schemas; a ferramenta rejeita esse destino preenchido.
   Prepare o banco de migração sem os quatro schemas antes da carga, preservando
   qualquer dado existente. Não execute a carga contra o banco de manutenção `postgres`.

   Sem `--create-database`, o banco já deve existir e não pode conter nenhum
   dos schemas `BPDATA`, `RFDATA`, `RFFUSION_SUMMARY` e `WEBFUSION`. Ensaios
   usam bancos descartáveis com prefixo `rffusion_test_`.

O relatório é gravado também em caso de falha. Ele contém contagens, totais de
controle, SRIDs e limites das coordenadas, sequências, comparação com os dados
do bootstrap e tamanhos observados; não contém caminhos, e-mails ou outros
valores das linhas.

## Comportamento da carga

- O checksum é conferido quando existe o arquivo `<dump>.sha256`.
- Cada script de DDL é aplicado em sua própria transação. A carga ocorre em uma
  transação única: uma falha desfaz todos os dados, mas o banco criado e o DDL
  permanecem. Remover o banco de ensaio antes de repetir, inclusive para liberar
  o espaço ocupado pela carga desfeita.
- As tabelas preenchidas pelo bootstrap (UF, municípios, tipos e unidades) são
  substituídas pelas linhas do dump, cujos IDs são referenciados pelos fatos. O
  relatório indica linhas idênticas, alteradas e exclusivas de cada lado.
- As chaves estrangeiras são removidas durante o `COPY` e recriadas antes do
  commit, o que valida a integridade referencial de toda a carga.
- Cada identidade continua após o maior entre o ID carregado e o
  `AUTO_INCREMENT` do MariaDB, preservando os IDs já emitidos.
- Contagens e soma da primeira coluna inteira são comparadas com o dump antes do
  commit; em seguida, as tabelas carregadas recebem `ANALYZE`.
- `GEO_POINT` é convertido do formato interno do MariaDB (SRID + WKB) para EWKB,
  preservando X = longitude, Y = latitude e o SRID de origem. SRID 0 permanece
  desconhecido no PostGIS.
- Strings com caractere NUL, datas `0000-00-00`, JSON inválido e duplicatas das
  unicidades exclusivas do PostgreSQL (rótulos de `RFDATA` e identidade de
  `DIM_SPECTRUM_FILE`) bloqueiam a migração. A consolidação desses registros,
  quando necessária, é decidida antes da carga.

## Pendências

- **Espaço em disco.** Verifique a capacidade para dados, índices e WAL no
  filesystem efetivamente usado pelo cluster. Em produção, o padrão é o volume
  nomeado do Podman sob `/home`; o DEV pode usar um diretório explícito, conforme
  o [deploy](../../../install/postgres/README.md). Ao carregar, aponte
  `--disk-guard-path` para um caminho desse mesmo filesystem visível no container
  appCataloga. Não use `/` se ele representar apenas o overlay do appCataloga.
- `DIM_SITE_TYPE` do dump usa os rótulos legados `'Fixed'` e `'Mobile'`, com
  aspas, enquanto o bootstrap grava `Fixed` e `Mobile`. A carga preserva a
  origem; a normalização é uma decisão de dados separada.
- Escritas posteriores ao dump não são capturadas. A carga final depende da
  janela de parada ou do mecanismo de captura definidos no planejamento.

## Validação

Os testes de unidade não acessam bancos:

```bash
/opt/conda/envs/appdata/bin/python -m unittest discover -s test/tests/db -p 'test_mariadb_dump_migration.py' -v
```
