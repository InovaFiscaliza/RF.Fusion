# Contrato inicial PostgreSQL

O PostgreSQL permanece uma instalação paralela aos bancos MariaDB em produção.
A adaptação do código inclui a base `DbHandler` e os handlers
`dbHandlerBKP`, `dbHandlerRFM` e `dbHandlerSummary` do appCataloga, conforme seu
[contrato de migração](../../src/appCataloga/.instructions/ARCHITECTURE.md#23-base-postgresql-e-migração-por-lotes).
Os entrypoints recebem conexão PostgreSQL explícita; o fluxo com estações e
appAnalise e os consumidores WebFusion ainda exigem validação própria. Para manter as
consultas que relacionam os modelos, os quatro
bancos lógicos são schemas `"BPDATA"`, `"RFDATA"`, `"RFFUSION_SUMMARY"` e
`"WEBFUSION"` dentro de um único banco PostgreSQL. Schemas, tabelas, views e
colunas usam identificadores delimitados por aspas duplas para preservar as
maiúsculas. Consultas e referências devem usar a mesma caixa e as aspas duplas.
O banco de conexão continua sendo `postgres`; a conexão só acessa um banco por
vez no PostgreSQL. Funções auxiliares, índices e triggers mantêm seus nomes
técnicos existentes. Instalações anteriores em minúsculas não são renomeadas
automaticamente; o deploy reconhece ambas as convenções e não mistura modelos.

BPDATA, RFDATA e WEBFUSION preservam os contratos de seus dados de origem.
O Summary segue o [redesenho autorizado](SUMMARY_POSTGRESQL.md), sem obrigação
de preservar tabelas e colunas de projeções legadas. Tipos, índices, ordenação, geografia e regras de
atualização são adaptados à plataforma; diferenças relevantes e limitações ficam
no README dos scripts. Os consumidores MariaDB existentes não são compatíveis
automaticamente: integração de aplicações, transferência de dados e validação
funcional são trabalho separado. Os contratos de SUMMARY e métricas continuam
regendo seu significado quando implementados nesta instalação.

`RFDATA.DIM_SPECTRUM_SITE.GEO_POINT` usa `public.geometry(Point)` do PostGIS,
com índice GiST. A coluna aceita SRIDs para preservar os dados de origem; o
futuro restore deve verificar SRID, ordem das coordenadas e sistema de referência
antes de qualquer conversão. A instalação fornece PostGIS na imagem PostgreSQL
17 e cria a extensão antes do schema. Bancos existentes não são alterados pelo
bootstrap e precisam de uma migração específica, sem recriação automática.

O pgAdmin e o PostgreSQL são containers separados no mesmo pod Podman, com
armazenamento próprio para suas configurações. A porta 9083 da VM acessa o
PostgreSQL; a 9084 acessa a interface web do pgAdmin. O SSH do PostgreSQL
é publicado na porta 2226 e o SSH do pgAdmin na 2227; dentro do pod usam,
respectivamente, as portas 22 e 2222. O deploy inicial só cria
os schemas quando nenhum dos quatro existe e executa a carga em uma transação.
Uma instalação existente não é recriada nem atualizada automaticamente.
