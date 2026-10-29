# Contrato inicial PostgreSQL

O PostgreSQL é uma instalação paralela, não uma migração nem substituição dos
bancos MariaDB. Para manter as consultas que relacionam os modelos, os quatro
bancos lógicos são schemas `bpdata`, `rfdata`, `rffusion_summary` e `webfusion`
dentro de um único banco PostgreSQL. Seus nomes não são delimitados por aspas e
são armazenados em minúsculas, como os nomes de tabelas e colunas. A conexão só
acessa um banco por vez no PostgreSQL.

Os scripts de criação devem preservar tabelas, colunas e granularidade dos
contratos existentes. Tipos, índices, ordenação, geografia e regras de
atualização são adaptados à plataforma; diferenças relevantes e limitações ficam
no README dos scripts. Os consumidores MariaDB existentes não são compatíveis
automaticamente: integração de aplicações, transferência de dados e validação
funcional são trabalho separado. Os contratos de SUMMARY e métricas continuam
regendo seu significado quando implementados nesta instalação.

O pgAdmin e o PostgreSQL são containers separados no mesmo pod Podman, com
armazenamento próprio para suas configurações. A porta 9083 da VM acessa o
PostgreSQL; a 9084 acessa a interface web do pgAdmin. O SSH do PostgreSQL
é publicado na porta 2226 e o SSH do pgAdmin na 2227; dentro do pod usam,
respectivamente, as portas 22 e 2222. O deploy inicial só cria
os schemas quando nenhum dos quatro existe e executa a carga em uma transação.
Uma instalação existente não é recriada nem atualizada automaticamente.
