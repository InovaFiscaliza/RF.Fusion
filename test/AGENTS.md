# Instruções para testes

Leia as [regras gerais](../docs/agents/INSTRUCTIONS.md), o
[README da suíte](README.md) e as instruções das aplicações cobertas.

| Testes | Instruções de ambiente |
|---|---|
| Exclusivos do appCataloga | [ENVIRONMENT.md](../src/appCataloga/.instructions/ENVIRONMENT.md) |
| Importam WebFusion, inclusive suítes mistas | [ENVIRONMENT.md](../src/webfusion/.instructions/ENVIRONMENT.md) |

Não iniciar a suíte inteira pelo Python do appCataloga sem verificar os imports.
O helper `test_all.sh` usa esse Python por padrão e não escolhe o container.
No webserver, usar exclusivamente `/usr/local/bin/python` e a raiz `/RF.Fusion`.

Testes não devem iniciar daemons reais, escrever em bancos compartilhados ou
usar os diretórios de dados de produção configurados pela aplicação. Usar
diretórios temporários e doubles com interfaces reais. Integrações com MariaDB
exigem banco isolado e seleção explícita; não tratar SQLite como validação de
SQL ou locks específicos do MariaDB.
