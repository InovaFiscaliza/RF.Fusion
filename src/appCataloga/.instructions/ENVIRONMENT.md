# Ambiente de execução do appCataloga

## Container atual

O workspace do agente executa dentro do container Debian 12 com Python definido
em [install/appCataloga](../../../install/appCataloga/README.md). Este é o
ambiente autorizado para o aplicativo localizado em `/RFFusion/src/appCataloga`.
Os launchers usam `/opt/conda/envs/appdata/bin/python`; o repositório fica em
`/RFFusion` e o runtime em
`/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga`.

Use este container para:

- executar os workers e utilitarios do `appCataloga`;
- instalar ou verificar dependencias do `appCataloga`;
- executar testes que cobrem exclusivamente o `appCataloga`.

## Limite do WebFusion

Este container nao e o ambiente de execucao do WebFusion e nao deve ser usado
para instalar dependencias ou executar testes desse aplicativo. Em particular,
a ausencia de `flask` neste ambiente nao e uma falha a ser corrigida aqui.

Para comandos que dependam do WebFusion, inclusive compilação ou importação,
siga seu [ENVIRONMENT.md](../../webfusion/.instructions/ENVIRONMENT.md).
Endereço, autenticação, interpretador e caminhos desse container têm uma única
fonte nesse documento.

Antes de executar uma suite de testes, identifique a aplicacao coberta. Suítes
que incluam WebFusion devem ser executadas no container do webserver, inclusive
quando forem iniciadas a partir do diretorio `/RFFusion/test`.
