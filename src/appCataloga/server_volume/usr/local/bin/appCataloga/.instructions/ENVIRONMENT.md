# Ambiente de Execucao do appCataloga

## Container atual

O workspace do agente executa dentro do container Debian 12 com Python definido
em `/RFFusion/install/appCataloga`. Este e o ambiente autorizado para trabalhar
com o aplicativo `appCataloga`, localizado em `/RFFusion/src/appCataloga`.

Use este container para:

- executar os workers e utilitarios do `appCataloga`;
- instalar ou verificar dependencias do `appCataloga`;
- executar testes que cobrem exclusivamente o `appCataloga`.

## Limite do WebFusion

Este container nao e o ambiente de execucao do WebFusion e nao deve ser usado
para instalar dependencias ou executar testes desse aplicativo. Em particular,
a ausencia de `flask` neste ambiente nao e uma falha a ser corrigida aqui.

O WebFusion e montado e executado pelo container definido em
`/RFFusion/install/webserver`. Para executar comandos que dependem do WebFusion,
conecte-se ao container do webserver:

```bash
ssh root@10.88.0.34
```

Antes de executar uma suite de testes, identifique a aplicacao coberta. Suítes
que incluam WebFusion devem ser executadas no container do webserver, inclusive
quando forem iniciadas a partir do diretorio `/RFFusion/test`.