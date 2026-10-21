# Ambiente de Execucao do WebFusion

## Container autorizado

O WebFusion em `/RFFusion/src/webfusion` e montado e executado pelo container
definido em `/RFFusion/install/webserver`. As dependencias do WebFusion, incluindo
`flask`, pertencem a esse container e nao ao container Debian 12 com Python do
`appCataloga`.

Para executar o WebFusion, verificar suas dependencias ou rodar testes que o
importem, conecte-se ao container do webserver:

```bash
ssh root@10.88.0.34
```

O acesso SSH requer autenticação interativa por senha. Solicite ou use a
credencial operacional fornecida pelo responsável pelo ambiente, sem registrá-la
em arquivos versionados, comandos, logs ou documentação.

Neste workspace, a credencial local do webserver fica em
`/RFFusion/src/webfusion/.webserver_ssh.env`. Esse arquivo é ignorado pelo Git
e deve permanecer local. Antes de acessar o container, leia-o somente pelo
ambiente de execução e não exponha seus valores na resposta, em comandos ou em
arquivos versionados.

## Interpretador e caminhos

O runtime definido em [Containerfile](../../../install/webserver/Containerfile)
usa a imagem `python:3.11-slim`, sem Conda. O interpretador autorizado no
container é `/usr/local/bin/python` (Python 3.11; a versão de patch pode mudar
ao reconstruir a imagem).

Use esse caminho absoluto para executar testes e comandos Python do WebFusion:

```bash
/usr/local/bin/python --version
```

Conforme o [deploy](../../../install/webserver/deploy-rffusionweb.sh), o projeto
é montado em `/RF.Fusion` no webserver. Portanto:

- aplicação: `/RF.Fusion/src/webfusion`;
- testes: `/RF.Fusion/test`;
- arquivos estáticos servidos pelo nginx: `/RF.Fusion/src/webfusion/static`.

O caminho `/RFFusion` pertence ao workspace do agente. Não presuma que ele
exista no webserver. Testes antigos que usem esse caminho literal precisam de
adequação de caminhos no ambiente de teste, sem alterar o runtime da aplicação.

Não use o interpretador do container do agente para importar, compilar ou testar
WebFusion. Dependências de validação pertencem ao webserver e podem ficar em um
diretório temporário separado das dependências da aplicação.

## Limite do appCataloga

O ambiente do appCataloga é definido em seu
[ENVIRONMENT.md](../../appCataloga/.instructions/ENVIRONMENT.md).

Não instale `flask` naquele container para viabilizar testes de WebFusion. A falta
desse pacote ali confirma a separacao correta entre os ambientes, e nao uma
dependencia ausente. Antes de executar uma suite em `/RFFusion/test`, determine
se ela importa WebFusion; se importar, execute-a no container do webserver.
