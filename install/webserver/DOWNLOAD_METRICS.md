# Contagem de downloads e reinícios

O contador `nginx_download_count` soma as respostas 200/206 para `/downloads/`
e `/_repo_download/` encontradas no log do NGINX. Ele representa requisições,
não arquivos únicos nem confirmação de download completo pelo cliente.

## Causa da recontagem

O checkpoint antigo identificava o arquivo por `st_dev:st_ino`. Mudanças de
dispositivo, inode ou data de modificação podiam fazer o leitor voltar ao byte
zero e acrescentar novamente todo o histórico aos contadores mensais. Um log
preservado após reiniciar o container ou a VM podia provocar esse comportamento.

## Identificação pelo conteúdo

A implementação usa `sha256:` seguido do hash de todos os bytes até o último
registro completo consumido. A assinatura cabe na coluna existente
`NA_FILE_SIGNATURE`; não há alteração de schema.

A cada sincronização:

1. Abre o log em modo binário e obtém os metadados do descritor aberto.
2. Confere o hash do trecho já consumido. Se o conteúdo continua igual, retoma
   do offset salvo, independentemente de inode, dispositivo ou mtime.
3. Se o prefixo mudou ou o arquivo foi truncado abaixo do offset, trata o
   conteúdo atual como um novo log, preservando os totais anteriores.
4. Conta somente linhas completas até o tamanho observado no início da leitura.
   Uma linha incompleta fica para a próxima sincronização.
5. Persiste os incrementos e o novo checkpoint na mesma transação, com bloqueio
   da linha de checkpoint para serializar leitores de processos diferentes.

O hash exige reler o prefixo do arquivo, mas não repetir seu processamento como
registros nem carregar todo o log em memória. Os blocos de leitura são definidos
em `src/webfusion/modules/server/config.py`. O custo é proporcional ao tamanho
do log; na verificação com o arquivo de aproximadamente 32 MB, duas passagens
levaram cerca de 0,23 segundo no webserver.

A rotação é tratada no arquivo ativo. Esta implementação não passa a coletar
arquivos rotacionados: registros ainda não consumidos quando o log sai do
caminho configurado podem ficar fora da contagem, como no fluxo anterior.

## Migração e ativação

O checkpoint legado é convertido na primeira sincronização da versão nova.
Se o arquivo ainda tem a identificação antiga, o offset é preservado. Se o
reinício já mudou a identificação, a migração compara os totais mensais do
prefixo consumido com os contadores persistidos. Isso permite migrar o histórico
completo que foi reconciliado sem somá-lo novamente.

Se essa comparação não puder validar um checkpoint legado, a operação falha
explicitamente, em vez de recontar silenciosamente. Nesse caso é necessária
uma reconciliação operacional, após conferir o histórico disponível. Essa
comparação é somente uma ponte de migração; checkpoints novos usam o hash.

Para ativar o código no processo WebFusion, execute **no host Podman**:

```bash
podman restart rffusion-web
```

Não rode a sincronização com o código novo em paralelo a uma instância antiga:
o leitor antigo não entende a assinatura SHA-256. Evite voltar ao código antigo
sem também planejar a compatibilidade do checkpoint.

Não é necessário modificar ou recarregar a configuração do NGINX. Os contadores
reconciliados permanecem preservados; a primeira sincronização normal publica
a assinatura nova. O reinício do container causa uma breve interrupção do serviço.

## Validação

No webserver autorizado, com Python 3.11:

```bash
cd /RF.Fusion
/usr/local/bin/python -m unittest test.tests.webfusion.test_server_routes -q
```

A suíte usa logs temporários e backend de métricas em memória. Inclui mudanças
de inode/mtime, reinicialização do módulo com checkpoint serializado, migração
legada, truncamento, rotação e linhas incompletas.

Também foi validada a migração contra o log real e o checkpoint reconciliado,
sem escrever no banco, simulando mudança de dispositivo e inode: nenhuma das
passagens acrescentou downloads já contabilizados. Não foi realizado um
reinício real da VM ou do container durante essa verificação.
