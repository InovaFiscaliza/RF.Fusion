# Upload de arquivos

Página: `/rffusion/upload`. O usuário precisa estar identificado pelo proxy
existente (e-mail válido); os papéis `admin` e `developer` não são obrigatórios.

Escolha o tipo de upload e complete o segundo filtro, quando necessário.
Selecione vários arquivos ou arraste-os para a página. O envio começa
automaticamente, em sequência, com progresso e resultado por arquivo. A lista
mostra somente os envios da página aberta. Não há envio de pastas nem retomada
automática depois de fechar a página.

## Armazenamento

- Raiz fixa: `/mnt/reposfi/upload`. Os destinos são:
  - Estações fixas: `fixas/<ID_HOST>/`, com estação cadastrada obrigatória.
  - Drive-test / SMP ROMES: `drive-test/smp-romes/`.
  - Drive-test / Espectro: `drive-test/espectro/`.
  - Radiação Não Ionizante RNI: `rni/`.
- As subpastas são criadas conforme o uso. O ID mantém o destino estável mesmo
  se o nome da estação mudar. A lista inclui estações offline.
- Cada arquivo mantém a classificação escolhida ao entrar na fila, inclusive
  ao usar Stop e Play. Play reinicia o envio do zero.
- Arquivos antigos na raiz não são movidos automaticamente: sua classificação
  precisa ser identificada antes de qualquer reorganização.
- Limite inicial: 512 MiB por arquivo, definido em `config.py`.
- O nome é normalizado para um nome seguro; acentos e espaços podem mudar.
  O nome salvo aparece na confirmação de sucesso.
- Arquivos existentes são preservados. Em caso de conflito, renomeie o arquivo
  local e selecione-o novamente.
- Falhas tratadas removem arquivos incompletos. Uma interrupção abrupta do
  processo pode deixar um arquivo parcial; a recuperação requer revisão manual.
- Os uploads não geram tarefas ou registros no catálogo e não ficam disponíveis
  pelas rotas públicas de download.

## Preparação do ambiente

O deploy em `install/webserver/deploy-rffusionweb.sh` cria a pasta no host,
mantém `/mnt/reposfi` somente leitura e monta a subpasta `upload` para escrita.
Uma instância já criada pode receber um ajuste temporário sem recriação, conforme
o procedimento rootless em [README do deploy](../../../../install/webserver/README.MD).
O próximo deploy registra a montagem na configuração do container, preservando-a
nos reinícios. O compartilhamento CIFS e suas credenciais também precisam permitir
escrita nessa pasta. A aplicação não cria um destino alternativo.

A configuração `install/webserver/nginx-webfusion.conf` permite 513 MiB apenas
na API de upload, incluindo o envelope multipart, e bloqueia downloads da pasta.
O proxy externo de autenticação também precisa aceitar esse tamanho e a duração
da transferência. O limite padrão de corpo do Waitress comporta esse envio.
Publicar envolve atualizar o código, a configuração nginx e a montagem juntos.

## Contrato da API

`POST /api/upload` recebe multipart com um arquivo `file`, o campo `category`
e o cabeçalho `X-WebFusion-Upload: 1`, além da identidade autenticada existente.

| category | Campo adicional obrigatório |
|---|---|
| `fixas` | `station`: ID_HOST da estação cadastrada |
| `drive-test` | `drive_type`: `smp-romes` ou `espectro` |
| `rni` | Nenhum |

Campos extras, repetidos ou incompatíveis com a categoria são rejeitados.
A resposta 201 inclui `name`, `size`, `elapsed_sec` e `folder` (subpasta relativa
à raiz). O servidor determina o caminho; o cliente não pode informá-lo.
Clientes anteriores que enviavam apenas `file` precisam incluir a classificação.

## Validação

No container webserver, com o projeto em `/RF.Fusion`:

```bash
cd /RF.Fusion
/usr/local/bin/python -m unittest discover -s test/tests/webfusion -p test_upload.py -v
nginx -t
```

Os testes usam pasta temporária e simulam integrações de identidade. Não gravam
arquivos no repositório real. Após publicar, validar no navegador a seleção
múltipla, o arrastar e soltar, a barra de progresso e a gravação no CIFS.
