# Fotos do Microsoft Graph

O conector recebe o e-mail de login no Entra (UPN) e retorna uma URL local da
foto, sem expor o token. O Graph retorna bytes da imagem, não uma URL pública
permanente. O conector salva esses bytes para o Nginx servir.

```python
from api.microsoft_api import get_profile_image_url

image_url = get_profile_image_url("usuario@dominio.gov.br")
```

O retorno é uma string no formato
`/rffusion/static/img/profiles/<hash-do-email>-<hash-da-foto>.jpg` ou `.png`.
Fotos iguais mantêm a URL; uma mudança de conteúdo gera uma nova URL e evita
que o navegador reutilize a imagem antiga. Os arquivos anteriores são mantidos
para que páginas já abertas continuem funcionando. Não versionar esse cache.

## Configuração futura

Enquanto não houver token, nenhuma requisição ao Graph é feita. O aplicativo
mantém a foto já armazenada ou exibe o fallback.

Disponibilizar `MICROSOFT_GRAPH_ACCESS_TOKEN` no ambiente do webserver ou no
arquivo local `api/microsoft_api/.env`, ignorado pelo Git:

```dotenv
MICROSOFT_GRAPH_ACCESS_TOKEN=valor_do_token
```

No arquivo, usar o valor sem aspas e restringir a leitura ao usuário do serviço.
O ambiente tem precedência. O arquivo é relido a cada consulta; o intervalo de
tentativa na interface pode atrasar em até cinco minutos o uso de um token novo.
A emissão e renovação automática de tokens não fazem parte desta etapa.
Um token expirado preserva a foto existente e gera aviso de consulta.

O token deve permitir a leitura de fotos no Graph, por exemplo com a permissão
de aplicativo `ProfilePhoto.Read.All` e consentimento administrativo. A chamada
é `GET /users/{userPrincipalName}/photo/$value`. O e-mail recebido precisa
coincidir com o UPN; aliases de e-mail diferentes exigem resolução prévia do
identificador do Entra. Não presumir que qualquer endereço de e-mail seja UPN.
Consulte a [documentação da Microsoft](https://learn.microsoft.com/en-us/graph/api/profilephoto-get?view=graph-rest-1.0).

## Responsabilidades e falhas

- `client.py`: token, HTTP, validação de tamanho/formato e gravação atômica.
  Aceita JPEG e PNG até 4 MiB, com timeout de cinco segundos e sem redirecionar
  o cabeçalho de autorização.
- `config.py`: endereço do Graph, paths, limite de tamanho e timeout.
- `auth/service.py`: ao renderizar uma página, carrega a foto armazenada e
  tenta atualizá-la no máximo a cada seis horas por usuário e processo. Em falha
  ou ausência de configuração, tenta novamente após cinco minutos. Não há
  consulta em segundo plano: a próxima navegação dispara uma consulta vencida.
- `auth/db_users.py`: atualiza somente `NA_URL_PROFILE_IMG`, sem alterar nome
  ou permissões. A atualização ocorre somente após uma foto válida e diferente.

Um cabeçalho válido `X-User-Avatar-Url` continua tendo precedência sobre o Graph.
O conector isolado não escreve no banco; a integração com a identidade faz isso.
Ele levanta `MicrosoftNotConfigured`, `MicrosoftPhotoNotFound` ou
`MicrosoftPhotoError` em vez de retornar uma URL fictícia. Um e-mail inválido
gera `ValueError`. HTTP 404, token inválido, falta de permissão e indisponibilidade
nunca apagam a foto existente. Os SVGs de fallback não são gravados no cadastro.

## Fallbacks

O cabeçalho usa `static/img/profile.svg` para um usuário identificado sem foto.
`GET /api/users/avatar.svg` substitui o texto pela inicial maiúscula do nome,
ou do e-mail quando não houver nome. A serialização XML escapa o caractere e a
resposta usa `Cache-Control: private, no-store`.

Usuários totalmente anônimos usam `static/img/profile_out.svg`. Se uma foto real
falhar no navegador, ela é removida da camada superior e o fallback permanece.

## Validação

No container webserver:

```bash
cd /RF.Fusion/test
/usr/local/bin/python -m unittest tests.webfusion.test_microsoft_api tests.webfusion.test_app_headers
RFF_DB_TEST=1 /usr/local/bin/python -m unittest tests.webfusion.test_user_schema
```

Os testes substituem o Graph por respostas controladas, usam diretórios
temporários e verificam fotos alteradas/inalteradas, ausência/expiração do token,
erros HTTP, formatos inválidos, atualização do banco e os fallbacks. A consulta
real ao tenant exige a credencial que ainda será configurada.
