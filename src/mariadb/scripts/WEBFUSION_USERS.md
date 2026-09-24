# Perfis e privilégios do WebFusion

## Modelo

- `USERS`: cadastro único por e-mail, com os dados pessoais e a URL opcional
  `NA_URL_PROFILE_IMG` (`varchar(2048)`).
- `USER_ROLES`: vínculo entre `ID_USER` e `NA_ROLE` (`admin` ou `developer`),
  com estado ativo e datas de criação/atualização. A chave estrangeira remove
  os vínculos quando um usuário é excluído.

Os papéis são independentes: um usuário pode ter ambos. O papel efetivo continua
priorizando `admin`. Os contratos `NA_ROLE`, `IS_ADMIN` e `IS_DEVELOPER` das APIs
continuam disponíveis. `/api/users/login` mantém HTTP 200 com corpo vazio e o
perfil JSON no cabeçalho `X-User-Profile`.

## Criação do banco

Para instalações novas, usar [createWebFusionDB.sql](createWebFusionDB.sql).
O script já contém o modelo completo: `USERS`, a coluna `NA_URL_PROFILE_IMG`,
`USER_ROLES`, seus índices e a chave estrangeira com exclusão em cascata.
Não é necessário aplicar migrações depois da criação.

Scripts de migração não são mantidos no repositório. O script de criação não
converte instalações legadas nem importa usuários de `ADMINS` e `DEVELOPERS`.
Bancos existentes exigem atualização operacional específica, com cópia de
recuperação e validação dos perfis e privilégios antes de ativar os consumidores.

### Estado do banco ativo

Em 18/09/2026, a migração foi aplicada e as tabelas legadas foram removidas com
autorização do responsável. Foram conferidos os 12 vínculos de administrador e
os 9 de desenvolvedor, sem vínculos ausentes ou diferenças de estado. A remoção
preservou integralmente os 16 registros de `USERS` e os 21 de `USER_ROLES`.

A cópia SQL anterior à remoção está no container webserver, em
`/root/webfusion-backups/legacy-roles-before-drop-20260918T165913Z.sql`, com acesso
restrito ao proprietário. Contém a estrutura e os registros das duas tabelas.

O banco ativo contém somente `USERS` e `USER_ROLES`. Os testes usam o script
de criação em um schema isolado para validar o modelo atual.

## Foto do usuário

O proxy pode fornecer `X-User-Avatar-Url` com uma URL HTTPS acessível ao navegador
ou um caminho local absoluto. URLs com credenciais, query string ou fragmento
não são aceitas, evitando persistir tokens em URLs. A ausência do cabeçalho
preserva a imagem armazenada. O painel usa o valor recebido ou, na sua ausência,
o valor do banco. Sem foto, o usuário identificado usa `profile.svg` com a
inicial do nome; o anônimo usa `profile_out.svg`.

Nome e e-mail não são suficientes para baixar a foto do Microsoft 365. O
[Microsoft Graph](https://learn.microsoft.com/en-us/graph/api/profilephoto-get?view=graph-rest-1.0)
disponibiliza `/me/photo/$value` e `/users/{id|userPrincipalName}/photo/$value`,
mas exige token de acesso com a permissão correspondente. A sessão do F5 não
equivale automaticamente a um token para o Graph. Sem URL fornecida pelo proxy,
a integração exige configurar o aplicativo no Entra ID e uma consulta
autenticada no servidor; não colocar token na URL da imagem nem no banco.
O [conector Microsoft](../../webfusion/api/microsoft_api/README.md) já prepara
essa consulta e a atualização de `NA_URL_PROFILE_IMG` quando a foto muda.

## Validação

No container webserver, com o projeto em `/RF.Fusion`:

```bash
cd /RF.Fusion/test
/usr/local/bin/python -m unittest tests.webfusion.test_app_headers
RFF_DB_TEST=1 /usr/local/bin/python -m unittest tests.webfusion.test_user_schema
```

Os testes de banco devem usar schema isolado e validar: criação do schema
atual, vínculos inativos e cumulativos,
precedência de administrador, revogação, exclusão em cascata e persistência da
foto quando o proxy omite o cabeçalho.
