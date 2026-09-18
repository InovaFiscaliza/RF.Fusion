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

## Migração

Para instalações novas, usar `createWebFusionDB.sql`. Para o banco existente,
usar `migrateWebFusionUsers.sql` antes de ativar a nova versão do aplicativo.
Suspender alterações de privilégios durante a transição, pois versões antigas
ainda escrevem nas tabelas legadas. O DDL é aditivo; as cópias de dados e vínculos
são transacionais. Executar com um cliente que interrompa ao primeiro erro.

A migração preserva perfis existentes em `USERS`, importa usuários exclusivos
das tabelas antigas e mantém os estados ativos/inativos e datas dos vínculos.
Entradas já migradas não são sobrescritas. Não repetir a importação depois de
iniciar a administração pelo novo modelo: registros legados não refletem mais
revogações ou exclusões posteriores.

Após validar os vínculos migrados e as dependências, salvar uma cópia das
tabelas legadas e remover `ADMINS` e `DEVELOPERS`. Retornar ao aplicativo antigo
após mudanças de privilégios exige reconciliar essas mudanças; a cópia legada
não é um espelho atualizado.

### Estado do banco ativo

Em 18/09/2026, a migração foi aplicada e as tabelas legadas foram removidas com
autorização do responsável. Foram conferidos os 12 vínculos de administrador e
os 9 de desenvolvedor, sem vínculos ausentes ou diferenças de estado. A remoção
preservou integralmente os 16 registros de `USERS` e os 21 de `USER_ROLES`.

A cópia SQL anterior à remoção está no container webserver, em
`/root/webfusion-backups/legacy-roles-before-drop-20260918T165913Z.sql`, com acesso
restrito ao proprietário. Contém a estrutura e os registros das duas tabelas.

O banco ativo contém somente `USERS` e `USER_ROLES`. Não executar novamente
`migrateWebFusionUsers.sql` nesse banco: o script atende instalações legadas
que ainda possuem as tabelas de origem. Seus testes continuam criando essas
tabelas apenas em um schema isolado para validar a migração histórica.

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

Os testes de banco devem usar schema isolado e validar: migração de usuários
exclusivos das tabelas legadas, preservação de vínculos inativos e cumulativos,
precedência de administrador, revogação, exclusão em cascata e persistência da
foto quando o proxy omite o cabeçalho.
