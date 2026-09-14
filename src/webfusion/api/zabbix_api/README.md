# Cliente da API do Zabbix

Este pacote implementa o cliente Python reutilizado pelo WebFusion para
consultar e alterar configurações no Zabbix. **Não é um blueprint REST** e não
publica `/rffusion/api/zabbix`. O tráfego de saída usa JSON-RPC no endpoint
externo configurado.

## Organização e consumidores

| Arquivo | Responsabilidade |
|---|---|
| [connection.py](connection.py) | Configuração, criação do cliente e consulta da URL |
| [client.py](client.py) | `ZabbixApiClient`, chamadas JSON-RPC e `ZabbixApiError` |

Os consumidores importam `api.zabbix_api`:

- `modules/configuration`: catálogo, macros efetivas, sobrescritas e alterações;
- `modules/tasks`: defaults de backup derivados das macros das estações;
- `modules/alarms`: problemas operacionais normalizados para a interface.

Validação da intenção do operador e sincronização de macros com `BPDATA.HOST`
pertencem aos serviços desses módulos. O cliente não decide estados de tasks
nem escreve no MariaDB.

## Configuração

| Variável | Significado |
|---|---|
| `ZABBIX_API_URL` | Endpoint JSON-RPC do Zabbix, normalmente terminado em `api_jsonrpc.php` |
| `ZABBIX_API_TOKEN` | Token técnico para a integração já existente |
| `ZABBIX_API_TIMEOUT_SECONDS` | Timeout por chamada; padrão efetivo de 10 segundos, limitado entre 3 e 30 |

Valores não vazios do ambiente têm precedência sobre os do arquivo local
`src/zabbix/.secret/zabbix_api.env`, fora deste pacote. O leitor reconhece apenas
essas três chaves, ignora linhas vazias e comentários iniciados por `#`, e não
remove aspas do valor: use linhas simples `CHAVE=valor`.

O arquivo é ignorado pelo Git. Sua ausência é permitida se o ambiente fornecer
URL e token; erro de leitura é propagado. URL ou token ausentes impedem a
criação do cliente. Um timeout não inteiro usa o padrão; números inteiros fora
da faixa são limitados pelo construtor.

O token segue no campo `auth` do corpo JSON-RPC da implementação atual. Não
é colocado na URL nem devolvido à interface. A configuração dessa integração
não é o mecanismo de autenticação dos usuários WebFusion ou do appAnalise.

## Interface Python

```python
from api.zabbix_api.connection import build_client

client = build_client()
catalog = client.list_catalog()
configuration = client.get_host_configuration(host_id="12345")
```

O ID acima é ilustrativo e deve corresponder a um host do Zabbix; não é
necessariamente o `ID_HOST` do BPDATA.

| Método | Retorno ou efeito |
|---|---|
| `list_catalog()` | Dicionário com `templates` e `hosts` dos perfis gerenciados |
| `list_appcataloga_problems()` | Lista de problemas para o fluxo operacional |
| `get_host_configuration(host_id)` | Configuração efetiva de uma estação |
| `get_host_configurations(host_ids)` | Configurações indexadas por ID, compartilhando consultas de herança |
| `get_template_configuration(template_id)` | Configuração efetiva de um template |
| `create_macro(owner_id=..., macro_name=..., macro_type=..., value=...)` | Cria uma macro direta |
| `update_macro(macro_id=..., value=...)` | Altera o valor de uma macro direta |
| `delete_macro(macro_id=...)` | Remove uma macro direta, permitindo aplicar a herança |

Configurações de host incluem `kind`, `target_id`, `title`, `technical_name`,
`profiles`, `macros`, `direct_macros` e `inherited_macro_groups`. A herança de
templates e as sobrescritas são resolvidas no cliente; apresentação e regras
de edição ficam no módulo consumidor.

A criação aceita os tipos de macro texto e segredo, com validação de nome.
Valores secretos e de cofre não são recuperados para exibição. Macros protegidas
são tratadas conforme as constantes do cliente e as validações dos serviços.
As permissões de edição do WebFusion continuam sendo aplicadas nas rotas e
serviços consumidores; não use os métodos de baixo nível para contorná-las.

## Erros e comportamento operacional

Falhas de conexão, status HTTP de erro, JSON inválido, erro JSON-RPC ou ausência
de `result` geram `ZabbixApiError`. O consumidor decide como apresentar o erro.
Não há cache persistido de resultados nem repetição automática de operações
neste cliente; chamadas que alteram macros não são repetidas silenciosamente.

Cada chamada usa `POST` e `Content-Type: application/json-rpc`, com `jsonrpc`,
`method`, `params`, `auth` e um `id` sequencial por instância. O transporte usa
`urllib` da biblioteca padrão, sem SDK adicional.

## Validação

No container webserver:

```bash
cd /RF.Fusion
/usr/local/bin/python -m unittest test.tests.zabbix.test_zabbix_api -q
/usr/local/bin/python -m unittest test.tests.webfusion.test_configuration_service -q
/usr/local/bin/python -m unittest test.tests.webfusion.test_alarms_service -q
```

Os testes substituem o transporte e não alteram o Zabbix real. Validações de
escrita ponta a ponta exigem uma estação/template de teste e o fluxo autorizado
da interface. A reorganização do pacote não deslocou o arquivo de configuração.

Voltar ao [índice das APIs](../README.md) ou ao [WebFusion](../../README.MD).
