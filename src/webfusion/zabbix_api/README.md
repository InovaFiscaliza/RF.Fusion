# Integração com a API do Zabbix

Este pacote centraliza a comunicação reutilizável do WebFusion com a API do
Zabbix. Módulos de funcionalidade não devem criar requisições JSON-RPC nem ler
credenciais diretamente.

- `connection.py`: lê as configurações de ambiente ou o segredo local e cria o
  cliente autenticado.
- `client.py`: implementa as chamadas JSON-RPC e captura dados compartilhados,
  como hosts, templates, macros e problemas.

Os módulos em `modules/` devem manter somente a validação, normalização e a
apresentação específicas de sua funcionalidade. Por exemplo,
`configuration` interpreta e sincroniza macros, enquanto `alarms`
normaliza problemas para a tabela operacional.

As variáveis `ZABBIX_API_URL`, `ZABBIX_API_TOKEN` e
`ZABBIX_API_TIMEOUT_SECONDS` podem ser fornecidas pelo ambiente. Na ausência
delas, a conexão usa o segredo local não versionado em
`src/zabbix/.secret/zabbix_api.env`.