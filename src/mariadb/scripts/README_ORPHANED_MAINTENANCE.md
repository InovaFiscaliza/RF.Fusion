# Manutenção de FILE_TASK_HISTORY Órfãos

## Documento de Instruções de Uso

---

## 📋 Sumário do Problema

**Encontrado**: Registros em `FILE_TASK_HISTORY` com status PENDING que não possuem `FILE_TASK` correspondente

**Exemplo reportado**: 
- `IID_HISTORY = 1123390`
- Host: `RFEye002073`

**Causa**: Operação normal do sistema (após backup bem-sucedido) ou falha de transação durante source drift

---

## 📁 Arquivos Fornecidos

### 1. **diagnostic_orphaned_file_tasks.sql** (Diagnóstico)
- Investigação completa do banco de dados
- Sem modificações
- Recomendado: Executar PRIMEIRO

**Conteúdo**:
- Investigação do caso específico (IID_HISTORY = 1123390)
- Lista de todos os registros órfãos por host
- Categorização por status (PENDING/DONE/ERROR)
- Categorização por idade
- Verificação de anomalias

### 2. **maintenance_cleanup_orphaned_file_tasks.sql** (Limpeza)
- 4 opções de limpeza com diferentes níveis de segurança
- Stored procedures reutilizáveis
- Todos com modo `dry-run` para preview

**Opções**:
1. `audit_orphaned_file_tasks()` - Auditoria
2. `clean_orphaned_by_status()` - Limpeza seletiva
3. `clean_orphaned_transactions()` - Limpeza transacional
4. `mark_orphaned_for_deletion()` - Soft delete (reversível)

### 3. **file_task_maintenance.py** (Programático)
- Interface Python para limpeza
- Logging integrado
- CLI com múltiplas opções

**Uso**: Para integração com scripts de maintenance ou CRON

### 4. **ANALYSIS_orphaned_file_tasks.md** (Análise Técnica)
- Explicação detalhada das 3 causas raízes
- Fluxo de trabalho recomendado
- Métricas de monitoramento

---

## 🚀 Instruções de Execução

### Passo 1: Diagóstico Inicial (SEM RISCO)

#### Opção A: Usando SQL (Recomendado)

```bash
# Conectar ao MariaDB
mysql -u root -p

# Selecionar banco
USE BPDATA;

# Executar diagnóstico
source /RFFusion/src/mariadb/scripts/diagnostic_orphaned_file_tasks.sql;
```

Ou em uma única linha:
```bash
mysql -u root -p BPDATA < /RFFusion/src/mariadb/scripts/diagnostic_orphaned_file_tasks.sql
```

**Saída esperada**:
- Informações detalhadas do caso IID_HISTORY = 1123390
- Lista de todos os órfãos
- Estatísticas por host
- Categorização por idade

#### Opção B: Usando Python

```bash
cd /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga
python3 file_task_maintenance.py --audit
```

**Saída esperada**:
```
======================================================================
AUDIT REPORT: Orphaned FILE_TASK_HISTORY Records
======================================================================

Total Orphaned Records: 145
Total Size: 34.56 GB (36,254,720 KB)

--- By Processing Status ---
  PENDING :    42 records (  12.34 GB)
  DONE    :    98 records (  22.10 GB)
  ERROR   :     5 records (   0.12 GB)

--- By Host ---
  RFEye002073           :   45 records (  10.20 GB)
  RFEye002074           :   32 records (   8.90 GB)
  RFEye002075           :   68 records (  15.46 GB)

--- Sample Records (First 10) ---
  [1] ID=1123390 Host=RFEye002073 File=data.log Status=PENDING Age=45d
  ...
```

---

### Passo 2: Investigação do Caso Específico

```sql
USE BPDATA;

-- Investigar IID_HISTORY = 1123390
SELECT 
    h.ID_HISTORY,
    h.FK_HOST,
    (SELECT NA_HOST_NAME FROM HOST WHERE ID_HOST = h.FK_HOST) AS host_name,
    h.NA_HOST_FILE_NAME,
    h.NA_HOST_FILE_PATH,
    h.DT_FILE_CREATED,
    h.NU_STATUS_DISCOVERY,
    h.NU_STATUS_BACKUP,
    h.NU_STATUS_PROCESSING,
    h.DT_PROCESSED,
    h.NA_ERROR_DOMAIN,
    h.NA_ERROR_CODE,
    h.NA_MESSAGE
FROM FILE_TASK_HISTORY h
WHERE h.ID_HISTORY = 1123390;
```

**Analisar resultado**:
- Se `NU_STATUS_PROCESSING = 0` e `DT_PROCESSED IS NOT NULL` → STATUS DONE (seguro deletar)
- Se `NU_STATUS_PROCESSING = 1` e `DT_PROCESSED IS NULL` → STATUS PENDING (análise necessária)
- Se `NU_STATUS_PROCESSING = -1` → STATUS ERROR (com erro associado)

---

### Passo 3: Preview da Limpeza (RECOMENDADO - SEM RISCO)

#### Opção A: SQL com Status DONE (Safest)

```sql
SOURCE /RFFusion/src/mariadb/scripts/maintenance_cleanup_orphaned_file_tasks.sql;

-- Preview: DONE records older than 30 days
CALL clean_orphaned_by_status(0, 30, TRUE);
```

**Parâmetros**:
- `0` = Filter by DONE status (-1=ERROR, 0=DONE, 1=PENDING, NULL=ALL)
- `30` = Minimum age in days
- `TRUE` = Dry-run mode (preview only)

#### Opção B: Python

```bash
# Preview DONE records older than 30 days
python3 file_task_maintenance.py --dry-run --status DONE --min-days 30

# Preview all orphans older than 7 days
python3 file_task_maintenance.py --dry-run --min-days 7

# Preview ERROR records only
python3 file_task_maintenance.py --dry-run --status ERROR
```

---

### Passo 4: Limpeza via Soft-Delete (RECOMENDADO - REVERSÍVEL)

Marca registros como deletados sem remover fisicamente. Pode ser revertido.

#### Opção A: SQL

```sql
SOURCE /RFFusion/src/mariadb/scripts/maintenance_cleanup_orphaned_file_tasks.sql;

-- Dry-run: Preview o que seria marcado
CALL mark_orphaned_for_deletion(7, TRUE);

-- Executar: Marcar como deletados (7+ dias)
CALL mark_orphaned_for_deletion(7, FALSE);
```

#### Opção B: Python

```bash
# Preview
python3 file_task_maintenance.py --soft-delete --min-days 7

# Execute (requires --confirm)
python3 file_task_maintenance.py --soft-delete --min-days 7 --confirm
```

---

### Passo 5: Limpeza via Hard-Delete (PERMANENTE)

Remove registros completamente do banco de dados. **NÃO REVERSÍVEL**.

#### Opção A: SQL (Transacional - Mais Segura)

```sql
SOURCE /RFFusion/src/mariadb/scripts/maintenance_cleanup_orphaned_file_tasks.sql;

-- Dry-run
CALL clean_orphaned_transactions(30, TRUE);

-- Executar
CALL clean_orphaned_transactions(30, FALSE);
```

**Parâmetro**: `30` = Mínimo de dias antigos

#### Opção B: SQL (Seletivo por Status)

```sql
SOURCE /RFFusion/src/mariadb/scripts/maintenance_cleanup_orphaned_file_tasks.sql;

-- Deletar DONE records apenas
CALL clean_orphaned_by_status(0, 30, TRUE);   -- Dry run
CALL clean_orphaned_by_status(0, 30, FALSE);  -- Execute

-- Deletar ERROR records apenas
CALL clean_orphaned_by_status(-1, 7, FALSE);

-- Deletar todas as categorias
CALL clean_orphaned_by_status(NULL, 60, FALSE);
```

#### Opção C: Python

```bash
# Hard delete DONE records older than 30 days
python3 file_task_maintenance.py --clean --status DONE --min-days 30 --confirm

# Hard delete all orphans older than 90 days
python3 file_task_maintenance.py --clean --min-days 90 --confirm
```

---

## 📊 Fluxo de Trabalho Recomendado

```
1. ANÁLISE (15 min)
   ├─ Executar: diagnostic_orphaned_file_tasks.sql
   └─ Resultado: Entender dimensão do problema

2. INVESTIGAÇÃO (10 min)
   ├─ Consultar caso específico (IID_HISTORY = 1123390)
   └─ Classificar: PENDING/DONE/ERROR?

3. SOFT DELETE (5 min - REVERSÍVEL)
   ├─ Preview: mark_orphaned_for_deletion(7, TRUE)
   └─ Execute: mark_orphaned_for_deletion(7, FALSE)

4. MONITORAR (3 dias)
   ├─ Observar se sistema funciona normalmente
   └─ Se OK, prosseguir para hard delete

5. HARD DELETE (2 min - PERMANENTE)
   ├─ Backup do banco se ainda não feito
   ├─ Preview: clean_orphaned_transactions(30, TRUE)
   └─ Execute: clean_orphaned_transactions(30, FALSE)

6. VALIDAÇÃO (10 min)
   ├─ Re-executar diagnostic_orphaned_file_tasks.sql
   └─ Confirmar que órfãos foram removidos
```

---

## ⚠️ Precauções Importante

### Antes de Executar Hard Delete

1. **Backup obrigatório**:
```bash
# Backup completo do banco
mysqldump -u root -p BPDATA > BPDATA_backup_$(date +%Y%m%d_%H%M%S).sql
```

2. **Verificar período calmo**:
   - Executar fora de horário de pico
   - Evitar durante processamento de tasks em lote

3. **Confirmar tipo de órfão**:
   - STATUS DONE: Seguro deletar
   - STATUS ERROR: Deletar com cuidado
   - STATUS PENDING: Análise recomendada

### Reverter Soft Delete

Se marcou registros como deletados e quer reverter:

```sql
UPDATE FILE_TASK_HISTORY
SET IS_PAYLOAD_DELETED = 0, DT_PAYLOAD_DELETED = NULL
WHERE ID_HISTORY IN (SELECT ID_HISTORY FROM ...);
```

### Recuperar Hard Delete

Não é possível recuperar hard deletes sem restore de backup.

---

## 📈 Monitoramento Contínuo

Adicione cron job para verificar novos órfãos:

```bash
# /etc/cron.d/file_task_maintenance

# Auditoria diária às 2 AM
0 2 * * * root /usr/bin/python3 /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/file_task_maintenance.py --audit >> /var/log/file_task_audit.log 2>&1

# Soft delete semanal (7+ dias) - terças às 3 AM
0 3 * * 2 root /usr/bin/python3 /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/file_task_maintenance.py --soft-delete --min-days 7 --confirm >> /var/log/file_task_cleanup.log 2>&1
```

---

## 🔍 Queries de Verificação

### Verificar resultado após limpeza

```sql
-- Conta órfãos remanescentes
SELECT COUNT(*) as orphaned_count
FROM FILE_TASK_HISTORY h
LEFT JOIN FILE_TASK ft ON 
    ft.FK_HOST = h.FK_HOST 
    AND ft.NA_HOST_FILE_PATH = h.NA_HOST_FILE_PATH 
    AND ft.NA_HOST_FILE_NAME = h.NA_HOST_FILE_NAME
WHERE ft.ID_FILE_TASK IS NULL;

-- Deve retornar 0 ou número muito menor
```

### Verificar crescimento de novos órfãos

```sql
-- Taxa semanal de novos órfãos
SELECT 
    DATE_FORMAT(h.DT_FILE_CREATED, '%Y-%W') as week,
    COUNT(*) as new_orphans,
    SUM(h.VL_FILE_SIZE_KB) / 1024 / 1024 as size_gb
FROM FILE_TASK_HISTORY h
LEFT JOIN FILE_TASK ft ON 
    ft.FK_HOST = h.FK_HOST 
    AND ft.NA_HOST_FILE_PATH = h.NA_HOST_FILE_PATH 
    AND ft.NA_HOST_FILE_NAME = h.NA_HOST_FILE_NAME
WHERE ft.ID_FILE_TASK IS NULL
GROUP BY DATE_FORMAT(h.DT_FILE_CREATED, '%Y-%W')
ORDER BY week DESC
LIMIT 12;
```

---

## 📞 Suporte

Para dúvidas ou problemas:

1. Consultar [ANALYSIS_orphaned_file_tasks.md](ANALYSIS_orphaned_file_tasks.md)
2. Revisar logs de aplicação:
   - `/var/log/appCataloga/` (aplicação)
   - MariaDB error log (banco de dados)
3. Verificar eventos no log:
   - `event=backup_missing_remote_file_task_delete_failed`
   - `event=cleanup_*`

---

## ✅ Checklist de Execução

```
[ ] 1. Executar diagnostic_orphaned_file_tasks.sql e revisar resultado
[ ] 2. Investigar o caso específico IID_HISTORY = 1123390
[ ] 3. Criar backup: mysqldump -u root -p BPDATA > backup.sql
[ ] 4. Executar preview (dry-run) da limpeza
[ ] 5. Executar soft-delete para órfãos de 7+ dias
[ ] 6. Monitorar sistema por 3 dias
[ ] 7. Verificar se novos órfãos estão sendo criados
[ ] 8. Se tudo OK, executar hard-delete
[ ] 9. Re-executar diagnostic para confirmar limpeza
[ ] 10. Adicionar monitoramento contínuo (cron job)
```

---

Versão: 1.0  
Data: 2026-05-13  
Baseado em: Análise de código RFFusion v2.0
