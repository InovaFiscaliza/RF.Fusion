# Análise: FILE_TASK_HISTORY Órfãos (Sem FILE_TASK Correspondente)

## Problema Reportado

**Caso específico**: `IID_HISTORY = 1123390` (RFEye002073)
- Registro encontrado em `FILE_TASK_HISTORY` com status PENDING
- Não existe `FILE_TASK` correspondente

## Causa Raiz

De acordo com a análise do código, existem **3 cenários legítimos** onde isso ocorre:

### Cenário A: Backup Bem-Sucedido (Mais Comum)
**Arquivo**: `appCataloga_file_bkp.py` (linhas 346-399)

Após conclusão bem-sucedida do backup:
1. `FILE_TASK_HISTORY` é atualizado com `NU_STATUS_BACKUP = DONE`
2. `FILE_TASK_HISTORY.NU_STATUS_PROCESSING` permanece `= 1 (PENDING)` (aguardando próxima fase)
3. `FILE_TASK` é **deletado** após histórico estar salvo

**Estado esperado**:
```
FILE_TASK_HISTORY (existe):
  NU_STATUS_DISCOVERY = 0 (DONE)
  NU_STATUS_BACKUP = 0 (DONE)
  NU_STATUS_PROCESSING = 1 (PENDING)
  DT_PROCESSED = NULL

FILE_TASK (não existe): deletado após backup bem-sucedido
```

### Cenário B: Source Drift (Arquivo Remoto Desaparecido)
**Arquivo**: `appCataloga_file_bkp.py` (linhas 427-480)

Quando um arquivo existe à descoberta mas é deletado no host remoto antes do backup:
1. Both `FILE_TASK` e `FILE_TASK_HISTORY` tentam ser deletados
2. Se uma das operações falha, fica um órfão (best-effort deletion)
3. Logado como: `"event=backup_missing_remote_file_task_delete_failed"`

**Estado esperado**:
```
FILE_TASK_HISTORY (pode conter):
  NU_STATUS_BACKUP = -1 (ERROR)
  NU_STATUS_PROCESSING = -1 (ERROR)
  NA_ERROR_DOMAIN = 'source_missing'
```

### Cenário C: Descoberta + Transição Backlog
**Arquivo**: `appCataloga_discovery.py` + `appCataloga_backlog_management.py`

Durante a transição de tarefas:
1. Discovery cria `FILE_TASK_HISTORY` com `NU_STATUS_DISCOVERY = DONE, NU_STATUS_BACKUP = 1`
2. Discovery cria `FILE_TASK` com `NU_TYPE = DISCOVERY`
3. Backlog management atualiza tipo para `BACKUP`

**Estado esperado**:
Ambas as tabelas devem existir simultaneamente durante a transição.

---

## Diagnóstico

### Scripts Fornecidos

#### 1. **diagnostic_orphaned_file_tasks.sql**
Executa investigação completa:
- Consulta o caso específico (IID_HISTORY = 1123390)
- Lista todos os registros órfãos por host
- Categoriza por status de processamento
- Categoriza por idade
- Verifica anomalias inversas (FILE_TASK sem FILE_TASK_HISTORY)

**Uso**:
```sql
mysql -u root -p BPDATA < diagnostic_orphaned_file_tasks.sql
```

#### 2. **maintenance_cleanup_orphaned_file_tasks.sql**
Oferece 4 opções de limpeza:

**Opção 1: Auditoria (Recomendada - Primeiro Passo)**
```sql
CALL audit_orphaned_file_tasks();
```
Resultado: Relatório sem modificações

**Opção 2: Limpeza Seletiva por Status**
```sql
-- Dry run: Ver quais registros seriam deletados
CALL clean_orphaned_by_status(0, 30, TRUE);
-- Parâmetros: (status=-1/0/1/NULL, dias_antigos, dry_run=true/false)

-- Executar: Deletar DONE registros com mais de 30 dias
CALL clean_orphaned_by_status(0, 30, FALSE);
```

**Opção 3: Limpeza Transacional (Mais Segura)**
```sql
-- Dry run
CALL clean_orphaned_transactions(90, TRUE);

-- Executar: Deletar registros órfãos com mais de 90 dias
CALL clean_orphaned_transactions(90, FALSE);
```

**Opção 4: Soft Delete (Recomendada para Auditoria)**
```sql
-- Marca como deletado sem remover fisicamente
CALL mark_orphaned_for_deletion(7, FALSE);
```

---

## Recomendações de Ação

### Imediatamente (Sem Risco)

**1. Executar auditoria completa**
```sql
CALL audit_orphaned_file_tasks();
```

**2. Investigar o caso específico**
```sql
-- Executar a primeira seção do diagnostic_orphaned_file_tasks.sql
SELECT * FROM FILE_TASK_HISTORY WHERE ID_HISTORY = 1123390;
```

**3. Categorizar o tipo de órfão**
- Se `NU_STATUS_PROCESSING = 0` (DONE): Probavlemente Cenário A (seguro deletar)
- Se `NU_STATUS_PROCESSING = -1` (ERROR): Cenário B (deletar com cuidado)
- Se `NU_STATUS_PROCESSING = 1` e `DT_PROCESSED IS NOT NULL`: Cenário A + DONE (seguro)
- Se `NU_STATUS_PROCESSING = 1` e `DT_PROCESSED IS NULL` e `DT_BACKUP IS NOT NULL`: Cenário A em trânsito

### Curto Prazo (1-2 semanas)

**1. Limpeza de registros antigos**
```sql
-- Ver quais seriam deletados (30+ dias, status DONE)
CALL clean_orphaned_by_status(0, 30, TRUE);

-- Deletar
CALL clean_orphaned_by_status(0, 30, FALSE);
```

**2. Limpeza preventiva com soft delete**
```sql
-- Para registros de 7+ dias, marcar como deletados
CALL mark_orphaned_for_deletion(7, FALSE);
```

### Longo Prazo (Próximos Meses)

**1. Investigar source drift**
- Monitorar logs de `event=backup_missing_remote_file_task_delete_failed`
- Ajustar retry policy se necessário

**2. Revisar garbage collector**
- Atual GC só coleta STATUS_PROCESSING = -1 (ERROR)
- Considerar incluir PENDING órfãos após período de quarentena

**3. Adicionar integridade transacional**
- Modificar código para usar transações ACID ao deletar de ambas as tabelas
- Adicionar trigger para validar integridade referencial

---

## Fluxo de Work Recomendado

### Passo 1: Análise (15 minutos)
```sql
USE BPDATA;
CALL audit_orphaned_file_tasks();

-- Investigar caso específico
SELECT * FROM FILE_TASK_HISTORY WHERE ID_HISTORY = 1123390;
```

### Passo 2: Classificação (10 minutos)
```sql
-- Ver distribuição por status
SELECT 
    CASE 
        WHEN NU_STATUS_PROCESSING = 1 THEN 'PENDING'
        WHEN NU_STATUS_PROCESSING = 0 THEN 'DONE'
        WHEN NU_STATUS_PROCESSING = -1 THEN 'ERROR'
    END AS status,
    COUNT(*) AS count
FROM FILE_TASK_HISTORY h
LEFT JOIN FILE_TASK ft ON 
    ft.FK_HOST = h.FK_HOST 
    AND ft.NA_HOST_FILE_PATH = h.NA_HOST_FILE_PATH 
    AND ft.NA_HOST_FILE_NAME = h.NA_HOST_FILE_NAME
WHERE ft.ID_FILE_TASK IS NULL
GROUP BY NU_STATUS_PROCESSING;
```

### Passo 3: Soft Delete (Reversível)
```sql
-- Marca como deletado mas mantém registro para auditoria
CALL mark_orphaned_for_deletion(7, TRUE);   -- Dry run
CALL mark_orphaned_for_deletion(7, FALSE);  -- Executar
```

### Passo 4: Hard Delete (Após Validação)
```sql
-- Após confirmar que soft delete funcionou e nenhuma API depende desses registros:
CALL clean_orphaned_by_status(0, 30, TRUE);   -- Dry run
CALL clean_orphaned_by_status(0, 30, FALSE);  -- Executar
```

---

## Métricas para Monitoramento

Adicionar consultas periódicas ao seu monitoring:

```sql
-- Verificar novos órfãos diariamente
SELECT 
    COUNT(*) AS orphaned_count,
    SUM(VL_FILE_SIZE_KB) / 1024 / 1024 AS total_gb
FROM FILE_TASK_HISTORY h
LEFT JOIN FILE_TASK ft ON 
    ft.FK_HOST = h.FK_HOST 
    AND ft.NA_HOST_FILE_PATH = h.NA_HOST_FILE_PATH 
    AND ft.NA_HOST_FILE_NAME = h.NA_HOST_FILE_NAME
WHERE ft.ID_FILE_TASK IS NULL
    AND DATEDIFF(NOW(), h.DT_FILE_CREATED) < 1;  -- Criados hoje

-- Taxa de crescimento semanal
SELECT 
    WEEK(h.DT_FILE_CREATED) AS week,
    COUNT(*) AS new_orphans
FROM FILE_TASK_HISTORY h
LEFT JOIN FILE_TASK ft ON 
    ft.FK_HOST = h.FK_HOST 
    AND ft.NA_HOST_FILE_PATH = h.NA_HOST_FILE_PATH 
    AND ft.NA_HOST_FILE_NAME = h.NA_HOST_FILE_NAME
WHERE ft.ID_FILE_TASK IS NULL
GROUP BY WEEK(h.DT_FILE_CREATED);
```

---

## Documentação Adicional

Consultar em `/RFFusion`:
- `README.md` (linhas 176-186): Explicação de FILE_TASK vs FILE_TASK_HISTORY
- `README.md` (linhas 386): Diretiva para usar FILE_TASK_HISTORY como source of truth
- `src/mariadb/scripts/README.md` (linhas 113): Descrição do esquema
- `src/webfusion/AGENTS.md`: Documentação de agentes de processamento
