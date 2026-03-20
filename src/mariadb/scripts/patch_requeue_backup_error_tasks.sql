-- Requeue legacy backup rows that are still stuck in ERROR/ERROR.
--
-- Scenario:
-- Some backup FILE_TASK rows and their matching FILE_TASK_HISTORY records are
-- both marked as TASK_ERROR (-1), but should be retried by the normal backup
-- worker flow. We intentionally send them back to TASK_PENDING (1).
--
-- Conservative rules:
-- 1. Touch only BACKUP FILE_TASK rows (NU_TYPE = 1).
-- 2. Touch only rows where FILE_TASK and FILE_TASK_HISTORY agree on
--    backup error (-1 / -1).
-- 3. Reopen PROCESSING as TASK_PENDING as well, so the replayed backup can
--    flow naturally into processing if it succeeds.
--
-- Expected candidate count at the time this patch was prepared: 28 rows.

USE BPDATA;

DROP TEMPORARY TABLE IF EXISTS tmp_backup_error_requeue;

CREATE TEMPORARY TABLE tmp_backup_error_requeue AS
SELECT
    ft.FK_HOST,
    ft.NA_HOST_FILE_PATH,
    ft.NA_HOST_FILE_NAME
FROM FILE_TASK ft
JOIN FILE_TASK_HISTORY fh
  ON fh.FK_HOST = ft.FK_HOST
 AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
WHERE ft.NU_TYPE = 1
  AND ft.NU_STATUS = -1
  AND fh.NU_STATUS_BACKUP = -1;

-- ==========================================================
-- Preview: exact rows eligible for retry
-- ==========================================================

SELECT COUNT(*) AS ELIGIBLE_ROWS
FROM tmp_backup_error_requeue;

SELECT
    t.FK_HOST,
    h.NA_HOST_NAME,
    h.IS_OFFLINE,
    t.NA_HOST_FILE_PATH,
    t.NA_HOST_FILE_NAME,
    fh.NU_STATUS_PROCESSING,
    fh.NA_MESSAGE
FROM tmp_backup_error_requeue t
JOIN FILE_TASK_HISTORY fh
  ON fh.FK_HOST = t.FK_HOST
 AND fh.NA_HOST_FILE_PATH = t.NA_HOST_FILE_PATH
 AND fh.NA_HOST_FILE_NAME = t.NA_HOST_FILE_NAME
LEFT JOIN HOST h
  ON h.ID_HOST = t.FK_HOST
ORDER BY h.NA_HOST_NAME, t.NA_HOST_FILE_NAME;

START TRANSACTION;

-- Requeue FILE_TASK itself.
UPDATE FILE_TASK ft
JOIN tmp_backup_error_requeue t
  ON t.FK_HOST = ft.FK_HOST
 AND t.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND t.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
SET
    ft.DT_FILE_TASK = NOW(),
    ft.NU_STATUS = 1,
    ft.NU_PID = NULL,
    ft.NA_MESSAGE = CONCAT(
        'Backup Pending | file=',
        ft.NA_HOST_FILE_PATH,
        '/',
        ft.NA_HOST_FILE_NAME,
        ' | Requeued manually from legacy backup error state'
    )
WHERE ft.NU_TYPE = 1
  AND ft.NU_STATUS = -1;

-- Reopen history so backup and downstream processing can replay coherently.
UPDATE FILE_TASK_HISTORY fh
JOIN tmp_backup_error_requeue t
  ON t.FK_HOST = fh.FK_HOST
 AND t.NA_HOST_FILE_PATH = fh.NA_HOST_FILE_PATH
 AND t.NA_HOST_FILE_NAME = fh.NA_HOST_FILE_NAME
SET
    fh.DT_BACKUP = NULL,
    fh.DT_PROCESSED = NULL,
    fh.NU_STATUS_BACKUP = 1,
    fh.NU_STATUS_PROCESSING = 1,
    fh.NA_MESSAGE = CONCAT(
        'Backup Pending | file=',
        fh.NA_HOST_FILE_PATH,
        '/',
        fh.NA_HOST_FILE_NAME,
        ' | Requeued manually from legacy backup error state'
    )
WHERE fh.NU_STATUS_BACKUP = -1;

COMMIT;

-- ==========================================================
-- Post-checks
-- ==========================================================

SELECT
    ft.NU_STATUS AS FILE_TASK_STATUS,
    fh.NU_STATUS_BACKUP AS HISTORY_BACKUP_STATUS,
    fh.NU_STATUS_PROCESSING AS HISTORY_PROCESSING_STATUS,
    COUNT(*) AS TOTAL
FROM FILE_TASK ft
JOIN FILE_TASK_HISTORY fh
  ON fh.FK_HOST = ft.FK_HOST
 AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
WHERE ft.NU_TYPE = 1
  AND fh.NA_MESSAGE LIKE 'Backup Pending | file=% | Requeued manually from legacy backup error state'
GROUP BY ft.NU_STATUS, fh.NU_STATUS_BACKUP, fh.NU_STATUS_PROCESSING
ORDER BY TOTAL DESC, ft.NU_STATUS, fh.NU_STATUS_BACKUP, fh.NU_STATUS_PROCESSING;

DROP TEMPORARY TABLE IF EXISTS tmp_backup_error_requeue;
