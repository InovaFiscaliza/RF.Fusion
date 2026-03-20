-- Recreate orphaned backup FILE_TASK rows from FILE_TASK_HISTORY.
--
-- Scenario:
-- Some history rows were left in an inconsistent "backup pending / processing
-- done" state without a corresponding FILE_TASK. We intentionally recreate the
-- backup task so the file can be fetched again and reprocessed end-to-end.
--
-- Conservative rules:
-- 1. Touch only FILE_TASK_HISTORY rows with no matching FILE_TASK.
-- 2. Touch only rows where PROCESSING is already marked DONE (0) while
--    BACKUP is not DONE, which is internally inconsistent.
-- 3. Reopen BACKUP and PROCESSING as PENDING so the normal workers can replay
--    the full host-dependent flow.
--
-- Expected candidate count at the time this patch was prepared: 16 rows.

USE BPDATA;

DROP TEMPORARY TABLE IF EXISTS tmp_history_only_backup_replay;

CREATE TEMPORARY TABLE tmp_history_only_backup_replay AS
SELECT
    fh.ID_HISTORY,
    fh.FK_HOST,
    fh.NA_HOST_FILE_PATH,
    fh.NA_HOST_FILE_NAME,
    fh.NA_EXTENSION,
    fh.VL_FILE_SIZE_KB,
    fh.DT_FILE_CREATED,
    fh.DT_FILE_MODIFIED
FROM FILE_TASK_HISTORY fh
LEFT JOIN FILE_TASK ft
  ON ft.FK_HOST = fh.FK_HOST
 AND ft.NA_HOST_FILE_PATH = fh.NA_HOST_FILE_PATH
 AND ft.NA_HOST_FILE_NAME = fh.NA_HOST_FILE_NAME
WHERE fh.NU_STATUS_PROCESSING = 0
  AND fh.NU_STATUS_BACKUP <> 0
  AND ft.ID_FILE_TASK IS NULL;

-- Preview the exact rows that will be replayed.
SELECT
    COUNT(*) AS CANDIDATE_ROWS
FROM tmp_history_only_backup_replay;

SELECT
    t.ID_HISTORY,
    t.FK_HOST,
    h.NA_HOST_NAME,
    h.IS_OFFLINE,
    t.NA_HOST_FILE_PATH,
    t.NA_HOST_FILE_NAME
FROM tmp_history_only_backup_replay t
LEFT JOIN HOST h
  ON h.ID_HOST = t.FK_HOST
ORDER BY t.FK_HOST, t.NA_HOST_FILE_NAME;

START TRANSACTION;

-- Recreate the missing FILE_TASK as a normal backup pending task.
INSERT INTO FILE_TASK (
    FK_HOST,
    DT_FILE_TASK,
    NU_TYPE,
    NA_HOST_FILE_PATH,
    NA_HOST_FILE_NAME,
    NU_HOST_FILE_MD5,
    NA_SERVER_FILE_PATH,
    NA_SERVER_FILE_NAME,
    NA_SERVER_FILE_MD5,
    NU_STATUS,
    NU_PID,
    NA_EXTENSION,
    VL_FILE_SIZE_KB,
    DT_FILE_CREATED,
    DT_FILE_MODIFIED,
    NA_MESSAGE
)
SELECT
    t.FK_HOST,
    NOW(),
    1,
    t.NA_HOST_FILE_PATH,
    t.NA_HOST_FILE_NAME,
    NULL,
    NULL,
    NULL,
    NULL,
    1,
    NULL,
    t.NA_EXTENSION,
    t.VL_FILE_SIZE_KB,
    t.DT_FILE_CREATED,
    t.DT_FILE_MODIFIED,
    CONCAT(
        'Backup Pending | file=',
        t.NA_HOST_FILE_PATH,
        '/',
        t.NA_HOST_FILE_NAME,
        ' | Recreated from FILE_TASK_HISTORY inconsistency for backup replay'
    )
FROM tmp_history_only_backup_replay t;

-- Reopen the history row so backup + processing are replayed coherently.
UPDATE FILE_TASK_HISTORY fh
JOIN tmp_history_only_backup_replay t
  ON t.ID_HISTORY = fh.ID_HISTORY
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
        ' | Recreated from FILE_TASK_HISTORY inconsistency for backup replay'
    );

COMMIT;

-- Post-checks.
SELECT
    COUNT(*) AS RECREATED_FILE_TASKS
FROM FILE_TASK ft
JOIN tmp_history_only_backup_replay t
  ON t.FK_HOST = ft.FK_HOST
 AND t.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND t.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
WHERE ft.NU_TYPE = 1
  AND ft.NU_STATUS = 1;

SELECT
    fh.NU_STATUS_BACKUP,
    fh.NU_STATUS_PROCESSING,
    COUNT(*) AS TOTAL
FROM FILE_TASK_HISTORY fh
JOIN tmp_history_only_backup_replay t
  ON t.ID_HISTORY = fh.ID_HISTORY
GROUP BY fh.NU_STATUS_BACKUP, fh.NU_STATUS_PROCESSING
ORDER BY TOTAL DESC, fh.NU_STATUS_BACKUP, fh.NU_STATUS_PROCESSING;

DROP TEMPORARY TABLE IF EXISTS tmp_history_only_backup_replay;
