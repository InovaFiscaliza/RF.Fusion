-- Synchronize legacy backup suspension state between FILE_TASK and
-- FILE_TASK_HISTORY in BPDATA.
--
-- Conservative scope:
-- 1. Touch only host-dependent BACKUP rows
-- 2. Touch only cases where FILE_TASK is already TASK_SUSPENDED (-2)
-- 3. Touch only FILE_TASK_HISTORY rows still marked as TASK_PENDING (1)
-- 4. Leave historical TASK_ERROR (-1) rows untouched for manual review

USE BPDATA;

-- ==========================================================
-- Preview: rows eligible for conservative repair
-- Expected on the inspected database: 506 rows
-- ==========================================================

SELECT COUNT(*) AS ELIGIBLE_ROWS
FROM FILE_TASK ft
JOIN FILE_TASK_HISTORY fh
  ON fh.FK_HOST = ft.FK_HOST
 AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
WHERE ft.NU_TYPE = 1
  AND ft.NU_STATUS = -2
  AND fh.NU_STATUS_BACKUP = 1;

-- ==========================================================
-- Conservative repair
-- ==========================================================

UPDATE FILE_TASK_HISTORY fh
JOIN FILE_TASK ft
  ON fh.FK_HOST = ft.FK_HOST
 AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
SET
    fh.NU_STATUS_BACKUP = -2,
    fh.NA_MESSAGE = 'Host unreachable — host-dependent history suspended by host_check service'
WHERE ft.NU_TYPE = 1
  AND ft.NU_STATUS = -2
  AND fh.NU_STATUS_BACKUP = 1;

-- ==========================================================
-- Post-check: suspended FILE_TASK should now match suspended history
-- ==========================================================

SELECT
    COUNT(*) AS TOTAL_SUSPENDED_FILE_TASKS,
    SUM(CASE WHEN fh.NU_STATUS_BACKUP = -2 THEN 1 ELSE 0 END) AS HISTORY_SUSPENDED,
    SUM(CASE WHEN fh.NU_STATUS_BACKUP = -1 THEN 1 ELSE 0 END) AS HISTORY_STILL_ERROR,
    SUM(CASE WHEN fh.NU_STATUS_BACKUP = 1 THEN 1 ELSE 0 END) AS HISTORY_STILL_PENDING,
    SUM(CASE WHEN fh.NU_STATUS_BACKUP IS NULL THEN 1 ELSE 0 END) AS HISTORY_MISSING
FROM FILE_TASK ft
LEFT JOIN FILE_TASK_HISTORY fh
  ON fh.FK_HOST = ft.FK_HOST
 AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
WHERE ft.NU_TYPE = 1
  AND ft.NU_STATUS = -2;

-- ==========================================================
-- Residual manual-review set: historical backup errors intentionally
-- left untouched by this conservative patch
-- ==========================================================

SELECT
    ft.ID_FILE_TASK,
    ft.FK_HOST,
    ft.NA_HOST_FILE_PATH,
    ft.NA_HOST_FILE_NAME,
    fh.ID_HISTORY,
    fh.NU_STATUS_BACKUP,
    fh.NA_MESSAGE
FROM FILE_TASK ft
JOIN FILE_TASK_HISTORY fh
  ON fh.FK_HOST = ft.FK_HOST
 AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
WHERE ft.NU_TYPE = 1
  AND ft.NU_STATUS = -2
  AND fh.NU_STATUS_BACKUP = -1
ORDER BY ft.FK_HOST, ft.NA_HOST_FILE_PATH, ft.NA_HOST_FILE_NAME;
