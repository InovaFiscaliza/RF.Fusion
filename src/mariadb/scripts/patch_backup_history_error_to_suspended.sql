-- Synchronize residual suspended backup FILE_TASK rows whose history still
-- carries TASK_ERROR (-1) instead of TASK_SUSPENDED (-2).
--
-- Scenario:
-- FILE_TASK is already suspended because the host became unreachable, but the
-- matching FILE_TASK_HISTORY backup phase was left in error. After manual
-- review, these rows should mirror the suspended state.
--
-- Conservative rules:
-- 1. Touch only BACKUP FILE_TASK rows (NU_TYPE = 1).
-- 2. Touch only FILE_TASK rows already suspended (NU_STATUS = -2).
-- 3. Touch only FILE_TASK_HISTORY rows still marked as backup error (-1).
-- 4. Leave processing untouched; it should remain pending (1).
--
-- Expected candidate count at the time this patch was prepared: 27 rows.

USE BPDATA;

SELECT COUNT(*) AS ELIGIBLE_ROWS
FROM FILE_TASK ft
JOIN FILE_TASK_HISTORY fh
  ON fh.FK_HOST = ft.FK_HOST
 AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
 AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
WHERE ft.NU_TYPE = 1
  AND ft.NU_STATUS = -2
  AND fh.NU_STATUS_BACKUP = -1;

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
  AND fh.NU_STATUS_BACKUP = -1;

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
  AND ft.NU_STATUS IN (-2, -1, 1)
  AND fh.NU_STATUS_BACKUP IN (-2, -1, 1)
GROUP BY ft.NU_STATUS, fh.NU_STATUS_BACKUP, fh.NU_STATUS_PROCESSING
ORDER BY TOTAL DESC, ft.NU_STATUS, fh.NU_STATUS_BACKUP, fh.NU_STATUS_PROCESSING;
