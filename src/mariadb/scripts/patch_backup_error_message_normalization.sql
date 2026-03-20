-- Normalize legacy backup error messages in BPDATA.
--
-- Goals:
-- 1. Give recurring backup failures a stable [code=...] for aggregation
-- 2. Replace overly generic labels such as "File transfer failed"
-- 3. Keep file identity in the message prefix (`file=...`) untouched

USE BPDATA;

-- ==========================================================
-- FILE_TASK_HISTORY
-- ==========================================================

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=TRANSFER] [type=SSHException] File transfer failed',
    '[ERROR] [stage=TRANSFER] [type=SSHException] [code=SSH_TRANSFER_FAILED] SSH/SFTP transfer failed'
)
WHERE NU_STATUS_BACKUP = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=TRANSFER] [type=SSHException] File transfer failed';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=TRANSFER] [type=TimeoutError] File transfer failed',
    '[ERROR] [stage=TRANSFER] [type=TimeoutError] [code=TRANSFER_TIMEOUT] File transfer timed out'
)
WHERE NU_STATUS_BACKUP = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=TRANSFER] [type=TimeoutError] File transfer failed';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=AUTH] [type=AuthenticationException] Authentication failed (bad credentials)',
    '[ERROR] [stage=AUTH] [type=AuthenticationException] [code=AUTH_FAILED] Authentication failed'
)
WHERE NU_STATUS_BACKUP = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=AUTH] [type=AuthenticationException] Authentication failed (bad credentials)';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=SSH] [type=SSHException] SSH negotiation failed',
    '[ERROR] [stage=SSH] [type=SSHException] [code=SSH_NEGOTIATION_FAILED] SSH negotiation failed'
)
WHERE NU_STATUS_BACKUP = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=SSH] [type=SSHException] SSH negotiation failed';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=CONNECT] [type=OSError] SSH/SFTP initialization failed',
    '[ERROR] [stage=CONNECT] [type=OSError] [code=SFTP_INIT_FAILED] SSH/SFTP initialization failed'
)
WHERE NU_STATUS_BACKUP = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=CONNECT] [type=OSError] SSH/SFTP initialization failed';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=CONNECT] [type=NoValidConnectionsError] SSH/SFTP initialization failed',
    '[ERROR] [stage=CONNECT] [type=NoValidConnectionsError] [code=SFTP_INIT_FAILED] SSH/SFTP initialization failed'
)
WHERE NU_STATUS_BACKUP = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=CONNECT] [type=NoValidConnectionsError] SSH/SFTP initialization failed';

-- ==========================================================
-- FILE_TASK
-- ==========================================================

UPDATE FILE_TASK
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=TRANSFER] [type=SSHException] File transfer failed',
    '[ERROR] [stage=TRANSFER] [type=SSHException] [code=SSH_TRANSFER_FAILED] SSH/SFTP transfer failed'
)
WHERE NU_TYPE = 1
  AND NU_STATUS = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=TRANSFER] [type=SSHException] File transfer failed';

UPDATE FILE_TASK
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=TRANSFER] [type=TimeoutError] File transfer failed',
    '[ERROR] [stage=TRANSFER] [type=TimeoutError] [code=TRANSFER_TIMEOUT] File transfer timed out'
)
WHERE NU_TYPE = 1
  AND NU_STATUS = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=TRANSFER] [type=TimeoutError] File transfer failed';

UPDATE FILE_TASK
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=AUTH] [type=AuthenticationException] Authentication failed (bad credentials)',
    '[ERROR] [stage=AUTH] [type=AuthenticationException] [code=AUTH_FAILED] Authentication failed'
)
WHERE NU_TYPE = 1
  AND NU_STATUS = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=AUTH] [type=AuthenticationException] Authentication failed (bad credentials)';

UPDATE FILE_TASK
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=SSH] [type=SSHException] SSH negotiation failed',
    '[ERROR] [stage=SSH] [type=SSHException] [code=SSH_NEGOTIATION_FAILED] SSH negotiation failed'
)
WHERE NU_TYPE = 1
  AND NU_STATUS = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=SSH] [type=SSHException] SSH negotiation failed';

UPDATE FILE_TASK
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=CONNECT] [type=OSError] SSH/SFTP initialization failed',
    '[ERROR] [stage=CONNECT] [type=OSError] [code=SFTP_INIT_FAILED] SSH/SFTP initialization failed'
)
WHERE NU_TYPE = 1
  AND NU_STATUS = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=CONNECT] [type=OSError] SSH/SFTP initialization failed';

UPDATE FILE_TASK
SET NA_MESSAGE = REPLACE(
    NA_MESSAGE,
    '[ERROR] [stage=CONNECT] [type=NoValidConnectionsError] SSH/SFTP initialization failed',
    '[ERROR] [stage=CONNECT] [type=NoValidConnectionsError] [code=SFTP_INIT_FAILED] SSH/SFTP initialization failed'
)
WHERE NU_TYPE = 1
  AND NU_STATUS = -1
  AND NA_MESSAGE LIKE 'Backup Error | % | [ERROR] [stage=CONNECT] [type=NoValidConnectionsError] SSH/SFTP initialization failed';

-- ==========================================================
-- Quick audit
-- ==========================================================

SELECT
    NA_MESSAGE,
    COUNT(*) AS OCCURRENCES
FROM FILE_TASK_HISTORY
WHERE NU_STATUS_BACKUP = -1
GROUP BY NA_MESSAGE
ORDER BY OCCURRENCES DESC, NA_MESSAGE ASC
LIMIT 50;
