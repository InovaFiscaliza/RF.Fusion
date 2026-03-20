-- Normalize high-volume processing error messages in BPDATA.
--
-- Goals:
-- 1. Collapse semantically identical legacy variants into one canonical text
-- 2. Preserve compatibility with the newer `[code=...]` message structure
-- 3. Avoid destroying highly specific per-file detail such as FileNotFound paths

USE BPDATA;

-- ==========================================================
-- FILE_TASK_HISTORY
-- ==========================================================

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] [code=GPS_GNSS_UNAVAILABLE] Invalid GPS reading: GNSS unavailable sentinel'
WHERE NU_STATUS_PROCESSING = -1
  AND NA_MESSAGE IN (
      'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Invalid GPS reading: GNSS unavailable sentinel',
      'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Invalid GPS reading: lat=lon=alt=-1 (GNSS unavailable sentinel)'
  );

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] [code=NO_VALID_SPECTRA] BIN discarded: no valid spectra after validation'
WHERE NU_STATUS_PROCESSING = -1
  AND NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] BIN discarded: no valid spectra after validation';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] [code=SPECTRUM_LIST_EMPTY] Spectrum list is empty'
WHERE NU_STATUS_PROCESSING = -1
  AND NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Spectrum list is empty';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=KeyError] [code=HOSTNAME_MISSING] Hostname missing or invalid'
WHERE NU_STATUS_PROCESSING = -1
  AND NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=KeyError] ''hostname''';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] [code=HOSTNAME_MISSING] Hostname missing or invalid'
WHERE NU_STATUS_PROCESSING = -1
  AND NA_MESSAGE IN (
      'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Hostname missing or invalid',
      'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Hostname resolution failed: invalid hostname'
  );

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=SITE] [type=Exception] [code=SITE_GEOGRAPHIC_CODES_NOT_FOUND] Error inserting site in DIM_SPECTRUM_SITE: geographic codes not found'
WHERE NU_STATUS_PROCESSING = -1
  AND NA_MESSAGE LIKE 'Processing Error | [ERROR] [stage=SITE] [type=Exception] Error inserting site in DIM_SPECTRUM_SITE: Error retrieving geographic codes:%';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=ValueError] [code=INVALID_DATETIME_MONTH] Invalid datetime string: month out of range'
WHERE NU_STATUS_PROCESSING = -1
  AND NA_MESSAGE LIKE 'Processing Error | [ERROR] [stage=PROCESS] [type=ValueError] Month out of range in datetime string "%';

UPDATE FILE_TASK_HISTORY
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=ValueError] [code=INVALID_BUFFER_SIZE] Invalid binary buffer size'
WHERE NU_STATUS_PROCESSING = -1
  AND NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=ValueError] buffer size must be a multiple of element size';

-- ==========================================================
-- FILE_TASK
-- ==========================================================

UPDATE FILE_TASK
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] [code=GPS_GNSS_UNAVAILABLE] Invalid GPS reading: GNSS unavailable sentinel'
WHERE NU_TYPE = 2
  AND NU_STATUS = -1
  AND NA_MESSAGE IN (
      'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Invalid GPS reading: GNSS unavailable sentinel',
      'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Invalid GPS reading: lat=lon=alt=-1 (GNSS unavailable sentinel)'
  );

UPDATE FILE_TASK
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] [code=NO_VALID_SPECTRA] BIN discarded: no valid spectra after validation'
WHERE NU_TYPE = 2
  AND NU_STATUS = -1
  AND NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] BIN discarded: no valid spectra after validation';

UPDATE FILE_TASK
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] [code=SPECTRUM_LIST_EMPTY] Spectrum list is empty'
WHERE NU_TYPE = 2
  AND NU_STATUS = -1
  AND NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Spectrum list is empty';

UPDATE FILE_TASK
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=KeyError] [code=HOSTNAME_MISSING] Hostname missing or invalid'
WHERE NU_TYPE = 2
  AND NU_STATUS = -1
  AND NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=KeyError] ''hostname''';

UPDATE FILE_TASK
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] [code=HOSTNAME_MISSING] Hostname missing or invalid'
WHERE NU_TYPE = 2
  AND NU_STATUS = -1
  AND NA_MESSAGE IN (
      'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Hostname missing or invalid',
      'Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Hostname resolution failed: invalid hostname'
  );

UPDATE FILE_TASK
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=SITE] [type=Exception] [code=SITE_GEOGRAPHIC_CODES_NOT_FOUND] Error inserting site in DIM_SPECTRUM_SITE: geographic codes not found'
WHERE NU_TYPE = 2
  AND NU_STATUS = -1
  AND NA_MESSAGE LIKE 'Processing Error | [ERROR] [stage=SITE] [type=Exception] Error inserting site in DIM_SPECTRUM_SITE: Error retrieving geographic codes:%';

UPDATE FILE_TASK
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=ValueError] [code=INVALID_DATETIME_MONTH] Invalid datetime string: month out of range'
WHERE NU_TYPE = 2
  AND NU_STATUS = -1
  AND NA_MESSAGE LIKE 'Processing Error | [ERROR] [stage=PROCESS] [type=ValueError] Month out of range in datetime string "%';

UPDATE FILE_TASK
SET NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=ValueError] [code=INVALID_BUFFER_SIZE] Invalid binary buffer size'
WHERE NU_TYPE = 2
  AND NU_STATUS = -1
  AND NA_MESSAGE = 'Processing Error | [ERROR] [stage=PROCESS] [type=ValueError] buffer size must be a multiple of element size';

-- ==========================================================
-- Quick audit
-- ==========================================================

SELECT
    NA_MESSAGE,
    COUNT(*) AS OCCURRENCES
FROM FILE_TASK_HISTORY
WHERE NU_STATUS_PROCESSING = -1
GROUP BY NA_MESSAGE
ORDER BY OCCURRENCES DESC, NA_MESSAGE ASC
LIMIT 50;
