USE BPDATA;

ALTER TABLE FILE_TASK
    ADD COLUMN NA_ERROR_DOMAIN VARCHAR(32) DEFAULT NULL COMMENT 'Structured error domain for grouping/reporting' AFTER NA_MESSAGE,
    ADD COLUMN NA_ERROR_STAGE VARCHAR(32) DEFAULT NULL COMMENT 'Structured error stage extracted from the persisted message' AFTER NA_ERROR_DOMAIN,
    ADD COLUMN NA_ERROR_CODE VARCHAR(64) DEFAULT NULL COMMENT 'Stable canonical error code' AFTER NA_ERROR_STAGE,
    ADD COLUMN NA_ERROR_SUMMARY TEXT COMMENT 'Stable aggregation-friendly error summary' AFTER NA_ERROR_CODE,
    ADD COLUMN NA_ERROR_DETAIL TEXT COMMENT 'Volatile contextual detail kept apart from the grouping summary' AFTER NA_ERROR_SUMMARY,
    ADD COLUMN NU_ERROR_CLASSIFIER_VERSION SMALLINT DEFAULT NULL COMMENT 'Version of the classifier used to populate the structured error fields' AFTER NA_ERROR_DETAIL;

CREATE INDEX idx_file_task_error_group
ON FILE_TASK (
    NU_STATUS,
    NU_TYPE,
    NA_ERROR_DOMAIN,
    NA_ERROR_STAGE,
    NA_ERROR_CODE
);

ALTER TABLE FILE_TASK_HISTORY
    ADD COLUMN NA_ERROR_DOMAIN VARCHAR(32) DEFAULT NULL COMMENT 'Structured error domain for grouping/reporting' AFTER NA_MESSAGE,
    ADD COLUMN NA_ERROR_STAGE VARCHAR(32) DEFAULT NULL COMMENT 'Structured error stage extracted from the persisted message' AFTER NA_ERROR_DOMAIN,
    ADD COLUMN NA_ERROR_CODE VARCHAR(64) DEFAULT NULL COMMENT 'Stable canonical error code' AFTER NA_ERROR_STAGE,
    ADD COLUMN NA_ERROR_SUMMARY TEXT COMMENT 'Stable aggregation-friendly error summary' AFTER NA_ERROR_CODE,
    ADD COLUMN NA_ERROR_DETAIL TEXT COMMENT 'Volatile contextual detail kept apart from the grouping summary' AFTER NA_ERROR_SUMMARY,
    ADD COLUMN NU_ERROR_CLASSIFIER_VERSION SMALLINT DEFAULT NULL COMMENT 'Version of the classifier used to populate the structured error fields' AFTER NA_ERROR_DETAIL;

CREATE INDEX idx_fth_backup_error_group
ON FILE_TASK_HISTORY (
    NU_STATUS_BACKUP,
    NA_ERROR_DOMAIN,
    NA_ERROR_STAGE,
    NA_ERROR_CODE
);

CREATE INDEX idx_fth_processing_error_group
ON FILE_TASK_HISTORY (
    NU_STATUS_PROCESSING,
    NA_ERROR_DOMAIN,
    NA_ERROR_STAGE,
    NA_ERROR_CODE
);
