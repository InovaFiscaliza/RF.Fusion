/* =====================================================================
   createProcessingDB-v10.sql
   - Canonical BPDATA schema with explicit host/server file metadata
   ===================================================================== */

CREATE DATABASE IF NOT EXISTS BPDATA
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_general_ci;

USE BPDATA;

CREATE TABLE `HOST` (
  `ID_HOST` int(11) NOT NULL COMMENT 'Primary Key to host table, from Zabbix host id',
  `NA_HOST_NAME` varchar(100) DEFAULT NULL COMMENT 'Human-readable hostname or identifier',
  `NA_HOST_ADDRESS` varchar(50) DEFAULT NULL COMMENT 'IP address or hostname',
  `NA_HOST_PORT` int(11) DEFAULT NULL COMMENT 'Port used to access the host',
  `NA_HOST_USER` varchar(50) DEFAULT NULL COMMENT 'Username for remote access',
  `NA_HOST_PASSWORD` varchar(50) DEFAULT NULL COMMENT 'Password to access the host',
  `DT_LAST_BACKUP` datetime DEFAULT NULL COMMENT 'Timestamp of the last successful backup',
  `DT_LAST_PROCESSING` datetime DEFAULT NULL COMMENT 'Timestamp of the last successful processing',
  `DT_LAST_DISCOVERY` datetime DEFAULT NULL COMMENT 'Timestamp of the last sucessful discovering',
  `IS_OFFLINE` tinyint(1) DEFAULT 0 COMMENT 'Flag: 1=Host temporarily offline (unreachable), 0=Online',
  `IS_BUSY` tinyint(1) DEFAULT 0 COMMENT 'Flag: 1=Host is Busy, 0 = Host is free',
  `NU_PID` int(11) DEFAULT 0 COMMENT 'PID of the worker currently handling this host',
  `DT_LAST_FAIL` datetime DEFAULT NULL COMMENT 'Timestamp of the last failed connection attempt',
  `DT_LAST_CHECK` datetime DEFAULT NULL COMMENT 'Timestamp of the last connectivity check attempt',
  `DT_BUSY` datetime DEFAULT NULL COMMENT 'Timestamp of when the host was last marked busy',
  `NU_DONE_FILE_DISCOVERY_TASKS` int(11) DEFAULT 0 COMMENT 'Number of completed file discovery tasks',
  `NU_ERROR_FILE_DISCOVERY_TASKS` int(11) DEFAULT 0 COMMENT 'Number of discovery tasks that ended in error',
  `NU_PENDING_FILE_BACKUP_TASKS` int(11) DEFAULT 0 COMMENT 'Number of pending file backup tasks',
  `NU_DONE_FILE_BACKUP_TASKS` int(11) DEFAULT 0 COMMENT 'Number of completed file backup tasks',
  `NU_ERROR_FILE_BACKUP_TASKS` int(11) DEFAULT 0 COMMENT 'Number of file backup tasks that ended in error',
  `NU_PENDING_FILE_PROCESS_TASKS` int(11) DEFAULT 0 COMMENT 'Number of pending file processing tasks',
  `NU_DONE_FILE_PROCESS_TASKS` int(11) DEFAULT 0 COMMENT 'Number of completed file processing tasks',
  `NU_ERROR_FILE_PROCESS_TASKS` int(11) DEFAULT 0 COMMENT 'Number of file processing tasks that ended in error',
  `VL_PENDING_BACKUP_KB` int(11) DEFAULT 0 COMMENT 'Total size in KB of pending backup files',
  `VL_DONE_BACKUP_KB` int(11) DEFAULT 0 COMMENT 'Total size in KB of completed backup files',
  `NU_HOST_FILES` int(11) DEFAULT 0 COMMENT 'Total number of discovered files for this host',
  `NU_HOST_CHECK_ERROR` int(11) DEFAULT 0 COMMENT 'Total number of host check failures',
  PRIMARY KEY (`ID_HOST`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `HOST_TASK` (
  `ID_HOST_TASK` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to host task table',
  `FK_HOST` int(11) DEFAULT NULL COMMENT 'Foreign key to host table',
  `NU_TYPE` tinyint(4) DEFAULT 0 COMMENT 'Host Task Type: 0=Not set; 1=Backup',
  `DT_HOST_TASK` datetime DEFAULT NULL COMMENT 'Date and time of the host task creation',
  `NU_STATUS` tinyint(4) DEFAULT 0 COMMENT 'Status flag: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution',
  `NU_PID` int(11) DEFAULT NULL COMMENT 'Process ID of the task when under execution',
  `FILTER` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT 'Structured filter: {"mode":"ALL|NONE|RANGE|LAST", "start_date":"YYYY-MM-DD", "end_date":"YYYY-MM-DD", "last_n_files":N, "extension":".ext"}' CHECK (json_valid(`FILTER`)),
  `NA_MESSAGE` text DEFAULT NULL COMMENT 'Error message and other information',
  PRIMARY KEY (`ID_HOST_TASK`),
  KEY `FK_HOST_TASK_HOST` (`FK_HOST`),
  CONSTRAINT `FK_HOST_TASK_HOST` FOREIGN KEY (`FK_HOST`) REFERENCES `HOST` (`ID_HOST`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `FILE_TASK` (
  `ID_FILE_TASK` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to file task table',
  `FK_HOST` int(11) DEFAULT NULL COMMENT 'Foreign key to host table',
  `DT_FILE_TASK` datetime DEFAULT NULL COMMENT 'Date and time of the file task creation',
  `NU_TYPE` tinyint(4) DEFAULT 0 COMMENT 'File Task Type: 0=Not set; 1=Backup; 2=Processing; 3=Metadata',
  `NA_HOST_FILE_PATH` varchar(3000) DEFAULT NULL COMMENT 'Path to the file in the host',
  `NA_HOST_FILE_NAME` varchar(512) DEFAULT NULL COMMENT 'Name of the file in the host',
  `NU_HOST_FILE_MD5` varchar(32) DEFAULT NULL COMMENT 'MD5 hash of the file in the host',
  `NA_SERVER_FILE_PATH` varchar(3000) DEFAULT NULL COMMENT 'Path to the file in the server',
  `NA_SERVER_FILE_NAME` varchar(512) DEFAULT NULL COMMENT 'Name of the file in the server',
  `NA_SERVER_FILE_MD5` varchar(32) DEFAULT NULL COMMENT 'MD5 hash of the file in the server',
  `NU_STATUS` tinyint(4) DEFAULT 0 COMMENT 'Status flag: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution',
  `NU_PID` int(11) DEFAULT NULL COMMENT 'Process ID of the task when under execution',
  `NA_EXTENSION_HOST` varchar(20) DEFAULT NULL COMMENT 'Host file extension (.zip, .bin, etc.)',
  `VL_FILE_SIZE_KB_HOST` bigint(20) DEFAULT NULL COMMENT 'Host file size in kilobytes',
  `DT_FILE_CREATED_HOST` datetime DEFAULT NULL COMMENT 'Host file creation timestamp',
  `DT_FILE_MODIFIED_HOST` datetime DEFAULT NULL COMMENT 'Host file last modification timestamp',
  `NA_EXTENSION_SERVER` varchar(20) DEFAULT NULL COMMENT 'Server or repository file extension',
  `VL_FILE_SIZE_KB_SERVER` bigint(20) DEFAULT NULL COMMENT 'Server or repository file size in kilobytes',
  `DT_FILE_CREATED_SERVER` datetime DEFAULT NULL COMMENT 'Server or repository file creation timestamp',
  `DT_FILE_MODIFIED_SERVER` datetime DEFAULT NULL COMMENT 'Server or repository file last modification timestamp',
  `NA_EXTENSION` varchar(20) DEFAULT NULL COMMENT 'File extension (.txt, .csv, .log, etc.)',
  `VL_FILE_SIZE_KB` bigint(20) DEFAULT NULL COMMENT 'File size in kilobytes',
  `DT_FILE_CREATED` datetime DEFAULT NULL COMMENT 'File creation timestamp',
  `DT_FILE_MODIFIED` datetime DEFAULT NULL COMMENT 'File last modification timestamp',
  `NA_MESSAGE` text DEFAULT NULL COMMENT 'Error message and other information',
  `NA_ERROR_DOMAIN` varchar(32) DEFAULT NULL COMMENT 'Structured error domain for grouping/reporting',
  `NA_ERROR_STAGE` varchar(32) DEFAULT NULL COMMENT 'Structured error stage extracted from the persisted message',
  `NA_ERROR_CODE` varchar(64) DEFAULT NULL COMMENT 'Stable canonical error code',
  `NA_ERROR_SUMMARY` text DEFAULT NULL COMMENT 'Stable aggregation-friendly error summary',
  `NA_ERROR_DETAIL` text DEFAULT NULL COMMENT 'Volatile contextual detail kept apart from the grouping summary',
  `NU_ERROR_CLASSIFIER_VERSION` smallint(6) DEFAULT NULL COMMENT 'Version of the classifier used to populate the structured error fields',
  PRIMARY KEY (`ID_FILE_TASK`),
  UNIQUE KEY `uq_fth_identity` (`FK_HOST`,`NA_HOST_FILE_PATH`,`NA_HOST_FILE_NAME`) USING HASH,
  KEY `FK_FILE_TASK_HOST` (`FK_HOST`),
  KEY `idx_file_task_error_group` (`NU_STATUS`,`NU_TYPE`,`NA_ERROR_DOMAIN`,`NA_ERROR_STAGE`,`NA_ERROR_CODE`),
  CONSTRAINT `FK_FILE_TASK_HOST` FOREIGN KEY (`FK_HOST`) REFERENCES `HOST` (`ID_HOST`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `FILE_TASK_HISTORY` (
  `ID_HISTORY` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to backup history',
  `FK_HOST` int(11) NOT NULL COMMENT 'Foreign key to host table',
  `DT_DISCOVERED` datetime DEFAULT NULL COMMENT 'Date/time when discovered was completed',
  `DT_BACKUP` datetime DEFAULT NULL COMMENT 'Date/time when backup was completed',
  `DT_PROCESSED` datetime DEFAULT NULL COMMENT 'Date/time when processing was completed',
  `NU_STATUS_DISCOVERY` int(11) DEFAULT 1 COMMENT 'Status flag - 1 - Pending, 0 - Done, -1 - Error',
  `NU_STATUS_BACKUP` int(11) DEFAULT 1 COMMENT 'Status flag - 1 - Pending, 0 - Done, -1 - Error',
  `NU_STATUS_PROCESSING` int(11) DEFAULT 1 COMMENT 'Status flag - 1 - Pending, 0 - Done, -1 - Error',
  `NA_HOST_FILE_PATH` varchar(3000) DEFAULT NULL,
  `NA_HOST_FILE_NAME` varchar(512) DEFAULT NULL,
  `NA_SERVER_FILE_PATH` varchar(3000) DEFAULT NULL,
  `NA_SERVER_FILE_NAME` varchar(512) DEFAULT NULL,
  `VL_FILE_SIZE_KB_HOST` bigint(20) DEFAULT NULL,
  `DT_FILE_CREATED_HOST` datetime DEFAULT NULL COMMENT 'Host file creation timestamp',
  `DT_FILE_MODIFIED_HOST` datetime DEFAULT NULL COMMENT 'Host file last modification timestamp',
  `NA_EXTENSION_HOST` varchar(20) DEFAULT NULL COMMENT 'Host file extension (.zip, .bin, etc.)',
  `VL_FILE_SIZE_KB_SERVER` bigint(20) DEFAULT NULL,
  `DT_FILE_CREATED_SERVER` datetime DEFAULT NULL COMMENT 'Server or repository file creation timestamp',
  `DT_FILE_MODIFIED_SERVER` datetime DEFAULT NULL COMMENT 'Server or repository file last modification timestamp',
  `NA_EXTENSION_SERVER` varchar(20) DEFAULT NULL COMMENT 'Server or repository file extension',
  `VL_FILE_SIZE_KB` bigint(20) DEFAULT NULL,
  `DT_FILE_CREATED` datetime DEFAULT NULL COMMENT 'File creation timestamp',
  `DT_FILE_MODIFIED` datetime DEFAULT NULL COMMENT 'File last modification timestamp',
  `NA_EXTENSION` varchar(20) DEFAULT NULL COMMENT 'File extension (.txt, .csv, .log, etc.)',
  `NA_MESSAGE` text DEFAULT NULL COMMENT 'Optional message',
  `NA_ERROR_DOMAIN` varchar(32) DEFAULT NULL COMMENT 'Structured error domain for grouping/reporting',
  `NA_ERROR_STAGE` varchar(32) DEFAULT NULL COMMENT 'Structured error stage extracted from the persisted message',
  `NA_ERROR_CODE` varchar(64) DEFAULT NULL COMMENT 'Stable canonical error code',
  `NA_ERROR_SUMMARY` text DEFAULT NULL COMMENT 'Stable aggregation-friendly error summary',
  `NA_ERROR_DETAIL` text DEFAULT NULL COMMENT 'Volatile contextual detail kept apart from the grouping summary',
  `NU_ERROR_CLASSIFIER_VERSION` smallint(6) DEFAULT NULL COMMENT 'Version of the classifier used to populate the structured error fields',
  `IS_PAYLOAD_DELETED` tinyint(1) NOT NULL DEFAULT 0,
  `DT_PAYLOAD_DELETED` datetime DEFAULT NULL,
  PRIMARY KEY (`ID_HISTORY`),
  UNIQUE KEY `uq_fth_identity` (`FK_HOST`,`NA_HOST_FILE_PATH`,`NA_HOST_FILE_NAME`) USING HASH,
  KEY `idx_fth_dedup_soft` (`FK_HOST`,`NA_HOST_FILE_NAME`,`DT_FILE_CREATED`,`VL_FILE_SIZE_KB`),
  KEY `idx_fth_dedup_soft_host` (`FK_HOST`,`NA_HOST_FILE_NAME`,`DT_FILE_CREATED_HOST`,`VL_FILE_SIZE_KB_HOST`),
  KEY `idx_fth_backup_error_group` (`NU_STATUS_BACKUP`,`NA_ERROR_DOMAIN`,`NA_ERROR_STAGE`,`NA_ERROR_CODE`),
  KEY `idx_fth_processing_error_group` (`NU_STATUS_PROCESSING`,`NA_ERROR_DOMAIN`,`NA_ERROR_STAGE`,`NA_ERROR_CODE`),
  CONSTRAINT `FK_HISTORY_HOST` FOREIGN KEY (`FK_HOST`) REFERENCES `HOST` (`ID_HOST`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Append-only invalidation queue consumed by the Python summary worker.
CREATE TABLE `SUMMARY_OUTBOX` (
  `ID_OUTBOX` bigint(20) NOT NULL AUTO_INCREMENT,
  `NA_EVENT_TYPE` varchar(64) NOT NULL,
  `NA_SOURCE_HANDLER` varchar(64) DEFAULT NULL,
  `JS_PAYLOAD` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`JS_PAYLOAD`)),
  `DT_CREATED_AT` datetime NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`ID_OUTBOX`),
  KEY `IX_SUMMARY_OUTBOX_EVENT` (`NA_EVENT_TYPE`,`ID_OUTBOX`),
  KEY `IX_SUMMARY_OUTBOX_CREATED` (`DT_CREATED_AT`,`ID_OUTBOX`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Durable checkpoint and heartbeat for each summary consumer.
CREATE TABLE `SUMMARY_WORKER_STATE` (
  `NA_CONSUMER` varchar(64) NOT NULL,
  `ID_LAST_OUTBOX` bigint(20) NOT NULL DEFAULT 0,
  `DT_LAST_START` datetime DEFAULT NULL,
  `DT_LAST_END` datetime DEFAULT NULL,
  `DT_LAST_SUCCESS` datetime DEFAULT NULL,
  `DT_LAST_FAILURE` datetime DEFAULT NULL,
  `NU_LAST_BATCH_SIZE` int(11) NOT NULL DEFAULT 0,
  `NU_LAST_EVENT_COUNT` int(11) NOT NULL DEFAULT 0,
  `NA_STATUS` varchar(32) NOT NULL DEFAULT 'idle',
  `NA_ERROR_MESSAGE` text DEFAULT NULL,
  PRIMARY KEY (`NA_CONSUMER`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
