/* =====================================================================
   createProcessingDB.sql
   - Canonical BPDATA schema after folding the retired alterProcessingDB
     migration chain into one creation script.
   - Host operational metrics introduced by v2/v3 and later removed by
     v4/v5 are intentionally absent here.
   - Legacy file metadata columns remain removed.
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
  `DT_LAST_OFFLINE_AT` datetime DEFAULT NULL COMMENT 'Timestamp of the most recent offline confirmation',
  `NA_LAST_OFFLINE_DESCRIPTION` text DEFAULT NULL COMMENT 'Context of the most recent offline confirmation',
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
  `NA_MESSAGE` text DEFAULT NULL COMMENT 'Task message or concise error summary',
  `NA_ERROR_CODE` varchar(64) DEFAULT NULL COMMENT 'Stable canonical error code',
  `NA_ERROR_DETAIL` text DEFAULT NULL COMMENT 'Useful technical context for one error',
  `NU_ERROR_CLASSIFIER_VERSION` smallint(6) DEFAULT NULL COMMENT 'Version of the classifier used to populate the structured error fields',
  PRIMARY KEY (`ID_FILE_TASK`),
  UNIQUE KEY `uq_fth_identity` (`FK_HOST`,`NA_HOST_FILE_PATH`,`NA_HOST_FILE_NAME`) USING HASH,
  KEY `FK_FILE_TASK_HOST` (`FK_HOST`),
  KEY `idx_file_task_identity` (`FK_HOST`,`NA_HOST_FILE_PATH`(191),`NA_HOST_FILE_NAME`(191)),
  KEY `idx_file_task_queue_host` (`NU_STATUS`,`NU_TYPE`,`FK_HOST`,`ID_FILE_TASK`),
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
  `NA_MESSAGE` text DEFAULT NULL COMMENT 'Task message or concise error summary',
  `NA_ERROR_CODE` varchar(64) DEFAULT NULL COMMENT 'Stable canonical error code',
  `NA_ERROR_DETAIL` text DEFAULT NULL COMMENT 'Useful technical context for one error',
  `NU_ERROR_CLASSIFIER_VERSION` smallint(6) DEFAULT NULL COMMENT 'Version of the classifier used to populate the structured error fields',
  `IS_PAYLOAD_DELETED` tinyint(1) NOT NULL DEFAULT 0,
  `DT_PAYLOAD_DELETED` datetime DEFAULT NULL,
  PRIMARY KEY (`ID_HISTORY`),
  UNIQUE KEY `uq_fth_identity` (`FK_HOST`,`NA_HOST_FILE_PATH`,`NA_HOST_FILE_NAME`) USING HASH,
  KEY `idx_fth_dedup_identity` (`FK_HOST`,`NA_HOST_FILE_PATH`(191),`NA_HOST_FILE_NAME`(191),`VL_FILE_SIZE_KB_HOST`),
  KEY `idx_fth_host_file_name` (`NA_HOST_FILE_NAME`,`ID_HISTORY`),
  KEY `idx_fth_server_file_name` (`NA_SERVER_FILE_NAME`,`ID_HISTORY`),
  KEY `idx_fth_host_date_host` (`FK_HOST`,`DT_FILE_CREATED_HOST`),
  KEY `idx_fth_backup_recreate` (`NU_STATUS_BACKUP`,`ID_HISTORY`),
  KEY `idx_fth_processing_recreate` (`NU_STATUS_BACKUP`,`NU_STATUS_PROCESSING`,`ID_HISTORY`),
  CONSTRAINT `FK_HISTORY_HOST` FOREIGN KEY (`FK_HOST`) REFERENCES `HOST` (`ID_HOST`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
