
CREATE DATABASE BPDATA
    DEFAULT CHARACTER SET = 'utf8mb4';

-- ACTIVATE DATABASE
USE BPDATA;

-- HOST STATISTICS
CREATE TABLE HOST (
    ID_HOST	INT NOT NULL PRIMARY KEY COMMENT 'Primary Key to host table, from Zabbix host id',
    NA_HOST_NAME VARCHAR(100) COMMENT 'Human-readable hostname or identifier',
    NA_HOST_ADDRESS VARCHAR(50) COMMENT 'IP address or hostname',
    NA_HOST_PORT INT COMMENT 'Port used to access the host',
    NA_HOST_USER VARCHAR(50) COMMENT 'Username for remote access',
    NA_HOST_PASSWORD VARCHAR(50) COMMENT 'Password to access the host',

    -- Backup and processing timestamps
    DT_LAST_BACKUP DATETIME COMMENT 'Timestamp of the last successful backup',
    DT_LAST_PROCESSING DATETIME COMMENT 'Timestamp of the last successful processing',
	DT_LAST_DISCOVERY DATETIME COMMENT 'Timestamp of the last sucessful discovering',

    -- Connectivity management
    IS_OFFLINE BOOLEAN DEFAULT 0 COMMENT 'Flag: 1=Host temporarily offline (unreachable), 0=Online',
    IS_BUSY BOOLEAN DEFAULT 0 COMMENT 'Flag: 1=Host is Busy, 0 = Host is free',
    NU_PID INT DEFAULT 0 COMMENT 'PID of the worker currently handling this host',
    DT_LAST_FAIL DATETIME NULL COMMENT 'Timestamp of the last failed connection attempt',
    DT_LAST_CHECK DATETIME NULL COMMENT 'Timestamp of the last connectivity check attempt',
    DT_BUSY DATETIME NULL COMMENT 'Timestamp of when the host was last marked busy',

    -- FILE TASK STATISTICS
    NU_DONE_FILE_DISCOVERY_TASKS INT DEFAULT 0 COMMENT 'Number of completed file discovery tasks',
    NU_ERROR_FILE_DISCOVERY_TASKS INT DEFAULT 0 COMMENT 'Number of discovery tasks that ended in error',
    
    NU_PENDING_FILE_BACKUP_TASKS INT DEFAULT 0 COMMENT 'Number of pending file backup tasks',
    NU_DONE_FILE_BACKUP_TASKS INT DEFAULT 0 COMMENT 'Number of completed file backup tasks',
    NU_ERROR_FILE_BACKUP_TASKS INT DEFAULT 0 COMMENT 'Number of file backup tasks that ended in error',

    NU_PENDING_FILE_PROCESS_TASKS INT DEFAULT 0 COMMENT 'Number of pending file processing tasks',
    NU_DONE_FILE_PROCESS_TASKS INT DEFAULT 0 COMMENT 'Number of completed file processing tasks',
    NU_ERROR_FILE_PROCESS_TASKS INT DEFAULT 0 COMMENT 'Number of file processing tasks that ended in error',

    -- FILE SIZE STATISTICS
    VL_PENDING_BACKUP_KB INT DEFAULT 0 COMMENT 'Total size in KB of pending backup files',
    VL_DONE_BACKUP_KB INT DEFAULT 0 COMMENT 'Total size in KB of completed backup files',

    -- HOST STATISTICS
    NU_HOST_FILES INT DEFAULT 0 COMMENT 'Total number of discovered files for this host',
    NU_HOST_CHECK_ERROR INT DEFAULT 0 COMMENT 'Total number of host check failures'
);


-- HOST BACKUP TASK LIST
CREATE TABLE HOST_TASK (
    ID_HOST_TASK INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key to host task table',
    FK_HOST INT COMMENT 'Foreign key to host table',
    NU_TYPE TINYINT DEFAULT 0 COMMENT 'Host Task Type: 0=Not set; 1=Backup',
    DT_HOST_TASK DATETIME COMMENT 'Date and time of the host task creation',
    NU_STATUS TINYINT DEFAULT 0 COMMENT 'Status flag: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution',
    NU_PID INT COMMENT 'Process ID of the task when under execution',
    FILTER JSON COMMENT 'Structured filter: {"mode":"ALL|NONE|RANGE|LAST", "start_date":"YYYY-MM-DD", "end_date":"YYYY-MM-DD", "last_n_files":N, "extension":".ext"}',
    NA_MESSAGE TEXT COMMENT 'Error message and other information',
    CONSTRAINT FK_HOST_TASK_HOST FOREIGN KEY (FK_HOST) REFERENCES HOST (ID_HOST)
);


-- LIST OF FILES FOR PROCESSING
CREATE TABLE FILE_TASK (
    ID_FILE_TASK INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key to file task table',
    FK_HOST INT COMMENT 'Foreign key to host table',
    DT_FILE_TASK DATETIME COMMENT 'Date and time of the file task creation',
    NU_TYPE TINYINT DEFAULT 0 COMMENT 'File Task Type: 0=Not set; 1=Backup; 2=Processing; 3=Metadata',
    NA_HOST_FILE_PATH VARCHAR(3000) COMMENT 'Path to the file in the host',
    NA_HOST_FILE_NAME VARCHAR(100) COMMENT 'Name of the file in the host',
    NU_HOST_FILE_MD5 VARCHAR(32) COMMENT 'MD5 hash of the file in the host',
    NA_SERVER_FILE_PATH VARCHAR(3000) COMMENT 'Path to the file in the server',
    NA_SERVER_FILE_NAME VARCHAR(100) COMMENT 'Name of the file in the server',
    NA_SERVER_FILE_MD5 VARCHAR(32) COMMENT 'MD5 hash of the file in the server',
    NU_STATUS TINYINT DEFAULT 0 COMMENT 'Status flag: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution',
    NU_PID INT COMMENT 'Process ID of the task when under execution',
    NA_EXTENSION VARCHAR(20) COMMENT 'File extension (.txt, .csv, .log, etc.)',
    VL_FILE_SIZE_KB BIGINT COMMENT 'File size in kilobytes',
    DT_FILE_CREATED DATETIME COMMENT 'File creation timestamp',
    DT_FILE_MODIFIED DATETIME COMMENT 'File last modification timestamp',
    NA_MESSAGE TEXT COMMENT 'Error message and other information',
    CONSTRAINT FK_FILE_TASK_HOST FOREIGN KEY (FK_HOST) REFERENCES HOST (ID_HOST)
);

-- ==========================================================
-- FILE_TASK_HISTORY (AUDIT TRAIL)
-- ==========================================================
CREATE TABLE FILE_TASK_HISTORY (
    ID_HISTORY INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key to backup history',
    FK_HOST INT NOT NULL COMMENT 'Foreign key to host table',
	DT_DISCOVERED DATETIME DEFAULT NULL COMMENT 'Date/time when discovered was completed',
    DT_BACKUP DATETIME DEFAULT NULL  COMMENT 'Date/time when backup was completed',
	DT_PROCESSED DATETIME DEFAULT NULL COMMENT 'Date/time when processing was completed',
	NU_STATUS_DISCOVERY INT DEFAULT 1 COMMENT 'Status flag - 1 - Pending, 0 - Done, -1 - Error', 
	NU_STATUS_BACKUP INT DEFAULT 1 COMMENT 'Status flag - 1 - Pending, 0 - Done, -1 - Error',
	NU_STATUS_PROCESSING INT DEFAULT 1 COMMENT 'Status flag - 1 - Pending, 0 - Done, -1 - Error',
    NA_HOST_FILE_PATH VARCHAR(3000),
    NA_HOST_FILE_NAME VARCHAR(100),
	NA_SERVER_FILE_PATH VARCHAR(3000),
    NA_SERVER_FILE_NAME VARCHAR(100),
    VL_FILE_SIZE_KB BIGINT,
	DT_FILE_CREATED DATETIME COMMENT 'File creation timestamp',
    DT_FILE_MODIFIED DATETIME COMMENT 'File last modification timestamp',
	NA_EXTENSION VARCHAR(20) COMMENT 'File extension (.txt, .csv, .log, etc.)',
    NA_MESSAGE TEXT COMMENT 'Optional message',
    CONSTRAINT FK_HISTORY_HOST FOREIGN KEY (FK_HOST) REFERENCES HOST(ID_HOST)
);