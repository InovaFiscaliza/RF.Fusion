
CREATE DATABASE BPDATA
    DEFAULT CHARACTER SET = 'utf8mb4';

-- ACTIVATE DATABASE
USE BPDATA;

-- HOST STATISTICS
CREATE TABLE HOST (
    ID_HOST INT NOT NULL PRIMARY KEY COMMENT 'Primary Key to host table, from zabbix host id',
    NA_HOST_UID VARCHAR(50) COMMENT 'Host unique identifier',
    FK_EQUIPMENT_RFDB INT COMMENT 'Foreign key to equipment table in RFDB',
    NA_HOST_ADDRESS VARCHAR(50) COMMENT 'IP address or hostname',
    NA_HOST_PORT INT COMMENT 'Port to access the host',
    NA_HOST_USER VARCHAR(50) COMMENT 'Username to access the host',
    NA_HOST_PASSWORD VARCHAR(50) COMMENT 'Password to access the host',
    NU_HOST_FILES BIGINT COMMENT 'Historic total number of files listed for a host in all repositories',
    NU_PENDING_HOST_TASK INT COMMENT 'Number of hosts tasks pending execution',
    DT_LAST_HOST_CHECK DATETIME COMMENT 'Date and time of the host check',
    NU_HOST_CHECK_ERROR INT COMMENT 'Historic total number of errors in host check process',
    NU_PENDING_BACKUP INT COMMENT 'Number of files pending backup',
    DT_LAST_BACKUP DATETIME COMMENT 'Date and time of the last backup',
    NU_BACKUP_ERROR INT COMMENT 'Historic total number of errors in file backup process',
    NU_PENDING_PROCESSING INT COMMENT 'Number of files pending processing',
    NU_PROCESSING_ERROR INT COMMENT 'Historic total number of errors in data processing',
    DT_LAST_PROCESSING DATETIME COMMENT 'Date and time of the last processing',
    NU_STATUS TINYINT DEFAULT 0 COMMENT 'Status flag: 0=No Errors or Warnings, 1=No daemon, 2=Halt flag alert'
);

-- HOST BACKUP TASK LIST
CREATE TABLE HOST_TASK (
    ID_HOST_TASK INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key to host task table',
    FK_HOST INT COMMENT 'Foreign key to host table',
    NU_TYPE TINYINT DEFAULT 0 COMMENT 'Host Task Type: 0=Not set; 1=Backup',
    DT_HOST_TASK DATETIME COMMENT 'Date and time of the host task creation',
    NU_STATUS TINYINT DEFAULT 0 COMMENT 'Status flag: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution',
    NU_PID INT COMMENT 'Process ID of the task when under execution',
    NA_MESSAGE TEXT COMMENT 'Error message and other information',
    CONSTRAINT FK_HOST_TASK_HOST FOREIGN KEY (FK_HOST) REFERENCES HOST (ID_HOST)
);

-- LIST OF FILES FOR PROCESSING
CREATE TABLE FILE_TASK (
    ID_FILE_TASK INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key to file task table',
    FK_HOST INT COMMENT 'Foreign key to host table',
    DT_FILE_TASK DATETIME COMMENT 'Date and time of the file task creation',
    NU_TYPE TINYINT DEFAULT 0 COMMENT 'File Task Type: 0=Not set; 1=Backup; 2=Processing',
    NA_HOST_FILE_PATH VARCHAR(3000) COMMENT 'Path to the file in the host',
    NA_HOST_FILE_NAME VARCHAR(100) COMMENT 'Name of the file in the host',
    NA_SERVER_FILE_PATH VARCHAR(3000) COMMENT 'Path to the file in the server',
    NA_SERVER_FILE_NAME VARCHAR(100) COMMENT 'Name of the file in the server',
    NU_STATUS TINYINT DEFAULT 0 COMMENT 'Status flag: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution',
    NU_PID INT COMMENT 'Process ID of the task when under execution',
    NA_MESSAGE TEXT COMMENT 'Error message and other information',
    CONSTRAINT FK_FILE_TASK_HOST FOREIGN KEY (FK_HOST) REFERENCES HOST (ID_HOST)
);
