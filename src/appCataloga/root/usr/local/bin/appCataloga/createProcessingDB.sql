
CREATE DATABASE BPDATA
    DEFAULT CHARACTER SET = 'utf8mb4';

-- ACTIVATE DATABASE
USE BPDATA;

-- HOST STATISTICS
CREATE TABLE HOST (
    ID_HOST INT NOT NULL PRIMARY KEY COMMENT 'Primary Key to host table, from zabbix host id',
    NA_HOST_UID VARCHAR(50) COMMENT 'Host unique identifier',
    FK_EQUIPMENT_RFDB INT COMMENT 'Foreign key to equipment table in RFDB',
    NU_HOST_FILES BIGINT COMMENT 'Historic total number of files listed for backup in the host',
    NU_PENDING_BACKUP INT COMMENT 'Number of files pending backup',
    DT_LAST_BACKUP DATETIME COMMENT 'Date and time of the last backup',
    NU_BACKUP_ERROR FLOAT COMMENT 'Historic total number of errors in backup process',
    NU_PENDING_PROCESSING INT COMMENT 'Number of files pending processing',
    NU_PROCESSING_ERROR FLOAT COMMENT 'Historic total number of errors in data processing',
    DT_LAST_PROCESSING DATETIME COMMENT 'Date and time of the last processing'
);

-- HOST BACKUP TASK LIST
CREATE TABLE BKP_TASK (
    ID_BKP_TASK INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key to backup task table',
    FK_HOST INT COMMENT 'Foreign key to host table',
    DT_BKP_TASK DATETIME COMMENT 'Date and time of the backup task was requested',
    NA_HOST_ADDRESS VARCHAR(50) COMMENT 'IP address or hostname',
    NA_HOST_PORT INT COMMENT 'Port to access the host',
    NA_HOST_USER VARCHAR(50) COMMENT 'Username to access the host',
    NA_HOST_PASSWORD VARCHAR(50) COMMENT 'Password to access the host',
    NU_STATUS TINYINT DEFAULT 0 COMMENT 'Status flag: 0=Not executed; -1=Executed with error; 1=In progress; 2=Executed successfully',
    NA_MESSAGE TEXT COMMENT 'Error message and other information',
    CONSTRAINT FK_BKP_TASK_HOST FOREIGN KEY (FK_HOST) REFERENCES HOST (ID_HOST)
);

-- LIST OF FILES FOR PROCESSING
CREATE TABLE PRC_TASK (
    ID_PRC_TASK INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key to processing task table',
    FK_HOST INT COMMENT 'Foreign key to host table',
    NA_HOST_FILE_PATH VARCHAR(3000) COMMENT 'Path to the file in the host',
    NA_HOST_FILE_NAME VARCHAR(100) COMMENT 'Name of the file in the host',
    NA_SERVER_FILE_PATH VARCHAR(3000) COMMENT 'Path to the file in the server',
    NA_SERVER_FILE_NAME VARCHAR(100) COMMENT 'Name of the file in the server',
    DT_PRC_TASK DATETIME COMMENT 'Date and time of the file processing',
    NU_STATUS TINYINT DEFAULT 0 COMMENT 'Status flag: 0=Not executed; -1=Executed with error',
    NA_MESSAGE TEXT COMMENT 'Error message and other information',
    CONSTRAINT FK_PRC_TASK_HOST FOREIGN KEY (FK_HOST) REFERENCES HOST (ID_HOST)
);
