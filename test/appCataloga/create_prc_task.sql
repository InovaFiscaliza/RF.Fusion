
-- Download files
--      wget -q --show-progress https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/test/appCataloga/f2073.csv
--      wget -q --show-progress https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/test/appCataloga/f2080.csv
--      wget -q --show-progress https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/test/appCataloga/f2086.csv
--      wget -q --show-progress https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/test/appCataloga/create_prc_task.sql
--
-- Run the script
-- # mysql -u root -p
-- MariaDB [] > SOURCE /tmp/appCataloga/create_prc_task.sql;

USE BPDATA;

UPDATE HOST SET NU_HOST_FILES = 157, NU_PENDING_BACKUP = 0, NU_BACKUP_ERROR = 0, NU_PENDING_PROCESSING = 157, NU_PROCESSING_ERROR = 0 WHERE ID_HOST = 10597;
UPDATE HOST SET NU_HOST_FILES = 196, NU_PENDING_BACKUP = 0, NU_BACKUP_ERROR = 0, NU_PENDING_PROCESSING = 196, NU_PROCESSING_ERROR = 0 WHERE ID_HOST = 10364;
UPDATE HOST SET NU_HOST_FILES = 1273, NU_PENDING_BACKUP = 0, NU_BACKUP_ERROR = 0, NU_PENDING_PROCESSING = 1273, NU_PROCESSING_ERROR = 0 WHERE ID_HOST = 10367;

LOAD DATA INFILE '/etc/appCataloga/f2073.csv'
    INTO TABLE PRC_TASK
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n' 
    IGNORE 1 LINES
    (FK_HOST, NA_HOST_FILE_PATH, NA_HOST_FILE_NAME, NA_SERVER_FILE_PATH, NA_SERVER_FILE_NAME, DT_PRC_TASK);

LOAD DATA INFILE '/etc/appCataloga/f2080.csv'
    INTO TABLE PRC_TASK
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n' 
    IGNORE 1 LINES
    (FK_HOST, NA_HOST_FILE_PATH, NA_HOST_FILE_NAME, NA_SERVER_FILE_PATH, NA_SERVER_FILE_NAME, DT_PRC_TASK);

LOAD DATA INFILE '/etc/appCataloga/f2086.csv'
    INTO TABLE PRC_TASK
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n' 
    IGNORE 1 LINES
    (FK_HOST, NA_HOST_FILE_PATH, NA_HOST_FILE_NAME, NA_SERVER_FILE_PATH, NA_SERVER_FILE_NAME, DT_PRC_TASK);
