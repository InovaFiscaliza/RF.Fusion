USE RFDATA;

ALTER TABLE `DIM_SPECTRUM_FILE` ADD COLUMN `DT_FILE_LOGGED` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the db entry was created' AFTER `ID_FILE`;
