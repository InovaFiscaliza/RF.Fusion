/* =====================================================================
   alterProcessingDB-v10-host-server-metadata.sql
   - Expand FILE_TASK and FILE_TASK_HISTORY with explicit host/server metadata
   ===================================================================== */

USE BPDATA;

ALTER TABLE `FILE_TASK`
  ADD COLUMN `NA_EXTENSION_HOST` varchar(20) DEFAULT NULL COMMENT 'Host file extension (.zip, .bin, etc.)' AFTER `NU_PID`,
  ADD COLUMN `VL_FILE_SIZE_KB_HOST` bigint(20) DEFAULT NULL COMMENT 'Host file size in kilobytes' AFTER `NA_EXTENSION_HOST`,
  ADD COLUMN `DT_FILE_CREATED_HOST` datetime DEFAULT NULL COMMENT 'Host file creation timestamp' AFTER `VL_FILE_SIZE_KB_HOST`,
  ADD COLUMN `DT_FILE_MODIFIED_HOST` datetime DEFAULT NULL COMMENT 'Host file last modification timestamp' AFTER `DT_FILE_CREATED_HOST`,
  ADD COLUMN `NA_EXTENSION_SERVER` varchar(20) DEFAULT NULL COMMENT 'Server or repository file extension' AFTER `DT_FILE_MODIFIED_HOST`,
  ADD COLUMN `VL_FILE_SIZE_KB_SERVER` bigint(20) DEFAULT NULL COMMENT 'Server or repository file size in kilobytes' AFTER `NA_EXTENSION_SERVER`,
  ADD COLUMN `DT_FILE_CREATED_SERVER` datetime DEFAULT NULL COMMENT 'Server or repository file creation timestamp' AFTER `VL_FILE_SIZE_KB_SERVER`,
  ADD COLUMN `DT_FILE_MODIFIED_SERVER` datetime DEFAULT NULL COMMENT 'Server or repository file last modification timestamp' AFTER `DT_FILE_CREATED_SERVER`;

ALTER TABLE `FILE_TASK_HISTORY`
  ADD COLUMN `VL_FILE_SIZE_KB_HOST` bigint(20) DEFAULT NULL AFTER `NA_SERVER_FILE_NAME`,
  ADD COLUMN `DT_FILE_CREATED_HOST` datetime DEFAULT NULL COMMENT 'Host file creation timestamp' AFTER `VL_FILE_SIZE_KB_HOST`,
  ADD COLUMN `DT_FILE_MODIFIED_HOST` datetime DEFAULT NULL COMMENT 'Host file last modification timestamp' AFTER `DT_FILE_CREATED_HOST`,
  ADD COLUMN `NA_EXTENSION_HOST` varchar(20) DEFAULT NULL COMMENT 'Host file extension (.zip, .bin, etc.)' AFTER `DT_FILE_MODIFIED_HOST`,
  ADD COLUMN `VL_FILE_SIZE_KB_SERVER` bigint(20) DEFAULT NULL AFTER `NA_EXTENSION_HOST`,
  ADD COLUMN `DT_FILE_CREATED_SERVER` datetime DEFAULT NULL COMMENT 'Server or repository file creation timestamp' AFTER `VL_FILE_SIZE_KB_SERVER`,
  ADD COLUMN `DT_FILE_MODIFIED_SERVER` datetime DEFAULT NULL COMMENT 'Server or repository file last modification timestamp' AFTER `DT_FILE_CREATED_SERVER`,
  ADD COLUMN `NA_EXTENSION_SERVER` varchar(20) DEFAULT NULL COMMENT 'Server or repository file extension' AFTER `DT_FILE_MODIFIED_SERVER`;

ALTER TABLE `FILE_TASK_HISTORY`
  ADD KEY `idx_fth_dedup_soft_host` (`FK_HOST`,`NA_HOST_FILE_NAME`,`DT_FILE_CREATED_HOST`,`VL_FILE_SIZE_KB_HOST`);
