/* ================================================================
   createWebFusionDB.sql
   Schema for WebFusion access-control identities.
   ================================================================ */

CREATE DATABASE IF NOT EXISTS WEBFUSION
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE WEBFUSION;

CREATE TABLE IF NOT EXISTS `USERS` (
  `ID_USER` bigint(20) NOT NULL AUTO_INCREMENT,
  `NA_USER_NAME` varchar(255) DEFAULT NULL,
  `NA_USER_EMAIL` varchar(320) NOT NULL,
  `NA_JOB_TITLE` varchar(255) DEFAULT NULL,
  `NA_DEPARTMENT` varchar(255) DEFAULT NULL,
  `NA_LOCATION` varchar(255) DEFAULT NULL,
  `NA_URL_PROFILE_IMG` varchar(2048) DEFAULT NULL,
  `DT_CREATED_AT` datetime NOT NULL DEFAULT current_timestamp(),
  `DT_UPDATED_AT` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`ID_USER`),
  UNIQUE KEY `UQ_USERS_USER_EMAIL` (`NA_USER_EMAIL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `USER_ROLES` (
  `ID_USER` bigint(20) NOT NULL,
  `NA_ROLE` enum('admin','developer') NOT NULL,
  `IS_ACTIVE` tinyint(1) NOT NULL DEFAULT 1,
  `DT_CREATED_AT` datetime NOT NULL DEFAULT current_timestamp(),
  `DT_UPDATED_AT` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`ID_USER`, `NA_ROLE`),
  KEY `IX_USER_ROLES_ROLE_ACTIVE` (`NA_ROLE`, `IS_ACTIVE`),
  CONSTRAINT `FK_USER_ROLES_USER` FOREIGN KEY (`ID_USER`)
    REFERENCES `USERS` (`ID_USER`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
