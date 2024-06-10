-- MySQL dump 10.19  Distrib 10.3.39-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: BPDATA
-- ------------------------------------------------------
-- Server version	10.3.39-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `FILE_TASK`
--

DROP TABLE IF EXISTS `FILE_TASK`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `FILE_TASK` (
  `ID_FILE_TASK` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to file task table',
  `FK_HOST` int(11) DEFAULT NULL COMMENT 'Foreign key to host table',
  `DT_FILE_TASK` datetime DEFAULT NULL COMMENT 'Date and time of the file task creation',
  `NU_TYPE` tinyint(4) DEFAULT 0 COMMENT 'File Task Type: 0=Not set; 1=Backup; 2=Processing',
  `NA_HOST_FILE_PATH` varchar(3000) DEFAULT NULL COMMENT 'Path to the file in the host',
  `NA_HOST_FILE_NAME` varchar(100) DEFAULT NULL COMMENT 'Name of the file in the host',
  `NA_SERVER_FILE_PATH` varchar(3000) DEFAULT NULL COMMENT 'Path to the file in the server',
  `NA_SERVER_FILE_NAME` varchar(100) DEFAULT NULL COMMENT 'Name of the file in the server',
  `NU_STATUS` tinyint(4) DEFAULT 0 COMMENT 'Status flag: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution',
  `NA_MESSAGE` text DEFAULT NULL COMMENT 'Error message and other information',
  `NU_PID` int(11) DEFAULT NULL COMMENT 'Process ID of the task when under execution',
  PRIMARY KEY (`ID_FILE_TASK`),
  KEY `FK_FILE_TASK_HOST` (`FK_HOST`),
  CONSTRAINT `FK_FILE_TASK_HOST` FOREIGN KEY (`FK_HOST`) REFERENCES `HOST` (`ID_HOST`)
) ENGINE=InnoDB AUTO_INCREMENT=316 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `FILE_TASK`
--

LOCK TABLES `FILE_TASK` WRITE;
/*!40000 ALTER TABLE `FILE_TASK` DISABLE KEYS */;
INSERT INTO `FILE_TASK` VALUES (315,10367,'2024-06-07 15:41:35',1,'/mnt/internal/data','rfeye002073_230317_T111345.bin',NULL,NULL,-1,'Error copying \'/mnt/internal/data/rfeye002073_230317_T111345.bin\' from host 192.168.1.138.\'dbHandler\' object has no attribute \'FILE_TASK_PROCESS_TYPE\'',1518);
/*!40000 ALTER TABLE `FILE_TASK` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `HOST`
--

DROP TABLE IF EXISTS `HOST`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `HOST` (
  `ID_HOST` int(11) NOT NULL COMMENT 'Primary Key to host table, from zabbix host id',
  `NA_HOST_UID` varchar(50) DEFAULT NULL COMMENT 'Host unique identifier',
  `FK_EQUIPMENT_RFDB` int(11) DEFAULT NULL COMMENT 'Foreign key to equipment table in RFDB',
  `NA_HOST_ADDRESS` varchar(50) DEFAULT NULL COMMENT 'IP address or hostname',
  `NA_HOST_PORT` int(11) DEFAULT NULL COMMENT 'Port to access the host',
  `NA_HOST_USER` varchar(50) DEFAULT NULL COMMENT 'Username to access the host',
  `NA_HOST_PASSWORD` varchar(50) DEFAULT NULL COMMENT 'Password to access the host',
  `NU_HOST_FILES` bigint(20) DEFAULT NULL COMMENT 'Historic total number of files listed for a host in all repositories',
  `NU_PENDING_HOST_TASK` int(11) DEFAULT NULL COMMENT 'Number of hosts tasks pending execution',
  `DT_LAST_HOST_CHECK` datetime DEFAULT NULL COMMENT 'Date and time of the host check',
  `NU_HOST_CHECK_ERROR` float DEFAULT NULL COMMENT 'Historic total number of errors in host check process',
  `NU_PENDING_BACKUP` int(11) DEFAULT NULL COMMENT 'Number of files pending backup',
  `DT_LAST_BACKUP` datetime DEFAULT NULL COMMENT 'Date and time of the last backup',
  `NU_BACKUP_ERROR` int(11) DEFAULT NULL COMMENT 'Historic total number of errors in file backup process',
  `NU_PENDING_PROCESSING` int(11) DEFAULT NULL COMMENT 'Number of files pending processing',
  `NU_PROCESSING_ERROR` int(11) DEFAULT NULL COMMENT 'Historic total number of errors in data processing',
  `DT_LAST_PROCESSING` datetime DEFAULT NULL COMMENT 'Date and time of the last processing',
  `NU_STATUS` tinyint(4) DEFAULT 0 COMMENT 'Status flag: 0=No Errors or Warnings, 1=No daemon, 2=Halt flag alert',
  PRIMARY KEY (`ID_HOST`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `HOST`
--

LOCK TABLES `HOST` WRITE;
/*!40000 ALTER TABLE `HOST` DISABLE KEYS */;
INSERT INTO `HOST` VALUES (10367,'RFEye002080',NULL,'192.168.1.138',22,'sshUser','sshuserpass',0,1,'2024-06-07 15:41:55',0,0,'2024-06-10 09:12:51',1,0,0,NULL,7);
/*!40000 ALTER TABLE `HOST` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `HOST_TASK`
--

DROP TABLE IF EXISTS `HOST_TASK`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `HOST_TASK` (
  `ID_HOST_TASK` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to host task table',
  `FK_HOST` int(11) DEFAULT NULL COMMENT 'Foreign key to host table',
  `NU_TYPE` tinyint(4) DEFAULT 0 COMMENT 'Host Task Type: 0=Not set; 1=Backup;',
  `DT_HOST_TASK` datetime DEFAULT NULL COMMENT 'Date and time of the host task creation',
  `NU_STATUS` tinyint(4) DEFAULT 0 COMMENT 'Status flag: -1=Executed with error; 0=Nothing to do; 1=Pending action',
  `NA_MESSAGE` text DEFAULT NULL COMMENT 'Error message and other information',
  `NU_PID` int(11) DEFAULT NULL COMMENT 'Process ID of the task when under execution',
  PRIMARY KEY (`ID_HOST_TASK`),
  KEY `FK_HOST_TASK_HOST` (`FK_HOST`),
  CONSTRAINT `FK_HOST_TASK_HOST` FOREIGN KEY (`FK_HOST`) REFERENCES `HOST` (`ID_HOST`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `HOST_TASK`
--

LOCK TABLES `HOST_TASK` WRITE;
/*!40000 ALTER TABLE `HOST_TASK` DISABLE KEYS */;
/*!40000 ALTER TABLE `HOST_TASK` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-06-10 10:49:42
