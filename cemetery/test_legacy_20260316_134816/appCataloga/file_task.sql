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
) ENGINE=InnoDB AUTO_INCREMENT=315 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `FILE_TASK`
--

LOCK TABLES `FILE_TASK` WRITE;
/*!40000 ALTER TABLE `FILE_TASK` DISABLE KEYS */;
INSERT INTO `FILE_TASK` VALUES (56,10367,'2024-03-15 19:06:55',1,'/mnt/internal/data','SCAN_M_450470_rfeye002088_170426_235831.bin','/mnt/reposfi/tmp','SCAN_M_450470_rfeye002088_170426_225406.bin',-1,'File \'/mnt/internal/data/SCAN_M_450470_rfeye002088_170426_235831.bin\' not found in remote host 192.168.1.138',10019),(80,10367,'2024-03-16 19:06:55',1,'/mnt/internal/data','SCAN_M_450470_rfeye002088_170426_235831.bin','/mnt/reposfi/tmp','SCAN_M_450470_rfeye002088_170426_225406.bin',2,'Test Running for type 1',10019),(81,123,'2024-05-16 19:00:53',2,NULL,NULL,'/mnt/reposfi/tmp','rfeye002073_231128_T162112.bin',2,'Test Running for type 2',1425),(301,123,'2024-05-15 19:00:53',2,NULL,NULL,'/mnt/reposfi/tmp','rfeye002073_231128_T162112.bin',-1,'Error processing task: \'NoneType\' object has no attribute \'raw\'',1425);
/*!40000 ALTER TABLE `FILE_TASK` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-05-24 12:42:41
