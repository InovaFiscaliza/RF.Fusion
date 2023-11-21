-- Active: 1692774226190@@127.0.0.1@3306@BPDATA
-- ACTIVATE DATABASE
CREATE DATABASE RFDATA
    DEFAULT CHARACTER SET = 'utf8mb4';

USE RFDATA;

-- MEASUREMENT EQUIPMENT DIMENSION
CREATE TABLE DIM_EQUIPMENT_TYPE (
    ID_EQUIPMENT_TYPE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to equipment type dimension table', 
    NA_EQUIPMENT_TYPE VARCHAR(50) COMMENT 'Description of the equipment type',
    NA_EQUIPMENT_TYPE_UID VARCHAR(50) COMMENT 'Unique identifier to the equipment type, used to file association',
    PRIMARY KEY (ID_EQUIPMENT_TYPE)
);

CREATE TABLE DIM_SPECTRUN_EQUIPMENT (
    ID_EQUIPMENT INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to equipment dimension table',
    NA_EQUIPMENT VARCHAR(50) COMMENT 'Unique identifier to the equipment - physical identification',
    FK_EQUIPMENT_TYPE INT COMMENT 'Foreign key to equipment type table', 
    PRIMARY KEY (ID_EQUIPMENT),
    CONSTRAINT FK_EQUIPMENT_TYPE_CONSTRAINT FOREIGN KEY (FK_EQUIPMENT_TYPE) REFERENCES DIM_EQUIPMENT_TYPE (ID_EQUIPMENT_TYPE),
    FULLTEXT(NA_EQUIPMENT) 
);

-- MEASUREMENT LOCATION DIMENSION
CREATE TABLE DIM_SITE_STATE (
    ID_STATE INT NOT NULL COMMENT 'Primary Key to file STATE dimension table',
    NA_STATE VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT 'Location State full name', -- use COLLATE utf8_general_ci to allow for case insensitive search for non-english characters
    LC_STATE VARCHAR(2) COMMENT 'Location State short form in two letters',
    PRIMARY KEY (ID_STATE),
    FULLTEXT(NA_STATE) -- add FULLTEXT index to NA_STATE column
);

CREATE TABLE DIM_SITE_COUNTY (
    ID_COUNTY INT NOT NULL COMMENT 'Primary Key to file type dimension table',
    FK_STATE INT COMMENT 'Foreign key to location state table',
    NA_COUNTY VARCHAR(60) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT 'Location County full name ', -- use COLLATE utf8_general_ci to allow for case insensitive search for non-english characters
    PRIMARY KEY (ID_COUNTY),
    CONSTRAINT FK_COUNTY_STATE FOREIGN KEY (FK_STATE) REFERENCES DIM_SITE_STATE (ID_STATE),
    FULLTEXT(NA_COUNTY) -- add FULLTEXT index to NA_COUNTY column
);

CREATE TABLE DIM_SITE_DISTRICT (
    ID_DISTRICT INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to file type dimension table',
    FK_COUNTY INT COMMENT 'Foreign key to location municipality table',
    NA_DISTRICT VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT 'Location County full name ', -- use COLLATE utf8_general_ci to allow for case insensitive search for non-english characters
    PRIMARY KEY (ID_DISTRICT),
    CONSTRAINT FK_DISTRICT_COUNTY FOREIGN KEY (FK_COUNTY) REFERENCES DIM_SITE_COUNTY (ID_COUNTY),
    FULLTEXT(NA_DISTRICT) -- add FULLTEXT index to NA_DISTRICT column
);

CREATE TABLE DIM_SITE_TYPE (
    ID_TYPE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to file type dimension table',
    NA_TYPE VARCHAR(50) COMMENT 'Site type description. e.g. point, route, area',
    PRIMARY KEY (ID_TYPE)
);

CREATE TABLE DIM_SPECTRUN_SITE (
    ID_SITE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to site location dimension table',
    FK_DISTRICT INT COMMENT 'Foreign key to District table Not related to County to handle case where there is no District', 
    FK_COUNTY INT COMMENT 'Foreign key to County table. Not related to State to handle case where there is no County', 
    FK_STATE INT COMMENT 'Foreign key to UF table', 
    FK_TYPE INT COMMENT 'Foreign key to site type table',
    NA_SITE VARCHAR(100) COMMENT 'Location description. Future use for easier referencing to location common name',
    GEOLOCATION POINT NOT NULL COMMENT 'Location tuple (longitude, latitude)',
    NU_ALTITUDE DECIMAL(10,3) COMMENT 'Altitude in meters',
    NU_GNSS_MEASUREMENTS BIGINT COMMENT 'Number of GNSS measurements used for the definition of the site location ',
    GEOGRAPHIC_PATH BLOB COMMENT 'Geographic path data stored as .mat or parquet file',
    PRIMARY KEY (ID_SITE),
    CONSTRAINT FK_SITE_DISTRICT FOREIGN KEY (FK_DISTRICT) REFERENCES DIM_SITE_DISTRICT (ID_DISTRICT),
    CONSTRAINT FK_SITE_COUNTY FOREIGN KEY (FK_COUNTY) REFERENCES DIM_SITE_COUNTY (ID_COUNTY),
    CONSTRAINT FK_SITE_STATE FOREIGN KEY (FK_STATE) REFERENCES DIM_SITE_STATE (ID_STATE),
    CONSTRAINT FK_SITE_TYPE FOREIGN KEY (FK_TYPE) REFERENCES DIM_SITE_TYPE (ID_TYPE),
    SPATIAL INDEX SP_INDEX (GEOLOCATION)
);

-- MEASUREMENT FILE DIMENSION
CREATE TABLE DIM_FILE_TYPE (
    ID_TYPE_FILE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to file type dimension table', 
    NA_TYPE_FILE VARCHAR(50) COMMENT 'Description of the file type',
    PRIMARY KEY (ID_TYPE_FILE)
);

CREATE TABLE DIM_SPECTRUN_FILE (
    ID_FILE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to file dimension table',
    ID_TYPE_FILE INT COMMENT 'Foreign key to file type table', 
    NA_FILE VARCHAR(100) COMMENT 'Unique identifier to the filename',
    NA_PATH VARCHAR(3000) COMMENT 'Unique identifier to the file path',
    NA_VOLUME VARCHAR(3000) COMMENT 'Unique identifier to the file storage volume',
    PRIMARY KEY (ID_FILE)
);

-- MEASUREMENT PARAMETERS DIMENSION
CREATE TABLE DIM_SPECTRUN_DETECTOR (
    ID_DETECTOR_TYPE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to measurement detector type dimension table', 
    NA_DETECTOR_TYPE VARCHAR(50) COMMENT 'Description of the measurement detector type. e.g. RMS, Sample, Positive Peak',
    PRIMARY KEY (ID_DETECTOR_TYPE)
);

CREATE TABLE DIM_SPECTRUN_TRACE (
    ID_TRACE_TYPE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to measurement trace type dimension table', 
    NA_TRACE_TYPE VARCHAR(50) COMMENT 'Description of the measurement trace type. e.g. Peak, Average, Minimum',
    PRIMARY KEY (ID_TRACE_TYPE)
);

CREATE TABLE DIM_SPECTRUN_UNIDADE (
    ID_MEASURE_UNIT INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to measurement trace type dimension table', 
    NA_MEASURE_UNIT VARCHAR(10) COMMENT 'Description of the measurement trace type. e.g. Peak, Average, Minimum',
    PRIMARY KEY (ID_MEASURE_UNIT)
);

-- DRAFT TO FEATURE IN BACKLOG - MEASURED EMITTERS DIMENSION
CREATE TABLE DIM_SPECTRUN_EMITTER (
    ID_EMITTER INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to emitter dimension table', 
    NA_EMITTER VARCHAR(50) COMMENT 'Unique identifier to emitter in the spectrum management database',
    PRIMARY KEY (ID_EMITTER)
);

CREATE TABLE DIM_MEASUREMENT_PROCEDURE (
    ID_PROCEDURE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to measurement trace type dimension table', 
    NA_PROCEDURE VARCHAR(100) COMMENT 'Description of the measurement trace type. e.g. Peak, Average, Minimum',
    PRIMARY KEY (ID_PROCEDURE)
);

-- MEASUREMENT DATA FACT
CREATE TABLE FACT_SPECTRUN (
    ID_FACT_SPECTRUN INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to the fact table of spectrum measurement data.',
    FK_SITE INT COMMENT 'Foreign key to location dimension table',
    FK_DETECTOR_TYPE INT COMMENT 'Foreign key to location dimension table',
    FK_TRACE_TYPE INT COMMENT 'Foreign key to location dimension table',
    FK_MEASURE_UNIT INT COMMENT 'Foreign key to location dimension table',
    FK_PROCEDURE INT COMMENT 'Foreign key to measurement procedure used',
    NA_DESCRIPTION VARCHAR(3000) COMMENT 'Text description for the measurement',
    NU_FREQ_START DECIMAL(14,6) NULL COMMENT 'Initial frequency expressed in MHz',
    NU_FREQ_END DECIMAL(14,6) NULL COMMENT 'End frequency expressed in MHz',
    DT_TIME_START DATETIME NULL COMMENT 'Initial local time',
    DT_TIME_END DATETIME NULL COMMENT 'End local time',
    NU_SAMPLE_DURATION DECIMAL(9,3) NULL COMMENT 'Sample duration, usually down to microsecond and up to seconds',
    NU_TRACE_COUNT INT NULL COMMENT 'Number of traces within the file and frequency band',
    NU_TRACE_LENGTH INT NULL COMMENT 'Measurement vector length describing a single trace. Number of bins in the FFT',
    NU_RBW DECIMAL(12,1) NULL COMMENT 'RBW in Hz', -- ! NEED CONFIRMATION 
    NU_VBW DECIMAL(12,1) NULL COMMENT 'VBW in Hz', -- ! NEED CONFIRMATION 
    NU_ATT_GAIN DECIMAL(4,1) NULL COMMENT 'Applied attenuation or gain set in negative or positive dB', -- ! NEED CONFIRMATION IF OVERLAPS WITH GAIN. USED DEVICES
    CONSTRAINT FK_SITE_CONSTRAINT FOREIGN KEY (FK_SITE) REFERENCES DIM_SPECTRUN_SITE (ID_SITE),
    CONSTRAINT FK_DETECTOR_TYPE_CONSTRAINT FOREIGN KEY (FK_DETECTOR_TYPE) REFERENCES DIM_SPECTRUN_DETECTOR (ID_DETECTOR_TYPE),
    CONSTRAINT FK_TRACE_TYPE_CONSTRAINT FOREIGN KEY (FK_TRACE_TYPE) REFERENCES DIM_SPECTRUN_TRACE (ID_TRACE_TYPE),
    CONSTRAINT FK_MEASURE_UNIT_CONSTRAINT FOREIGN KEY (FK_MEASURE_UNIT) REFERENCES DIM_SPECTRUN_UNIDADE (ID_MEASURE_UNIT),
    CONSTRAINT FK_PROCEDURE_CONSTRAINT FOREIGN KEY (FK_PROCEDURE) REFERENCES DIM_MEASUREMENT_PROCEDURE (ID_PROCEDURE),
    PRIMARY KEY (ID_FACT_SPECTRUN)
);

-- BRIDGE TABLES TO ALLOW FOR N-N RELATIONSHIPS TO THE FACT TABLE
CREATE TABLE BRIDGE_SPECTRUN_EQUIPMENT (
    ID_BRIDGE_EQUIPMENT INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to bridge table to equipment dimension table. Allows n-n equipment-measurement associations', 
    FK_EQUIPMENT INT COMMENT 'Foreign key to equipment table', 
    FK_SPECTRUN INT COMMENT 'Foreign key to spectrum measurement table',
    CONSTRAINT FK_EQUIPMENT_SPEC_CONSTRAINT FOREIGN KEY (FK_EQUIPMENT) REFERENCES DIM_SPECTRUN_EQUIPMENT (ID_EQUIPMENT),
    CONSTRAINT FK_SPECTRUN_EQUI_CONSTRAINT FOREIGN KEY (FK_SPECTRUN) REFERENCES FACT_SPECTRUN (ID_FACT_SPECTRUN),
    PRIMARY KEY (ID_BRIDGE_EQUIPMENT)
);

-- BRIDGE TABLES TO ALLOW FOR N-N RELATIONSHIPS TO THE FACT TABLE
CREATE TABLE BRIDGE_SPECTRUN_EMITTER (
    ID_BRIDGE_EMITTER INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to bridge table to emitter dimension table. Allows n-n emitter-measurement associations', 
    FK_EMITTER INT COMMENT 'Foreign key to equipment table', 
    FK_SPECTRUN INT COMMENT 'Foreign key to spectrum measurement table', 
    CONSTRAINT FK_EMITTER_SPEC_CONSTRAINT FOREIGN KEY (FK_EMITTER) REFERENCES DIM_SPECTRUN_EMITTER (ID_EMITTER), 
    CONSTRAINT FK_SPECTRUN_EMIT_CONSTRAINT FOREIGN KEY (FK_SPECTRUN) REFERENCES FACT_SPECTRUN (ID_FACT_SPECTRUN), 
    PRIMARY KEY (ID_BRIDGE_EMITTER)
);

-- BRIDGE TABLES TO ALLOW FOR N-N RELATIONSHIPS TO THE FACT TABLE
CREATE TABLE BRIDGE_SPECTRUN_FILE (
    ID_BRIDGE_FILE INT NOT NULL AUTO_INCREMENT COMMENT 'Primary Key to bridge table to file dimension table. Allows n-n file-measurement associations', 
    FK_FILE INT COMMENT 'Foreign key to file table', 
    FK_SPECTRUN INT COMMENT 'Foreign key to spectrum measurement table', 
    CONSTRAINT FK_FILE_SPEC_CONSTRAINT FOREIGN KEY (FK_FILE) REFERENCES DIM_SPECTRUN_FILE (ID_FILE), 
    CONSTRAINT FK_SPECTRUN_FILE_CONSTRAINT FOREIGN KEY (FK_SPECTRUN) REFERENCES FACT_SPECTRUN (ID_FACT_SPECTRUN), 
    PRIMARY KEY (ID_BRIDGE_FILE)
);

-- Data Uploads

-- Upload Data to DIM_EQUIPMENT_TYPE
LOAD DATA INFILE '/etc/appCataloga/equipmentType.csv'
INTO TABLE DIM_EQUIPMENT_TYPE
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_EQUIPMENT_TYPE, NA_EQUIPMENT_TYPE);

-- Upload Data to DIM_SITE_STATE
LOAD DATA INFILE '/etc/appCataloga/IBGE-BR_UF_2020_BULKLOAD.csv'
INTO TABLE DIM_SITE_STATE
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_STATE, NA_STATE, LC_STATE);

-- Upload Data to DIM_SITE_COUNTY
LOAD DATA INFILE '/etc/appCataloga/IBGE-BR_Municipios_2020_BULKLOAD.csv'
INTO TABLE DIM_SITE_COUNTY
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_COUNTY, FK_STATE, NA_COUNTY);

-- Upload Data to DIM_SPECTRUN_UNIDADE
LOAD DATA INFILE '/etc/appCataloga/measurementUnit.csv'
INTO TABLE DIM_SPECTRUN_UNIDADE
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_MEASURE_UNIT, NA_MEASURE_UNIT);

-- Upload Data to DIM_FILE_TYPE
LOAD DATA INFILE '/etc/appCataloga/fileType.csv'
INTO TABLE DIM_FILE_TYPE
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_TYPE_FILE, NA_TYPE_FILE);