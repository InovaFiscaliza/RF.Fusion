/* =====================================================================
   createMeasureDB-v5.sql
   - Correção dos LOAD DATA (LF '\n' ao invés de CRLF '\r\n')
   - Mantém estrutura e lógica da versão v4 original
   - Usar diretamente: mysql -u root -p < createMeasureDB-v4.1.sql
   ===================================================================== */

-- =====================================================================
-- CREATE DATABASE
-- =====================================================================
CREATE DATABASE IF NOT EXISTS RFDATA
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE RFDATA;

-- =====================================================================
-- DIM_EQUIPMENT_TYPE
-- =====================================================================
CREATE TABLE DIM_EQUIPMENT_TYPE (
    ID_EQUIPMENT_TYPE INT NOT NULL AUTO_INCREMENT,
    NA_EQUIPMENT_TYPE VARCHAR(50),
    NA_EQUIPMENT_TYPE_UID VARCHAR(50),
    PRIMARY KEY (ID_EQUIPMENT_TYPE)
) CHARACTER SET utf8mb4;

LOAD DATA INFILE '/RFFusion/src/mariadb/scripts/equipmentType.csv'
    INTO TABLE DIM_EQUIPMENT_TYPE
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (ID_EQUIPMENT_TYPE, NA_EQUIPMENT_TYPE, NA_EQUIPMENT_TYPE_UID);

-- =====================================================================
-- DIM_SPECTRUM_EQUIPMENT
-- =====================================================================
CREATE TABLE DIM_SPECTRUM_EQUIPMENT (
    ID_EQUIPMENT INT NOT NULL AUTO_INCREMENT,
    FK_EQUIPMENT_TYPE INT,
	NA_EQUIPMENT VARCHAR(100),
    PRIMARY KEY (ID_EQUIPMENT),
    FOREIGN KEY (FK_EQUIPMENT_TYPE) REFERENCES DIM_EQUIPMENT_TYPE (ID_EQUIPMENT_TYPE),
    FULLTEXT (NA_EQUIPMENT)
) CHARACTER SET utf8mb4;

-- =====================================================================
-- DIM_SITE_STATE
-- =====================================================================
CREATE TABLE DIM_SITE_STATE (
    ID_STATE INT NOT NULL,
    NA_STATE VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    LC_STATE VARCHAR(2),
    PRIMARY KEY (ID_STATE),
    FULLTEXT (NA_STATE)
) CHARACTER SET utf8mb4;

LOAD DATA INFILE '/RFFusion/src/mariadb/scripts/IBGE-BR_UF_2020_BULKLOAD.csv'
    INTO TABLE DIM_SITE_STATE
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (ID_STATE, NA_STATE, LC_STATE);

-- =====================================================================
-- DIM_SITE_COUNTY
-- =====================================================================
CREATE TABLE DIM_SITE_COUNTY (
    ID_COUNTY INT NOT NULL,
    FK_STATE INT,
    NA_COUNTY VARCHAR(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    PRIMARY KEY (ID_COUNTY),
    FOREIGN KEY (FK_STATE) REFERENCES DIM_SITE_STATE (ID_STATE),
    FULLTEXT (NA_COUNTY)
) CHARACTER SET utf8mb4;

LOAD DATA INFILE '/RFFusion/src/mariadb/scripts/IBGE-BR_Municipios_2020_BULKLOAD.csv'
    INTO TABLE DIM_SITE_COUNTY
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (ID_COUNTY, FK_STATE, @county)
SET NA_COUNTY = TRIM(REPLACE(REPLACE(REPLACE(REPLACE(@county, '|',''), CHAR(13),''), CHAR(10),''), CHAR(9), ''));

-- Remove caracteres indesejados (|, CR, LF, TAB)
UPDATE DIM_SITE_COUNTY
SET NA_COUNTY = TRIM(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(NA_COUNTY, '|', ''),
                CHAR(13), ''),
            CHAR(10), ''),
        CHAR(9), '')
);

-- Remove county com tamanho inválido (caso tenha sobrado lixo)
UPDATE DIM_SITE_COUNTY
SET NA_COUNTY = TRIM(NA_COUNTY)
WHERE LENGTH(NA_COUNTY) > 0;

-- =====================================================================
-- DIM_SITE_DISTRICT
-- =====================================================================
CREATE TABLE DIM_SITE_DISTRICT (
    ID_DISTRICT INT NOT NULL AUTO_INCREMENT,
    FK_COUNTY INT,
    NA_DISTRICT VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    PRIMARY KEY (ID_DISTRICT),
    FOREIGN KEY (FK_COUNTY) REFERENCES DIM_SITE_COUNTY (ID_COUNTY),
    FULLTEXT (NA_DISTRICT)
) CHARACTER SET utf8mb4;

-- =====================================================================
-- DIM_SITE_TYPE
-- =====================================================================
CREATE TABLE DIM_SITE_TYPE (
    ID_TYPE INT NOT NULL AUTO_INCREMENT,
    NA_TYPE VARCHAR(50),
    PRIMARY KEY (ID_TYPE)
) CHARACTER SET utf8mb4;

-- =====================================================================
-- DIM_SPECTRUM_SITE
-- =====================================================================
CREATE TABLE DIM_SPECTRUM_SITE (
    ID_SITE INT NOT NULL AUTO_INCREMENT,
    FK_DISTRICT INT,
    FK_COUNTY INT,
    FK_STATE INT,
    FK_TYPE INT,
    NA_SITE VARCHAR(100),
    GEO_POINT POINT NOT NULL,
    NU_ALTITUDE DECIMAL(10,3),
    NU_GNSS_MEASUREMENTS BIGINT,
    GEOGRAPHIC_PATH BLOB,
    PRIMARY KEY (ID_SITE),
    FOREIGN KEY (FK_DISTRICT) REFERENCES DIM_SITE_DISTRICT(ID_DISTRICT),
    FOREIGN KEY (FK_COUNTY) REFERENCES DIM_SITE_COUNTY(ID_COUNTY),
    FOREIGN KEY (FK_STATE) REFERENCES DIM_SITE_STATE(ID_STATE),
    FOREIGN KEY (FK_TYPE) REFERENCES DIM_SITE_TYPE(ID_TYPE),
    SPATIAL INDEX SP_INDEX (GEO_POINT)
) CHARACTER SET utf8mb4;

-- =====================================================================
-- DIM_FILE_TYPE
-- =====================================================================
CREATE TABLE DIM_FILE_TYPE (
    ID_TYPE_FILE INT NOT NULL AUTO_INCREMENT,
    NA_TYPE_FILE VARCHAR(50),
    NA_EQUIPMENT VARCHAR(50),
    PRIMARY KEY (ID_TYPE_FILE)
) CHARACTER SET utf8mb4;

LOAD DATA INFILE '/RFFusion/src/mariadb/scripts/fileType.csv'
    INTO TABLE DIM_FILE_TYPE
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (ID_TYPE_FILE, NA_TYPE_FILE, NA_EQUIPMENT);

-- =====================================================================
-- DIM_SPECTRUM_FILE
-- =====================================================================
CREATE TABLE DIM_SPECTRUM_FILE (
    ID_FILE INT NOT NULL AUTO_INCREMENT,
    DT_FILE_LOGGED DATETIME DEFAULT CURRENT_TIMESTAMP,
    ID_TYPE_FILE INT,
    NA_FILE VARCHAR(100),
    NA_PATH VARCHAR(3000),
    NA_VOLUME VARCHAR(3000),
    NA_EXTENSION VARCHAR(20),
    NU_MD5 VARCHAR(32),
    VL_FILE_SIZE_KB BIGINT,
    DT_FILE_CREATED DATETIME,
    DT_FILE_MODIFIED DATETIME,
    PRIMARY KEY (ID_FILE),
    FOREIGN KEY (ID_TYPE_FILE) REFERENCES DIM_FILE_TYPE(ID_TYPE_FILE)
) CHARACTER SET utf8mb4;

-- =====================================================================
-- DETECTOR / TRACE TYPE / UNIT
-- =====================================================================
CREATE TABLE DIM_SPECTRUM_DETECTOR (
    ID_DETECTOR INT NOT NULL AUTO_INCREMENT,
    NA_DETECTOR VARCHAR(50),
    PRIMARY KEY (ID_DETECTOR)
) CHARACTER SET utf8mb4;

CREATE TABLE DIM_SPECTRUM_TRACE_TYPE (
    ID_TRACE_TYPE INT NOT NULL AUTO_INCREMENT,
    NA_TRACE_TYPE VARCHAR(50),
    PRIMARY KEY (ID_TRACE_TYPE)
) CHARACTER SET utf8mb4;

CREATE TABLE DIM_SPECTRUM_UNIT (
    ID_MEASURE_UNIT INT NOT NULL AUTO_INCREMENT,
    NA_MEASURE_UNIT VARCHAR(10),
    PRIMARY KEY (ID_MEASURE_UNIT)
) CHARACTER SET utf8mb4;

LOAD DATA INFILE '/RFFusion/src/mariadb/scripts/measurementUnit.csv'
    INTO TABLE DIM_SPECTRUM_UNIT
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (ID_MEASURE_UNIT, NA_MEASURE_UNIT);

-- =====================================================================
-- EMITTER & PROCEDURE
-- =====================================================================
CREATE TABLE DIM_SPECTRUM_EMITTER (
    ID_EMITTER INT NOT NULL AUTO_INCREMENT,
    NA_EMITTER VARCHAR(50),
    PRIMARY KEY (ID_EMITTER)
) CHARACTER SET utf8mb4;

CREATE TABLE DIM_SPECTRUM_PROCEDURE (
    ID_PROCEDURE INT NOT NULL AUTO_INCREMENT,
    NA_PROCEDURE VARCHAR(100),
    PRIMARY KEY (ID_PROCEDURE)
) CHARACTER SET utf8mb4;

-- =====================================================================
-- FACT TABLE
-- =====================================================================
CREATE TABLE FACT_SPECTRUM (
    ID_SPECTRUM INT NOT NULL AUTO_INCREMENT,
    FK_SITE INT,
    FK_DETECTOR INT,
    FK_TRACE_TYPE INT,
    FK_MEASURE_UNIT INT,
    FK_PROCEDURE INT,
    FK_EQUIPMENT INT,
    NA_DESCRIPTION VARCHAR(3000),
    NU_FREQ_START DECIMAL(14,6),
    NU_FREQ_END DECIMAL(14,6),
    DT_TIME_START DATETIME,
    DT_TIME_END DATETIME,
    NU_SAMPLE_DURATION DECIMAL(9,3),
    NU_TRACE_COUNT INT,
    NU_TRACE_LENGTH INT,
    NU_RBW DECIMAL(12,1),
    NU_VBW DECIMAL(12,1),
    NU_ATT_GAIN DECIMAL(4,1),
    JS_METADATA JSON COMMENT 'Additional spectrum metadata (non-dimensional)',
    PRIMARY KEY (ID_SPECTRUM),
    FOREIGN KEY (FK_SITE) REFERENCES DIM_SPECTRUM_SITE(ID_SITE),
    FOREIGN KEY (FK_DETECTOR) REFERENCES DIM_SPECTRUM_DETECTOR(ID_DETECTOR),
    FOREIGN KEY (FK_TRACE_TYPE) REFERENCES DIM_SPECTRUM_TRACE_TYPE(ID_TRACE_TYPE),
    FOREIGN KEY (FK_MEASURE_UNIT) REFERENCES DIM_SPECTRUM_UNIT(ID_MEASURE_UNIT),
    FOREIGN KEY (FK_PROCEDURE) REFERENCES DIM_SPECTRUM_PROCEDURE(ID_PROCEDURE),
    FOREIGN KEY (FK_EQUIPMENT) REFERENCES DIM_SPECTRUM_EQUIPMENT(ID_EQUIPMENT)
) CHARACTER SET utf8mb4;

-- =====================================================================
-- BRIDGE TABLES
-- =====================================================================

CREATE TABLE BRIDGE_SPECTRUM_EMITTER (
    FK_EMITTER INT,
    FK_SPECTRUM INT,
    PRIMARY KEY (FK_EMITTER, FK_SPECTRUM),
    FOREIGN KEY (FK_EMITTER) REFERENCES DIM_SPECTRUM_EMITTER(ID_EMITTER),
    FOREIGN KEY (FK_SPECTRUM) REFERENCES FACT_SPECTRUM(ID_SPECTRUM)
) CHARACTER SET utf8mb4;

CREATE TABLE BRIDGE_SPECTRUM_FILE (
    FK_FILE INT,
    FK_SPECTRUM INT,
    PRIMARY KEY (FK_FILE, FK_SPECTRUM),
    INDEX IX_BRIDGE_SPECTRUM_FILE_SPECTRUM (FK_SPECTRUM, FK_FILE),
    FOREIGN KEY (FK_FILE) REFERENCES DIM_SPECTRUM_FILE(ID_FILE),
    FOREIGN KEY (FK_SPECTRUM) REFERENCES FACT_SPECTRUM(ID_SPECTRUM)
) CHARACTER SET utf8mb4;
