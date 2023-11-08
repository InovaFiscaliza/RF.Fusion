-- ACTIVATE DATABASE
CREATE DATABASE RFDATA
    DEFAULT CHARACTER SET = 'utf8mb4';

USE RFDATA;

-- MEASUREMENT EQUIPMENT DIMENSION
CREATE TABLE DIM_EQUIPAMENTO_TIPO (
    ID_TIPO_EQUIPAMENTO INT NOT NULL AUTO_INCREMENT, -- Primary Key to equipment type dimension table -- 
    NO_TIPO_EQUIPAMENTO VARCHAR(50), -- Description of the equipment type --
    PRIMARY KEY (ID_TIPO_EQUIPAMENTO)
);

CREATE TABLE DIM_SPECTRUN_EQUIPAMENTO (
    ID_EQUIPAMENTO INT NOT NULL AUTO_INCREMENT, -- Primary Key to equipment dimension table --
    NO_EQUIPAMENTO VARCHAR(50), -- Unique identifier to the equipment - physical identification --
    FK_TIPO_EQUIPAMENTO INT, -- Foreign key to equipment type table -- 
    FOREIGN KEY (ID_TIPO_EQUIPAMENTO) REFERENCES DIM_EQUIPAMENTO_TIPO(ID_TIPO_EQUIPAMENTO),
    PRIMARY KEY (ID_EQUIPAMENTO)
);

-- MEASUREMENT LOCATION DIMENSION
CREATE TABLE DIM_SITE_STATE (
    ID_STATE_CODE INT NOT NULL, -- Primary Key to file STATE dimension table --
    NO_STATE VARCHAR(50), -- Location State full name --
    SG_STATE VARCHAR(2), -- Location State short form --
    PRIMARY KEY (ID_STATE_CODE)
);

CREATE TABLE DIM_SITE_COUNTY (
    ID_COUNTY_CODE INT NOT NULL, -- Primary Key to file type dimension table --
    FK_STATE_CODE INT, -- Foreign key to location state table --
    NO_COUNTY NVARCHAR(60), -- Location County full name  --
    PRIMARY KEY (ID_COUNTY_CODE),
    CONSTRAINT FK_COUNTY_STATE FOREIGN KEY (ID_STATE_CODE) REFERENCES DIM_SITE_STATE (ID_STATE_CODE)
);

CREATE TABLE DIM_SITE_DISTRICT (
    ID_DISTRICT INT NOT NULL AUTO_INCREMENT, -- Primary Key to file type dimension table --
    FK_COUNTY_CODE INT, -- Foreign key to location municipality table --
    NO_DISTRICT NVARCHAR(50), -- Location County full name  --
    PRIMARY KEY (ID_DISTRICT),
    CONSTRAINT FK_DISTRICT_COUNTY FOREIGN KEY (ID_COUNTY_CODE) REFERENCES DIM_SITE_COUNTY (ID_COUNTY_CODE)
);

CREATE TABLE DIM_SPECTRUN_SITE (
    ID_SITE INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to site location dimension table --
    PK_SITE_DISTRICT INT FOREIGN KEY REFERENCES DIM_SITE_DISTRICT(ID_DISTRICT), -- Foreign key to District table Not related to County to handle case where there is no District -- 
    PK_COUNTY_CODE INT FOREIGN KEY REFERENCES DIM_SITE_COUNTY(ID_COUNTY_CODE), -- Foreign key to County table. Not related to State to handle case where there is no County -- 
    PK_STATE_CODE INT FOREIGN KEY REFERENCES DIM_SITE_UF(ID_UF_CODE), -- Foreign key to UF table -- 
    NO_SITE NVARCHAR(100), -- Location description. Future use for easier referencing to location common name. 
    GEOLOCATION POINT NULL, -- Location with longitude, latitude and altitude (Z-=10.3) with 0 for measure (M)- POINT(-122.34900 47.65100 10.3 0)
    NU_GNSS_MEASUREMENTS BIGINT NULL, -- Number of GNSS measurements used for the definition of the site location  --
);

    -- create spatial index to allow for spatial queries
    CREATE SPATIAL INDEX SP_INDEX on DIM_SPECTRUN_SITE(GEOLOCATION)

    -- Create full text catalogs used create fulltext search indexes 
        -- Language 1046 = Português (Brasil) / Alternative 0 = Neutral
        -- https://www.sqlshack.com/hands-full-text-search-sql-server/ TYPE COLUMN OBJ_FILE_IDX_DOCTYPE 

    CREATE FULLTEXT CATALOG FullTextCatalog AS DEFAULT;

    -- Do not create a fulltext search for State. Due to the small number of entries, use LIKE statement in the select

    -- Create fulltext search indexes for UF
        CREATE FULLTEXT
        INDEX ON  DIM_SITE_UF (NO_UF LANGUAGE 1046)
        KEY INDEX ID_BUSCA_TEXTUAL_UF WITH 
            CHANGE_TRACKING = AUTO, 
            STOPLIST=OFF;

    -- Create fulltext search indexes for county
        CREATE FULLTEXT
        INDEX ON  DIM_SITE_COUNTY (NO_COUNTY LANGUAGE 1046)
        KEY INDEX ID_BUSCA_TEXTUAL_COUNTY WITH 
            CHANGE_TRACKING = AUTO, 
            STOPLIST=OFF;

    -- Create fulltext search indexes for district
        CREATE FULLTEXT
        INDEX ON  DIM_SITE_DISTRICT (NO_DISTRICT LANGUAGE 1046)
        KEY INDEX ID_BUSCA_TEXTUAL_DISTRICT WITH 
            CHANGE_TRACKING = AUTO, 
            STOPLIST=OFF;


-- MEASUREMENT FILE DIMENSION
CREATE TABLE DIM_ARQUIVO_TIPO (
    ID_TIPO_ARQUIVO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to file type dimension table -- 
    NO_TIPO_ARQUIVO NVARCHAR(50) -- Description of the file type --
);

CREATE TABLE DIM_SPECTRUN_ARQUIVO (
    ID_ARQUIVO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to file dimension table --
    ID_TIPO_ARQUIVO INT, -- Foreign key to file type table -- 
    NO_ARQUIVO NVARCHAR(100), -- Unique identifier to the filename --
    NO_DIR_E_ARQUIVO NVARCHAR(3000), -- Unique identifier to the file path --
    NO_URL NVARCHAR(3000) -- Unique identifier to the file path --
);

-- MEASUREMENT PARAMETERS DIMENSION
CREATE TABLE DIM_SPECTRUN_DETECTOR (
    ID_TIPO_DETECTOR INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to measurement detector type dimension table -- 
    NO_TIPO_DETECTOR NVARCHAR(50) -- Description of the measurement detector type. e.g. RMS, Sample, Positive Peak --
);

CREATE TABLE DIM_SPECTRUN_TRACO (
    ID_TIPO_TRACO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
    NO_TIPO_TRACO NVARCHAR(50) -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
);

CREATE TABLE DIM_SPECTRUN_UNIDADE (
    ID_UNIDADE_MEDIDA INT NOT NULL PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
    NO_UNIDADE_MEDIDA NVARCHAR(10) -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
);

-- DRAFT TO FEATURE IN BACKLOG - MEASURED EMITTERS DIMENSION
CREATE TABLE DIM_SPECTRUN_EMISSOR (
    ID_EMISSOR INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to emitter dimension table -- 
    NO_EMISSOR NVARCHAR(50) -- Unique identifier to emitter in the spectrum management database --
);

CREATE TABLE DIM_MEDICAO_PROCEDIMENTO (
    ID_PROCEDIMENTO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
    NO_PROCEDIMENTO NVARCHAR(100) -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
);

-- MEASUREMENT DATA FACT
CREATE TABLE FATO_SPECTRUN (
    ID_FATO_SPECTRUN INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to the fact table of spectrum measurement data. --
    FK_ARQUIVO INT, -- Foreign key to file dimension table --
    FK_SITE INT, -- Foreign key to location dimension table --
    FK_TIPO_DETECTOR INT, -- Foreign key to location dimension table --
    FK_TIPO_TRACO INT, -- Foreign key to location dimension table --
    FK_UNIDADE_MEDIDA INT, -- Foreign key to location dimension table --
    FK_PROCEDIMENTO INT, -- Foreign key to measurement procedure used --
    NO_DESCRIPTION NVARCHAR(3000), -- Text description for the measurement --
    NU_FREQUENCIA_INICIAL DECIMAL(14,6) NULL, -- Initial frequency expressed in MHz --
    NU_FREQUENCIA_FINAL DECIMAL(14,6) NULL, -- End frequency expressed in MHz --
    DT_TEMPO_INICIAL DATETIME NULL, -- Initial local time --
    DT_TEMPO_FINAL DATETIME NULL, -- End local time --
    NU_DURACAO_AMOSTRA DECIMAL(9,3) NULL, -- Sample duration, usually down to microsecond and up to seconds
    NU_NUMERO_TRACOS INT NULL, -- Number of traces within the file and frequency band
    NU_TAMANHO_VETOR INT NULL, -- Measurement vector length describing a single trace. Number of bins in the FFT.
    NU_RBW DECIMAL(12,1) NULL, -- RBW in Hz -- ! NEED CONFIRMATION 
    NU_VBW DECIMAL(12,1) NULL, -- VBW in Hz -- ! NEED CONFIRMATION 
    NU_ATENUACAO_GANHO DECIMAL(4,1) NULL -- Attenuation set in dB -- ! NEED CONFIRMATION IF OVERLAPS WITH GAIN. USED DEVICES 
);

-- BRIDGE TABLES TO ALLOW FOR N-N RELATIONSHIPS TO THE FACT TABLE
CREATE TABLE PONTE_SPECTRUN_EQUIPAMENTO (
    ID_PONTE_EQUIPAMENTO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to bridge table to equipment dimension table. Allows multiple equipment to be associated with a single measurement and many measurements to be associated with the same equipment -- 
    FK_EQUIPAMENTO INT, -- Foreign key to equipment table -- 
    FK_SPECTRUN INT -- Foreign key to spectrum measurement table -- 
);

-- BRIDGE TABLES TO ALLOW FOR N-N RELATIONSHIPS TO THE FACT TABLE
CREATE TABLE PONTE_SPECTRUN_EMISSOR (
    ID_PONTE_EMISSOR INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to bridge table to equipment dimension table. Allows multiple equipment to be associated with a single measurement and many measurements to be associated with the same equipment -- 
    FK_EMISSOR INT, -- Foreign key to equipment table -- 
    FK_SPECTRUN INT -- Foreign key to spectrum measurement table -- 
);

-- Data Uploads

-- Upload Data to DIM_EQUIPAMENTO_TIPO
LOAD DATA INFILE '/etc/appCataloga/equipmentType.csv'
INTO TABLE DIM_EQUIPAMENTO_TIPO
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_TIPO_EQUIPAMENTO, NO_TIPO_EQUIPAMENTO);

-- Upload Data to DIM_SITE_STATE
LOAD DATA INFILE '/etc/appCataloga/CODE-BR_STATE_2020_BULKLOAD.csv'
INTO TABLE DIM_SITE_STATE
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_STATE_CODE, NO_STATE, SG_STATE);

-- Upload Data to DIM_SITE_COUNTY
LOAD DATA INFILE '/etc/appCataloga/CODE-BR_Municipios_2020_BULKLOAD.csv'
INTO TABLE DIM_SITE_COUNTY
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_COUNTY_CODE, ID_STATE_CODE, NO_COUNTY);

-- Upload Data to DIM_SPECTRUN_UNIDADE
LOAD DATA INFILE '/etc/appCataloga/measurementUnit.csv'
INTO TABLE DIM_SPECTRUN_UNIDADE
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_UNIDADE_MEDIDA, NO_UNIDADE_MEDIDA);

-- Upload Data to DIM_ARQUIVO_TIPO
LOAD DATA INFILE '/etc/appCataloga/fileType.csv'
INTO TABLE DIM_ARQUIVO_TIPO
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_TIPO_ARQUIVO, NO_TIPO_ARQUIVO);