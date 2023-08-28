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

CREATE TABLE DIM_MEDICAO_ESPECTRO_EQUIPAMENTO (
    ID_EQUIPAMENTO INT NOT NULL AUTO_INCREMENT, -- Primary Key to equipment dimension table --
    NO_EQUIPAMENTO VARCHAR(50), -- Unique identifier to the equipment - physical identification --
    ID_TIPO_EQUIPAMENTO INT, -- Foreign key to equipment type table -- 
    FOREIGN KEY (ID_TIPO_EQUIPAMENTO) REFERENCES DIM_EQUIPAMENTO_TIPO(ID_TIPO_EQUIPAMENTO),
    PRIMARY KEY (ID_EQUIPAMENTO)
);

-- MEASUREMENT LOCATION DIMENSION
CREATE TABLE DIM_LOCALIZACAO_UF (
    ID_UF_IBGE INT NOT NULL, -- Primary Key to file UF dimension table --
    NO_UF VARCHAR(50), -- Location State full name --
    SG_UF VARCHAR(2), -- Location State short form --
    PRIMARY KEY (ID_UF_IBGE)
);

CREATE TABLE DIM_LOCALIZACAO_MUNICIPIO (
    ID_MUNICIPIO_IBGE INT NOT NULL, -- Primary Key to file type dimension table --
    ID_UF_IBGE INT, -- Foreign key to location state table --
    NO_MUNICIPIO NVARCHAR(60), -- Location County full name  --
    PRIMARY KEY (ID_MUNICIPIO_IBGE),
    CONSTRAINT FK_MUNICIPIO_UF FOREIGN KEY (ID_UF_IBGE) REFERENCES DIM_LOCALIZACAO_UF (ID_UF_IBGE)
);

CREATE TABLE DIM_LOCALIZACAO_DISTRITO (
    ID_DISTRITO INT NOT NULL AUTO_INCREMENT, -- Primary Key to file type dimension table --
    ID_MUNICIPIO_IBGE INT, -- Foreign key to location municipality table --
    NO_DISTRITO NVARCHAR(50), -- Location County full name  --
    PRIMARY KEY (ID_DISTRITO),
    CONSTRAINT FK_DISTRITO_MUNICIPIO FOREIGN KEY (ID_MUNICIPIO_IBGE) REFERENCES DIM_LOCALIZACAO_MUNICIPIO (ID_MUNICIPIO_IBGE)
);

-- MEASUREMENT FILE DIMENSION
CREATE TABLE DIM_ARQUIVO_TIPO (
    ID_TIPO_ARQUIVO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to file type dimension table -- 
    NO_TIPO_ARQUIVO NVARCHAR(50) -- Description of the file type --
);

CREATE TABLE DIM_MEDICAO_ESPECTRO_ARQUIVO (
    ID_ARQUIVO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to file dimension table --
    ID_TIPO_ARQUIVO INT, -- Foreign key to file type table -- 
    NO_ARQUIVO NVARCHAR(100), -- Unique identifier to the filename --
    NO_DIR_E_ARQUIVO NVARCHAR(3000), -- Unique identifier to the file path --
    NO_URL NVARCHAR(3000) -- Unique identifier to the file path --
);

-- MEASUREMENT PARAMETERS DIMENSION
CREATE TABLE DIM_MEDICAO_ESPECTRO_DETECTOR (
    ID_TIPO_DETECTOR INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to measurement detector type dimension table -- 
    NO_TIPO_DETECTOR NVARCHAR(50) -- Description of the measurement detector type. e.g. RMS, Sample, Positive Peak --
);

CREATE TABLE DIM_MEDICAO_ESPECTRO_TRACO (
    ID_TIPO_TRACO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
    NO_TIPO_TRACO NVARCHAR(50) -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
);

CREATE TABLE DIM_MEDICAO_ESPECTRO_UNIDADE (
    ID_UNIDADE_MEDIDA INT NOT NULL PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
    NO_UNIDADE_MEDIDA NVARCHAR(10) -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
);

-- DRAFT TO FEATURE IN BACKLOG - MEASURED EMITTERS DIMENSION
CREATE TABLE DIM_MEDICAO_ESPECTRO_EMISSOR (
    ID_EMISSOR INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to emitter dimension table -- 
    NO_EMISSOR NVARCHAR(50) -- Unique identifier to emitter in the spectrum management database --
);

CREATE TABLE DIM_MEDICAO_PROCEDIMENTO (
    ID_PROCEDIMENTO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
    NO_PROCEDIMENTO NVARCHAR(100) -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
);

-- MEASUREMENT DATA FACT
CREATE TABLE FATO_MEDICAO_ESPECTRO (
    ID_FATO_MEDICAO_ESPECTRO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to the fact table of spectrum measurement data. --
    ID_ARQUIVO INT, -- Foreign key to file dimension table --
    ID_LOCALIZACAO INT, -- Foreign key to location dimension table --
    ID_TIPO_DETECTOR INT, -- Foreign key to location dimension table --
    ID_TIPO_TRACO INT, -- Foreign key to location dimension table --
    ID_UNIDADE_MEDIDA INT, -- Foreign key to location dimension table --
    ID_PROCEDIMENTO INT, -- Foreign key to measurement procedure used --
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
CREATE TABLE PONTE_MEDICAO_ESPECTRO_EQUIPAMENTO (
    ID_PONTE_EQUIPAMENTO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to bridge table to equipment dimension table. Allows multiple equipment to be associated with a single measurement and many measurements to be associated with the same equipment -- 
    ID_EQUIPAMENTO INT, -- Foreign key to equipment table -- 
    ID_MEDICAO_ESPECTRO INT -- Foreign key to spectrum measurement table -- 
);

-- BRIDGE TABLES TO ALLOW FOR N-N RELATIONSHIPS TO THE FACT TABLE
CREATE TABLE PONTE_MEDICAO_ESPECTRO_EMISSOR (
    ID_PONTE_EMISSOR INT NOT NULL AUTO_INCREMENT PRIMARY KEY, -- Primary Key to bridge table to equipment dimension table. Allows multiple equipment to be associated with a single measurement and many measurements to be associated with the same equipment -- 
    ID_EMISSOR INT, -- Foreign key to equipment table -- 
    ID_MEDICAO_ESPECTRO INT -- Foreign key to spectrum measurement table -- 
);

-- Data Uploads

-- Upload Data to DIM_EQUIPAMENTO_TIPO
LOAD DATA INFILE '/etc/appCataloga/equipmentType.csv'
INTO TABLE DIM_EQUIPAMENTO_TIPO
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_TIPO_EQUIPAMENTO, NO_TIPO_EQUIPAMENTO);

-- Upload Data to DIM_LOCALIZACAO_UF
LOAD DATA INFILE '/etc/appCataloga/IBGE-BR_UF_2020_BULKLOAD.csv'
INTO TABLE DIM_LOCALIZACAO_UF
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_UF_IBGE, NO_UF, SG_UF);

-- Upload Data to DIM_LOCALIZACAO_MUNICIPIO
LOAD DATA INFILE '/etc/appCataloga/IBGE-BR_Municipios_2020_BULKLOAD.csv'
INTO TABLE DIM_LOCALIZACAO_MUNICIPIO
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(ID_MUNICIPIO_IBGE, ID_UF_IBGE, NO_MUNICIPIO);

-- Upload Data to DIM_MEDICAO_ESPECTRO_UNIDADE
LOAD DATA INFILE '/etc/appCataloga/measurementUnit.csv'
INTO TABLE DIM_MEDICAO_ESPECTRO_UNIDADE
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