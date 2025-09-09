--CREATE DATABASE
--This script creates the MariaDB Database used to store the metadata to index the files and populate essential tables with the required information
CREATE DATABASE RFDATA;

USE RFDATA;

-- MEASUREMENT EQUIPMENT DIMENSION

CREATE TABLE DIM_EQUIPAMENTO_TIPO (
        ID_TIPO_EQUIPAMENTO INT NOT NULL, -- Primary Key to equipment type dimension table -- 
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

LOAD DATA LOCAL INFILE '/home/lobao.lx/RFDATA/equipmentType.csv'
    INTO TABLE DIM_EQUIPAMENTO_TIPO
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n' 
    IGNORE 1 LINES
    (ID_TIPO_EQUIPAMENTO,NO_TIPO_EQUIPAMENTO);

-- MEASUREMENT LOCATION DIMENSION
    CREATE TABLE DIM_LOCALIZACAO_UF (
        ID_UF_IBGE INT NOT NULL, -- Primary Key to file UF dimension table --
        NO_UF VARCHAR(50), -- Location State full name --
        SG_UF VARCHAR(2), -- Location State short form --
        PRIMARY KEY (ID_UF_IBGE);
		CONSTRAINT [ID_BUSCA_TEXTUAL_UF] PRIMARY KEY CLUSTERED ([ID_UF_IBGE]) -- create constraint to the text search index column -- 
    ); 
GO
    CREATE TABLE DIM_LOCALIZACAO_MUNICIPIO (
        ID_MUNICIPIO_IBGE INT NOT NULL, -- Primary Key to file type dimension table --
        ID_UF_IBGE INT FOREIGN KEY REFERENCES DIM_LOCALIZACAO_UF(ID_UF_IBGE), -- Foreign key to equipment type table --
        NO_MUNICIPIO NVARCHAR(60), -- Location County full name  --
        CONSTRAINT [ID_BUSCA_TEXTUAL_MUNICIPIO] PRIMARY KEY CLUSTERED ([ID_MUNICIPIO_IBGE]) -- create constraint to the text search index column -- 
    );
GO
    CREATE TABLE DIM_LOCALIZACAO_DISTRITO (
        ID_DISTRITO INT NOT NULL IDENTITY(1,1), -- Primary Key to file type dimension table --
        ID_MUNICIPIO_IBGE INT FOREIGN KEY REFERENCES DIM_LOCALIZACAO_MUNICIPIO(ID_MUNICIPIO_IBGE), -- Foreign key to equipment type table --
        NO_DISTRITO NVARCHAR(50), -- Location County full name  --
        CONSTRAINT [ID_BUSCA_TEXTUAL_DISTRITO] PRIMARY KEY CLUSTERED ([ID_DISTRITO]) -- create constraint to the text search index column -- 
        
    );
GO

    -- INSERT DATA FROM REFERENCE FILES
    BULK INSERT DIM_LOCALIZACAO_UF
        FROM '\home\lobao.lx\RFDATA\IBGE-BR_UF_2020_BULKLOAD.csv'
        WITH
        (
            CODEPAGE = '65001',     -- UTF8 Codepage
            FIELDTERMINATOR = ',',  -- CSV field delimiter
            ROWTERMINATOR = '\n',   -- Use to shift the control to next row
            FIRSTROW = 2
        );
GO

    BULK INSERT DIM_LOCALIZACAO_MUNICIPIO
        FROM '\home\lobao.lx\RFDATA\IBGE-BR_Municipios_2020_BULKLOAD.csv'
        WITH
        (
            CODEPAGE = '65001',     -- UTF8 Codepage
            FIELDTERMINATOR = ',',  -- CSV field delimiter
            ROWTERMINATOR = '\n',   -- Use to shift the control to next row
            FIRSTROW = 2
        );

    CREATE TABLE DIM_MEDICAO_ESPECTRO_LOCALIZACAO (
        ID_LOCALIZACAO INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to site location dimension table --
        ID_DISTRITO INT FOREIGN KEY REFERENCES DIM_LOCALIZACAO_DISTRITO(ID_DISTRITO), -- Foreign key to District table Not related to County to handle case where there is no District -- 
        ID_MUNICIPIO_IBGE INT FOREIGN KEY REFERENCES DIM_LOCALIZACAO_MUNICIPIO(ID_MUNICIPIO_IBGE), -- Foreign key to County table. Not related to State to handle case where there is no County -- 
        ID_UF_IBGE INT FOREIGN KEY REFERENCES DIM_LOCALIZACAO_UF(ID_UF_IBGE), -- Foreign key to UF table -- 
        NO_LOCAL NVARCHAR(100), -- Location description. Future use for easier referencing to location common name. 
        GEO_PONTO GEOGRAPHY NULL, -- Location with longitude, latitude and altitude (Z-=10.3) with 0 for measure (M)- POINT(-122.34900 47.65100 10.3 0)
        NU_QUANTIDADE_MEDIDAS_GNSS BIGINT NULL, -- Number of GNSS measurements used for the definition of the site location  --
    );
GO

    -- create spatial index to allow for spatial queries
    CREATE SPATIAL INDEX IX_LOCALIZACAO_PONTO on DIM_MEDICAO_ESPECTRO_LOCALIZACAO(GEO_PONTO) using geography_auto_grid
GO

    -- Create full text catalogs used create fulltext search indexes 
        -- Language 1046 = Português (Brasil) / Alternative 0 = Neutral
        -- https://www.sqlshack.com/hands-full-text-search-sql-server/ TYPE COLUMN OBJ_FILE_IDX_DOCTYPE 

    CREATE FULLTEXT CATALOG FullTextCatalog AS DEFAULT;
GO

    -- Do not create a fulltext search for State. Due to the small number of entries, use LIKE statement in the select

    -- Create fulltext search indexes for UF
        CREATE FULLTEXT
        INDEX ON  DIM_LOCALIZACAO_UF (NO_UF LANGUAGE 1046)
        KEY INDEX ID_BUSCA_TEXTUAL_UF WITH 
            CHANGE_TRACKING = AUTO, 
            STOPLIST=OFF;

    -- Create fulltext search indexes for county
        CREATE FULLTEXT
        INDEX ON  DIM_LOCALIZACAO_MUNICIPIO (NO_MUNICIPIO LANGUAGE 1046)
        KEY INDEX ID_BUSCA_TEXTUAL_MUNICIPIO WITH 
            CHANGE_TRACKING = AUTO, 
            STOPLIST=OFF;

    -- Create fulltext search indexes for district
        CREATE FULLTEXT
        INDEX ON  DIM_LOCALIZACAO_DISTRITO (NO_DISTRITO LANGUAGE 1046)
        KEY INDEX ID_BUSCA_TEXTUAL_DISTRITO WITH 
            CHANGE_TRACKING = AUTO, 
            STOPLIST=OFF;

GO

-- MEASUREMENT FILE DIMENSION
    CREATE TABLE DIM_ARQUIVO_TIPO (
        ID_TIPO_ARQUIVO INT NOT NULL PRIMARY KEY, -- Primary Key to file type dimension table -- 
        NO_TIPO_ARQUIVO NVARCHAR(50) -- Description of the file type --
    );
GO
    CREATE TABLE DIM_MEDICAO_ESPECTRO_ARQUIVO (
        ID_ARQUIVO INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to file dimension table --
        ID_TIPO_ARQUIVO INT FOREIGN KEY REFERENCES DIM_ARQUIVO_TIPO(ID_TIPO_ARQUIVO), -- Foreign key to file type table -- 
        NO_ARQUIVO NVARCHAR(100), -- Unique identifier to the filename --
        NO_DIR_E_ARQUIVO NVARCHAR(3000), -- Unique identifier to the file path --
        NO_URL NVARCHAR(3000) -- Unique identifier to the file path --
    );

    BULK INSERT DIM_ARQUIVO_TIPO
        FROM '\home\lobao.lx\RFDATA\fileType.csv'
        WITH
        (
            CODEPAGE = '65001',     -- UTF8 Codepage
            FIELDTERMINATOR = ',',  -- CSV field delimiter
            ROWTERMINATOR = '\n',   -- Use to shift the control to next row
            FIRSTROW = 2
        );

-- MEASUREMENT PARAMETERS DIMENSION
    CREATE TABLE DIM_MEDICAO_ESPECTRO_DETECTOR (
        ID_TIPO_DETECTOR INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to measurement detector type dimension table -- 
        NO_TIPO_DETECTOR NVARCHAR(50), -- Description of the measurement detector type. e.g. RMS, Sample, Positive Peak --
    );
GO

    INSERT INTO DIM_MEDICAO_ESPECTRO_DETECTOR 
        (NO_TIPO_DETECTOR)
        VALUES
        ('RMS')

    CREATE TABLE DIM_MEDICAO_ESPECTRO_TRACO (
        ID_TIPO_TRACO INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
        NO_TIPO_TRACO NVARCHAR(50), -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
    );
    
    CREATE TABLE DIM_MEDICAO_ESPECTRO_UNIDADE (
        ID_UNIDADE_MEDIDA INT NOT NULL PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
        NO_UNIDADE_MEDIDA NVARCHAR(10), -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
    );
GO

    BULK INSERT DIM_MEDICAO_ESPECTRO_UNIDADE
        FROM '\home\lobao.lx\RFDATA\measurementUnit.csv'
        WITH
        (
            CODEPAGE = '65001',     -- UTF8 Codepage
            FIELDTERMINATOR = ',',  -- CSV field delimiter
            ROWTERMINATOR = '\n',   -- Use to shift the control to next row
            FIRSTROW = 2
        );

-- DRAFT TO FEATURE IN BACKLOG - MEASURED EMITTERS DIMENSION
    CREATE TABLE DIM_MEDICAO_ESPECTRO_EMISSOR (
        ID_EMISSOR INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to emitter dimension table -- 
        NO_EMISSOR NVARCHAR(50), -- Unique identifier to emitter in the spectrum management database --
    );
GO

    CREATE TABLE DIM_MEDICAO_PROCEDIMENTO (
        ID_PROCEDIMENTO INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to measurement trace type dimension table -- 
        NO_PROCEDIMENTO NVARCHAR(100), -- Description of the measurement trace type. e.g. Peak, Average, Minimum --
    );
GO

-- MEASUREMENT DATA FACT
    CREATE TABLE FATO_MEDICAO_ESPECTRO (
        ID_FATO_MEDICAO_ESPECTRO INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to the fact table of spectrum measurement data. --
        ID_ARQUIVO INT FOREIGN KEY REFERENCES DIM_MEDICAO_ESPECTRO_ARQUIVO(ID_ARQUIVO), -- Foreign key to file dimension table --
        ID_LOCALIZACAO INT FOREIGN KEY REFERENCES DIM_MEDICAO_ESPECTRO_LOCALIZACAO(ID_LOCALIZACAO), -- Foreign key to location dimension table --
        ID_TIPO_DETECTOR INT FOREIGN KEY REFERENCES DIM_MEDICAO_ESPECTRO_DETECTOR(ID_TIPO_DETECTOR), -- Foreign key to location dimension table --
        ID_TIPO_TRACO INT FOREIGN KEY REFERENCES DIM_MEDICAO_ESPECTRO_TRACO(ID_TIPO_TRACO), -- Foreign key to location dimension table --
        ID_UNIDADE_MEDIDA INT FOREIGN KEY REFERENCES DIM_MEDICAO_ESPECTRO_UNIDADE(ID_UNIDADE_MEDIDA), -- Foreign key to location dimension table --
        ID_PROCEDIMENTO INT FOREIGN KEY REFERENCES DIM_MEDICAO_PROCEDIMENTO(ID_PROCEDIMENTO), -- Foreign key to measurement procedure used --
        NO_DESCRIPTION NVARCHAR(3000), -- Text description for the measurement --
        NU_FREQUENCIA_INICIAL DECIMAL(14,6) NULL, -- Initial frequency expressed in MHz --
        NU_FREQUENCIA_FINAL DECIMAL(14,6) NULL, -- End frequency expressed in MHz --
        DT_TEMPO_INICIAL DATETIME2 NULL, -- Initial local time --
        DT_TEMPO_FINAL DATETIME2 NULL, -- End local time --
        NU_DURACAO_AMOSTRA DECIMAL(9,3) NULL, -- Sample duration, usually down to microsecond and up to seconds
        NU_NUMERO_TRACOS int NULL, -- Number of traces within the file and frequency band
        NU_TAMANHO_VETOR int NULL, -- Measurement vector length describing a single trace. Number of bins in the FFT.
        NU_RBW DECIMAL(12,1) NULL, -- RBW in Hz -- ! NEED CONFIRMATION 
        NU_VBW DECIMAL(12,1) NULL, -- VBW in Hz -- ! NEED CONFIRMATION 
        NU_ATENUACAO_GANHO DECIMAL(4,1) NULL, -- Attenuation set in dB -- ! NEED CONFIRMATION IF OVERLAPS WITH GAIN. USED DEVICES 
    );
GO
-- BRIDGE TABLES TO ALLOW FOR N-N RELATIONSHIPS TO THE FACT TABLE
    CREATE TABLE PONTE_MEDICAO_ESPECTRO_EQUIPAMENTO (
        ID_PONTE_EQUIPAMENTO INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to bridge table to equipment dimension table. Allows multiple equipment to be associated with a single measurement and many measurements to be associated with the same equipment -- 
        ID_EQUIPAMENTO INT FOREIGN KEY REFERENCES DIM_MEDICAO_ESPECTRO_EQUIPAMENTO(ID_EQUIPAMENTO), -- Foreign key to equipment table -- 
        ID_MEDICAO_ESPECTRO INT FOREIGN KEY REFERENCES FATO_MEDICAO_ESPECTRO(ID_FATO_MEDICAO_ESPECTRO), -- Foreign key to spectrum measurement table -- 
    );

-- BRIDGE TABLES TO ALLOW FOR N-N RELATIONSHIPS TO THE FACT TABLE
    CREATE TABLE PONTE_MEDICAO_ESPECTRO_EMISSOR (
        ID_PONTE_EMISSOR INT NOT NULL IDENTITY(1,1) PRIMARY KEY, -- Primary Key to bridge table to equipment dimension table. Allows multiple equipment to be associated with a single measurement and many measurements to be associated with the same equipment -- 
        ID_EMISSOR INT FOREIGN KEY REFERENCES DIM_MEDICAO_ESPECTRO_EMISSOR(ID_EMISSOR), -- Foreign key to equipment table -- 
        ID_MEDICAO_ESPECTRO INT FOREIGN KEY REFERENCES FATO_MEDICAO_ESPECTRO(ID_FATO_MEDICAO_ESPECTRO), -- Foreign key to spectrum measurement table -- 
    );
GO