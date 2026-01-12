-- =====================================================================
-- fix.sql - limpeza completa dos dados poluídos da DIM_SITE_COUNTY
-- =====================================================================

USE RFDATA;

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

-- Verificação opcional: listar registros com caracteres inválidos remanescentes
-- SELECT ID_COUNTY, NA_COUNTY, HEX(NA_COUNTY)
-- FROM DIM_SITE_COUNTY
-- WHERE HEX(NA_COUNTY) LIKE '%0D%' OR HEX(NA_COUNTY) LIKE '%7C%';