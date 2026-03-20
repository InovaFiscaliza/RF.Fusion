USE RFDATA;

/*
Backfill seguro para DIM_SPECTRUM_SITE.

Achados validados na base atual:
- 188/188 sites com FK_TYPE nulo
- 188/188 sites com NA_SITE nulo
- 8/188 sites com FK_DISTRICT nulo

Regras aplicadas aqui:
1. NA_SITE:
   - usa o distrito quando existir
   - senão usa o município
2. FK_DISTRICT:
   - só preenche quando o município possui exatamente um distrito cadastrado

FK_TYPE nao e atualizado automaticamente neste patch porque:
- DIM_SITE_TYPE esta vazia na base atual
- o codigo de ingestao nao popula esse campo
- o historico do projeto sugere que SITE_TYPE depende de regra propria
  baseada no comportamento do GPS, e nao apenas no tipo do equipamento
*/

START TRANSACTION;

/* ---------------------------------------------------------------
   Preview rapido antes da correcao
   --------------------------------------------------------------- */
SELECT
    COUNT(*) AS total_sites,
    SUM(FK_TYPE IS NULL) AS fk_type_null,
    SUM(NA_SITE IS NULL OR NA_SITE = '') AS na_site_blank,
    SUM(FK_DISTRICT IS NULL) AS fk_district_null
FROM DIM_SPECTRUM_SITE;

/* ---------------------------------------------------------------
   1. Preenche FK_DISTRICT quando o municipio tiver distrito unico
   --------------------------------------------------------------- */
UPDATE DIM_SPECTRUM_SITE s
JOIN (
    SELECT
        FK_COUNTY,
        MIN(ID_DISTRICT) AS ID_DISTRICT
    FROM DIM_SITE_DISTRICT
    GROUP BY FK_COUNTY
    HAVING COUNT(*) = 1
) d_unique
    ON d_unique.FK_COUNTY = s.FK_COUNTY
SET s.FK_DISTRICT = d_unique.ID_DISTRICT
WHERE s.FK_DISTRICT IS NULL;

/* ---------------------------------------------------------------
   1b. Completa casos legados restantes com distritos curados
       a partir das coordenadas ja persistidas
   --------------------------------------------------------------- */
INSERT INTO DIM_SITE_DISTRICT (FK_COUNTY, NA_DISTRICT)
SELECT src.FK_COUNTY, src.NA_DISTRICT
FROM (
    SELECT 2400208 AS FK_COUNTY, 'Centro' AS NA_DISTRICT
    UNION ALL SELECT 3109303, 'Buritis'
    UNION ALL SELECT 2407104, 'Macaíba'
    UNION ALL SELECT 1600501, 'Oiapoque'
    UNION ALL SELECT 1600501, 'Santo Antonio'
    UNION ALL SELECT 1600279, 'Vila Coração de Jesus'
) src
LEFT JOIN DIM_SITE_DISTRICT d
    ON d.FK_COUNTY = src.FK_COUNTY
   AND d.NA_DISTRICT = src.NA_DISTRICT
WHERE d.ID_DISTRICT IS NULL;

UPDATE DIM_SPECTRUM_SITE s
JOIN (
    SELECT 116 AS ID_SITE, 2400208 AS FK_COUNTY, 'Centro' AS NA_DISTRICT
    UNION ALL SELECT 135, 3109303, 'Buritis'
    UNION ALL SELECT 167, 2407104, 'Macaíba'
    UNION ALL SELECT 184, 1600501, 'Oiapoque'
    UNION ALL SELECT 185, 1600501, 'Santo Antonio'
    UNION ALL SELECT 186, 1600279, 'Vila Coração de Jesus'
    UNION ALL SELECT 187, 1600501, 'Oiapoque'
) legacy_fix
    ON legacy_fix.ID_SITE = s.ID_SITE
JOIN DIM_SITE_DISTRICT d
    ON d.FK_COUNTY = legacy_fix.FK_COUNTY
   AND d.NA_DISTRICT = legacy_fix.NA_DISTRICT
SET s.FK_DISTRICT = d.ID_DISTRICT
WHERE s.FK_DISTRICT IS NULL;

/* ---------------------------------------------------------------
   2. Preenche NA_SITE a partir da melhor granularidade disponivel
   --------------------------------------------------------------- */
UPDATE DIM_SPECTRUM_SITE s
LEFT JOIN DIM_SITE_DISTRICT d
    ON d.ID_DISTRICT = s.FK_DISTRICT
LEFT JOIN DIM_SITE_COUNTY c
    ON c.ID_COUNTY = s.FK_COUNTY
SET s.NA_SITE = COALESCE(NULLIF(d.NA_DISTRICT, ''), c.NA_COUNTY)
WHERE s.NA_SITE IS NULL
   OR s.NA_SITE = '';

/* ---------------------------------------------------------------
   Preview apos a correcao
   --------------------------------------------------------------- */
SELECT
    COUNT(*) AS total_sites,
    SUM(FK_TYPE IS NULL) AS fk_type_null,
    SUM(NA_SITE IS NULL OR NA_SITE = '') AS na_site_blank,
    SUM(FK_DISTRICT IS NULL) AS fk_district_null
FROM DIM_SPECTRUM_SITE;

/* ---------------------------------------------------------------
   Pendencias intencionais apos o patch
   --------------------------------------------------------------- */
SELECT
    s.ID_SITE,
    s.FK_DISTRICT,
    s.FK_COUNTY,
    c.NA_COUNTY,
    s.FK_STATE,
    st.NA_STATE,
    s.FK_TYPE,
    s.NA_SITE
FROM DIM_SPECTRUM_SITE s
LEFT JOIN DIM_SITE_COUNTY c
    ON c.ID_COUNTY = s.FK_COUNTY
LEFT JOIN DIM_SITE_STATE st
    ON st.ID_STATE = s.FK_STATE
WHERE s.FK_DISTRICT IS NULL
   OR s.FK_TYPE IS NULL
ORDER BY s.ID_SITE;

/* ---------------------------------------------------------------
   FK_TYPE: diagnostico apenas
   --------------------------------------------------------------- */
SELECT *
FROM DIM_SITE_TYPE
ORDER BY ID_TYPE;

COMMIT;
