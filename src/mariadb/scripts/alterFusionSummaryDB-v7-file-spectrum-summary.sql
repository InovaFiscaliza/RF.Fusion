/* =====================================================================
   alterFusionSummaryDB-v7-file-spectrum-summary.sql
   - Add FILE_SPECTRUM_SUMMARY to RFFUSION_SUMMARY as a spectrum-aware
     search read model for WebFusion spectrum module
   - Schema: ONE ROW PER REPOSITORY FILE (not per spectrum)
   - Search is spectrum-aware (filters on equipment/site/date/frequency)
   - Results are file-oriented (return files that contain matching spectra)
   - Keep refresh logic inside MariaDB procedures, following the existing
     summary-layer contract instead of coupling appCataloga writers
   - Use atomic build/swap refresh so readers never observe a half-built state
   ===================================================================== */

USE RFFUSION_SUMMARY;

DROP TABLE IF EXISTS FILE_SPECTRUM_SUMMARY;

CREATE TABLE FILE_SPECTRUM_SUMMARY (
    FK_FILE INT NOT NULL PRIMARY KEY,
    NA_FILE VARCHAR(512) NOT NULL,
    NA_PATH VARCHAR(3000) NOT NULL,
    NA_VOLUME VARCHAR(100) NOT NULL,
    NA_EXTENSION VARCHAR(20),
    VL_FILE_SIZE_KB BIGINT,
    -- Aggregates: min/max time and frequency across all spectra in file
    DT_TIME_START DATETIME,
    DT_TIME_END DATETIME,
    NU_FREQ_START DECIMAL(14,6),
    NU_FREQ_END DECIMAL(14,6),
    -- Counts and summary
    NU_SPECTRA_COUNT INT NOT NULL DEFAULT 0,
    NU_LOCALITY_COUNT INT NOT NULL DEFAULT 0,
    -- Delimited locality labels (||) from GROUP_CONCAT
    NA_LOCALITY_LABELS TEXT,
    DT_REFRESHED_AT DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Simple indices for webfusion spectrum searches
    INDEX IX_FILE_SPECTRUM_SUMMARY_TIME (DT_TIME_END, DT_TIME_START),
    INDEX IX_FILE_SPECTRUM_SUMMARY_FREQ (NU_FREQ_START, NU_FREQ_END),
    INDEX IX_FILE_SPECTRUM_SUMMARY_COUNT (NU_SPECTRA_COUNT)
) CHARACTER SET utf8mb4;

DELIMITER $$

DROP PROCEDURE IF EXISTS PRC_REFRESH_FILE_SPECTRUM_SUMMARY_FULL$$
DROP PROCEDURE IF EXISTS PRC_REFRESH_ALL_RFFUSION_SUMMARY_FULL$$

CREATE PROCEDURE PRC_REFRESH_FILE_SPECTRUM_SUMMARY_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;

    -- Mark refresh as started
    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('FILE_SPECTRUM_SUMMARY', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    SET SESSION group_concat_max_len = 16384;

    -- Atomic build/swap pattern
    DROP TABLE IF EXISTS FILE_SPECTRUM_SUMMARY__BUILD;
    DROP TABLE IF EXISTS FILE_SPECTRUM_SUMMARY__OLD;
    CREATE TABLE FILE_SPECTRUM_SUMMARY__BUILD LIKE FILE_SPECTRUM_SUMMARY;

    -- Materialize: for each reposfi file, aggregate its spectrum metadata
    INSERT INTO FILE_SPECTRUM_SUMMARY__BUILD (
        FK_FILE,
        NA_FILE,
        NA_PATH,
        NA_VOLUME,
        NA_EXTENSION,
        VL_FILE_SIZE_KB,
        DT_TIME_START,
        DT_TIME_END,
        NU_FREQ_START,
        NU_FREQ_END,
        NU_SPECTRA_COUNT,
        NU_LOCALITY_COUNT,
        NA_LOCALITY_LABELS,
        DT_REFRESHED_AT
    )
    SELECT
        repos.ID_FILE,
        repos.NA_FILE,
        repos.NA_PATH,
        repos.NA_VOLUME,
        repos.NA_EXTENSION,
        repos.VL_FILE_SIZE_KB,
        MIN(fact.DT_TIME_START) AS DT_TIME_START,
        MAX(fact.DT_TIME_END) AS DT_TIME_END,
        MIN(fact.NU_FREQ_START) AS NU_FREQ_START,
        MAX(fact.NU_FREQ_END) AS NU_FREQ_END,
        COUNT(DISTINCT fact.ID_SPECTRUM) AS NU_SPECTRA_COUNT,
        COUNT(DISTINCT fact.FK_SITE) AS NU_LOCALITY_COUNT,
        GROUP_CONCAT(
            DISTINCT TRIM(
                CONCAT(
                    COALESCE(
                        NULLIF(site.NA_SITE, ''),
                        NULLIF(district.NA_DISTRICT, ''),
                        county.NA_COUNTY,
                        CONCAT('Site ', site.ID_SITE)
                    ),
                    CASE
                        WHEN county.NA_COUNTY IS NOT NULL
                         AND (
                            site.NA_SITE IS NULL
                            OR site.NA_SITE = ''
                            OR NOT (
                                COALESCE(CONVERT(site.NA_SITE USING utf8mb4) COLLATE utf8mb4_unicode_ci, '')
                                <=>
                                COALESCE(CONVERT(county.NA_COUNTY USING utf8mb4) COLLATE utf8mb4_unicode_ci, '')
                            )
                         )
                        THEN CONCAT(
                            ' (',
                            county.NA_COUNTY,
                            CASE
                                WHEN state.LC_STATE IS NOT NULL THEN CONCAT('/', state.LC_STATE)
                                ELSE ''
                            END,
                            ')'
                        )
                        WHEN state.LC_STATE IS NOT NULL THEN CONCAT('/', state.LC_STATE)
                        ELSE ''
                    END,
                    ''
                )
            )
            ORDER BY TRIM(
                CONCAT(
                    COALESCE(
                        NULLIF(site.NA_SITE, ''),
                        NULLIF(district.NA_DISTRICT, ''),
                        county.NA_COUNTY,
                        CONCAT('Site ', site.ID_SITE)
                    ),
                    CASE
                        WHEN county.NA_COUNTY IS NOT NULL
                         AND (
                            site.NA_SITE IS NULL
                            OR site.NA_SITE = ''
                            OR NOT (
                                COALESCE(CONVERT(site.NA_SITE USING utf8mb4) COLLATE utf8mb4_unicode_ci, '')
                                <=>
                                COALESCE(CONVERT(county.NA_COUNTY USING utf8mb4) COLLATE utf8mb4_unicode_ci, '')
                            )
                         )
                        THEN CONCAT(
                            ' (',
                            county.NA_COUNTY,
                            CASE
                                WHEN state.LC_STATE IS NOT NULL THEN CONCAT('/', state.LC_STATE)
                                ELSE ''
                            END,
                            ')'
                        )
                        WHEN state.LC_STATE IS NOT NULL THEN CONCAT('/', state.LC_STATE)
                        ELSE ''
                    END,
                    ''
                )
            ) SEPARATOR '||'
        ) AS NA_LOCALITY_LABELS,
        UTC_TIMESTAMP()
    FROM RFDATA.DIM_SPECTRUM_FILE repos
    JOIN RFDATA.BRIDGE_SPECTRUM_FILE bridge
        ON bridge.FK_FILE = repos.ID_FILE
    JOIN RFDATA.FACT_SPECTRUM fact
        ON fact.ID_SPECTRUM = bridge.FK_SPECTRUM
    JOIN RFDATA.DIM_SPECTRUM_SITE site
        ON site.ID_SITE = fact.FK_SITE
    LEFT JOIN RFDATA.DIM_SITE_DISTRICT district
        ON district.ID_DISTRICT = site.FK_DISTRICT
    LEFT JOIN RFDATA.DIM_SITE_COUNTY county
        ON county.ID_COUNTY = site.FK_COUNTY
    LEFT JOIN RFDATA.DIM_SITE_STATE state
        ON state.ID_STATE = site.FK_STATE
    WHERE repos.NA_VOLUME = 'reposfi'
    GROUP BY
        repos.ID_FILE,
        repos.NA_FILE,
        repos.NA_PATH,
        repos.NA_VOLUME,
        repos.NA_EXTENSION,
        repos.VL_FILE_SIZE_KB;

    SET v_row_count = (SELECT COUNT(*) FROM FILE_SPECTRUM_SUMMARY__BUILD);

    RENAME TABLE
        FILE_SPECTRUM_SUMMARY TO FILE_SPECTRUM_SUMMARY__OLD,
        FILE_SPECTRUM_SUMMARY__BUILD TO FILE_SPECTRUM_SUMMARY;
    DROP TABLE FILE_SPECTRUM_SUMMARY__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'max_spectrum_id=',
            COALESCE((SELECT MAX(ID_SPECTRUM) FROM RFDATA.FACT_SPECTRUM), 0),
            ';max_file_id=',
            COALESCE((SELECT MAX(ID_FILE) FROM RFDATA.DIM_SPECTRUM_FILE), 0)
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'FILE_SPECTRUM_SUMMARY';

    INSERT INTO SUMMARY_REFRESH_LOG (
        NA_OBJECT_NAME,
        DT_STARTED_AT,
        DT_FINISHED_AT,
        IS_SUCCESS,
        NU_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES (
        'FILE_SPECTRUM_SUMMARY',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'max_spectrum_id=',
            COALESCE((SELECT MAX(ID_SPECTRUM) FROM RFDATA.FACT_SPECTRUM), 0),
            ';max_file_id=',
            COALESCE((SELECT MAX(ID_FILE) FROM RFDATA.DIM_SPECTRUM_FILE), 0)
        ),
        NULL
    );
END$$

CREATE PROCEDURE PRC_REFRESH_ALL_RFFUSION_SUMMARY_FULL()
BEGIN
    CALL PRC_REFRESH_HOST_EQUIPMENT_LINK_FULL();
    CALL PRC_REFRESH_SITE_EQUIPMENT_OBS_SUMMARY_FULL();
    CALL PRC_REFRESH_HOST_LOCATION_SUMMARY_FULL();
    CALL PRC_REFRESH_MAP_SITE_STATION_SUMMARY_FULL();
    CALL PRC_REFRESH_MAP_SITE_SUMMARY_FULL();
    CALL PRC_REFRESH_HOST_MONTHLY_METRIC_FULL();
    CALL PRC_REFRESH_HOST_ERROR_SUMMARY_FULL();
    CALL PRC_REFRESH_SERVER_ERROR_SUMMARY_FULL();
    CALL PRC_REFRESH_HOST_CURRENT_SNAPSHOT_FULL();
    CALL PRC_REFRESH_SERVER_CURRENT_SUMMARY_FULL();
    CALL PRC_REFRESH_FILE_SPECTRUM_SUMMARY_FULL();
END$$

DELIMITER ;