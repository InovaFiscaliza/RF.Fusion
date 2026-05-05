USE RFFUSION_SUMMARY;

DELIMITER $$

DROP PROCEDURE IF EXISTS PRC_REFRESH_SITE_EQUIPMENT_OBS_SUMMARY_FULL$$
DROP PROCEDURE IF EXISTS PRC_REFRESH_HOST_LOCATION_SUMMARY_FULL$$
DROP PROCEDURE IF EXISTS PRC_REFRESH_MAP_SITE_STATION_SUMMARY_FULL$$
DROP PROCEDURE IF EXISTS PRC_REFRESH_MAP_SITE_SUMMARY_FULL$$
DROP PROCEDURE IF EXISTS PRC_REFRESH_HOST_ERROR_SUMMARY_FULL$$
DROP PROCEDURE IF EXISTS PRC_REFRESH_SERVER_ERROR_SUMMARY_FULL$$
DROP PROCEDURE IF EXISTS PRC_REFRESH_HOST_CURRENT_SNAPSHOT_FULL$$
DROP PROCEDURE IF EXISTS PRC_REFRESH_SERVER_CURRENT_SUMMARY_FULL$$

CREATE PROCEDURE PRC_REFRESH_SITE_EQUIPMENT_OBS_SUMMARY_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('SITE_EQUIPMENT_OBS_SUMMARY', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    DROP TABLE IF EXISTS SITE_EQUIPMENT_OBS_SUMMARY__BUILD;
    DROP TABLE IF EXISTS SITE_EQUIPMENT_OBS_SUMMARY__OLD;
    CREATE TABLE SITE_EQUIPMENT_OBS_SUMMARY__BUILD LIKE SITE_EQUIPMENT_OBS_SUMMARY;

    INSERT INTO SITE_EQUIPMENT_OBS_SUMMARY__BUILD (
        FK_SITE,
        FK_EQUIPMENT,
        NA_SITE_NAME,
        NA_SITE_LABEL,
        NA_COUNTY_NAME,
        NA_DISTRICT_NAME,
        ID_STATE,
        NA_STATE_NAME,
        NA_STATE_CODE,
        VL_LATITUDE,
        VL_LONGITUDE,
        VL_ALTITUDE,
        NU_GNSS_MEASUREMENTS,
        NA_EQUIPMENT,
        DT_FIRST_SEEN_AT,
        DT_LAST_SEEN_AT,
        NU_SPECTRUM_COUNT,
        ID_LAST_SPECTRUM,
        IS_CURRENT_LOCATION,
        DT_REFRESHED_AT
    )
    SELECT
        ranked.FK_SITE,
        ranked.FK_EQUIPMENT,
        ranked.NA_SITE_NAME,
        ranked.NA_SITE_LABEL,
        ranked.NA_COUNTY_NAME,
        ranked.NA_DISTRICT_NAME,
        ranked.ID_STATE,
        ranked.NA_STATE_NAME,
        ranked.NA_STATE_CODE,
        ranked.VL_LATITUDE,
        ranked.VL_LONGITUDE,
        ranked.VL_ALTITUDE,
        ranked.NU_GNSS_MEASUREMENTS,
        ranked.NA_EQUIPMENT,
        ranked.DT_FIRST_SEEN_AT,
        ranked.DT_LAST_SEEN_AT,
        ranked.NU_SPECTRUM_COUNT,
        ranked.ID_LAST_SPECTRUM,
        CASE
            WHEN ranked.RN_FOR_EQUIPMENT = 1 THEN 1
            ELSE 0
        END,
        UTC_TIMESTAMP()
    FROM (
        SELECT
            base.*,
            ROW_NUMBER() OVER (
                PARTITION BY base.FK_EQUIPMENT
                ORDER BY COALESCE(base.DT_LAST_SEEN_AT, base.DT_FIRST_SEEN_AT) DESC,
                         COALESCE(base.DT_FIRST_SEEN_AT, base.DT_LAST_SEEN_AT) DESC,
                         base.FK_SITE DESC
            ) AS RN_FOR_EQUIPMENT
        FROM (
            SELECT
                f.FK_SITE,
                f.FK_EQUIPMENT,
                s.NA_SITE AS NA_SITE_NAME,
                COALESCE(NULLIF(s.NA_SITE, ''), CONCAT('Site ', s.ID_SITE)) AS NA_SITE_LABEL,
                c.NA_COUNTY AS NA_COUNTY_NAME,
                d.NA_DISTRICT AS NA_DISTRICT_NAME,
                st.ID_STATE,
                st.NA_STATE AS NA_STATE_NAME,
                st.LC_STATE AS NA_STATE_CODE,
                ST_Y(s.GEO_POINT) AS VL_LATITUDE,
                ST_X(s.GEO_POINT) AS VL_LONGITUDE,
                s.NU_ALTITUDE AS VL_ALTITUDE,
                s.NU_GNSS_MEASUREMENTS,
                e.NA_EQUIPMENT,
                MIN(f.DT_TIME_START) AS DT_FIRST_SEEN_AT,
                MAX(f.DT_TIME_END) AS DT_LAST_SEEN_AT,
                COUNT(*) AS NU_SPECTRUM_COUNT,
                MAX(f.ID_SPECTRUM) AS ID_LAST_SPECTRUM
            FROM RFDATA.FACT_SPECTRUM f
            JOIN RFDATA.DIM_SPECTRUM_EQUIPMENT e
              ON e.ID_EQUIPMENT = f.FK_EQUIPMENT
            JOIN RFDATA.DIM_SPECTRUM_SITE s
              ON s.ID_SITE = f.FK_SITE
            LEFT JOIN RFDATA.DIM_SITE_COUNTY c
              ON c.ID_COUNTY = s.FK_COUNTY
            LEFT JOIN RFDATA.DIM_SITE_DISTRICT d
              ON d.ID_DISTRICT = s.FK_DISTRICT
            LEFT JOIN RFDATA.DIM_SITE_STATE st
              ON st.ID_STATE = s.FK_STATE
            GROUP BY
                f.FK_SITE,
                f.FK_EQUIPMENT,
                s.NA_SITE,
                c.NA_COUNTY,
                d.NA_DISTRICT,
                st.ID_STATE,
                st.NA_STATE,
                st.LC_STATE,
                ST_Y(s.GEO_POINT),
                ST_X(s.GEO_POINT),
                s.NU_ALTITUDE,
                s.NU_GNSS_MEASUREMENTS,
                e.NA_EQUIPMENT
        ) base
    ) ranked;

    SET v_row_count = (SELECT COUNT(*) FROM SITE_EQUIPMENT_OBS_SUMMARY__BUILD);

    RENAME TABLE
        SITE_EQUIPMENT_OBS_SUMMARY TO SITE_EQUIPMENT_OBS_SUMMARY__OLD,
        SITE_EQUIPMENT_OBS_SUMMARY__BUILD TO SITE_EQUIPMENT_OBS_SUMMARY;
    DROP TABLE SITE_EQUIPMENT_OBS_SUMMARY__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'max_spectrum_id=',
            COALESCE((SELECT MAX(ID_SPECTRUM) FROM RFDATA.FACT_SPECTRUM), 0)
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'SITE_EQUIPMENT_OBS_SUMMARY';

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
        'SITE_EQUIPMENT_OBS_SUMMARY',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'max_spectrum_id=',
            COALESCE((SELECT MAX(ID_SPECTRUM) FROM RFDATA.FACT_SPECTRUM), 0)
        ),
        NULL
    );
END$$

CREATE PROCEDURE PRC_REFRESH_HOST_LOCATION_SUMMARY_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('HOST_LOCATION_SUMMARY', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    DROP TABLE IF EXISTS HOST_LOCATION_SUMMARY__BUILD;
    DROP TABLE IF EXISTS HOST_LOCATION_SUMMARY__OLD;
    CREATE TABLE HOST_LOCATION_SUMMARY__BUILD LIKE HOST_LOCATION_SUMMARY;

    INSERT INTO HOST_LOCATION_SUMMARY__BUILD (
        FK_HOST,
        FK_SITE,
        NA_HOST_NAME,
        NA_LOCALITY_LABEL,
        NA_SITE_LABEL,
        NA_COUNTY_NAME,
        NA_DISTRICT_NAME,
        ID_STATE,
        NA_STATE_NAME,
        NA_STATE_CODE,
        VL_LATITUDE,
        VL_LONGITUDE,
        VL_ALTITUDE,
        DT_FIRST_SEEN_AT,
        DT_LAST_SEEN_AT,
        NU_SPECTRUM_COUNT,
        NU_MATCHED_EQUIPMENT_TOTAL,
        IS_CURRENT_LOCATION,
        IS_OFFLINE_SNAPSHOT,
        VL_MAX_MATCH_CONFIDENCE,
        DT_REFRESHED_AT
    )
    SELECT
        link.FK_HOST,
        obs.FK_SITE,
        host.NA_HOST_NAME,
        TRIM(
            CONCAT(
                COALESCE(NULLIF(obs.NA_SITE_NAME, ''), NULLIF(obs.NA_DISTRICT_NAME, ''), obs.NA_COUNTY_NAME, CONCAT('Site ', obs.FK_SITE)),
                CASE
                    WHEN obs.NA_COUNTY_NAME IS NOT NULL
                     AND (
                            obs.NA_SITE_NAME IS NULL
                         OR obs.NA_SITE_NAME = ''
                         OR LOWER(obs.NA_SITE_NAME) <> LOWER(obs.NA_COUNTY_NAME)
                     )
                    THEN CONCAT(' · ', obs.NA_COUNTY_NAME)
                    ELSE ''
                END,
                CASE
                    WHEN obs.NA_STATE_CODE IS NOT NULL THEN CONCAT('/', obs.NA_STATE_CODE)
                    ELSE ''
                END
            )
        ) AS NA_LOCALITY_LABEL,
        obs.NA_SITE_LABEL,
        obs.NA_COUNTY_NAME,
        obs.NA_DISTRICT_NAME,
        obs.ID_STATE,
        obs.NA_STATE_NAME,
        obs.NA_STATE_CODE,
        obs.VL_LATITUDE,
        obs.VL_LONGITUDE,
        obs.VL_ALTITUDE,
        MIN(obs.DT_FIRST_SEEN_AT),
        MAX(obs.DT_LAST_SEEN_AT),
        SUM(obs.NU_SPECTRUM_COUNT),
        COUNT(DISTINCT obs.FK_EQUIPMENT),
        MAX(obs.IS_CURRENT_LOCATION),
        MAX(COALESCE(host.IS_OFFLINE, 0)),
        MAX(link.VL_MATCH_CONFIDENCE),
        UTC_TIMESTAMP()
    FROM SITE_EQUIPMENT_OBS_SUMMARY obs
    JOIN HOST_EQUIPMENT_LINK link
      ON link.FK_EQUIPMENT = obs.FK_EQUIPMENT
     AND link.IS_ACTIVE = 1
     AND link.IS_PRIMARY_LINK = 1
    JOIN BPDATA.HOST host
      ON host.ID_HOST = link.FK_HOST
    GROUP BY
        link.FK_HOST,
        obs.FK_SITE,
        host.NA_HOST_NAME,
        obs.NA_SITE_NAME,
        obs.NA_SITE_LABEL,
        obs.NA_COUNTY_NAME,
        obs.NA_DISTRICT_NAME,
        obs.ID_STATE,
        obs.NA_STATE_NAME,
        obs.NA_STATE_CODE,
        obs.VL_LATITUDE,
        obs.VL_LONGITUDE,
        obs.VL_ALTITUDE;

    SET v_row_count = (SELECT COUNT(*) FROM HOST_LOCATION_SUMMARY__BUILD);

    RENAME TABLE
        HOST_LOCATION_SUMMARY TO HOST_LOCATION_SUMMARY__OLD,
        HOST_LOCATION_SUMMARY__BUILD TO HOST_LOCATION_SUMMARY;
    DROP TABLE HOST_LOCATION_SUMMARY__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'links=',
            (SELECT COUNT(*) FROM HOST_EQUIPMENT_LINK WHERE IS_ACTIVE = 1),
            ';site_obs=',
            (SELECT COUNT(*) FROM SITE_EQUIPMENT_OBS_SUMMARY)
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'HOST_LOCATION_SUMMARY';

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
        'HOST_LOCATION_SUMMARY',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'links=',
            (SELECT COUNT(*) FROM HOST_EQUIPMENT_LINK WHERE IS_ACTIVE = 1),
            ';site_obs=',
            (SELECT COUNT(*) FROM SITE_EQUIPMENT_OBS_SUMMARY)
        ),
        NULL
    );
END$$

CREATE PROCEDURE PRC_REFRESH_MAP_SITE_STATION_SUMMARY_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('MAP_SITE_STATION_SUMMARY', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    DROP TABLE IF EXISTS MAP_SITE_STATION_SUMMARY__BUILD;
    DROP TABLE IF EXISTS MAP_SITE_STATION_SUMMARY__OLD;
    CREATE TABLE MAP_SITE_STATION_SUMMARY__BUILD LIKE MAP_SITE_STATION_SUMMARY;

    INSERT INTO MAP_SITE_STATION_SUMMARY__BUILD (
        FK_SITE,
        FK_EQUIPMENT,
        FK_HOST,
        NA_SITE_LABEL,
        NA_EQUIPMENT,
        NA_HOST_NAME,
        IS_OFFLINE,
        IS_CURRENT_LOCATION,
        NA_MAP_STATE,
        NU_STATE_PRIORITY,
        DT_FIRST_SEEN_AT,
        DT_LAST_SEEN_AT,
        NU_SPECTRUM_COUNT,
        NA_MATCH_TYPE,
        VL_MATCH_CONFIDENCE,
        DT_REFRESHED_AT
    )
    SELECT
        obs.FK_SITE,
        obs.FK_EQUIPMENT,
        link.FK_HOST,
        obs.NA_SITE_LABEL,
        obs.NA_EQUIPMENT,
        host.NA_HOST_NAME,
        host.IS_OFFLINE,
        obs.IS_CURRENT_LOCATION,
        FN_SUMMARY_MAP_STATE(
            CASE WHEN link.FK_HOST IS NULL THEN 0 ELSE 1 END,
            host.IS_OFFLINE,
            obs.IS_CURRENT_LOCATION
        ) AS NA_MAP_STATE,
        FN_SUMMARY_MAP_STATE_PRIORITY(
            FN_SUMMARY_MAP_STATE(
                CASE WHEN link.FK_HOST IS NULL THEN 0 ELSE 1 END,
                host.IS_OFFLINE,
                obs.IS_CURRENT_LOCATION
            )
        ) AS NU_STATE_PRIORITY,
        obs.DT_FIRST_SEEN_AT,
        obs.DT_LAST_SEEN_AT,
        obs.NU_SPECTRUM_COUNT,
        link.NA_MATCH_TYPE,
        link.VL_MATCH_CONFIDENCE,
        UTC_TIMESTAMP()
    FROM SITE_EQUIPMENT_OBS_SUMMARY obs
    LEFT JOIN HOST_EQUIPMENT_LINK link
      ON link.FK_EQUIPMENT = obs.FK_EQUIPMENT
     AND link.IS_ACTIVE = 1
     AND link.IS_PRIMARY_LINK = 1
    LEFT JOIN BPDATA.HOST host
      ON host.ID_HOST = link.FK_HOST;

    SET v_row_count = (SELECT COUNT(*) FROM MAP_SITE_STATION_SUMMARY__BUILD);

    RENAME TABLE
        MAP_SITE_STATION_SUMMARY TO MAP_SITE_STATION_SUMMARY__OLD,
        MAP_SITE_STATION_SUMMARY__BUILD TO MAP_SITE_STATION_SUMMARY;
    DROP TABLE MAP_SITE_STATION_SUMMARY__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'site_obs=',
            (SELECT COUNT(*) FROM SITE_EQUIPMENT_OBS_SUMMARY),
            ';links=',
            (SELECT COUNT(*) FROM HOST_EQUIPMENT_LINK WHERE IS_ACTIVE = 1 AND IS_PRIMARY_LINK = 1)
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'MAP_SITE_STATION_SUMMARY';

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
        'MAP_SITE_STATION_SUMMARY',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'site_obs=',
            (SELECT COUNT(*) FROM SITE_EQUIPMENT_OBS_SUMMARY),
            ';links=',
            (SELECT COUNT(*) FROM HOST_EQUIPMENT_LINK WHERE IS_ACTIVE = 1 AND IS_PRIMARY_LINK = 1)
        ),
        NULL
    );
END$$

CREATE PROCEDURE PRC_REFRESH_MAP_SITE_SUMMARY_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('MAP_SITE_SUMMARY', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    DROP TABLE IF EXISTS MAP_SITE_SUMMARY__BUILD;
    DROP TABLE IF EXISTS MAP_SITE_SUMMARY__OLD;
    CREATE TABLE MAP_SITE_SUMMARY__BUILD LIKE MAP_SITE_SUMMARY;

    INSERT INTO MAP_SITE_SUMMARY__BUILD (
        FK_SITE,
        NA_SITE_LABEL,
        NA_COUNTY_NAME,
        NA_DISTRICT_NAME,
        ID_STATE,
        NA_STATE_NAME,
        NA_STATE_CODE,
        VL_LATITUDE,
        VL_LONGITUDE,
        VL_ALTITUDE,
        NU_GNSS_MEASUREMENTS,
        NA_MARKER_STATE,
        NU_STATION_COUNT,
        NU_KNOWN_HOST_COUNT,
        NU_ONLINE_CURRENT_COUNT,
        NU_ONLINE_PREVIOUS_COUNT,
        NU_OFFLINE_CURRENT_COUNT,
        NU_OFFLINE_PREVIOUS_COUNT,
        NU_NO_HOST_COUNT,
        HAS_ONLINE_STATION,
        HAS_ONLINE_HOST,
        HAS_KNOWN_HOST,
        DT_REFRESHED_AT
    )
    SELECT
        s.ID_SITE,
        COALESCE(NULLIF(s.NA_SITE, ''), CONCAT('Site ', s.ID_SITE)) AS NA_SITE_LABEL,
        c.NA_COUNTY,
        d.NA_DISTRICT,
        st.ID_STATE,
        st.NA_STATE,
        st.LC_STATE,
        ST_Y(s.GEO_POINT) AS VL_LATITUDE,
        ST_X(s.GEO_POINT) AS VL_LONGITUDE,
        s.NU_ALTITUDE,
        s.NU_GNSS_MEASUREMENTS,
        CASE
            WHEN agg.MIN_PRIORITY = 0 THEN 'online_current'
            WHEN agg.MIN_PRIORITY = 1 THEN 'online_previous'
            WHEN agg.MIN_PRIORITY = 2 THEN 'offline_current'
            WHEN agg.MIN_PRIORITY = 3 THEN 'offline_previous'
            ELSE 'no_host'
        END AS NA_MARKER_STATE,
        COALESCE(agg.NU_STATION_COUNT, 0),
        COALESCE(agg.NU_KNOWN_HOST_COUNT, 0),
        COALESCE(agg.NU_ONLINE_CURRENT_COUNT, 0),
        COALESCE(agg.NU_ONLINE_PREVIOUS_COUNT, 0),
        COALESCE(agg.NU_OFFLINE_CURRENT_COUNT, 0),
        COALESCE(agg.NU_OFFLINE_PREVIOUS_COUNT, 0),
        COALESCE(agg.NU_NO_HOST_COUNT, 0),
        CASE WHEN COALESCE(agg.NU_ONLINE_CURRENT_COUNT, 0) + COALESCE(agg.NU_ONLINE_PREVIOUS_COUNT, 0) > 0 THEN 1 ELSE 0 END,
        CASE WHEN COALESCE(agg.NU_ONLINE_CURRENT_COUNT, 0) + COALESCE(agg.NU_ONLINE_PREVIOUS_COUNT, 0) > 0 THEN 1 ELSE 0 END,
        CASE WHEN COALESCE(agg.NU_KNOWN_HOST_COUNT, 0) > 0 THEN 1 ELSE 0 END,
        UTC_TIMESTAMP()
    FROM RFDATA.DIM_SPECTRUM_SITE s
    LEFT JOIN RFDATA.DIM_SITE_COUNTY c
      ON c.ID_COUNTY = s.FK_COUNTY
    LEFT JOIN RFDATA.DIM_SITE_DISTRICT d
      ON d.ID_DISTRICT = s.FK_DISTRICT
    LEFT JOIN RFDATA.DIM_SITE_STATE st
      ON st.ID_STATE = s.FK_STATE
    LEFT JOIN (
        SELECT
            ms.FK_SITE,
            COUNT(*) AS NU_STATION_COUNT,
            SUM(CASE WHEN ms.FK_HOST IS NOT NULL THEN 1 ELSE 0 END) AS NU_KNOWN_HOST_COUNT,
            SUM(CASE WHEN ms.NA_MAP_STATE = 'online_current' THEN 1 ELSE 0 END) AS NU_ONLINE_CURRENT_COUNT,
            SUM(CASE WHEN ms.NA_MAP_STATE = 'online_previous' THEN 1 ELSE 0 END) AS NU_ONLINE_PREVIOUS_COUNT,
            SUM(CASE WHEN ms.NA_MAP_STATE = 'offline_current' THEN 1 ELSE 0 END) AS NU_OFFLINE_CURRENT_COUNT,
            SUM(CASE WHEN ms.NA_MAP_STATE = 'offline_previous' THEN 1 ELSE 0 END) AS NU_OFFLINE_PREVIOUS_COUNT,
            SUM(CASE WHEN ms.NA_MAP_STATE = 'no_host' THEN 1 ELSE 0 END) AS NU_NO_HOST_COUNT,
            MIN(ms.NU_STATE_PRIORITY) AS MIN_PRIORITY
        FROM MAP_SITE_STATION_SUMMARY ms
        GROUP BY ms.FK_SITE
    ) agg
      ON agg.FK_SITE = s.ID_SITE
    WHERE s.GEO_POINT IS NOT NULL;

    SET v_row_count = (SELECT COUNT(*) FROM MAP_SITE_SUMMARY__BUILD);

    RENAME TABLE
        MAP_SITE_SUMMARY TO MAP_SITE_SUMMARY__OLD,
        MAP_SITE_SUMMARY__BUILD TO MAP_SITE_SUMMARY;
    DROP TABLE MAP_SITE_SUMMARY__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'sites=',
            (SELECT COUNT(*) FROM RFDATA.DIM_SPECTRUM_SITE WHERE GEO_POINT IS NOT NULL),
            ';map_site_station=',
            (SELECT COUNT(*) FROM MAP_SITE_STATION_SUMMARY)
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'MAP_SITE_SUMMARY';

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
        'MAP_SITE_SUMMARY',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'sites=',
            (SELECT COUNT(*) FROM RFDATA.DIM_SPECTRUM_SITE WHERE GEO_POINT IS NOT NULL),
            ';map_site_station=',
            (SELECT COUNT(*) FROM MAP_SITE_STATION_SUMMARY)
        ),
        NULL
    );
END$$

CREATE PROCEDURE PRC_REFRESH_HOST_ERROR_SUMMARY_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('HOST_ERROR_SUMMARY', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    DROP TABLE IF EXISTS HOST_ERROR_SUMMARY__BUILD;
    DROP TABLE IF EXISTS HOST_ERROR_SUMMARY__OLD;
    CREATE TABLE HOST_ERROR_SUMMARY__BUILD LIKE HOST_ERROR_SUMMARY;

    INSERT INTO HOST_ERROR_SUMMARY__BUILD (
        FK_HOST,
        NA_HOST_NAME,
        NA_ERROR_SCOPE,
        NA_ERROR_DOMAIN,
        NA_ERROR_STAGE,
        NA_ERROR_CODE,
        NA_ERROR_SUMMARY_HASH,
        NA_ERROR_SUMMARY,
        NU_ERROR_COUNT,
        DT_FIRST_SEEN_AT,
        DT_LAST_SEEN_AT,
        NA_LAST_SOURCE_TABLE,
        ID_LAST_SOURCE_ROW,
        NA_LAST_ERROR_DETAIL,
        NA_LAST_RAW_MESSAGE,
        DT_REFRESHED_AT
    )
    SELECT
        grouped.FK_HOST,
        grouped.NA_HOST_NAME,
        grouped.NA_ERROR_SCOPE,
        grouped.NA_ERROR_DOMAIN,
        grouped.NA_ERROR_STAGE,
        grouped.NA_ERROR_CODE,
        grouped.NA_ERROR_SUMMARY_HASH,
        grouped.NA_ERROR_SUMMARY,
        grouped.NU_ERROR_COUNT,
        grouped.DT_FIRST_SEEN_AT,
        grouped.DT_LAST_SEEN_AT,
        last_row.NA_SOURCE_TABLE,
        last_row.ID_SOURCE_ROW,
        last_row.NA_ERROR_DETAIL,
        last_row.NA_RAW_MESSAGE,
        UTC_TIMESTAMP()
    FROM (
        SELECT
            e.FK_HOST,
            COALESCE(e.NA_HOST_NAME, CONCAT('Host ', e.FK_HOST)) AS NA_HOST_NAME,
            e.NA_ERROR_SCOPE,
            e.NA_ERROR_DOMAIN,
            e.NA_ERROR_STAGE,
            e.NA_ERROR_CODE,
            e.NA_ERROR_SUMMARY_HASH,
            e.NA_ERROR_SUMMARY,
            COUNT(*) AS NU_ERROR_COUNT,
            MIN(e.DT_EVENT_AT) AS DT_FIRST_SEEN_AT,
            MAX(e.DT_EVENT_AT) AS DT_LAST_SEEN_AT
        FROM VW_ERROR_EVENT_CANONICAL e
        WHERE e.FK_HOST IS NOT NULL
        GROUP BY
            e.FK_HOST,
            COALESCE(e.NA_HOST_NAME, CONCAT('Host ', e.FK_HOST)),
            e.NA_ERROR_SCOPE,
            e.NA_ERROR_DOMAIN,
            e.NA_ERROR_STAGE,
            e.NA_ERROR_CODE,
            e.NA_ERROR_SUMMARY_HASH,
            e.NA_ERROR_SUMMARY
    ) grouped
    LEFT JOIN (
        SELECT
            ranked.FK_HOST,
            ranked.NA_ERROR_SCOPE,
            ranked.NA_ERROR_DOMAIN,
            ranked.NA_ERROR_STAGE,
            ranked.NA_ERROR_CODE,
            ranked.NA_ERROR_SUMMARY_HASH,
            ranked.NA_SOURCE_TABLE,
            ranked.ID_SOURCE_ROW,
            ranked.NA_ERROR_DETAIL,
            ranked.NA_RAW_MESSAGE
        FROM (
            SELECT
                e.FK_HOST,
                e.NA_ERROR_SCOPE,
                e.NA_ERROR_DOMAIN,
                e.NA_ERROR_STAGE,
                e.NA_ERROR_CODE,
                e.NA_ERROR_SUMMARY_HASH,
                e.NA_SOURCE_TABLE,
                e.ID_SOURCE_ROW,
                e.NA_ERROR_DETAIL,
                e.NA_RAW_MESSAGE,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        e.FK_HOST,
                        e.NA_ERROR_SCOPE,
                        e.NA_ERROR_DOMAIN,
                        e.NA_ERROR_STAGE,
                        e.NA_ERROR_CODE,
                        e.NA_ERROR_SUMMARY_HASH
                    ORDER BY
                        COALESCE(e.DT_EVENT_AT, '1970-01-01 00:00:00') DESC,
                        e.ID_SOURCE_ROW DESC
                ) AS RN_LAST
            FROM VW_ERROR_EVENT_CANONICAL e
            WHERE e.FK_HOST IS NOT NULL
        ) ranked
        WHERE ranked.RN_LAST = 1
    ) last_row
      ON grouped.FK_HOST = last_row.FK_HOST
     AND grouped.NA_ERROR_SCOPE = last_row.NA_ERROR_SCOPE
     AND grouped.NA_ERROR_DOMAIN <=> last_row.NA_ERROR_DOMAIN
     AND grouped.NA_ERROR_STAGE <=> last_row.NA_ERROR_STAGE
     AND grouped.NA_ERROR_CODE <=> last_row.NA_ERROR_CODE
     AND grouped.NA_ERROR_SUMMARY_HASH = last_row.NA_ERROR_SUMMARY_HASH;

    SET v_row_count = (SELECT COUNT(*) FROM HOST_ERROR_SUMMARY__BUILD);

    RENAME TABLE
        HOST_ERROR_SUMMARY TO HOST_ERROR_SUMMARY__OLD,
        HOST_ERROR_SUMMARY__BUILD TO HOST_ERROR_SUMMARY;
    DROP TABLE HOST_ERROR_SUMMARY__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'max_history_id=',
            COALESCE((SELECT MAX(ID_HISTORY) FROM BPDATA.FILE_TASK_HISTORY), 0),
            ';max_file_task_id=',
            COALESCE((SELECT MAX(ID_FILE_TASK) FROM BPDATA.FILE_TASK), 0),
            ';max_host_task_id=',
            COALESCE((SELECT MAX(ID_HOST_TASK) FROM BPDATA.HOST_TASK), 0)
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'HOST_ERROR_SUMMARY';

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
        'HOST_ERROR_SUMMARY',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'max_history_id=',
            COALESCE((SELECT MAX(ID_HISTORY) FROM BPDATA.FILE_TASK_HISTORY), 0),
            ';max_file_task_id=',
            COALESCE((SELECT MAX(ID_FILE_TASK) FROM BPDATA.FILE_TASK), 0),
            ';max_host_task_id=',
            COALESCE((SELECT MAX(ID_HOST_TASK) FROM BPDATA.HOST_TASK), 0)
        ),
        NULL
    );
END$$

CREATE PROCEDURE PRC_REFRESH_SERVER_ERROR_SUMMARY_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('SERVER_ERROR_SUMMARY', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    DROP TABLE IF EXISTS SERVER_ERROR_SUMMARY__BUILD;
    DROP TABLE IF EXISTS SERVER_ERROR_SUMMARY__OLD;
    CREATE TABLE SERVER_ERROR_SUMMARY__BUILD LIKE SERVER_ERROR_SUMMARY;

    INSERT INTO SERVER_ERROR_SUMMARY__BUILD (
        NA_ERROR_SCOPE,
        NA_ERROR_DOMAIN,
        NA_ERROR_STAGE,
        NA_ERROR_CODE,
        NA_ERROR_SUMMARY_HASH,
        NA_ERROR_SUMMARY,
        NU_ERROR_COUNT,
        DT_FIRST_SEEN_AT,
        DT_LAST_SEEN_AT,
        NA_LAST_SOURCE_TABLE,
        ID_LAST_SOURCE_ROW,
        NA_LAST_ERROR_DETAIL,
        NA_LAST_RAW_MESSAGE,
        DT_REFRESHED_AT
    )
    SELECT
        grouped.NA_ERROR_SCOPE,
        grouped.NA_ERROR_DOMAIN,
        grouped.NA_ERROR_STAGE,
        grouped.NA_ERROR_CODE,
        grouped.NA_ERROR_SUMMARY_HASH,
        grouped.NA_ERROR_SUMMARY,
        grouped.NU_ERROR_COUNT,
        grouped.DT_FIRST_SEEN_AT,
        grouped.DT_LAST_SEEN_AT,
        last_row.NA_SOURCE_TABLE,
        last_row.ID_SOURCE_ROW,
        last_row.NA_ERROR_DETAIL,
        last_row.NA_RAW_MESSAGE,
        UTC_TIMESTAMP()
    FROM (
        SELECT
            e.NA_ERROR_SCOPE,
            e.NA_ERROR_DOMAIN,
            e.NA_ERROR_STAGE,
            e.NA_ERROR_CODE,
            e.NA_ERROR_SUMMARY_HASH,
            e.NA_ERROR_SUMMARY,
            COUNT(*) AS NU_ERROR_COUNT,
            MIN(e.DT_EVENT_AT) AS DT_FIRST_SEEN_AT,
            MAX(e.DT_EVENT_AT) AS DT_LAST_SEEN_AT
        FROM VW_ERROR_EVENT_CANONICAL e
        GROUP BY
            e.NA_ERROR_SCOPE,
            e.NA_ERROR_DOMAIN,
            e.NA_ERROR_STAGE,
            e.NA_ERROR_CODE,
            e.NA_ERROR_SUMMARY_HASH,
            e.NA_ERROR_SUMMARY
    ) grouped
    LEFT JOIN (
        SELECT
            ranked.NA_ERROR_SCOPE,
            ranked.NA_ERROR_DOMAIN,
            ranked.NA_ERROR_STAGE,
            ranked.NA_ERROR_CODE,
            ranked.NA_ERROR_SUMMARY_HASH,
            ranked.NA_SOURCE_TABLE,
            ranked.ID_SOURCE_ROW,
            ranked.NA_ERROR_DETAIL,
            ranked.NA_RAW_MESSAGE
        FROM (
            SELECT
                e.NA_ERROR_SCOPE,
                e.NA_ERROR_DOMAIN,
                e.NA_ERROR_STAGE,
                e.NA_ERROR_CODE,
                e.NA_ERROR_SUMMARY_HASH,
                e.NA_SOURCE_TABLE,
                e.ID_SOURCE_ROW,
                e.NA_ERROR_DETAIL,
                e.NA_RAW_MESSAGE,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        e.NA_ERROR_SCOPE,
                        e.NA_ERROR_DOMAIN,
                        e.NA_ERROR_STAGE,
                        e.NA_ERROR_CODE,
                        e.NA_ERROR_SUMMARY_HASH
                    ORDER BY
                        COALESCE(e.DT_EVENT_AT, '1970-01-01 00:00:00') DESC,
                        e.ID_SOURCE_ROW DESC
                ) AS RN_LAST
            FROM VW_ERROR_EVENT_CANONICAL e
        ) ranked
        WHERE ranked.RN_LAST = 1
    ) last_row
      ON grouped.NA_ERROR_SCOPE = last_row.NA_ERROR_SCOPE
     AND grouped.NA_ERROR_DOMAIN <=> last_row.NA_ERROR_DOMAIN
     AND grouped.NA_ERROR_STAGE <=> last_row.NA_ERROR_STAGE
     AND grouped.NA_ERROR_CODE <=> last_row.NA_ERROR_CODE
     AND grouped.NA_ERROR_SUMMARY_HASH = last_row.NA_ERROR_SUMMARY_HASH;

    SET v_row_count = (SELECT COUNT(*) FROM SERVER_ERROR_SUMMARY__BUILD);

    RENAME TABLE
        SERVER_ERROR_SUMMARY TO SERVER_ERROR_SUMMARY__OLD,
        SERVER_ERROR_SUMMARY__BUILD TO SERVER_ERROR_SUMMARY;
    DROP TABLE SERVER_ERROR_SUMMARY__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'max_history_id=',
            COALESCE((SELECT MAX(ID_HISTORY) FROM BPDATA.FILE_TASK_HISTORY), 0),
            ';max_file_task_id=',
            COALESCE((SELECT MAX(ID_FILE_TASK) FROM BPDATA.FILE_TASK), 0),
            ';max_host_task_id=',
            COALESCE((SELECT MAX(ID_HOST_TASK) FROM BPDATA.HOST_TASK), 0)
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'SERVER_ERROR_SUMMARY';

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
        'SERVER_ERROR_SUMMARY',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'max_history_id=',
            COALESCE((SELECT MAX(ID_HISTORY) FROM BPDATA.FILE_TASK_HISTORY), 0),
            ';max_file_task_id=',
            COALESCE((SELECT MAX(ID_FILE_TASK) FROM BPDATA.FILE_TASK), 0),
            ';max_host_task_id=',
            COALESCE((SELECT MAX(ID_HOST_TASK) FROM BPDATA.HOST_TASK), 0)
        ),
        NULL
    );
END$$

CREATE PROCEDURE PRC_REFRESH_HOST_CURRENT_SNAPSHOT_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;
    DECLARE v_current_month_start DATETIME;

    SET v_current_month_start = STR_TO_DATE(DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-01 00:00:00'), '%Y-%m-%d %H:%i:%s');

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('HOST_CURRENT_SNAPSHOT', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    DROP TABLE IF EXISTS HOST_CURRENT_SNAPSHOT__BUILD;
    DROP TABLE IF EXISTS HOST_CURRENT_SNAPSHOT__OLD;
    CREATE TABLE HOST_CURRENT_SNAPSHOT__BUILD LIKE HOST_CURRENT_SNAPSHOT;

    INSERT INTO HOST_CURRENT_SNAPSHOT__BUILD (
        ID_HOST,
        NA_HOST_NAME,
        NA_HOST_ADDRESS,
        NA_HOST_PORT,
        IS_OFFLINE,
        IS_BUSY,
        NU_PID,
        DT_BUSY,
        DT_LAST_FAIL,
        DT_LAST_CHECK,
        NU_HOST_CHECK_ERROR,
        DT_LAST_DISCOVERY,
        NU_DONE_FILE_DISCOVERY_TASKS,
        NU_ERROR_FILE_DISCOVERY_TASKS,
        DT_LAST_BACKUP,
        NU_PENDING_FILE_BACKUP_TASKS,
        NU_DONE_FILE_BACKUP_TASKS,
        NU_ERROR_FILE_BACKUP_TASKS,
        VL_PENDING_BACKUP_GB,
        VL_DONE_BACKUP_GB,
        DT_LAST_PROCESSING,
        NU_PENDING_FILE_PROCESS_TASKS,
        NU_DONE_FILE_PROCESS_TASKS,
        NU_ERROR_FILE_PROCESS_TASKS,
        NU_HOST_FILES,
        NU_BACKUP_QUEUE_FILES_TOTAL,
        VL_BACKUP_QUEUE_GB_TOTAL,
        NU_PROCESSING_QUEUE_FILES_TOTAL,
        VL_PROCESSING_QUEUE_GB_TOTAL,
        NU_MATCHED_EQUIPMENT_TOTAL,
        NU_FACT_SPECTRUM_TOTAL,
        FK_CURRENT_SITE,
        NA_CURRENT_SITE_LABEL,
        NA_CURRENT_STATE_CODE,
        VL_CURRENT_LATITUDE,
        VL_CURRENT_LONGITUDE,
        DT_CURRENT_SITE_LAST_SEEN,
        NA_LAST_ERROR_SCOPE,
        NA_LAST_ERROR_CODE,
        NA_LAST_ERROR_SUMMARY,
        DT_LAST_ERROR_AT,
        DT_REFRESHED_AT
    )
    SELECT
        h.ID_HOST,
        h.NA_HOST_NAME,
        h.NA_HOST_ADDRESS,
        h.NA_HOST_PORT,
        COALESCE(h.IS_OFFLINE, 0),
        COALESCE(h.IS_BUSY, 0),
        h.NU_PID,
        h.DT_BUSY,
        h.DT_LAST_FAIL,
        h.DT_LAST_CHECK,
        h.NU_HOST_CHECK_ERROR,
        h.DT_LAST_DISCOVERY,
        h.NU_DONE_FILE_DISCOVERY_TASKS,
        h.NU_ERROR_FILE_DISCOVERY_TASKS,
        h.DT_LAST_BACKUP,
        h.NU_PENDING_FILE_BACKUP_TASKS,
        h.NU_DONE_FILE_BACKUP_TASKS,
        h.NU_ERROR_FILE_BACKUP_TASKS,
        ROUND(COALESCE(h.VL_PENDING_BACKUP_KB, 0) / 1024 / 1024, 2),
        ROUND(COALESCE(h.VL_DONE_BACKUP_KB, 0) / 1024 / 1024, 2),
        h.DT_LAST_PROCESSING,
        h.NU_PENDING_FILE_PROCESS_TASKS,
        h.NU_DONE_FILE_PROCESS_TASKS,
        h.NU_ERROR_FILE_PROCESS_TASKS,
        COALESCE(monthly_stats.NU_DISCOVERED_FILES_TOTAL, h.NU_HOST_FILES, 0),
        COALESCE(queue.NU_BACKUP_QUEUE_FILES_TOTAL, 0),
        COALESCE(queue.VL_BACKUP_QUEUE_GB_TOTAL, 0.00),
        COALESCE(queue.NU_PROCESSING_QUEUE_FILES_TOTAL, 0),
        COALESCE(queue.VL_PROCESSING_QUEUE_GB_TOTAL, 0.00),
        COALESCE(link_stats.NU_MATCHED_EQUIPMENT_TOTAL, 0),
        COALESCE(spectrum_stats.NU_FACT_SPECTRUM_TOTAL, 0),
        current_location.FK_SITE,
        current_location.NA_SITE_LABEL,
        current_location.NA_STATE_CODE,
        current_location.VL_LATITUDE,
        current_location.VL_LONGITUDE,
        current_location.DT_LAST_SEEN_AT,
        last_error.NA_ERROR_SCOPE,
        last_error.NA_ERROR_CODE,
        last_error.NA_ERROR_SUMMARY,
        last_error.DT_EVENT_AT,
        UTC_TIMESTAMP()
    FROM BPDATA.HOST h
    LEFT JOIN (
        SELECT
            t.FK_HOST,
            SUM(CASE WHEN t.NU_TYPE = 1 AND t.NU_STATUS = 1 THEN 1 ELSE 0 END) AS NU_BACKUP_QUEUE_FILES_TOTAL,
            ROUND(
                COALESCE(SUM(CASE WHEN t.NU_TYPE = 1 AND t.NU_STATUS = 1 THEN t.VL_FILE_SIZE_KB ELSE 0 END), 0)
                / 1024 / 1024,
                2
            ) AS VL_BACKUP_QUEUE_GB_TOTAL,
            SUM(CASE WHEN t.NU_TYPE = 2 AND t.NU_STATUS = 1 THEN 1 ELSE 0 END) AS NU_PROCESSING_QUEUE_FILES_TOTAL,
            ROUND(
                COALESCE(SUM(CASE WHEN t.NU_TYPE = 2 AND t.NU_STATUS = 1 THEN t.VL_FILE_SIZE_KB ELSE 0 END), 0)
                / 1024 / 1024,
                2
            ) AS VL_PROCESSING_QUEUE_GB_TOTAL
        FROM BPDATA.FILE_TASK t
        GROUP BY t.FK_HOST
    ) queue
      ON queue.FK_HOST = h.ID_HOST
    LEFT JOIN (
        SELECT
            metric.FK_HOST,
            SUM(metric.NU_DISCOVERED_FILES) AS NU_DISCOVERED_FILES_TOTAL
        FROM HOST_MONTHLY_METRIC metric
        GROUP BY metric.FK_HOST
    ) monthly_stats
      ON monthly_stats.FK_HOST = h.ID_HOST
    LEFT JOIN (
        SELECT
            l.FK_HOST,
            COUNT(*) AS NU_MATCHED_EQUIPMENT_TOTAL
        FROM HOST_EQUIPMENT_LINK l
        WHERE l.IS_ACTIVE = 1
          AND l.IS_PRIMARY_LINK = 1
        GROUP BY l.FK_HOST
    ) link_stats
      ON link_stats.FK_HOST = h.ID_HOST
    LEFT JOIN (
        SELECT
            l.FK_HOST,
            SUM(obs.NU_SPECTRUM_COUNT) AS NU_FACT_SPECTRUM_TOTAL
        FROM HOST_EQUIPMENT_LINK l
        JOIN SITE_EQUIPMENT_OBS_SUMMARY obs
          ON obs.FK_EQUIPMENT = l.FK_EQUIPMENT
        WHERE l.IS_ACTIVE = 1
          AND l.IS_PRIMARY_LINK = 1
        GROUP BY l.FK_HOST
    ) spectrum_stats
      ON spectrum_stats.FK_HOST = h.ID_HOST
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT
                loc.*,
                ROW_NUMBER() OVER (
                    PARTITION BY loc.FK_HOST
                    ORDER BY loc.IS_CURRENT_LOCATION DESC,
                             COALESCE(loc.DT_LAST_SEEN_AT, loc.DT_FIRST_SEEN_AT) DESC,
                             loc.FK_SITE DESC
                ) AS RN_LOCATION
            FROM HOST_LOCATION_SUMMARY loc
            WHERE loc.IS_CURRENT_LOCATION = 1
        ) ranked_loc
        WHERE ranked_loc.RN_LOCATION = 1
    ) current_location
      ON current_location.FK_HOST = h.ID_HOST
    LEFT JOIN (
        SELECT
            ranked_error.FK_HOST,
            ranked_error.NA_ERROR_SCOPE,
            ranked_error.NA_ERROR_CODE,
            ranked_error.NA_ERROR_SUMMARY,
            ranked_error.DT_EVENT_AT
        FROM (
            SELECT
                e.FK_HOST,
                e.NA_ERROR_SCOPE,
                e.NA_ERROR_CODE,
                e.NA_ERROR_SUMMARY,
                e.DT_EVENT_AT,
                e.ID_SOURCE_ROW,
                ROW_NUMBER() OVER (
                    PARTITION BY e.FK_HOST
                    ORDER BY
                        COALESCE(e.DT_EVENT_AT, '1970-01-01 00:00:00') DESC,
                        e.ID_SOURCE_ROW DESC
                ) AS RN_ERROR
            FROM VW_ERROR_EVENT_CANONICAL e
            WHERE e.FK_HOST IS NOT NULL
        ) ranked_error
        WHERE ranked_error.RN_ERROR = 1
    ) last_error
      ON last_error.FK_HOST = h.ID_HOST;

    SET v_row_count = (SELECT COUNT(*) FROM HOST_CURRENT_SNAPSHOT__BUILD);

    RENAME TABLE
        HOST_CURRENT_SNAPSHOT TO HOST_CURRENT_SNAPSHOT__OLD,
        HOST_CURRENT_SNAPSHOT__BUILD TO HOST_CURRENT_SNAPSHOT;
    DROP TABLE HOST_CURRENT_SNAPSHOT__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'hosts=',
            (SELECT COUNT(*) FROM BPDATA.HOST),
            ';current_month=',
            DATE_FORMAT(v_current_month_start, '%Y-%m')
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'HOST_CURRENT_SNAPSHOT';

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
        'HOST_CURRENT_SNAPSHOT',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'hosts=',
            (SELECT COUNT(*) FROM BPDATA.HOST),
            ';current_month=',
            DATE_FORMAT(v_current_month_start, '%Y-%m')
        ),
        NULL
    );
END$$

CREATE PROCEDURE PRC_REFRESH_SERVER_CURRENT_SUMMARY_FULL()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_row_count BIGINT DEFAULT 0;
    DECLARE v_current_month_start DATETIME;
    DECLARE v_next_month_start DATETIME;

    SET v_current_month_start = STR_TO_DATE(DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-01 00:00:00'), '%Y-%m-%d %H:%i:%s');

    IF MONTH(v_current_month_start) = 12 THEN
        SET v_next_month_start = STR_TO_DATE(
            CONCAT(YEAR(v_current_month_start) + 1, '-01-01 00:00:00'),
            '%Y-%m-%d %H:%i:%s'
        );
    ELSE
        SET v_next_month_start = STR_TO_DATE(
            CONCAT(YEAR(v_current_month_start), '-', LPAD(MONTH(v_current_month_start) + 1, 2, '0'), '-01 00:00:00'),
            '%Y-%m-%d %H:%i:%s'
        );
    END IF;

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES ('SERVER_CURRENT_SUMMARY', v_started_at, NULL, 0, NULL, NULL, NULL)
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    DROP TABLE IF EXISTS SERVER_CURRENT_SUMMARY__BUILD;
    DROP TABLE IF EXISTS SERVER_CURRENT_SUMMARY__OLD;
    CREATE TABLE SERVER_CURRENT_SUMMARY__BUILD LIKE SERVER_CURRENT_SUMMARY;

    INSERT INTO SERVER_CURRENT_SUMMARY__BUILD (
        ID_SUMMARY,
        NA_CURRENT_MONTH_LABEL,
        NU_TOTAL_HOSTS,
        NU_ONLINE_HOSTS,
        NU_OFFLINE_HOSTS,
        NU_BUSY_HOSTS,
        NU_DISCOVERED_FILES_TOTAL,
        NU_BACKUP_PENDING_FILES_TOTAL,
        VL_BACKUP_PENDING_GB_TOTAL,
        NU_BACKUP_ERROR_FILES_TOTAL,
        NU_BACKUP_QUEUE_FILES_TOTAL,
        VL_BACKUP_QUEUE_GB_TOTAL,
        NU_PROCESSING_PENDING_FILES_TOTAL,
        NU_PROCESSING_DONE_FILES_TOTAL,
        NU_PROCESSING_ERROR_FILES_TOTAL,
        NU_PROCESSING_QUEUE_FILES_TOTAL,
        VL_PROCESSING_QUEUE_GB_TOTAL,
        NU_FACT_SPECTRUM_TOTAL,
        NU_BACKUP_DONE_THIS_MONTH,
        VL_BACKUP_DONE_GB_THIS_MONTH,
        NU_BACKUP_ERROR_GROUPS,
        NU_PROCESSING_ERROR_GROUPS,
        DT_REFRESHED_AT
    )
    SELECT
        1,
        DATE_FORMAT(v_current_month_start, '%Y-%m'),
        COUNT(*) AS NU_TOTAL_HOSTS,
        SUM(CASE WHEN snap.IS_OFFLINE = 0 THEN 1 ELSE 0 END) AS NU_ONLINE_HOSTS,
        SUM(CASE WHEN snap.IS_OFFLINE = 1 THEN 1 ELSE 0 END) AS NU_OFFLINE_HOSTS,
        SUM(CASE WHEN snap.IS_BUSY = 1 THEN 1 ELSE 0 END) AS NU_BUSY_HOSTS,
        COALESCE(SUM(snap.NU_HOST_FILES), 0) AS NU_DISCOVERED_FILES_TOTAL,
        COALESCE(SUM(snap.NU_PENDING_FILE_BACKUP_TASKS), 0) AS NU_BACKUP_PENDING_FILES_TOTAL,
        ROUND(COALESCE(SUM(snap.VL_PENDING_BACKUP_GB), 0), 2) AS VL_BACKUP_PENDING_GB_TOTAL,
        COALESCE(SUM(snap.NU_ERROR_FILE_BACKUP_TASKS), 0) AS NU_BACKUP_ERROR_FILES_TOTAL,
        COALESCE(SUM(snap.NU_BACKUP_QUEUE_FILES_TOTAL), 0) AS NU_BACKUP_QUEUE_FILES_TOTAL,
        ROUND(COALESCE(SUM(snap.VL_BACKUP_QUEUE_GB_TOTAL), 0), 2) AS VL_BACKUP_QUEUE_GB_TOTAL,
        COALESCE(SUM(snap.NU_PENDING_FILE_PROCESS_TASKS), 0) AS NU_PROCESSING_PENDING_FILES_TOTAL,
        COALESCE(SUM(snap.NU_DONE_FILE_PROCESS_TASKS), 0) AS NU_PROCESSING_DONE_FILES_TOTAL,
        COALESCE(SUM(snap.NU_ERROR_FILE_PROCESS_TASKS), 0) AS NU_PROCESSING_ERROR_FILES_TOTAL,
        COALESCE(SUM(snap.NU_PROCESSING_QUEUE_FILES_TOTAL), 0) AS NU_PROCESSING_QUEUE_FILES_TOTAL,
        ROUND(COALESCE(SUM(snap.VL_PROCESSING_QUEUE_GB_TOTAL), 0), 2) AS VL_PROCESSING_QUEUE_GB_TOTAL,
        COALESCE(SUM(snap.NU_FACT_SPECTRUM_TOTAL), 0) AS NU_FACT_SPECTRUM_TOTAL,
        COALESCE((
            SELECT COUNT(*)
            FROM BPDATA.FILE_TASK_HISTORY f
            WHERE f.NU_STATUS_BACKUP = 0
              AND f.DT_BACKUP >= v_current_month_start
              AND f.DT_BACKUP < v_next_month_start
        ), 0) AS NU_BACKUP_DONE_THIS_MONTH,
        COALESCE((
            SELECT ROUND(COALESCE(SUM(f.VL_FILE_SIZE_KB), 0) / 1024 / 1024, 2)
            FROM BPDATA.FILE_TASK_HISTORY f
            WHERE f.NU_STATUS_BACKUP = 0
              AND f.DT_BACKUP >= v_current_month_start
              AND f.DT_BACKUP < v_next_month_start
        ), 0.00) AS VL_BACKUP_DONE_GB_THIS_MONTH,
        COALESCE((
            SELECT COUNT(*)
            FROM SERVER_ERROR_SUMMARY se
            WHERE se.NA_ERROR_SCOPE = 'BACKUP'
        ), 0) AS NU_BACKUP_ERROR_GROUPS,
        COALESCE((
            SELECT COUNT(*)
            FROM SERVER_ERROR_SUMMARY se
            WHERE se.NA_ERROR_SCOPE = 'PROCESSING'
        ), 0) AS NU_PROCESSING_ERROR_GROUPS,
        UTC_TIMESTAMP()
    FROM HOST_CURRENT_SNAPSHOT snap;

    SET v_row_count = (SELECT COUNT(*) FROM SERVER_CURRENT_SUMMARY__BUILD);

    RENAME TABLE
        SERVER_CURRENT_SUMMARY TO SERVER_CURRENT_SUMMARY__OLD,
        SERVER_CURRENT_SUMMARY__BUILD TO SERVER_CURRENT_SUMMARY;
    DROP TABLE SERVER_CURRENT_SUMMARY__OLD;

    UPDATE SUMMARY_REFRESH_STATE
    SET
        DT_LAST_END = UTC_TIMESTAMP(),
        IS_SUCCESS = 1,
        NU_LAST_ROW_COUNT = v_row_count,
        NA_SOURCE_HIGH_WATERMARK = CONCAT(
            'current_month=',
            DATE_FORMAT(v_current_month_start, '%Y-%m'),
            ';hosts=',
            (SELECT COUNT(*) FROM HOST_CURRENT_SNAPSHOT)
        ),
        NA_ERROR_MESSAGE = NULL
    WHERE NA_OBJECT_NAME = 'SERVER_CURRENT_SUMMARY';

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
        'SERVER_CURRENT_SUMMARY',
        v_started_at,
        UTC_TIMESTAMP(),
        1,
        v_row_count,
        CONCAT(
            'current_month=',
            DATE_FORMAT(v_current_month_start, '%Y-%m'),
            ';hosts=',
            (SELECT COUNT(*) FROM HOST_CURRENT_SNAPSHOT)
        ),
        NULL
    );
END$$

DELIMITER ;
