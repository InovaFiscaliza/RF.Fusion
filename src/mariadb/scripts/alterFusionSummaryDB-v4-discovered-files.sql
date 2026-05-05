USE RFFUSION_SUMMARY;

DELIMITER $$

DROP PROCEDURE IF EXISTS PRC_REFRESH_HOST_CURRENT_SNAPSHOT_FULL$$

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

    TRUNCATE TABLE HOST_CURRENT_SNAPSHOT;

    INSERT INTO HOST_CURRENT_SNAPSHOT (
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

    SET v_row_count = (SELECT COUNT(*) FROM HOST_CURRENT_SNAPSHOT);

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

DELIMITER ;
