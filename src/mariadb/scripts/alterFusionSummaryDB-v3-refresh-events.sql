/* =====================================================================
   alterFusionSummaryDB-v3-refresh-events.sql
   - Schedule periodic RFFUSION_SUMMARY refreshes inside MariaDB Events
   - Use a named lock to avoid overlapping executions
   - Enable the event scheduler on the current runtime
   ===================================================================== */

USE RFFUSION_SUMMARY;

DELIMITER $$

DROP PROCEDURE IF EXISTS PRC_REFRESH_ALL_RFFUSION_SUMMARY_SAFE$$
DROP EVENT IF EXISTS EVT_REFRESH_ALL_RFFUSION_SUMMARY_10MIN$$

CREATE PROCEDURE PRC_REFRESH_ALL_RFFUSION_SUMMARY_SAFE()
BEGIN
    DECLARE v_started_at DATETIME DEFAULT UTC_TIMESTAMP();
    DECLARE v_finished_at DATETIME DEFAULT NULL;
    DECLARE v_lock_acquired INT DEFAULT 0;
    DECLARE v_log_object_name VARCHAR(100) DEFAULT 'RFFUSION_SUMMARY_EVENT_10MIN';
    DECLARE v_high_watermark VARCHAR(255) DEFAULT 'interval=10m';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_finished_at = UTC_TIMESTAMP();

        IF v_lock_acquired = 1 THEN
            DO RELEASE_LOCK('RFFUSION_SUMMARY_REFRESH_LOCK');
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
        VALUES (
            v_log_object_name,
            v_started_at,
            v_finished_at,
            0,
            NULL,
            CONCAT(v_high_watermark, ';lock=acquired'),
            'scheduled refresh failed'
        )
        ON DUPLICATE KEY UPDATE
            DT_LAST_START = VALUES(DT_LAST_START),
            DT_LAST_END = VALUES(DT_LAST_END),
            IS_SUCCESS = VALUES(IS_SUCCESS),
            NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
            NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
            NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

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
            v_log_object_name,
            v_started_at,
            v_finished_at,
            0,
            NULL,
            CONCAT(v_high_watermark, ';lock=acquired'),
            'scheduled refresh failed'
        );

        RESIGNAL;
    END;

    INSERT INTO SUMMARY_REFRESH_STATE (
        NA_OBJECT_NAME,
        DT_LAST_START,
        DT_LAST_END,
        IS_SUCCESS,
        NU_LAST_ROW_COUNT,
        NA_SOURCE_HIGH_WATERMARK,
        NA_ERROR_MESSAGE
    )
    VALUES (
        v_log_object_name,
        v_started_at,
        NULL,
        0,
        NULL,
        CONCAT(v_high_watermark, ';lock=pending'),
        NULL
    )
    ON DUPLICATE KEY UPDATE
        DT_LAST_START = VALUES(DT_LAST_START),
        DT_LAST_END = VALUES(DT_LAST_END),
        IS_SUCCESS = VALUES(IS_SUCCESS),
        NU_LAST_ROW_COUNT = VALUES(NU_LAST_ROW_COUNT),
        NA_SOURCE_HIGH_WATERMARK = VALUES(NA_SOURCE_HIGH_WATERMARK),
        NA_ERROR_MESSAGE = VALUES(NA_ERROR_MESSAGE);

    SELECT GET_LOCK('RFFUSION_SUMMARY_REFRESH_LOCK', 0) INTO v_lock_acquired;

    IF COALESCE(v_lock_acquired, 0) = 0 THEN
        SET v_finished_at = UTC_TIMESTAMP();

        UPDATE SUMMARY_REFRESH_STATE
        SET
            DT_LAST_END = v_finished_at,
            IS_SUCCESS = 0,
            NU_LAST_ROW_COUNT = NULL,
            NA_SOURCE_HIGH_WATERMARK = CONCAT(v_high_watermark, ';lock=skipped'),
            NA_ERROR_MESSAGE = 'scheduled refresh skipped because a previous run is still active'
        WHERE NA_OBJECT_NAME = v_log_object_name;

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
            v_log_object_name,
            v_started_at,
            v_finished_at,
            0,
            NULL,
            CONCAT(v_high_watermark, ';lock=skipped'),
            'scheduled refresh skipped because a previous run is still active'
        );
    ELSE
        CALL PRC_REFRESH_ALL_RFFUSION_SUMMARY_FULL();

        SET v_finished_at = UTC_TIMESTAMP();
        DO RELEASE_LOCK('RFFUSION_SUMMARY_REFRESH_LOCK');

        UPDATE SUMMARY_REFRESH_STATE
        SET
            DT_LAST_END = v_finished_at,
            IS_SUCCESS = 1,
            NU_LAST_ROW_COUNT = 1,
            NA_SOURCE_HIGH_WATERMARK = CONCAT(v_high_watermark, ';lock=acquired'),
            NA_ERROR_MESSAGE = NULL
        WHERE NA_OBJECT_NAME = v_log_object_name;

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
            v_log_object_name,
            v_started_at,
            v_finished_at,
            1,
            1,
            CONCAT(v_high_watermark, ';lock=acquired'),
            NULL
        );
    END IF;
END$$

DELIMITER ;

SET GLOBAL event_scheduler = ON;

CREATE EVENT EVT_REFRESH_ALL_RFFUSION_SUMMARY_10MIN
    ON SCHEDULE EVERY 10 MINUTE
    STARTS CURRENT_TIMESTAMP + INTERVAL 10 MINUTE
    ON COMPLETION PRESERVE
    ENABLE
    DO CALL PRC_REFRESH_ALL_RFFUSION_SUMMARY_SAFE();
