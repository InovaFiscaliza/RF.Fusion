USE RFDATA;

-- WebFusion `/spectrum` always starts from one equipment and then narrows the
-- result set by observation window before joining files. This index gives the
-- optimizer a much cheaper entry path than the idempotency-oriented index.
SET @ddl = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'FACT_SPECTRUM'
          AND index_name = 'IX_FACT_SPECTRUM_WEBFUSION_SEARCH'
    ),
    "SELECT 'IX_FACT_SPECTRUM_WEBFUSION_SEARCH already exists' AS message",
    "ALTER TABLE FACT_SPECTRUM ADD INDEX IX_FACT_SPECTRUM_WEBFUSION_SEARCH (FK_EQUIPMENT, DT_TIME_END, DT_TIME_START, ID_SPECTRUM)"
);

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
