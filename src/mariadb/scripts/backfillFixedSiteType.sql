-- Classify legacy fixed sites without changing an existing classification.
-- The join guards against an incompatible type catalog (including quoted labels).
START TRANSACTION;
UPDATE DIM_SPECTRUM_SITE AS site
JOIN DIM_SITE_TYPE AS type ON type.ID_TYPE = 1
    AND TRIM(BOTH CHAR(39) FROM type.NA_TYPE) = 'Fixed'
SET site.FK_TYPE = type.ID_TYPE
WHERE site.FK_TYPE IS NULL;
SELECT ROW_COUNT() AS classified_fixed_sites;
COMMIT;
SELECT FK_TYPE, COUNT(*) AS sites FROM DIM_SPECTRUM_SITE GROUP BY FK_TYPE;
