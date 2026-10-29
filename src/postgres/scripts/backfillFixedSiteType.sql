-- Execute apenas depois de conferir que os sites sem tipo são fixos.
BEGIN;
WITH updated AS (
    UPDATE "RFDATA"."DIM_SPECTRUM_SITE" AS site
    SET "FK_TYPE" = type."ID_TYPE"
    FROM "RFDATA"."DIM_SITE_TYPE" AS type
    WHERE type."ID_TYPE" = 1
      AND btrim(type."NA_TYPE", chr(39)) = 'Fixed'
      AND site."FK_TYPE" IS NULL
    RETURNING 1
)
SELECT count(*) AS classified_fixed_sites FROM updated;
COMMIT;
SELECT "FK_TYPE", count(*) AS sites FROM "RFDATA"."DIM_SPECTRUM_SITE" GROUP BY "FK_TYPE";
