-- Execute apenas depois de conferir que os sites sem tipo são fixos.
BEGIN;
WITH updated AS (
    UPDATE rfdata.dim_spectrum_site AS site
    SET fk_type = type.id_type
    FROM rfdata.dim_site_type AS type
    WHERE type.id_type = 1
      AND btrim(type.na_type, chr(39)) = 'Fixed'
      AND site.fk_type IS NULL
    RETURNING 1
)
SELECT count(*) AS classified_fixed_sites FROM updated;
COMMIT;
SELECT fk_type, count(*) AS sites FROM rfdata.dim_spectrum_site GROUP BY fk_type;
