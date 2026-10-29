\set ON_ERROR_STOP on
SELECT '/usr/local/share/rffusion-postgres/sql/verify' || current_database() || '.sql' AS verification_file
\gset
\i :verification_file
