\set ON_ERROR_STOP on
BEGIN READ ONLY;
DO $pending$
BEGIN
    RAISE EXCEPTION 'A1 PENDING: approve lifecycle compatibility policy before executing persistence/recreate fixtures';
END;
$pending$;
ROLLBACK;
