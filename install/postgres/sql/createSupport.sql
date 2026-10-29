CREATE SCHEMA rffusion_support;
REVOKE ALL ON SCHEMA rffusion_support FROM PUBLIC;
CREATE COLLATION rffusion_support.general_ci (provider = icu, locale = 'und-u-ks-level1', deterministic = false);
CREATE COLLATION rffusion_support.unicode_ci (provider = icu, locale = 'und-u-ks-level1', deterministic = false);

CREATE TABLE rffusion_support.id_guard (
    sequence_name text PRIMARY KEY,
    last_id bigint NOT NULL
);
CREATE TABLE rffusion_support.identity_guard (
    table_name text NOT NULL,
    host_id bigint NOT NULL,
    version bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (table_name, host_id)
);

CREATE FUNCTION rffusion_support.allocate_id() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, rffusion_support
AS $allocate$
DECLARE
    supplied bigint;
    allocated bigint;
    high_water bigint;
    sequence_called boolean;
    seq regclass;
BEGIN
    supplied := (to_jsonb(NEW)->>TG_ARGV[0])::bigint;
    SELECT to_regclass(format('%I.%I', TG_ARGV[1], TG_ARGV[2])) INTO seq;
    UPDATE rffusion_support.id_guard SET last_id=last_id
      WHERE sequence_name=TG_ARGV[2] RETURNING last_id INTO high_water;
    IF NOT FOUND OR seq IS NULL THEN
        RAISE EXCEPTION 'Missing ID guard/sequence for %.%', TG_TABLE_NAME, TG_ARGV[0];
    END IF;
    IF TG_OP='INSERT' AND (supplied IS NULL OR supplied=0) THEN
        allocated := nextval(seq);
        NEW := jsonb_populate_record(NEW, jsonb_build_object(TG_ARGV[0], allocated));
    ELSE
        allocated := supplied;
    END IF;
    IF allocated IS NOT NULL AND allocated > 0 THEN
        UPDATE rffusion_support.id_guard SET last_id=greatest(last_id,allocated) WHERE sequence_name=TG_ARGV[2];
        EXECUTE format('SELECT last_value,is_called FROM %s', seq) INTO high_water,sequence_called;
        IF allocated > high_water OR (allocated=high_water AND NOT sequence_called) THEN
            PERFORM setval(seq, allocated, true);
        END IF;
    END IF;
    RETURN NEW;
END;
$allocate$;

CREATE FUNCTION rffusion_support.reset_generators() RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, rffusion_support
AS $reset$
DECLARE
    item record;
    maximum bigint;
    current_value bigint;
    sequence_called boolean;
    seq regclass;
BEGIN
    FOR item IN
        SELECT ns.nspname, s.relname sequence_name, tn.nspname table_schema,
               t.relname table_name, a.attname column_name
        FROM pg_class s
        JOIN pg_namespace ns ON ns.oid=s.relnamespace
        JOIN pg_depend d ON d.objid=s.oid AND d.classid='pg_class'::regclass
                         AND d.refclassid='pg_class'::regclass AND d.deptype='a'
        JOIN pg_class t ON t.oid=d.refobjid
        JOIN pg_namespace tn ON tn.oid=t.relnamespace
        JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
        WHERE s.relkind='S'
          AND EXISTS (SELECT 1 FROM rffusion_support.id_guard g WHERE g.sequence_name=s.relname)
    LOOP
        UPDATE rffusion_support.id_guard SET last_id=last_id WHERE sequence_name=item.sequence_name;
        IF NOT FOUND THEN RAISE EXCEPTION 'Missing sequence guard %', item.sequence_name; END IF;
        seq := to_regclass(format('%I.%I', item.nspname, item.sequence_name));
        EXECUTE format('SELECT coalesce(max(%I),0) FROM %I.%I',
            item.column_name, item.table_schema, item.table_name) INTO maximum;
        EXECUTE format('SELECT last_value,is_called FROM %s', seq) INTO current_value,sequence_called;
        IF maximum > current_value OR (maximum=current_value AND NOT sequence_called) THEN
            PERFORM setval(seq, maximum, true);
        END IF;
        UPDATE rffusion_support.id_guard SET last_id=greatest(last_id,maximum,current_value)
          WHERE sequence_name=item.sequence_name;
    END LOOP;
END;
$reset$;

CREATE FUNCTION rffusion_support.update_timestamp() RETURNS trigger
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog
AS $timestamp$
BEGIN
    IF to_jsonb(NEW)-TG_ARGV[0] IS DISTINCT FROM to_jsonb(OLD)-TG_ARGV[0]
       AND to_jsonb(NEW)->TG_ARGV[0] IS NOT DISTINCT FROM to_jsonb(OLD)->TG_ARGV[0] THEN
        NEW := jsonb_populate_record(NEW, jsonb_build_object(
            TG_ARGV[0], date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC')));
    END IF;
    RETURN NEW;
END;
$timestamp$;

CREATE FUNCTION rffusion_support.normalize_role() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog
AS $role$
BEGIN
    CASE NEW."NA_ROLE" COLLATE rffusion_support.unicode_ci
        WHEN 'admin' THEN NEW."NA_ROLE" := 'admin';
        WHEN 'developer' THEN NEW."NA_ROLE" := 'developer';
        ELSE RAISE check_violation USING MESSAGE='Unknown role';
    END CASE;
    RETURN NEW;
END;
$role$;

CREATE FUNCTION rffusion_support.file_identity() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, rffusion_support
AS $identity$
DECLARE
    previous_host bigint;
    next_host bigint;
    host bigint;
    current_pk bigint;
    collision boolean;
BEGIN
    IF TG_OP<>'INSERT' THEN previous_host := OLD."FK_HOST"; END IF;
    IF TG_OP<>'DELETE' THEN next_host := NEW."FK_HOST"; END IF;
    FOR host IN SELECT DISTINCT h FROM unnest(ARRAY[previous_host,next_host]) h
                WHERE h IS NOT NULL ORDER BY h LOOP
        INSERT INTO rffusion_support.identity_guard AS g (table_name,host_id,version)
        VALUES (TG_TABLE_NAME,host,1)
        ON CONFLICT (table_name,host_id) DO UPDATE SET version=g.version+1;
    END LOOP;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    IF NEW."FK_HOST" IS NULL OR NEW."NA_HOST_FILE_PATH" IS NULL
       OR NEW."NA_HOST_FILE_NAME" IS NULL THEN RETURN NEW; END IF;
    IF TG_OP='UPDATE' THEN current_pk := (to_jsonb(OLD)->>TG_ARGV[0])::bigint; END IF;
    EXECUTE format(
        'SELECT EXISTS (SELECT 1 FROM %I.%I WHERE "FK_HOST"=$1 AND "NA_HOST_FILE_PATH"=$2 '
        'AND "NA_HOST_FILE_NAME"=$3 AND ($4 IS NULL OR %I<>$4))',
        TG_TABLE_SCHEMA,TG_TABLE_NAME,TG_ARGV[0])
        INTO collision USING NEW."FK_HOST",NEW."NA_HOST_FILE_PATH",NEW."NA_HOST_FILE_NAME",current_pk;
    IF collision THEN
        RAISE unique_violation USING MESSAGE='Duplicate complete file identity',
            CONSTRAINT='uq_fth_identity', SCHEMA=TG_TABLE_SCHEMA, TABLE=TG_TABLE_NAME;
    END IF;
    RETURN NEW;
END;
$identity$;
REVOKE ALL ON ALL TABLES IN SCHEMA rffusion_support FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA rffusion_support FROM PUBLIC;
