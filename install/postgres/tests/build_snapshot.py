"""Build or compare the bounded canonical SQL/CSV snapshot without database access."""

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]
SOURCE = PACKAGE.parents[1] / "src/mariadb/scripts"
BASELINE = {
    "createProcessingDB.sql": "a38f0dccf777ad4404224d1ac958cb129fffe1103eb137254e725e305426a199",
    "createMeasureDB.sql": "9a61ee62f57d69331c0046191146d19777b7ae8270f41acb92a193b1d7f5e8fe",
    "createFusionSummaryDB.sql": "28e6b8bbd6427d9ea9f09aa1f471109e11ed3b691b9c2612a0d9cc419659f327",
    "createWebFusionDB.sql": "cae97a77e1580a6544059b4d7a2fc63e60256dd48e585e10e5226b838fed8880",
    "equipmentType.csv": "8f24243133ed700fa262393a1f21e8356b341a4803fd4f6d766f113c87ca324a",
    "IBGE-BR_UF_2020_BULKLOAD.csv": "8f9f51f7418df0b8b2f47d3387885aeed15f4a75fe001fefc1a8f4d94fcb18dd",
    "IBGE-BR_Municipios_2020_BULKLOAD.csv": "48f9875413283dec56c4e349b0d18b1ef09b58d7dcb2535c4eb9357ee948371c",
    "fileType.csv": "d44d971f334045d42907887818cb98ae273a37b909bdfc71391bcdc4a4790554",
    "measurementUnit.csv": "7dcccd627725e85d7f1bf046c313a0973698fa7f9259616388a78bf99f984b4d",
}
DOMAINS = dict(zip(list(BASELINE)[:4], ("BPDATA", "RFDATA", "RFFUSION_SUMMARY", "WEBFUSION")))
SEEDS = [
    ("equipmentType.csv", "DIM_EQUIPMENT_TYPE", ["ID_EQUIPMENT_TYPE", "NA_EQUIPMENT_TYPE", "NA_EQUIPMENT_TYPE_UID"]),
    ("IBGE-BR_UF_2020_BULKLOAD.csv", "DIM_SITE_STATE", ["ID_STATE", "NA_STATE", "LC_STATE"]),
    ("IBGE-BR_Municipios_2020_BULKLOAD.csv", "DIM_SITE_COUNTY", ["ID_COUNTY", "FK_STATE", "NA_COUNTY"]),
    ("fileType.csv", "DIM_FILE_TYPE", ["ID_TYPE_FILE", "NA_TYPE_FILE", "NA_EQUIPMENT"]),
    ("measurementUnit.csv", "DIM_SPECTRUM_UNIT", ["ID_MEASURE_UNIT", "NA_MEASURE_UNIT"]),
]
STAMP = "date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC')"


def quote(value: str) -> str:
    """Return one quoted SQL identifier."""
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    """Return one standard SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def name(value: str) -> str:
    """Make deterministic, collision-resistant names within 63 ASCII bytes."""
    return value if len(value.encode()) <= 63 else value[:50] + "_" + hashlib.sha256(value.encode()).hexdigest()[:10]


def split_items(value: str) -> list[str]:
    """Split SQL list members while preserving nested calls and quoted strings."""
    result, start, depth, quoted, index = [], 0, 0, False, 0
    while index < len(value):
        char = value[index]
        if char == "'":
            if quoted and index + 1 < len(value) and value[index + 1] == "'":
                index += 2
                continue
            quoted = not quoted
        elif not quoted:
            depth += (char == "(") - (char == ")")
            if char == "," and depth == 0:
                result.append(value[start:index].strip())
                start = index + 1
        index += 1
    if quoted or depth:
        raise ValueError("Unbalanced canonical SQL list")
    result.append(value[start:].strip())
    return result


def members(value: str) -> list[dict]:
    """Parse only supported source index members; reject unsupported syntax."""
    result = []
    for member in split_items(value):
        match = re.fullmatch(r"`?(\w+)`?(?:\((\d+)\))?(?:\s+(ASC|DESC))?", member)
        if not match:
            raise ValueError(f"Unsupported index member: {member}")
        result.append({"column": match[1], "prefix": int(match[2]) if match[2] else None,
                       "descending": match[3] == "DESC"})
    return result


def column(item: str, position: int, collation: str) -> dict:
    """Translate a supported column, retaining source and target attributes."""
    clean = re.sub(r"\s+COMMENT\s+'(?:[^']|'')*'", "", item)
    match = re.match(r"`?(\w+)`?\s+((?:enum\([^)]*\))|(?:\w+(?:\([\d,]+\))?))(.*)$", clean, re.S)
    if not match:
        raise ValueError(f"Unsupported column: {item}")
    field, original, tail = match.groups()
    base = original.split("(")[0].lower()
    simple = {"int": "integer", "bigint": "bigint", "smallint": "smallint",
              "tinyint": "smallint", "datetime": "timestamp(0) without time zone",
              "date": "date", "text": "text", "longtext": "json",
              "blob": "bytea", "point": "public.geometry(Point)", "enum": "character varying(9)"}
    if base in ("varchar", "char", "decimal"):
        target = {"varchar": "character varying", "char": "character", "decimal": "numeric"}[base] + original[original.index("("):]
    elif base in simple:
        target = simple[base]
    else:
        raise ValueError(f"Unsupported type: {original}")
    default = re.search(r"\bDEFAULT\s+(current_timestamp\(\)|NULL|'(?:[^']|'')*'|-?[\d.]+)", tail, re.I)
    expression = default[1] if default else None
    if expression and expression.upper() == "NULL":
        expression = None
    if expression and expression.lower() == "current_timestamp()":
        expression = STAMP
    override = re.search(r"\bCOLLATE\s+utf8mb4_(general_ci|unicode_ci|bin)", tail)
    profile = override[1] if override else collation
    textual = base in ("varchar", "char", "text", "enum")
    return {
        "name": field, "position": position, "source_type": original,
        "source_definition": item, "type": target, "nullable": "NOT NULL" not in tail.upper(),
        "default": expression, "generated": "AUTO_INCREMENT" in tail.upper(),
        "on_update": "ON UPDATE" in tail.upper(), "collation": profile if textual else None,
        "test": "test_structure.sql; test_rules.sql",
    }


def parse_tables(text: str, domain: str) -> list[dict]:
    """Extract all canonical tables and reject unknown table-level definitions."""
    result = []
    pattern = r"CREATE TABLE (?:IF NOT EXISTS )?`?(\w+)`?\s*\((.*?)\)\s*ENGINE=InnoDB\s+DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_(\w+);"
    for match in re.finditer(pattern, text, re.S):
        table = {"database": domain, "name": match[1], "columns": [], "primary_key": [],
                 "unique": [], "indexes": [], "foreign_keys": []}
        for item in split_items(match[2]):
            pk = re.fullmatch(r"PRIMARY KEY\s*\((.*)\)", item, re.S)
            fk = re.fullmatch(r"CONSTRAINT\s+`?(\w+)`?\s+FOREIGN KEY\s*\((.*?)\)\s+REFERENCES\s+`?(\w+)`?\s*\((.*?)\)(.*)", item, re.S)
            idx = re.fullmatch(r"(?:(UNIQUE|FULLTEXT|SPATIAL)\s+)?KEY\s+`?(\w+)`?\s*\((.*)\)(?:\s+USING HASH)?", item, re.S)
            if pk:
                table["primary_key"] = [part["column"] for part in members(pk[1])]
            elif fk:
                table["foreign_keys"].append({
                    "name": fk[1], "columns": [part["column"] for part in members(fk[2])],
                    "table": fk[3], "references": [part["column"] for part in members(fk[4])],
                    "delete": "CASCADE" if "ON DELETE CASCADE" in fk[5] else "NO ACTION",
                    "update": "NO ACTION",
                })
            elif idx:
                item_map = {"name": idx[2], "physical": name(table["name"] + "__" + idx[2]),
                            "members": members(idx[3]), "kind": {"FULLTEXT": "gin", "SPATIAL": "gist"}.get(idx[1], "btree")}
                if idx[1] == "UNIQUE":
                    item_map["mechanism"] = "host_guard" if "USING HASH" in item else "unique"
                    table["unique"].append(item_map)
                else:
                    table["indexes"].append(item_map)
            else:
                table["columns"].append(column(item, len(table["columns"]) + 1, match[3]))
        if not table["primary_key"]:
            raise ValueError(f"Missing primary key: {domain}.{table['name']}")
        result.append(table)
    return result


def col_sql(col: dict) -> str:
    """Return one complete destination column declaration."""
    field = quote(col["name"])
    sql = field + " " + col["type"]
    if col["collation"]:
        sql += " COLLATE rffusion_support." + quote(col["collation"])
    if not col["nullable"]:
        sql += " NOT NULL"
    if col["default"] is not None:
        sql += " DEFAULT " + col["default"]
    if col["source_type"].startswith("tinyint"):
        sql += f" CHECK ({field} BETWEEN -128 AND 127)"
    if col["source_type"] in ("text", "blob"):
        sql += f" CHECK (octet_length({field}) <= 65535)"
    if col["source_type"].startswith("enum"):
        sql += f" CHECK ({field} COLLATE \"C\" IN ('admin', 'developer'))"
    return sql


def index_expression(index: dict) -> str:
    """Return complete destination index keys, preserving source key ordering."""
    keys = []
    for member in index["members"]:
        field = quote(member["column"])
        if index["kind"] == "gin":
            field = f"to_tsvector('simple'::regconfig, coalesce({field}::text, '') COLLATE \"C\")"
        elif member["prefix"]:
            field = f"left({field}, {member['prefix']})"
        if index["kind"] == "btree":
            field += " DESC NULLS LAST" if member["descending"] else " NULLS FIRST"
        keys.append(field)
    return ", ".join(keys)


def create_domain(domain: str, tables: list[dict]) -> str:
    """Emit transactional destination DDL and required technical triggers."""
    owner = "rffusion_owner_" + domain.lower()
    lines = ["\\set ON_ERROR_STOP on", "BEGIN;"]
    if domain == "RFDATA":
        lines.append("CREATE EXTENSION postgis;")
    lines += [f"SET LOCAL ROLE {quote(owner)};",
              f"CREATE SCHEMA {quote(domain)} AUTHORIZATION {quote(owner)};",
              f"SET LOCAL search_path = {quote(domain)}, public, pg_catalog;",
              "\\ir createSupport.sql"]
    for table in tables:
        qualified = f'{quote(domain)}.{quote(table["name"])}'
        definitions = [col_sql(col) for col in table["columns"]]
        definitions.append("PRIMARY KEY (" + ", ".join(map(quote, table["primary_key"])) + ")")
        for unique in table["unique"]:
            if unique["mechanism"] == "unique":
                definitions.append("CONSTRAINT " + quote(unique["physical"]) + " UNIQUE (" +
                                   ", ".join(quote(part["column"]) for part in unique["members"]) + ")")
        for fk in table["foreign_keys"]:
            definitions.append(f'CONSTRAINT {quote(fk["name"])} FOREIGN KEY (' +
                               ", ".join(map(quote, fk["columns"])) + f') REFERENCES {quote(domain)}.{quote(fk["table"])} (' +
                               ", ".join(map(quote, fk["references"])) + f') ON DELETE {fk["delete"]} ON UPDATE {fk["update"]}')
        lines.append(f"CREATE TABLE {qualified} (\n  " + ",\n  ".join(definitions) + "\n);")
        for col in table["columns"]:
            if col["generated"]:
                seq = name(table["name"] + "__" + col["name"] + "__seq")
                col["sequence"] = seq
                seqtype = "bigint" if col["type"] == "bigint" else "integer"
                lines += [f"CREATE SEQUENCE {quote(domain)}.{quote(seq)} AS {seqtype};",
                          f"ALTER SEQUENCE {quote(domain)}.{quote(seq)} OWNED BY {qualified}.{quote(col['name'])};",
                          f"INSERT INTO rffusion_support.id_guard VALUES ({literal(seq)}, 0);",
                          f"CREATE TRIGGER a00_allocate BEFORE INSERT OR UPDATE OF {quote(col['name'])} ON {qualified} "
                          f"FOR EACH ROW EXECUTE FUNCTION rffusion_support.allocate_id({literal(col['name'])}, {literal(domain)}, {literal(seq)});"]
            if col["on_update"]:
                lines.append(f"CREATE TRIGGER z90_updated BEFORE UPDATE ON {qualified} "
                             f"FOR EACH ROW EXECUTE FUNCTION rffusion_support.update_timestamp({literal(col['name'])});")
        if any(col["source_type"].startswith("enum") for col in table["columns"]):
            lines.append(f"CREATE TRIGGER b00_role BEFORE INSERT OR UPDATE ON {qualified} "
                         "FOR EACH ROW EXECUTE FUNCTION rffusion_support.normalize_role();")
        for unique in table["unique"]:
            if unique["mechanism"] == "host_guard":
                lines.append(f"CREATE TRIGGER b10_identity BEFORE INSERT OR UPDATE OR DELETE ON {qualified} "
                             f"FOR EACH ROW EXECUTE FUNCTION rffusion_support.file_identity({literal(table['primary_key'][0])});")
        for index in table["indexes"]:
            index["expression"] = index_expression(index)
            lines.append(f'CREATE INDEX {quote(index["physical"])} ON {qualified} USING {index["kind"]} ({index["expression"]});')
    if domain == "RFDATA":
        for file, target_table, fields in SEEDS:
            lines.append("\\copy " + quote(domain) + "." + quote(target_table) +
                         " (" + ", ".join(map(quote, fields)) + ") FROM '/usr/local/share/rffusion-postgres/data/" +
                         file + "' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')")
        lines.append('UPDATE "RFDATA"."DIM_SITE_COUNTY" SET "NA_COUNTY" = btrim(translate("NA_COUNTY", E\'|\\r\\n\\t\', \'\'));')
    lines += ["SELECT rffusion_support.reset_generators();", "COMMIT;", ""]
    return "\n".join(lines)


def cross_database(tables: list[dict]) -> str:
    """Emit the read-only local FDW bridge and translate the canonical UNION view."""
    lines = ["\\set ON_ERROR_STOP on", "BEGIN;", "CREATE EXTENSION postgres_fdw;",
             'CREATE SCHEMA "BPDATA";',
             "CREATE SERVER rffusion_bpdata FOREIGN DATA WRAPPER postgres_fdw OPTIONS "
             "(host '/var/run/postgresql', port '5432', dbname 'BPDATA', updatable 'false');",
             "GRANT USAGE ON FOREIGN SERVER rffusion_bpdata TO rffusion_view_owner;",
             "CREATE USER MAPPING FOR rffusion_view_owner SERVER rffusion_bpdata "
             "OPTIONS (user 'rffusion_fdw_reader', password_required 'false');"]
    for table in tables:
        columns = []
        for col in table["columns"]:
            value = quote(col["name"]) + " " + col["type"]
            if col["collation"]:
                value += " COLLATE rffusion_support." + quote(col["collation"])
            columns.append(value)
        lines.append(f'CREATE FOREIGN TABLE "BPDATA".{quote(table["name"])} (\n  ' + ",\n  ".join(columns) +
                     f"\n) SERVER rffusion_bpdata OPTIONS (schema_name 'BPDATA', table_name {literal(table['name'])}, updatable 'false');")
    source = (SOURCE / "createFusionSummaryDB.sql").read_text()
    view = source[source.index("CREATE ALGORITHM="):source.index(";\n\n\n-- The canonical")]
    view = re.sub(r"^CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW", "CREATE VIEW", view)
    view = re.sub(r"`([^`]+)`", lambda match: quote(match[1]), view)
    view = re.sub(r"SHA2\((COALESCE\([^\n]+?\)), 256\)",
                  lambda match: "encode(sha256(convert_to(" + match[1] + ", 'UTF8')), 'hex')", view)
    if "SHA2(" in view or view.count("UNION ALL") != 3:
        raise ValueError("Untranslated canonical view")
    lines += [view + ";",
              'GRANT USAGE ON SCHEMA "BPDATA", "RFFUSION_SUMMARY" TO rffusion_view_owner;',
              'GRANT CREATE ON SCHEMA "RFFUSION_SUMMARY" TO rffusion_view_owner;',
              'GRANT SELECT ON ALL TABLES IN SCHEMA "BPDATA" TO rffusion_view_owner;',
              'ALTER VIEW "RFFUSION_SUMMARY"."VW_ERROR_EVENT_CANONICAL" OWNER TO rffusion_view_owner;',
              'REVOKE CREATE ON SCHEMA "RFFUSION_SUMMARY" FROM rffusion_view_owner;', "COMMIT;", ""]
    return "\n".join(lines)


def verifier(domain: str, tables: list[dict], seeds: list[dict]) -> str:
    """Generate read-only per-attribute catalog and exact reference-value assertions."""
    payload = json.dumps(tables, ensure_ascii=False, separators=(",", ":"))
    lines = ["\\set ON_ERROR_STOP on", "BEGIN READ ONLY;", "SET LOCAL search_path = pg_catalog, public;",
             "DO $verify$\nDECLARE t jsonb; c jsonb; x jsonb; a record; rel oid; "
             "actual_default text; equal_default boolean; keys text[];\nBEGIN",
             f"FOR t IN SELECT value FROM jsonb_array_elements({literal(payload)}::jsonb) LOOP",
             f"rel := to_regclass(format('%I.%I', {literal(domain)}, t->>'name'));",
             "IF rel IS NULL THEN RAISE EXCEPTION 'Missing table %', t->>'name'; END IF;",
             "IF (SELECT relkind FROM pg_class WHERE oid=rel) <> 'r' THEN RAISE EXCEPTION 'Not a base table %', t->>'name'; END IF;",
             "IF (SELECT count(*) FROM pg_attribute WHERE attrelid=rel AND attnum>0 AND NOT attisdropped) <> jsonb_array_length(t->'columns') THEN RAISE EXCEPTION 'Column count %', t->>'name'; END IF;",
             "FOR c IN SELECT value FROM jsonb_array_elements(t->'columns') LOOP",
             "SELECT at.*, format_type(at.atttypid, at.atttypmod) typ, co.collname, co.collprovider, co.collisdeterministic, co.colllocale, pg_get_expr(d.adbin,d.adrelid) def "
             "INTO a FROM pg_attribute at LEFT JOIN pg_attrdef d ON d.adrelid=at.attrelid AND d.adnum=at.attnum "
             "LEFT JOIN pg_collation co ON co.oid=at.attcollation WHERE at.attrelid=rel AND at.attname=c->>'name';",
             "IF NOT FOUND OR a.attnum <> (c->>'position')::int OR a.typ <> replace(c->>'type','public.','') "
             "OR a.attnotnull = (c->>'nullable')::boolean THEN RAISE EXCEPTION 'Column attributes %.% expected %, observed %', t->>'name',c->>'name',c,row_to_json(a); END IF;",
             "IF c->>'collation' IS NOT NULL AND a.collname IS DISTINCT FROM c->>'collation' THEN RAISE EXCEPTION 'Collation %.%',t->>'name',c->>'name'; END IF;",
             "IF c->>'collation' IS NOT NULL AND (a.collprovider<>'i' OR a.collisdeterministic OR a.colllocale<>'und-u-ks-level1') THEN RAISE EXCEPTION 'ICU profile %.%',t->>'name',c->>'name'; END IF;",
             "actual_default := a.def;",
             "IF (c->>'default' IS NULL) <> (actual_default IS NULL) THEN RAISE EXCEPTION 'Default presence %.%',t->>'name',c->>'name'; END IF;",
             "IF actual_default IS NOT NULL THEN EXECUTE format('SELECT ((%s)::%s)::text COLLATE \"C\" IS NOT DISTINCT FROM ((%s)::%s)::text COLLATE \"C\"',"
             "actual_default,c->>'type',c->>'default',c->>'type') INTO equal_default; "
             "IF NOT equal_default THEN RAISE EXCEPTION 'Default %.%',t->>'name',c->>'name'; END IF; END IF;",
             "IF (c->>'generated')::boolean AND NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=rel AND tgname='a00_allocate' AND tgenabled='O' AND tgtype=23 "
             "AND tgfoid='rffusion_support.allocate_id()'::regprocedure AND tgnargs=3 "
             f"AND tgargs=convert_to(c->>'name','UTF8')||decode('00','hex')||convert_to({literal(domain)},'UTF8')||decode('00','hex')||convert_to(c->>'sequence','UTF8')||decode('00','hex') "
             "AND ARRAY(SELECT unnest(tgattr))=ARRAY[a.attnum]) THEN RAISE EXCEPTION 'ID trigger binding %',t->>'name'; END IF;",
             f"IF (c->>'generated')::boolean AND NOT EXISTS (SELECT 1 FROM pg_sequence s JOIN pg_class sq ON sq.oid=s.seqrelid JOIN pg_namespace ns ON ns.oid=sq.relnamespace JOIN pg_depend d ON d.objid=sq.oid AND d.refobjid=rel AND d.refobjsubid=a.attnum AND d.deptype='a' "
             f"WHERE ns.nspname={literal(domain)} AND sq.relname=c->>'sequence' AND s.seqincrement=1 AND NOT s.seqcycle AND s.seqmin=1 "
             "AND s.seqmax=CASE WHEN c->>'type'='bigint' THEN 9223372036854775807::bigint ELSE 2147483647 END) THEN RAISE EXCEPTION 'Sequence attributes %.%',t->>'name',c->>'name'; END IF;",
             "IF (c->>'on_update')::boolean AND NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=rel AND tgname='z90_updated' AND tgenabled='O' AND tgtype=19 "
             "AND tgfoid='rffusion_support.update_timestamp()'::regprocedure AND tgnargs=1 "
             "AND tgargs=convert_to(c->>'name','UTF8')||decode('00','hex')) THEN RAISE EXCEPTION 'Timestamp trigger binding %',t->>'name'; END IF;",
             "END LOOP;",
             "IF (SELECT count(*) FROM pg_constraint WHERE conrelid=rel AND contype='f')<>jsonb_array_length(t->'foreign_keys') THEN RAISE EXCEPTION 'Unexpected FK %',t->>'name'; END IF;",
             "IF (SELECT count(*) FROM pg_constraint WHERE conrelid=rel AND contype='u')<>(SELECT count(*) FROM jsonb_array_elements(t->'unique') u WHERE u->>'mechanism'='unique') THEN RAISE EXCEPTION 'Unexpected unique constraint %',t->>'name'; END IF;",
             "IF (SELECT count(*) FROM pg_index WHERE indrelid=rel)<>1+jsonb_array_length(t->'indexes')+(SELECT count(*) FROM jsonb_array_elements(t->'unique') u WHERE u->>'mechanism'='unique') THEN RAISE EXCEPTION 'Unexpected/missing index %',t->>'name'; END IF;",
             "SELECT array_agg(at.attname::text ORDER BY k.ordinality) INTO keys FROM pg_constraint p "
             "CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY k(num,ordinality) JOIN pg_attribute at ON at.attrelid=p.conrelid AND at.attnum=k.num WHERE p.conrelid=rel AND p.contype='p';",
             "IF to_jsonb(keys) IS DISTINCT FROM t->'primary_key' THEN RAISE EXCEPTION 'Primary key %',t->>'name'; END IF;",
             "FOR x IN SELECT value FROM jsonb_array_elements(t->'foreign_keys') LOOP",
             "IF NOT EXISTS (SELECT 1 FROM pg_constraint f WHERE f.conrelid=rel AND f.conname=x->>'name' AND f.contype='f' "
             f"AND f.confrelid=to_regclass(format('%I.%I',{literal(domain)},x->>'table')) "
             "AND f.confdeltype=CASE WHEN x->>'delete'='CASCADE' THEN 'c'::\"char\" ELSE 'a'::\"char\" END "
             "AND f.confupdtype='a' AND f.convalidated "
             "AND to_jsonb(ARRAY(SELECT at.attname::text FROM unnest(f.conkey) WITH ORDINALITY k(num,ord) JOIN pg_attribute at ON at.attrelid=rel AND at.attnum=k.num ORDER BY k.ord))=x->'columns' "
             "AND to_jsonb(ARRAY(SELECT at.attname::text FROM unnest(f.confkey) WITH ORDINALITY k(num,ord) JOIN pg_attribute at ON at.attrelid=f.confrelid AND at.attnum=k.num ORDER BY k.ord))=x->'references') THEN RAISE EXCEPTION 'Foreign key %.%',t->>'name',x->>'name'; END IF; END LOOP;",
             "FOR x IN SELECT value FROM jsonb_array_elements(t->'unique') LOOP",
             "IF x->>'mechanism'='host_guard' THEN "
             "IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=rel AND tgname='b10_identity' AND tgenabled='O' AND tgtype=31 "
             "AND tgfoid='rffusion_support.file_identity()'::regprocedure AND tgnargs=1 "
             "AND tgargs=convert_to(t->'primary_key'->>0,'UTF8')||decode('00','hex')) THEN RAISE EXCEPTION 'Identity guard binding %',t->>'name'; END IF; "
             "ELSE IF NOT EXISTS (SELECT 1 FROM pg_constraint u JOIN pg_index i ON i.indexrelid=u.conindid WHERE u.conrelid=rel AND u.conname=x->>'physical' AND u.contype='u' AND NOT i.indnullsnotdistinct AND "
             "to_jsonb(ARRAY(SELECT at.attname::text FROM unnest(u.conkey) WITH ORDINALITY k(num,ord) JOIN pg_attribute at ON at.attrelid=rel AND at.attnum=k.num ORDER BY k.ord))="
             "(SELECT jsonb_agg(m->'column') FROM jsonb_array_elements(x->'members') m)) THEN RAISE EXCEPTION 'Unique %.%',t->>'name',x->>'name'; END IF; END IF; END LOOP;",
             "FOR x IN SELECT value FROM jsonb_array_elements(t->'indexes') LOOP",
             "IF NOT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid JOIN pg_am am ON am.oid=ic.relam "
             "WHERE i.indrelid=rel AND ic.relname=x->>'physical' AND am.amname=x->>'kind' AND i.indisvalid AND i.indisready AND NOT i.indisunique "
             "AND regexp_replace(lower(substring(pg_get_indexdef(i.indexrelid) FROM 'USING [^ ]+ (.*)$')), '(::text|::regconfig|::character varying|[[:space:]\"()])', '', 'g') "
             "=regexp_replace(lower(x->>'expression'), '(::text|::regconfig|::character varying|[[:space:]\"()])', '', 'g')) THEN RAISE EXCEPTION 'Index %.%',t->>'name',x->>'name'; END IF; END LOOP;",
             "END LOOP;",
             f"IF (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname={literal(domain)} AND c.relkind='r')<>{len(tables)} THEN RAISE EXCEPTION 'Unexpected business tables'; END IF;",
             "END;\n$verify$;",
             "\\ir verifyTechnical.sql"]
    for seed in seeds:
        table = next(t for t in tables if t["name"] == seed["table"])
        fields = seed["columns"]
        rows = []
        with (SOURCE / seed["file"]).open(newline="") as stream:
            source_rows = list(csv.reader(stream))[1:]
        for row in source_rows:
            item = {}
            for field, value in zip(fields, row):
                col = next(c for c in table["columns"] if c["name"] == field)
                if field == "NA_COUNTY":
                    value = re.sub(r"[|\r\n\t]", "", value).strip(" ")
                item[field] = int(value) if col["type"] in ("integer", "bigint") else value
            rows.append(item)
        declarations = ", ".join(quote(field) + " " + next(c["type"] for c in table["columns"] if c["name"] == field) for field in fields)
        values = literal(json.dumps(rows, ensure_ascii=False, separators=(",", ":")))
        selection = ", ".join(quote(field) for field in fields)
        # Text is compared as bytes, independently of the approved search collation.
        compare = ", ".join(quote(field) + ' COLLATE "C"' if "character" in next(c["type"] for c in table["columns"] if c["name"] == field) else quote(field) for field in fields)
        lines.append("DO $seed$ BEGIN IF EXISTS (WITH expected AS (SELECT * FROM jsonb_to_recordset(" +
                     values + "::jsonb) AS e(" + declarations + ")), actual AS (SELECT " + selection + " FROM " +
                     quote(domain) + "." + quote(seed["table"]) + ") (SELECT " + compare + " FROM expected EXCEPT ALL SELECT " +
                     compare + " FROM actual) UNION ALL (SELECT " + compare + " FROM actual EXCEPT ALL SELECT " + compare +
                     " FROM expected)) THEN RAISE EXCEPTION 'Reference values differ: " + seed["table"] + "'; END IF; END; $seed$;")
    lines += ["COMMIT;", f"\\echo verified {domain}", ""]
    return "\n".join(lines)


def main() -> None:
    """Generate reproducible artifacts, or fail on any difference with --check."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    for file, digest in BASELINE.items():
        if hashlib.sha256((SOURCE / file).read_bytes()).hexdigest() != digest:
            raise SystemExit(f"Canonical baseline changed: {file}; re-evaluate conversion.")
    tables = []
    outputs = {}
    for file, domain in DOMAINS.items():
        domain_tables = parse_tables((SOURCE / file).read_text(), domain)
        tables.extend(domain_tables)
        outputs["sql/" + file] = create_domain(domain, domain_tables).encode()
    if len(tables) != 40 or sum(len(table["columns"]) for table in tables) != 451:
        raise SystemExit("Incomplete table/column inventory")
    seeds = []
    for file, table, fields in SEEDS:
        with (SOURCE / file).open(newline="") as stream:
            rows = list(csv.reader(stream))
        if any(len(row) != len(fields) for row in rows):
            raise SystemExit(f"Malformed reference CSV: {file}")
        seeds.append({"file": file, "table": table, "columns": fields, "rows": len(rows)-1})
        outputs["data/" + file] = (SOURCE / file).read_bytes()
    manifest = {"format": 1, "bootstrap_version": "1", "postgres_major": 18,
                "status": "generated; SQL/runtime proof pending; A1 unresolved",
                "sources": [{"file": file, "sha256": digest} for file, digest in BASELINE.items()],
                "tables": tables, "seeds": seeds,
                "view": {"database": "RFFUSION_SUMMARY", "name": "VW_ERROR_EVENT_CANONICAL", "columns": 13, "branches": 4},
                "canonical_business_routines": 0}
    outputs["sql/createCrossDatabaseView.sql"] = cross_database([t for t in tables if t["database"] == "BPDATA"]).encode()
    for domain in DOMAINS.values():
        outputs[f"sql/verify{domain}.sql"] = verifier(domain, [t for t in tables if t["database"] == domain],
                                                   seeds if domain == "RFDATA" else []).encode()
    support = (PACKAGE / "sql/createSupport.sql").read_text()
    functions = re.findall(r"CREATE FUNCTION rffusion_support\.(\w+)\(\).*?AS \$(\w+)\$(.*?)\$\2\$;", support, re.S)
    if len(functions) != 5:
        raise SystemExit("Incomplete technical function inventory")
    manifest["technical_objects"] = {
        "schema_per_database": "rffusion_support",
        "tables_per_database": ["id_guard", "identity_guard"],
        "collations_per_database": ["general_ci", "unicode_ci"],
        "functions_per_database": [
            {"name": function, "body_sha256": hashlib.sha256(body.encode()).hexdigest()}
            for function, _, body in functions
        ],
        "extensions": {"RFDATA": ["postgis"], "RFFUSION_SUMMARY": ["postgres_fdw"]},
        "foreign_tables": [{"database": "RFFUSION_SUMMARY", "schema": "BPDATA", "name": table["name"]}
                           for table in tables if table["database"] == "BPDATA"],
        "trigger_functions": {"a00_allocate": "allocate_id", "z90_updated": "update_timestamp",
                              "b00_role": "normalize_role", "b10_identity": "file_identity"},
    }
    outputs["sql/object-map.json"] = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode()
    technical = ["\\set ON_ERROR_STOP on", "DO $technical$ DECLARE rec record; BEGIN"]
    for function, _, body in functions:
        digest = hashlib.sha256(body.encode()).hexdigest()
        technical += [
            f"SELECT p.* INTO rec FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='rffusion_support' AND p.proname={literal(function)} AND p.pronargs=0;",
            f"IF NOT FOUND OR encode(sha256(convert_to(rec.prosrc,'UTF8')),'hex')<>{literal(digest)} "
            "OR rec.proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(rec.proacl) acl WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE') "
            f"THEN RAISE EXCEPTION 'Technical function missing, changed or public: {function}'; END IF;",
        ]
    technical += [
        "IF has_schema_privilege('rffusion_appcataloga','rffusion_support','CREATE') OR has_schema_privilege('rffusion_webfusion','rffusion_support','CREATE') "
        "OR has_table_privilege('rffusion_appcataloga','rffusion_support.id_guard','SELECT,INSERT,UPDATE,DELETE') "
        "OR has_table_privilege('rffusion_webfusion','rffusion_support.identity_guard','SELECT,INSERT,UPDATE,DELETE') "
        "THEN RAISE EXCEPTION 'Application can bypass technical guards'; END IF;",
        "END; $technical$;", "",
    ]
    outputs["sql/verifyTechnical.sql"] = "\n".join(technical).encode()
    static_sql = {str(path.relative_to(PACKAGE)): path.read_bytes()
                  for path in (PACKAGE / "sql").glob("*.sql")
                  if str(path.relative_to(PACKAGE)) not in outputs}
    checksummed = {**static_sql, **outputs}
    outputs["snapshot.sha256"] = "".join(
        hashlib.sha256(data).hexdigest() + "  " + file + "\n"
        for file, data in sorted(checksummed.items())
    ).encode()
    for file, data in outputs.items():
        path = PACKAGE / file
        if args.check:
            if not path.is_file() or path.read_bytes() != data:
                raise SystemExit(f"Snapshot differs: {file}")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    print(f"Snapshot {'checked' if args.check else 'generated'}: 40 tables, 451 columns, {sum(s['rows'] for s in seeds)} reference rows; no SQL executed.")


if __name__ == "__main__":
    main()
