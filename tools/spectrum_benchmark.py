#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spectrum query benchmark para RFEye002129.
Usage:
    /opt/conda/envs/appdata/bin/python /RFFusion/tools/spectrum_benchmark.py
"""
import sys, time
import mysql.connector

DB = {"host":"10.88.0.33","port":3306,"user":"root","password":"changeme","database":"RFDATA"}

def new_conn():
    c = mysql.connector.connect(**DB)
    c.autocommit = True
    return c

def qry(c, sql, params=()):
    cur = c.cursor(dictionary=True)
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    cur.close()
    return rows

def qone(c, sql, params=()):
    r = qry(c, sql, params)
    return r[0] if r else {}

def discover(c):
    eq = qone(c,"SELECT ID_EQUIPMENT,NA_EQUIPMENT FROM DIM_SPECTRUM_EQUIPMENT WHERE NA_EQUIPMENT LIKE %s LIMIT 1",("%RFEye002129%",))
    if not eq: print("ERRO: RFEye002129 não encontrada"); sys.exit(1)
    eid = eq["ID_EQUIPMENT"]
    print(f"\n{'='*70}\nEquipamento : {eq['NA_EQUIPMENT']}  (ID={eid})")
    sites = qry(c,"SELECT DISTINCT f.FK_SITE, s.NA_SITE FROM FACT_SPECTRUM f JOIN DIM_SPECTRUM_SITE s ON s.ID_SITE=f.FK_SITE WHERE f.FK_EQUIPMENT=%s LIMIT 5",(eid,))
    print(f"Sites amostrados : {[(r['FK_SITE'],r['NA_SITE']) for r in sites]}")
    st = qone(c,"SELECT MIN(DT_TIME_START) AS dt0,MAX(DT_TIME_END) AS dt1,COUNT(*) AS total,MIN(NU_FREQ_START) AS f0,MAX(NU_FREQ_END) AS f1 FROM FACT_SPECTRUM WHERE FK_EQUIPMENT=%s",(eid,))
    print(f"FACT_SPECTRUM rows: {st['total']:,}\nPeríodo: {st['dt0']} → {st['dt1']}\nFreq: {st['f0']} – {st['f1']} MHz")
    descs = [r["NA_DESCRIPTION"] for r in qry(c,"SELECT DISTINCT NA_DESCRIPTION FROM FACT_SPECTRUM WHERE FK_EQUIPMENT=%s AND NA_DESCRIPTION IS NOT NULL LIMIT 5",(eid,))]
    print(f"Descriptions: {descs}")
    ds = str(st["dt0"])[:10] if st["dt0"] else "2025-01-01"
    from datetime import date,timedelta
    d1 = min(date.fromisoformat(str(st["dt1"])[:10]), date.fromisoformat(ds)+timedelta(days=60))
    de = str(d1)
    return {"equipment_id":eid,"site_id":sites[0]["FK_SITE"] if sites else None,
            "date_start":ds,"date_end":de,
            "freq_start":float(st["f0"]) if st["f0"] else 70.0,
            "freq_end":float(st["f0"])+200.0 if st["f0"] else 270.0,
            "description":descs[0][:8] if descs else None}

LOC_SQL="""TRIM(CONCAT(COALESCE(NULLIF(s.NA_SITE,''),NULLIF(d.NA_DISTRICT,''),c.NA_COUNTY,CONCAT('Site ',s.ID_SITE)),CASE WHEN c.NA_COUNTY IS NOT NULL AND (s.NA_SITE IS NULL OR s.NA_SITE='' OR NOT (COALESCE(CONVERT(s.NA_SITE USING utf8mb4) COLLATE utf8mb4_unicode_ci,'') <=> COALESCE(CONVERT(c.NA_COUNTY USING utf8mb4) COLLATE utf8mb4_unicode_ci,''))) THEN CONCAT(' (',c.NA_COUNTY,CASE WHEN st.LC_STATE IS NOT NULL THEN CONCAT('/',st.LC_STATE) ELSE '' END,')') WHEN st.LC_STATE IS NOT NULL THEN CONCAT('/',st.LC_STATE) ELSE '' END,''))"""

def _clauses(eid,site,ds,de,fs,fe,desc,alias="f"):
    cl=[f"{alias}.FK_EQUIPMENT = %s"]; pa=[eid]
    if site: cl.append(f"{alias}.FK_SITE = %s"); pa.append(site)
    if ds:   cl.append(f"{alias}.DT_TIME_END >= %s"); pa.append(ds)
    if de:   cl.append(f"{alias}.DT_TIME_START <= %s"); pa.append(de+" 23:59:59")
    if fs is not None: cl.append(f"{alias}.NU_FREQ_END >= %s"); pa.append(fs)
    if fe is not None: cl.append(f"{alias}.NU_FREQ_START <= %s"); pa.append(fe)
    if desc: cl.append(f"{alias}.NA_DESCRIPTION LIKE %s"); pa.append(f"%{desc}%")
    return cl, pa

def q_exists(eid,site=None,ds=None,de=None,fs=None,fe=None,desc=None,loc=False,limit=None):
    cl,pa = _clauses(eid,site,ds,de,fs,fe,desc,"match_f")
    msql = " AND ".join(cl)
    exists = f"EXISTS (SELECT 1 FROM BRIDGE_SPECTRUM_FILE match_b JOIN FACT_SPECTRUM match_f ON match_f.ID_SPECTRUM=match_b.FK_SPECTRUM WHERE match_b.FK_FILE=repos.ID_FILE AND {msql})"
    if loc:
        inner=f"""SELECT repos.ID_FILE,MIN(all_f.DT_TIME_START) dt0,MAX(all_f.DT_TIME_END) dt1,
            COUNT(DISTINCT all_f.ID_SPECTRUM) nu_spec,
            GROUP_CONCAT(DISTINCT {LOC_SQL} ORDER BY {LOC_SQL} SEPARATOR '||') locs
            FROM DIM_SPECTRUM_FILE repos
            JOIN BRIDGE_SPECTRUM_FILE all_b ON all_b.FK_FILE=repos.ID_FILE
            JOIN FACT_SPECTRUM all_f ON all_f.ID_SPECTRUM=all_b.FK_SPECTRUM
            JOIN DIM_SPECTRUM_SITE s ON s.ID_SITE=all_f.FK_SITE
            LEFT JOIN DIM_SITE_DISTRICT d ON d.ID_DISTRICT=s.FK_DISTRICT
            LEFT JOIN DIM_SITE_COUNTY c ON c.ID_COUNTY=s.FK_COUNTY
            LEFT JOIN DIM_SITE_STATE st ON st.ID_STATE=s.FK_STATE
            WHERE repos.NA_VOLUME='reposfi' AND {exists}
            GROUP BY repos.ID_FILE"""
    else:
        inner=f"""SELECT repos.ID_FILE,MIN(all_f.DT_TIME_START) dt0,MAX(all_f.DT_TIME_END) dt1,
            COUNT(DISTINCT all_f.ID_SPECTRUM) nu_spec
            FROM DIM_SPECTRUM_FILE repos
            JOIN BRIDGE_SPECTRUM_FILE all_b ON all_b.FK_FILE=repos.ID_FILE
            JOIN FACT_SPECTRUM all_f ON all_f.ID_SPECTRUM=all_b.FK_SPECTRUM
            WHERE repos.NA_VOLUME='reposfi' AND {exists}
            GROUP BY repos.ID_FILE"""
    suffix = f"ORDER BY dt1 DESC LIMIT {limit}" if limit else ""
    q = f"SELECT * FROM ({inner}) t {suffix}" if limit else f"SELECT COUNT(*) AS total FROM ({inner}) t"
    return q, pa

def q_join(eid,site=None,ds=None,de=None,fs=None,fe=None,desc=None,limit=None):
    cl,pa = _clauses(eid,site,ds,de,fs,fe,desc,"f")
    where = " AND ".join(cl)
    inner=f"""SELECT DISTINCT b.FK_FILE id
        FROM FACT_SPECTRUM f
        JOIN BRIDGE_SPECTRUM_FILE b ON b.FK_SPECTRUM=f.ID_SPECTRUM
        JOIN DIM_SPECTRUM_FILE r ON r.ID_FILE=b.FK_FILE AND r.NA_VOLUME='reposfi'
        WHERE {where}"""
    if limit:
        q=f"""SELECT m.id,MIN(af.DT_TIME_START) dt0,MAX(af.DT_TIME_END) dt1,COUNT(DISTINCT af.ID_SPECTRUM) nu_spec
            FROM ({inner}) m
            JOIN BRIDGE_SPECTRUM_FILE ab ON ab.FK_FILE=m.id
            JOIN FACT_SPECTRUM af ON af.ID_SPECTRUM=ab.FK_SPECTRUM
            GROUP BY m.id ORDER BY dt1 DESC LIMIT {limit}"""
    else:
        q=f"SELECT COUNT(*) AS total FROM ({inner}) t"
    return q, pa

def run(c,label,q,params,explain=False):
    t0=time.perf_counter()
    rows=qry(c,q,params)
    ms=(time.perf_counter()-t0)*1000
    s=rows[0] if rows else {}
    print(f"  {ms:8.1f} ms  |  {label}")
    if s: print(f"           → {s}")
    if explain:
        for r in qry(c,"EXPLAIN "+q,params):
            print(f"    id={r.get('id')} type={str(r.get('type')):10} table={str(r.get('table')):30} key={r.get('key')} rows={r.get('rows')} extra={r.get('Extra')}")
    return ms

def show_sizes(c,eid):
    print(f"\n{'='*70}\nCardinalidade:\n")
    for tbl,w,p in [
        ("FACT_SPECTRUM","FK_EQUIPMENT=%s",(eid,)),
        ("FACT_SPECTRUM","1=1",()),
        ("BRIDGE_SPECTRUM_FILE","1=1",()),
        ("DIM_SPECTRUM_FILE","NA_VOLUME='reposfi'",()),
    ]:
        n=qone(c,f"SELECT COUNT(*) AS n FROM {tbl} WHERE {w}",p).get("n",0)
        print(f"  {tbl:35s}  WHERE {w:38s} → {n:,}")

def show_indexes(c):
    print(f"\n{'='*70}\nÍndices:\n")
    for tbl in ("FACT_SPECTRUM","BRIDGE_SPECTRUM_FILE","DIM_SPECTRUM_FILE"):
        rows=qry(c,f"SHOW INDEX FROM {tbl}")
        print(f"  {tbl}:")
        for r in rows:
            u="" if r.get("Non_unique")==0 else " NON-UNIQUE"
            print(f"    [{r['Key_name']}] column={r['Column_name']} seq={r['Seq_in_index']}{u}")
        print()

def check_idx(c):
    print(f"\n{'='*70}")
    n=qone(c,"SELECT COUNT(*) AS n FROM information_schema.statistics WHERE table_schema='RFDATA' AND table_name='FACT_SPECTRUM' AND index_name='IX_FACT_SPECTRUM_WEBFUSION_SEARCH'").get("n",0)
    print(f"Índice IX_FACT_SPECTRUM_WEBFUSION_SEARCH: {'✓ EXISTE' if n else '✗ NÃO existe'}")

def benchmark(c,info):
    eq=info["equipment_id"]; site=info["site_id"]
    ds=info["date_start"];   de=info["date_end"]
    fs=info["freq_start"];   fe=info["freq_end"]
    desc=info["description"]
    CASES=[
        ("só equipamento  [sem loc]",  dict(eid=eq)),
        ("só equipamento  [com loc]",  dict(eid=eq,loc=True)),
        ("+ site_id       [sem loc]",  dict(eid=eq,site=site)),
        ("+ site_id       [com loc]",  dict(eid=eq,site=site,loc=True)),
        ("+ data (60d)    [sem loc]",  dict(eid=eq,ds=ds,de=de)),
        ("+ data (60d)    [com loc]",  dict(eid=eq,ds=ds,de=de,loc=True)),
        ("+ frequencia    [sem loc]",  dict(eid=eq,fs=fs,fe=fe)),
        ("+ frequencia    [com loc]",  dict(eid=eq,fs=fs,fe=fe,loc=True)),
        ("+ description   [sem loc]",  dict(eid=eq,desc=desc)),
        ("+ description   [com loc]",  dict(eid=eq,desc=desc,loc=True)),
        ("data+site       [sem loc]",  dict(eid=eq,site=site,ds=ds,de=de)),
        ("data+freq+desc  [sem loc]",  dict(eid=eq,ds=ds,de=de,fs=fs,fe=fe,desc=desc)),
        ("data+freq+desc  [com loc]",  dict(eid=eq,ds=ds,de=de,fs=fs,fe=fe,desc=desc,loc=True)),
        ("todos filtros   [sem loc]",  dict(eid=eq,site=site,ds=ds,de=de,fs=fs,fe=fe,desc=desc)),
        ("todos filtros   [com loc]",  dict(eid=eq,site=site,ds=ds,de=de,fs=fs,fe=fe,desc=desc,loc=True)),
    ]

    print(f"\n{'='*70}\nBLOCO A – COUNT via EXISTS duplo (implementação atual)\n")
    ra={}
    for label,kw in CASES:
        q,p=q_exists(**kw)
        ra[label]=run(c,label,q,p)

    print(f"\n{'='*70}\nBLOCO B – COUNT via DISTINCT JOIN (alternativa)\n")
    rb={}
    for label,kw in CASES:
        kw2={k:v for k,v in kw.items() if k!="loc"}
        q,p=q_join(**kw2)
        rb[label]=run(c,label,q,p)

    print(f"\n{'='*70}\nBLOCO C – paginação LIMIT 50 via JOIN (1ª página)\n")
    for label,kw in CASES:
        kw2={k:v for k,v in kw.items() if k!="loc"}
        q,p=q_join(**kw2,limit=50)
        run(c,label,q,p)

    print(f"\n{'='*70}\nBLOCO D – EXPLAIN consultas críticas\n")
    for label,kw in [
        ("só equipamento  [sem loc]", dict(eid=eq)),
        ("só equipamento  [com loc]", dict(eid=eq,loc=True)),
        ("+ data (60d)    [sem loc]", dict(eid=eq,ds=ds,de=de)),
        ("+ data (60d)    [com loc]", dict(eid=eq,ds=ds,de=de,loc=True)),
        ("+ frequencia    [sem loc]", dict(eid=eq,fs=fs,fe=fe)),
    ]:
        q,p=q_exists(**kw)
        print(f"\n  {label}")
        for r in qry(c,"EXPLAIN "+q,p):
            print(f"    id={r.get('id')} type={str(r.get('type')):10} table={str(r.get('table')):30} key={r.get('key')} rows={r.get('rows')} extra={r.get('Extra')}")

    print(f"\n{'='*70}\nCOMPARATIVO A (EXISTS) vs B (JOIN)\n")
    print(f"  {'Caso':<38} {'A-EXISTS':>10} {'B-JOIN':>10} {'Ganho':>10}")
    print("  "+"-"*72)
    for label,_ in CASES:
        a=ra.get(label,0); b=rb.get(label,0); g=a-b
        print(f"  {label:<38} {a:>10.1f} {b:>10.1f} {'→ +:'+str(round(g)):>10}")

    print(f"\n{'='*70}\nSCAN BASE (custo mínimo de acesso):\n")
    for label,q,p in [
        ("FK_EQUIPMENT only",
         "SELECT COUNT(*) AS n FROM FACT_SPECTRUM WHERE FK_EQUIPMENT=%s",(eq,)),
        ("FK_EQUIPMENT + date range",
         "SELECT COUNT(*) AS n FROM FACT_SPECTRUM WHERE FK_EQUIPMENT=%s AND DT_TIME_END>=%s AND DT_TIME_START<=%s",(eq,ds,de+" 23:59:59")),
        ("DISTINCT files via bridge",
         "SELECT COUNT(DISTINCT b.FK_FILE) AS n FROM FACT_SPECTRUM f JOIN BRIDGE_SPECTRUM_FILE b ON b.FK_SPECTRUM=f.ID_SPECTRUM JOIN DIM_SPECTRUM_FILE r ON r.ID_FILE=b.FK_FILE AND r.NA_VOLUME='reposfi' WHERE f.FK_EQUIPMENT=%s",(eq,)),
    ]:
        run(c,label,q,p,explain=True)

if __name__=="__main__":
    print("Conectando ao banco de dados…")
    c=new_conn()
    try:
        info=discover(c)
        show_sizes(c,info["equipment_id"])
        show_indexes(c)
        check_idx(c)
        benchmark(c,info)
    finally:
        c.close()
    print(f"\n{'='*70}\nBenchmark concluído.")
