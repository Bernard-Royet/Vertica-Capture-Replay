#!/usr/bin/env python3
r"""
vertica_replay.py - Capture queries from Vertica's Data Collector and replay them.

WORKFLOW (two steps, so the workload can be reviewed/archived before it runs)
  1) extract : v_internal.dc_requests_issued (source DB)  ->  workload.jsonl
  2) replay  : workload.jsonl  ->  target DB, plus a CSV comparison report

USAGE
  vertica_replay.py extract -H HOST -d DB -u USER --since TS --until TS -o FILE [options]
  vertica_replay.py replay  -H HOST -d DB -u USER -i FILE [-r REPORT] [options]
  vertica_replay.py {extract|replay} -h        show all options of one command

CONNECTION OPTIONS (both commands)
  -H, --host HOST        Vertica host (required)
  -P, --port PORT        port (default 5433)
  -d, --database DB      database name (required)
  -u, --user USER        login user (required). Password: VERTICA_PASSWORD env var
  --tls                  require TLS
  --label LABEL          session label for this tool's connections (default 'replay')
  --debug                verbose trace on stderr (see DEBUG MODE)
  --debug-file FILE      also write the trace to FILE (implies --debug)

EXTRACT OPTIONS
  --since TS             start of capture window, e.g. '2026-10-07 08:00:00' (required)
  --until TS             end of capture window, exclusive (required)
  -o, --output FILE      JSONL output file (required)
  --types LIST           request_type values to capture, comma-separated
                         (default ALL = every request_type, no filter; to restrict,
                         e.g. QUERY or QUERY,DDL,LOAD,UTILITY,TXN,SET,...)
  --source-user USER     capture only statements issued by this user
  --exclude-user USER    drop statements from this user; repeatable
                         (monitoring/service accounts)
  --exclude-regex REGEX  drop statements matching this Vertica regex
                         (case-insensitive); repeatable
  --include-system       keep the internal/system statements that are excluded by
                         default (see below)
  --no-duration          do not join v_monitor.query_requests: skips the original
                         duration/success (use if that view is not accessible)
  --summary-file FILE    where to save the end-of-run summary
                         (default <output>.summary.txt; a .json twin is also written)

  End-of-run summary (printed, and saved as .txt and .json): command line and all
  effective flags (defaults included), source, window, filters and the exclusion
  regexes applied, candidates vs captured (what the filters removed), first/last
  statement, sessions, users, request types, read-only vs non-read-only counts,
  original-duration coverage and stats, output size and SHA-256, and warnings.

  Excluded by default: statements referencing v_catalog., v_monitor. or
  v_internal. (vsql \d, BI-tool metadata scans, monitoring), keepalives such as
  SELECT 1 / version() / current_*(), and anything touching dc_requests_issued.
  Order of the capture: time, session_id, statement_id, request_id.
  Each JSONL record: time, node_name, session_id, user_name, transaction_id,
  statement_id, request_id, request_type, request, request_duration_ms, success.

REPLAY OPTIONS
  -i, --input FILE       JSONL produced by extract (required)
  -r, --report FILE      CSV report (default replay_report.csv)
  --mode {serial,session}
                         serial  (default): one connection, global order.
                         session: one connection per original session_id, run in
                         parallel; order kept within each session.
  --max-concurrency N    session mode: max simultaneous connections
                         (default 0 = one per session). If lower than the number of
                         sessions, later sessions wait for a free slot, so overlap
                         is lower than the original.
  --respect-timing       reproduce the original time offsets between statements
                         (without it: serial runs back to back; session mode starts
                         all sessions at once, i.e. a stress test)
  --speed X              with --respect-timing: 2.0 = twice as fast (default 1.0)
  --max-rows N           stop fetching after N rows per query (default 0 = all)
  --allow-dml            also run non-SELECT statements. DANGEROUS: by default only
                         SELECT/WITH run and everything else is reported SKIPPED
  --impersonate          run each statement as its original user using
                         SET SESSION AUTHORIZATION (needs a privileged replay
                         account; the users must exist on the target)
  --server-timing        tag each statement with a leading comment, then read its
                         exact server-side duration from v_monitor.query_requests
                         on the target and add server_duration_ms / server_ratio
  --server-timing-wait S seconds to wait before reading that view (default 3.0)
  --stop-on-error        stop at the first failing statement (default: continue)
  --dry-run              print what would run (with the original user), execute nothing

DEBUG MODE (--debug / --debug-file, both commands)
  Timestamped, thread-tagged trace; the password is never logged. It shows:
    extract : the exact SQL and bind parameters sent to the source database
    replay  : effective settings and tag; connections (settings, connect time);
              statements skipped and why; waits for --respect-timing and
              "LATE" warnings when replay falls behind the original schedule;
              impersonation switches, with a check of the effective user;
              every statement (session, user, original duration, full SQL,
              execute vs total time, rows, whether --max-rows cut the fetch);
              errors with SQLSTATE and traceback; session start/end in session
              mode; --server-timing rows read back and the unmatched seq numbers
  With --impersonate, debug mode runs one extra SELECT CURRENT_USER() per switch.
  Tip: use --debug-file for large runs and grep by seq=N or by thread name.

REPORT COLUMNS (CSV; sort by seq for the original order)
  seq, original_time, session_id, original_user, statement_id,
  status (OK | ERROR | SKIPPED | ABORTED),
  original_duration_ms   server-side duration at capture
  elapsed_ms             replay time, client-side (execute + fetch)
  ratio                  elapsed_ms / original_duration_ms (>1 = slower on replay)
  original_rows, rows, original_success, error, request (first 500 chars)
  With --server-timing, two more columns:
  server_duration_ms     server-side duration of the replayed statement
  server_ratio           server_duration_ms / original_duration_ms (like-for-like)
  The end-of-run summary prints counts per status plus total/median/p95 ratios.

EXAMPLES
  export VERTICA_PASSWORD=...

  # 1. capture one hour of production activity, minus monitoring noise
  python vertica_replay.py extract -H prod-host -d prod -u dbadmin \
      --since "2026-10-07 08:00:00" --until "2026-10-07 09:00:00" \
      --exclude-user nagios_mon -o workload.jsonl

  # 2. check what would run
  python vertica_replay.py replay -H test-host -d test -u replayer \
      -i workload.jsonl --dry-run

  # 3a. strict global order, original pacing
  python vertica_replay.py replay -H test-host -d test -u replayer \
      -i workload.jsonl -r report.csv --respect-timing

  # 3b. reproduce concurrency, original users, like-for-like server timings
  python vertica_replay.py replay -H test-host -d test -u replay_admin \
      -i workload.jsonl -r report.csv --mode session --respect-timing \
      --max-concurrency 100 --impersonate --server-timing

NOTES
  * Requirements: pip install vertica-python.
  * extract needs access to v_internal DC tables (typically dbadmin or a granted
    role) and, for durations, v_monitor.query_requests. DC/monitoring retention
    is limited: older statements may have no duration.
  * Never replay writes on production. Run on a non-production cluster with
    comparable data, and check resource pools before high-concurrency runs.
  * All request types are captured by default, but replay runs only SELECT/WITH
    unless --allow-dml: SET, SHOW, TXN (BEGIN/COMMIT), DDL, LOAD, ... are reported
    SKIPPED. Replayed sessions therefore start clean: session settings, search
    path and open transactions are not reproduced. Use --types QUERY if you only
    want the queries in the file.
  * elapsed_ms includes client fetch/network time; the original duration is
    server-side. Use --server-timing for an exact comparison.
  * --server-timing adds a comment prefix to the replayed SQL text.
"""
import argparse
import csv
import hashlib
import json
import logging
import os
import re
import shlex
import statistics
import sys
import threading
import time
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import vertica_python

_LEADING_NOISE = re.compile(r"^(?:\s+|--[^\n]*\n|/\*.*?\*/)+", re.S)

# Patterns (Vertica REGEXP_ILIKE, case-insensitive) for internal/system noise.
# Applied at extract time unless --include-system is given.
SYSTEM_EXCLUDE_PATTERNS = [
    # anything touching the Vertica system schemas
    r"(^|[^a-z0-9_])(v_catalog|v_monitor|v_internal)[[:space:]]*\.",
    # driver/tool keepalives and handshakes: SELECT 1, version(), current_*()
    r"^[[:space:]]*select[[:space:]]+(1|version\(\)|current_[a-z_]+(\(\))?)"
    r"[[:space:]]*;?[[:space:]]*$",
]


def connection_info(a):
    info = {
        "host": a.host,
        "port": a.port,
        "database": a.database,
        "user": a.user,
        "password": os.environ.get("VERTICA_PASSWORD", ""),
        "autocommit": True,
        "connection_timeout": 30,
        "session_label": a.label,
    }
    if a.tls:
        info["tlsmode"] = "require"
    return info


log = logging.getLogger("vreplay")
log.propagate = False


def setup_logging(a):
    """--debug: verbose trace on stderr (and --debug-file). Otherwise silent."""
    if not (a.debug or a.debug_file):
        log.addHandler(logging.NullHandler())
        return
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)-5s [%(threadName)s] %(message)s",
        "%H:%M:%S")
    handlers = [logging.StreamHandler(sys.stderr)]
    if a.debug_file:
        handlers.append(logging.FileHandler(a.debug_file, mode="w", encoding="utf-8"))
    for h in handlers:
        h.setFormatter(fmt)
        log.addHandler(h)


def one_line(sql, width=2000):
    sql = " ".join(sql.split())
    return sql if len(sql) <= width else sql[:width] + f"...(+{len(sql) - width} chars)"


def open_connection(a):
    info = connection_info(a)
    safe = {k: v for k, v in info.items() if k != "password"}
    log.debug("connecting %s password=%s", safe, "set" if info["password"] else "EMPTY")
    t = time.monotonic()
    conn = vertica_python.connect(**info)
    log.debug("connected in %.1f ms", (time.monotonic() - t) * 1000)
    return conn


def is_read_only(sql):
    s = _LEADING_NOISE.sub("", sql).lstrip("( \t\r\n").lower()
    return s.startswith(("select", "with"))


# --------------------------------------------------------------------------
# extract
# --------------------------------------------------------------------------
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def count_candidates(conn, a, types):
    """Statements in the window/types (and --source-user) BEFORE the exclusion
    filters, to report how many the filters removed. None if the count fails."""
    sql = ("SELECT COUNT(*) FROM v_internal.dc_requests_issued i "
           "WHERE i.time >= %s AND i.time < %s")
    params = [a.since, a.until]
    if types:  # empty list = all request types
        sql += " AND i.request_type IN (" + ",".join(["%s"] * len(types)) + ")"
        params += types
    if a.source_user:
        sql += " AND i.user_name = %s"
        params.append(a.source_user)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return int(cur.fetchone()[0])
    except Exception as e:  # summary only: never fail the capture over it
        log.debug("candidate count failed: %s", e)
        return None


def format_extract_summary(d):
    L = []
    add = L.append
    add("=" * 72)
    add("EXTRACT SUMMARY")
    add("=" * 72)
    add("Run")
    add(f"  started   : {d['started_utc']}")
    add(f"  finished  : {d['finished_utc']}  (elapsed {d['elapsed_s']}s)")
    add(f"  source    : {d['source']['user']}@{d['source']['host']}:"
        f"{d['source']['port']}/{d['source']['database']}")
    add(f"  command   : {d['command_line']}")
    add("")
    add("Flags (effective values, including defaults)")
    for k, v in d["flags"].items():
        add(f"  --{k.replace('_', '-'):<20} {v!r}")
    add("")
    add("Capture scope")
    add(f"  window          : {d['window']['since']}  ->  {d['window']['until']} (exclusive)")
    add(f"  request types   : {', '.join(d['request_types'])}")
    f = d["filters"]
    add(f"  system filter   : {'OFF (--include-system)' if f['include_system'] else 'ON'}")
    add(f"  source user     : {f['source_user'] or '(any)'}")
    add(f"  excluded users  : {', '.join(f['exclude_users']) or '(none)'}")
    add(f"  duration join   : {'OFF (--no-duration)' if f['no_duration'] else 'v_monitor.query_requests'}")
    add("  exclusion regexes applied:")
    for pat in f["regexes"] or ["(none)"]:
        add(f"    {pat}")
    add("")
    r = d["results"]
    add("Results")
    add(f"  candidates before exclusion filters : {r['candidates']}")
    add(f"  statements captured                 : {r['captured']}")
    if r["candidates"] is not None:
        add(f"  removed by exclusion filters        : {r['candidates'] - r['captured']}")
    add(f"  first / last statement              : {r['first_time']} / {r['last_time']}")
    add(f"  distinct sessions / users           : {r['sessions']} / {len(r['by_user'])}")
    add(f"  read-only (replayed by default)     : {r['read_only']}")
    add(f"  non-read-only (skipped by replay)   : {r['non_read_only']}")
    add("  by request_type : " + ", ".join(f"{k}={v}" for k, v in r["by_type"].items()))
    add("  top users       : " + ", ".join(f"{k}={v}" for k, v in list(r["by_user"].items())[:10]))
    add("")
    add("Original durations (v_monitor.query_requests)")
    dd = r["durations"]
    add(f"  with duration   : {dd['count']} of {r['captured']}"
        f"  (missing: {r['captured'] - dd['count']})")
    if dd["count"]:
        add(f"  total {dd['total_s']}s | median {dd['median_ms']}ms | "
            f"p95 {dd['p95_ms']}ms | max {dd['max_ms']}ms")
    add(f"  recorded as failed originally: {r['failed']}")
    add("")
    add("Output")
    add(f"  file    : {d['output']['file']}")
    add(f"  size    : {d['output']['bytes']} bytes")
    add(f"  sha256  : {d['output']['sha256']}")
    if d["warnings"]:
        add("")
        add("Warnings")
        for w in d["warnings"]:
            add(f"  - {w}")
    add("=" * 72)
    return "\n".join(L)


def cmd_extract(a):
    started = datetime.now(timezone.utc)
    types = [t.strip().upper() for t in a.types.split(",") if t.strip()]
    if not types or "ALL" in types:
        types = []  # no request_type filter: capture every type
    type_clause = (f"AND i.request_type IN ({','.join(['%s'] * len(types))})"
                   if types else "")
    # dc_requests_issued has no duration. It is exposed by the documented view
    # v_monitor.query_requests (request_duration_ms, success), keyed by
    # node/session/transaction/statement/request. LEFT JOIN so statements
    # without a completion row (still running, aged out of retention) are kept
    # with a null duration. --no-duration skips the join entirely.
    if a.no_duration:
        extra_cols, join = "NULL, NULL", ""
    else:
        extra_cols = "c.request_duration_ms, c.success"
        join = """LEFT JOIN v_monitor.query_requests c
          ON  c.node_name  = i.node_name
          AND c.session_id = i.session_id
          AND COALESCE(c.transaction_id, -1) = COALESCE(i.transaction_id, -1)
          AND COALESCE(c.statement_id, -1)   = COALESCE(i.statement_id, -1)
          AND c.request_id = i.request_id"""
    sql = f"""
        SELECT i.time, i.node_name, i.session_id, i.user_name, i.transaction_id,
               i.statement_id, i.request_id, i.request_type, i.request,
               {extra_cols}
        FROM v_internal.dc_requests_issued i
        {join}
        WHERE i.time >= %s AND i.time < %s
          {type_clause}
          AND i.request NOT ILIKE %s
    """
    params = [a.since, a.until] + types + ["%dc_requests_issued%"]
    if a.source_user:
        sql += " AND i.user_name = %s"
        params.append(a.source_user)
    for u in a.exclude_user or []:
        sql += " AND i.user_name <> %s"
        params.append(u)
    patterns = [] if a.include_system else list(SYSTEM_EXCLUDE_PATTERNS)
    patterns += a.exclude_regex or []
    for pat in patterns:
        sql += " AND NOT REGEXP_ILIKE(i.request, %s)"
        params.append(pat)
    # Deterministic order: time first, then tie-breakers
    sql += " ORDER BY i.time, i.session_id, i.statement_id, i.request_id"

    log.debug("extract SQL: %s", one_line(sql, 4000))
    log.debug("extract params: %r", params)
    count = 0
    by_user, by_type, sessions = Counter(), Counter(), set()
    durs, failed, read_only, first_t, last_t = [], 0, 0, None, None
    with open_connection(a) as conn, \
            open(a.output, "w", encoding="utf-8") as out:
        candidates = count_candidates(conn, a, types)
        cur = conn.cursor()
        cur.execute(sql, params)
        for row in cur.iterate():
            rec = {
                "time": row[0].isoformat(),
                "node_name": row[1],
                "session_id": row[2],
                "user_name": row[3],
                "transaction_id": row[4],
                "statement_id": row[5],
                "request_id": row[6],
                "request_type": row[7],
                "request": row[8],
                "request_duration_ms": float(row[9]) if row[9] is not None else None,
                "success": row[10],
            }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            count += 1
            by_user[rec["user_name"]] += 1
            by_type[rec["request_type"]] += 1
            sessions.add(rec["session_id"])
            first_t = first_t or rec["time"]
            last_t = rec["time"]
            read_only += is_read_only(rec["request"] or "")
            if rec["request_duration_ms"] is not None:
                durs.append(rec["request_duration_ms"])
            if rec["success"] is False:
                failed += 1
    log.debug("extract finished: %d statements", count)

    finished = datetime.now(timezone.utc)
    ds = sorted(durs)
    warnings = []
    if count == 0:
        warnings.append("No statements captured: check the window, --types and "
                        "exclusion filters (try --include-system).")
    if not a.no_duration and count and not durs:
        warnings.append("No original durations found: v_monitor.query_requests "
                        "returned nothing for this window (retention?).")
    elif not a.no_duration and count and len(durs) < count:
        warnings.append(f"{count - len(durs)} statements have no original duration "
                        f"(retention or still running).")
    summary = {
        "tool": "vertica_replay.py extract",
        "started_utc": started.isoformat(timespec="seconds"),
        "finished_utc": finished.isoformat(timespec="seconds"),
        "elapsed_s": round((finished - started).total_seconds(), 1),
        "command_line": shlex.join(sys.argv),
        "flags": {k: v for k, v in vars(a).items() if k not in ("func", "cmd")},
        "source": {"host": a.host, "port": a.port, "database": a.database,
                   "user": a.user},
        "window": {"since": a.since, "until": a.until},
        "request_types": types or ["ALL"],
        "filters": {"include_system": a.include_system, "source_user": a.source_user,
                    "exclude_users": a.exclude_user or [], "regexes": patterns,
                    "no_duration": a.no_duration},
        "results": {
            "candidates": candidates, "captured": count,
            "first_time": first_t, "last_time": last_t,
            "sessions": len(sessions),
            "by_user": dict(by_user.most_common()),
            "by_type": dict(by_type.most_common()),
            "read_only": read_only, "non_read_only": count - read_only,
            "failed": failed,
            "durations": {
                "count": len(ds),
                "total_s": round(sum(ds) / 1000, 1),
                "median_ms": round(statistics.median(ds), 1) if ds else None,
                "p95_ms": round(ds[min(len(ds) - 1, int(0.95 * len(ds)))], 1) if ds else None,
                "max_ms": round(ds[-1], 1) if ds else None,
            },
        },
        "output": {"file": a.output, "bytes": os.path.getsize(a.output),
                   "sha256": sha256_file(a.output)},
        "warnings": warnings,
    }
    text = format_extract_summary(summary)
    print(text)
    spath = a.summary_file or a.output + ".summary.txt"
    jpath = os.path.splitext(spath)[0] + ".json"
    with open(spath, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    with open(jpath, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    print(f"Extracted {count} statements -> {a.output}")
    print(f"Summary written to {spath} and {jpath}")


# --------------------------------------------------------------------------
# replay helpers
# --------------------------------------------------------------------------
class Reporter:
    """Thread-safe CSV report writer with status counters."""

    HEADER = ["seq", "original_time", "session_id", "original_user",
              "statement_id", "status", "original_duration_ms", "elapsed_ms",
              "ratio", "original_rows", "rows", "original_success", "error",
              "request"]

    def __init__(self, path):
        self._f = open(path, "w", newline="", encoding="utf-8")
        self._w = csv.writer(self._f)
        self._lock = threading.Lock()
        self.counts = Counter()
        self._orig_total = 0.0   # sums over successfully replayed statements
        self._replay_total = 0.0  # that have an original duration
        self._ratios = []
        self._w.writerow(self.HEADER)

    def write(self, status, seq, r, sql, elapsed="", rows="", err=""):
        with self._lock:
            self.counts[status] += 1
            orig = r.get("request_duration_ms")
            ratio = ""
            if status == "OK" and orig and elapsed != "":
                ratio = round(elapsed / orig, 2)  # >1 means slower on replay
                self._orig_total += orig
                self._replay_total += elapsed
                self._ratios.append(ratio)
            self._w.writerow([seq, r["time"], r["session_id"], r.get("user_name", ""),
                              r["statement_id"], status,
                              "" if orig is None else round(orig, 1), elapsed, ratio,
                              r.get("processed_row_count", ""), rows,
                              r.get("success", ""), err, sql[:500]])

    def summary(self):
        if not self._ratios:
            return "No statements with both original and replay durations."
        rs = sorted(self._ratios)
        p95 = rs[min(len(rs) - 1, int(0.95 * len(rs)))]
        return (f"Duration comparison over {len(rs)} statements: "
                f"original total {self._orig_total / 1000:.1f}s, "
                f"replay total {self._replay_total / 1000:.1f}s; "
                f"replay/original ratio median {statistics.median(rs):.2f}, "
                f"p95 {p95:.2f}")

    def close(self):
        with self._lock:
            self._f.close()


def load_workload(a, rep):
    """Read JSONL; return runnable [(seq, record, sql)] in original order.
    Statements that are skipped are written to the report immediately."""
    items = []
    with open(a.input, encoding="utf-8") as inp:
        for seq, line in enumerate(inp, 1):
            if not line.strip():
                continue
            r = json.loads(line)
            sql = r["request"].strip().rstrip(";").strip()
            if not sql or (not a.allow_dml and not is_read_only(sql)):
                log.debug("seq=%d SKIPPED (not read-only or empty): %s",
                          seq, one_line(sql, 200))
                rep.write("SKIPPED", seq, r, sql, err="not read-only")
                continue
            items.append((seq, r, sql))
    log.debug("workload loaded: %d runnable statements from %s", len(items), a.input)
    return items


def wait_until(a, ts_iso, t0_orig, t0_wall):
    """Sleep until this statement's original offset (scaled by --speed)."""
    if not a.respect_timing:
        return
    ts = datetime.fromisoformat(ts_iso)
    target = (ts - t0_orig).total_seconds() / a.speed
    wait = target - (time.monotonic() - t0_wall)
    if wait > 0:
        log.debug("waiting %.3fs for original offset %.3fs", wait, target)
        time.sleep(wait)
    elif wait < -1.0:
        log.debug("running %.3fs LATE vs original offset %.3fs "
                  "(replay cannot keep up / concurrency capped?)", -wait, target)


def ensure_user(cur, a, rep, seq, r, sql, state):
    """With --impersonate, make the connection run as the statement's original
    user (SET SESSION AUTHORIZATION). Returns False if the switch failed, in
    which case the statement is logged as ERROR and must not be executed."""
    if not a.impersonate:
        return True
    want = r.get("user_name")
    if not want or want == state.get("user"):
        return True
    try:
        log.debug("seq=%d impersonation: %s -> %s", seq, state.get("user"), want)
        if state.get("user"):
            cur.execute("RESET SESSION AUTHORIZATION")
        cur.execute('SET SESSION AUTHORIZATION "%s"' % want.replace('"', '""'))
        state["user"] = want
        if log.isEnabledFor(logging.DEBUG):
            try:  # debug only: confirm the switch really took effect
                cur.execute("SELECT CURRENT_USER()")
                log.debug("seq=%d effective user now: %s", seq, cur.fetchone()[0])
            except vertica_python.errors.Error as e2:
                log.debug("seq=%d could not verify CURRENT_USER(): %s", seq, e2)
        return True
    except vertica_python.errors.Error as e:
        log.debug("seq=%d impersonation of %s FAILED", seq, want, exc_info=True)
        state["user"] = None
        rep.write("ERROR", seq, r, sql,
                  err=f"impersonation of {want} failed: "
                      f"{str(e).replace(chr(10), ' ')[:400]}")
        return False


def execute_one(cur, a, rep, seq, r, sql):
    """Run one statement, fetch results, record outcome. Returns True on success."""
    start = time.monotonic()
    rows = 0
    # --server-timing: prefix a unique comment so the statement can be found
    # again in v_monitor.query_requests on the target (the report keeps clean SQL)
    stmt = f"/* {a.tag}:{seq} */ {sql}" if a.server_timing else sql
    log.debug("seq=%d EXEC session=%s user=%s orig_duration_ms=%s: %s",
              seq, r.get("session_id"), r.get("user_name"),
              r.get("request_duration_ms"), one_line(stmt))
    try:
        cur.execute(stmt)
        exec_ms = (time.monotonic() - start) * 1000
        if cur.description:
            for _ in cur.iterate():
                rows += 1
                if a.max_rows and rows >= a.max_rows:
                    break
        elapsed = round((time.monotonic() - start) * 1000, 1)
        log.debug("seq=%d OK execute=%.1fms total=%.1fms rows=%d%s",
                  seq, exec_ms, elapsed, rows,
                  " (fetch truncated by --max-rows)"
                  if a.max_rows and rows >= a.max_rows else "")
        rep.write("OK", seq, r, sql,
                  elapsed=round((time.monotonic() - start) * 1000, 1), rows=rows)
        return True
    except vertica_python.errors.Error as e:
        log.debug("seq=%d ERROR %s sqlstate=%s after %.1fms",
                  seq, type(e).__name__, getattr(e, "sqlstate", None),
                  (time.monotonic() - start) * 1000, exc_info=True)
        rep.write("ERROR", seq, r, sql,
                  elapsed=round((time.monotonic() - start) * 1000, 1), rows=rows,
                  err=str(e).replace("\n", " ")[:500])
        return False


# --------------------------------------------------------------------------
# replay: serial
# --------------------------------------------------------------------------
def replay_serial(a, rep, items):
    t0_orig = datetime.fromisoformat(items[0][1]["time"]) if items else None
    t0_wall = time.monotonic()

    if a.dry_run:
        for seq, r, sql in items:
            print(f"[{seq}] {r['time']} user={r.get('user_name')} {sql[:120]!r}")
        return

    with open_connection(a) as conn:
        cur = conn.cursor()
        state = {}
        for seq, r, sql in items:
            wait_until(a, r["time"], t0_orig, t0_wall)
            ok = (ensure_user(cur, a, rep, seq, r, sql, state)
                  and execute_one(cur, a, rep, seq, r, sql))
            if not ok and a.stop_on_error:
                break


# --------------------------------------------------------------------------
# replay: session-parallel
# --------------------------------------------------------------------------
def run_session(a, rep, items, t0_orig, t0_wall, stop):
    """Replay one original session's statements, in order, on its own connection."""
    conn = None
    state = {}
    log.debug("session %s start: %d statements", items[0][1]["session_id"], len(items))
    try:
        for seq, r, sql in items:
            if stop.is_set():
                rep.write("ABORTED", seq, r, sql, err="stopped after earlier error")
                continue
            wait_until(a, r["time"], t0_orig, t0_wall)
            if conn is None:
                try:
                    conn = open_connection(a)
                    cur = conn.cursor()
                except Exception as e:  # connect failure (limit reached, auth, network)
                    rep.write("ERROR", seq, r, sql,
                              err=f"connect failed: {str(e)[:400]}")
                    if a.stop_on_error:
                        stop.set()
                    # remaining statements of this session cannot run
                    conn = False
                    continue
            if conn is False:
                rep.write("ABORTED", seq, r, sql, err="session connection failed")
                continue
            ok = (ensure_user(cur, a, rep, seq, r, sql, state)
                  and execute_one(cur, a, rep, seq, r, sql))
            if not ok and a.stop_on_error:
                stop.set()
    finally:
        log.debug("session %s done", items[0][1]["session_id"])
        if conn:
            conn.close()


def replay_sessions(a, rep, items):
    sessions = OrderedDict()  # keeps sessions ordered by first statement time
    for item in items:
        sessions.setdefault(item[1]["session_id"], []).append(item)

    peak = len(sessions)
    print(f"{len(items)} statements across {peak} sessions")

    if a.dry_run:
        for sid, its in sessions.items():
            users = sorted({i[1].get("user_name") or "?" for i in its})
            print(f"session {sid} (user={','.join(users)}): {len(its)} statements, "
                  f"{its[0][1]['time']} -> {its[-1][1]['time']}")
        return

    workers = a.max_concurrency or peak
    if a.max_concurrency and a.max_concurrency < peak:
        print(f"WARNING: --max-concurrency {a.max_concurrency} < {peak} sessions; "
              f"later sessions start only when a slot frees up, so concurrency "
              f"will be lower than the original workload.")

    t0_orig = datetime.fromisoformat(items[0][1]["time"]) if items else None
    t0_wall = time.monotonic()
    stop = threading.Event()

    with ThreadPoolExecutor(max_workers=max(workers, 1)) as pool:
        futures = [pool.submit(run_session, a, rep, its, t0_orig, t0_wall, stop)
                   for its in sessions.values()]
        for f in futures:
            f.result()  # surface unexpected exceptions


# --------------------------------------------------------------------------
def merge_server_timing(a, since):
    """Read the replayed statements' server-side durations from
    v_monitor.query_requests on the target (matched via their tag comment) and
    append server_duration_ms / server_ratio columns to the report."""
    time.sleep(a.server_timing_wait)  # let DC records become visible
    tag_re = re.compile(r"/\*\s*" + re.escape(a.tag) + r":(\d+)\s*\*/")
    found = {}
    with open_connection(a) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT SUBSTR(request, 1, 120), request_duration_ms "
            "FROM v_monitor.query_requests "
            "WHERE start_timestamp >= %s AND request LIKE %s",
            [since, f"/* {a.tag}:%"])
        log.debug("server-timing lookup: tag=%s since=%s", a.tag, since)
        for text, dur in cur.iterate():
            log.debug("server-timing row: %r duration=%s", text, dur)
            m = tag_re.match(text or "")
            if m and dur is not None:
                seq = int(m.group(1))
                found[seq] = max(float(dur), found.get(seq, 0.0))  # retries: keep max

    with open(a.report, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    header, body = rows[0], rows[1:]
    i_seq = header.index("seq")
    i_orig = header.index("original_duration_ms")
    header += ["server_duration_ms", "server_ratio"]
    ratios, orig_total, srv_total, matched = [], 0.0, 0.0, 0
    for row in body:
        d = found.get(int(row[i_seq]))
        orig = float(row[i_orig]) if row[i_orig] else 0.0
        row.append("" if d is None else round(d, 1))
        if d is not None and orig > 0:
            ratio = round(d / orig, 2)
            ratios.append(ratio)
            orig_total += orig
            srv_total += d
        else:
            ratio = ""
        row.append(ratio)
        matched += d is not None
    with open(a.report, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(body)

    executed = sum(1 for r in body if r[header.index("status")] == "OK")
    if log.isEnabledFor(logging.DEBUG):
        miss = [r[i_seq] for r in body
                if r[header.index("status")] == "OK" and int(r[i_seq]) not in found]
        log.debug("server-timing: %d found, %d executed, unmatched seqs (first 30): %s",
                  len(found), executed, miss[:30])
    print(f"Server timing: matched {matched} of {executed} executed statements.")
    if matched < executed:
        print("  Unmatched statements: DC retention/visibility, a different "
              "target user lacking v_internal access, or request text rewritten.")
    if ratios:
        rs = sorted(ratios)
        p95 = rs[min(len(rs) - 1, int(0.95 * len(rs)))]
        print(f"Server-side comparison over {len(rs)} statements: "
              f"original total {orig_total / 1000:.1f}s, replay total "
              f"{srv_total / 1000:.1f}s; ratio median {statistics.median(rs):.2f}, "
              f"p95 {p95:.2f}")


def cmd_replay(a):
    a.tag = f"vreplay:{int(time.time())}"
    log.debug("replay config: %s", {k: v for k, v in vars(a).items() if k != "func"})
    started = datetime.now(timezone.utc) - timedelta(minutes=1)
    rep = Reporter(a.report)
    try:
        items = load_workload(a, rep)
        if a.mode == "session":
            replay_sessions(a, rep, items)
        else:
            replay_serial(a, rep, items)
    finally:
        rep.close()
    c = rep.counts
    print(f"OK={c['OK']} ERROR={c['ERROR']} SKIPPED={c['SKIPPED']} "
          f"ABORTED={c['ABORTED']} -> {a.report} (sort by 'seq' for original order)")
    print(rep.summary())
    if a.server_timing and not a.dry_run:
        merge_server_timing(a, started)


def build_parser():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-H", "--host", required=True, help="Vertica host")
    common.add_argument("-P", "--port", type=int, default=5433, help="port (default 5433)")
    common.add_argument("-d", "--database", required=True, help="database name")
    common.add_argument("-u", "--user", required=True,
                        help="login user (password from VERTICA_PASSWORD)")
    common.add_argument("--tls", action="store_true", help="require TLS")
    common.add_argument("--debug", action="store_true",
                        help="verbose trace on stderr: connections, timing waits, "
                             "impersonation, every statement with timings, "
                             "error details/tracebacks, filter/query SQL")
    common.add_argument("--debug-file", metavar="FILE",
                        help="also write the debug trace to FILE (implies --debug)")
    common.add_argument("--label", default="replay",
                        help="session label for this tool's connections (default replay)")

    sub = p.add_subparsers(dest="cmd", required=True)

    raw = argparse.RawDescriptionHelpFormatter
    e = sub.add_parser(
        "extract", parents=[common], formatter_class=raw,
        help="capture statements from dc_requests_issued into a JSONL file",
        description="Capture statements from v_internal.dc_requests_issued in "
                    "chronological order, with original user and (unless "
                    "--no-duration) original duration/success. System/internal "
                    "statements are excluded by default.",
        epilog="example:\n  %(prog)s -H prod -d prod -u dbadmin "
               "--since '2026-10-07 08:00:00' --until '2026-10-07 09:00:00' "
               "--exclude-user nagios_mon -o workload.jsonl")
    e.add_argument("--since", required=True, help="e.g. '2026-10-07 08:00:00'")
    e.add_argument("--until", required=True, help="end of window, exclusive")
    e.add_argument("--types", default="ALL",
                   help="comma list of request_type to capture (default ALL = no "
                        "filter; e.g. QUERY or QUERY,DDL,LOAD,UTILITY,TXN,SET)")
    e.add_argument("--source-user", help="only statements issued by this user")
    e.add_argument("--include-system", action="store_true",
                   help="keep queries on v_catalog/v_monitor/v_internal and "
                        "keepalives (excluded by default)")
    e.add_argument("--exclude-user", action="append", metavar="USER",
                   help="drop statements from this user (repeatable), "
                        "e.g. monitoring or service accounts")
    e.add_argument("--exclude-regex", action="append", metavar="REGEX",
                   help="drop statements matching this Vertica regex "
                        "(case-insensitive, repeatable)")
    e.add_argument("--no-duration", action="store_true",
                   help="do not join v_monitor.query_requests (no original "
                        "duration/success stored)")
    e.add_argument("--summary-file", metavar="FILE",
                   help="write the end-of-run summary here (default "
                        "<output>.summary.txt); a machine-readable .json twin is "
                        "written next to it")
    e.add_argument("-o", "--output", required=True, help="JSONL output file")
    e.set_defaults(func=cmd_extract)

    r = sub.add_parser(
        "replay", parents=[common], formatter_class=raw,
        help="replay a captured JSONL workload and write a comparison report",
        description="Replay a workload captured by 'extract' in its original "
                    "order (serial) or with per-session order and concurrency "
                    "(session). Read-only statements only unless --allow-dml.",
        epilog="example:\n  %(prog)s -H test -d test -u replay_admin "
               "-i workload.jsonl -r report.csv --mode session "
               "--respect-timing --impersonate --server-timing")
    r.add_argument("-i", "--input", required=True, help="JSONL file from extract")
    r.add_argument("-r", "--report", default="replay_report.csv",
                   help="CSV report (default replay_report.csv)")
    r.add_argument("--mode", choices=["serial", "session"], default="serial",
                   help="serial: one connection, global order; "
                        "session: one connection per original session, in parallel")
    r.add_argument("--max-concurrency", type=int, default=0,
                   help="session mode: max simultaneous connections (default 0 = one per "
                        "session; a lower cap reduces overlap)")
    r.add_argument("--respect-timing", action="store_true",
                   help="reproduce the original time offsets between statements")
    r.add_argument("--speed", type=float, default=1.0,
                   help="with --respect-timing: 2.0 = twice as fast (default 1.0)")
    r.add_argument("--max-rows", type=int, default=0,
                   help="stop fetching after N rows per query (0 = fetch all)")
    r.add_argument("--allow-dml", action="store_true",
                   help="also run non-SELECT statements (DANGEROUS)")
    r.add_argument("--impersonate", action="store_true",
                   help="run each statement as its original user via "
                        "SET SESSION AUTHORIZATION (needs a privileged replay account)")
    r.add_argument("--server-timing", action="store_true",
                   help="tag each statement with a comment, then read the exact "
                        "server-side duration from v_monitor.query_requests on the "
                        "target and add server_duration_ms/server_ratio to the report")
    r.add_argument("--server-timing-wait", type=float, default=3.0,
                   help="seconds to wait before reading v_monitor.query_requests (default 3)")
    r.add_argument("--stop-on-error", action="store_true",
                   help="stop at the first failing statement (default: continue)")
    r.add_argument("--dry-run", action="store_true",
                   help="print what would run, execute nothing")
    r.set_defaults(func=cmd_replay)
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    setup_logging(args)
    if args.cmd == "replay" and args.speed <= 0:
        sys.exit("--speed must be > 0")
    args.func(args)
