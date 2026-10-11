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
