# ROWID Parallel Extract Design

**Date:** 2026-06-11
**Purpose:** Make `save_tables.py` read large Oracle tables in parallel by default, using ROWID-range predicates, so single-table extracts drop from 1d+ to hours.

## Problem

`save_tables.py` reads each table through one JDBC partition unless a numeric
partition column is configured in `DATAGEN_JDBC_PARTITION_COLUMNS`. A single
partition means one Oracle session streaming the whole table on one executor
core while the rest of the Data Flow cluster idles. The largest tables take
more than a day. Cross-table parallelism is handled by orchestration (one
Data Flow job per big table), so the fix targets intra-table read parallelism.

## Approach

Default every full-table read to Spark's predicate-based JDBC parallelism,
with one `ROWID BETWEEN 'start' AND 'end'` predicate per partition. ROWID
ranges are derived from the table's extent map, so partitions are even in
physical bytes regardless of column skew, each partition scans only its own
blocks (total DB I/O is approximately one full scan), and no numeric column
is required. The range computation is ported from the proven implementation
in `scripts/migrate_rowid_to_oci.py`.

Alternatives considered:

- **Configure `DATAGEN_JDBC_PARTITION_COLUMNS` per table (no code change):**
  requires each big table to have a roughly uniform numeric column; skewed or
  sparse IDs produce one huge partition. Automatic discovery of such columns
  was previously removed (`542f7dd`) because it was unreliable.
- **`MOD(ORA_HASH(ROWID), N) = i` predicates:** evenly distributed but each
  of the N partitions performs a full table scan, multiplying source DB I/O
  by N. Rejected.

## Reader Strategy

`build_jdbc_reader` resolves the read mode per table, in order:

1. `--limit` set → single partition (unchanged). The `FETCH FIRST` subquery
   does not expose ROWID, and samples are fast anyway.
2. Partition-column override present in `DATAGEN_JDBC_PARTITION_COLUMNS` →
   existing numeric `partitionColumn`/bounds path (unchanged).
3. Otherwise → compute ROWID ranges and read with
   `spark.read.jdbc(url, table, predicates=..., properties=...)`.
4. Any failure computing ranges (missing extent-view privileges, empty
   table, the object is a view) → log a warning and fall back to the current
   single-partition read. Behavior never regresses below today's.

## ROWID Range Computation

New function in `save_tables.py`, logic ported from
`scripts/migrate_rowid_to_oci.py` (`fetch_rowid_ranges`,
`get_data_object_id`, `execute_extent_query`) but executed through Spark
JDBC using the existing `read_single_value` pattern, so the script keeps a
single connection configuration (JDBC URL) instead of adding an `oracledb`
DSN.

1. **Data object ID:** `SELECT dbms_rowid.rowid_object(ROWID) FROM <table>
   WHERE ROWNUM = 1`; if the table is empty, fall back to `ALL_OBJECTS` then
   `USER_OBJECTS`.
2. **Extents:** query `DBA_EXTENTS`, falling back to `ALL_EXTENTS` then
   `USER_EXTENTS`, building per-extent start/end ROWIDs with
   `DBMS_ROWID.ROWID_CREATE(1, data_object_id, relative_fno, block_id, 0)`
   and `(..., block_id + blocks - 1, 32767)`, ordered by file and block.
   `USER_EXTENTS` has no `owner` column; the per-view SQL mirrors the
   migration script.
3. **Chunk merge:** merge consecutive extents into approximately
   `DATAGEN_JDBC_NUM_PARTITIONS` (default 32) chunks targeting
   `total_blocks / num_partitions` blocks each — the same merging loop as
   `fetch_rowid_ranges`, retargeted from a fixed blocks-per-chunk to a chunk
   count.
4. **Predicates:** return one `ROWID BETWEEN '<start>' AND '<end>'` string
   per chunk.

Spark JDBC cannot bind parameters, so owner and table identifiers are
validated against the `IDENTIFIER_PATTERN` regex
(`^[A-Z][A-Z0-9_$#]*$`, as in `scripts/oracle_table_sizes.py`) before
interpolation; `data_object_id` is cast to `int`. Extent row counts are
small (hundreds to a few thousand), so collecting them to the driver is
cheap.

## Configuration

No new environment variables. `DATAGEN_JDBC_NUM_PARTITIONS` (default 32) now
also controls the ROWID chunk count. `DATAGEN_JDBC_FETCH_SIZE` continues to
apply to every partition's connection.

## Error Handling

Every metadata query (object ID, extents) is wrapped; any exception logs a
warning naming the table and falls back to a single-partition read. An empty
extent list (empty table) also falls back. Read/write failures keep the
existing `--continue-on-error` semantics.

## Known Limitation: Snapshot Consistency

Parallel JDBC partitions open separate Oracle sessions, each reading at its
own SCN, so the extract is not a single consistent snapshot if the source is
written during the run. This already applies to the existing
partition-column path. If it becomes a problem, a flashback `AS OF SCN`
clause per predicate is the natural extension; it is out of scope now.

## Testing

- Unit tests for the pure-Python pieces: extent-to-chunk merging (chunk
  count, block balance, full coverage with no gaps) and predicate string
  construction, including identifier validation rejecting bad names.
- Real-DB validation: extract one mid-size table with the new path and
  compare its row count against the current single-partition extract.

## Expected Outcome

With 32 partitions on a Data Flow cluster, wall-clock time for a large table
should drop roughly in proportion to partition count, bounded by Oracle and
network throughput — a table that takes ~20h serially should land in the
~1–2h range. One Data Flow job per big table multiplies this across tables.
