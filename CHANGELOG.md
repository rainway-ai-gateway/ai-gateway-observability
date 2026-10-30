<!--
This changelog should always be read on `develop` branch. Its contents on other branches
does not necessarily reflect the changes.
-->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Missing-price signal columns in all three backends (`ai_price_miss`, `ai_price_warn`) for bfenetworks/bfe#1479: when an interface has no price entry, bfe bills through a fallback, bills zero, or refuses the request, and reports which shape applied. The columns are nullable everywhere and `NULL` means "not reported" — never `0`/`''` — so a report cannot mistake "not applicable" for "billed wrong".
  - ClickHouse: `Nullable(UInt8)` / `Nullable(String)` on both `bfe_ai_request_log` and the `bfe_ai_log_kafka` staging table, projected by `bfe_ai_log_load_mv`; upgrade script `sqls/upgrade/2026-10-30-issue-1479-price-miss-columns/`.
  - Doris: `BOOLEAN` / `VARCHAR(32)` plus two `COLUMNS` entries in the Routine Load; upgrade script `sqls/upgrade/2026-10-30-issue-1479-price-miss-columns/`.
  - StarRocks: `BOOLEAN` / `VARCHAR(32)` plus two `COLUMNS` entries; upgrade is plain `ADD COLUMN` (no table rebuild). `ai_price_miss` must be `BOOLEAN` — a `TINYINT` target silently nulls JSON booleans (#10).
- Integration coverage: clickhouse-it TC01/TC02 assertions plus new TC06 (upgrade path), doris-it TC01/TC02 assertions plus new TC07, starrocks-it TC01/TC02 assertions plus new TC06. The `normal_request.json` demo sample now carries `ai_price_miss: true` / `ai_price_warn: "subitem_missing"`; the other two samples deliberately omit them to pin the NULL semantics.

### Fixed
- ClickHouse: the Kafka staging table cannot be altered at all (`Code: 48 ... Alter of type 'ADD_COLUMN' is not supported by storage Kafka`, DROP likewise), so its upgrade path is DROP + recreate; the upgrade script documents the order and clickhouse-it TC06 asserts the limitation negatively.
- Doris: the `2026-10-13` unique-key rebuild script selected an explicit 99-column list, which broke as soon as the detail table gained the two new columns (`insert into cols should be corresponding to the query output`). The replay list is extended to the current 101-column schema and its header now states the prerequisite.

## [v0.0.2] - 2026-10-24

### Added
- ClickHouse asset tree (`clickhouse/`): table DDL, Kafka ingestion, materialized-view aggregation, one-shot `setup.sh` / `cleanup.sh`, integration tests and docs.
- StarRocks asset tree (`starrocks/`): detail-table DDL (DUPLICATE KEY, nested columns carried as VARCHAR JSON text), async minute-aggregate materialized view (`ts_day` partition column plus `partition_ttl`), Routine Load flattening via `get_json_string`, `setup.sh` / `cleanup.sh`, the `starrocks-it` suite (TC01–TC05) and HOWTO / TABLE_DESIGN docs. The real-consumption round of that suite surfaced and fixed three StarRocks 3.5 dialect incompatibilities (missing `json_extract` signature, complex columns needing VARCHAR, VARCHAR not castable to `ARRAY<STRUCT>` directly).
- Doris upgrade scripts under `doris/sqls/upgrade/`, one directory per change (`2026-09-29-report-cache-mirror-intent`, `2026-10-13-issue-1417-detail-unique-key-logid`); run once per environment.
- Integration suites and test-design docs: `doris/tests/integration` (DORIS01, 5 cases validated against Doris 3.0.8) and the DORIS01 TC-level design documents (TC-01–TC-05).

### Changed
- Doris detail table gains 13 columns: 10 for the report phase-2 AI cache / traffic mirror / intent fields (names and types aligned with the MySQL side) plus 3 rate-limit columns flattened at ingest time, because Doris 3.0 cannot dereference `ARRAY<STRUCT>` element fields. The Routine Load `COLUMNS` mapping is extended to match.
- Doris minute aggregate `bfe_ai_metrics_1m` is rebuilt with 3 additional KEY dimensions (37 → 40); the INSERT JOB `GROUP BY` follows.
- Doris docs restructured from `docs/modification` to `docs/modifications`, one directory per change (aligned with the bfe convention).

### Fixed
- Doris `log_time` is now written as the UTC wall clock, and the Routine Load starts at `OFFSET_BEGINNING` so log records produced before the job existed are not skipped.
- The Doris detail table `UNIQUE KEY` now carries `logid` (5th key column; `logid` is also `NOT NULL`, since Doris key columns cannot be null), so concurrent requests of one API key and model inside the same second are no longer merged into a single row. With a second-granularity `log_time` the full-row upsert kept only 1 of n rows, losing exactly (n-1)/n — the same defect measured as 13.5% of a production hour on the MySQL link — and the dropped rows' tokens and cost disappeared from every sum (bfenetworks/bfe#1417).
- StarRocks detail table now declares `ai_stream` / `is_trust_src_ip` as BOOLEAN instead of TINYINT, because StarRocks JSON import only converts JSON true/false on BOOLEAN target columns and silently writes NULL into TINYINT ones (no error, not counted in `NumberFilteredRows`) — `stream_requests` stayed 0 and `logs?stream=true|false` matched no rows. Existing deployments upgrade by table rebuild plus Kafka replay, since StarRocks cannot ALTER TINYINT to BOOLEAN in place (HOWTO §8); integration cases TC01–TC03 gained assertions that would have caught this (#10).

## [v0.0.1] - 2026-08-30

### Added
- Initial release: Doris storage layer — detail table `bfe_ai_request_log`, minute aggregate `bfe_ai_metrics_1m`, Kafka Routine Load `bfe_ai_log_load` and INSERT JOB `bfe_ai_metrics_1m_job` — with one-shot `doris/setup.sh` / `doris/cleanup.sh` scripts.
- Grafana display layer: MySQL datasource pointing at Doris FE (9030) plus dashboards, deployed by `grafana/setup.sh`.
- `api/depends_api` and the `skills/gen-req-log-api` skill for generating the request-log API dependencies.

[v0.0.2]: https://github.com/rainway-ai-gateway/ai-gateway-observability/compare/v0.0.1...v0.0.2
[v0.0.1]: https://github.com/rainway-ai-gateway/ai-gateway-observability/releases/tag/v0.0.1
