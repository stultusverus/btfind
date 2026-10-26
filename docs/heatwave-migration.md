# HeatWave migration runbook

The application uses standard MySQL/InnoDB and built-in ngram indexes. The
optional HeatWave analytics cluster is not required. Both existing user services
remain on vm-sub01; the database is private. Create the database yourself using
the steps below. Do not schedule the outage until connectivity and a rehearsal
have succeeded.

## 1. Create the database in OCI

1. In the tenancy home region, open **Databases → MySQL HeatWave → DB systems →
   Create DB system** and select **Always Free**. Name it btfind.
2. Select the VM's VCN and a private subnet. Confirm the MySQL.Free shape and
   50 GiB storage. Disable the optional HeatWave cluster. Keep automatic backups
   (one-day retention); no paid shape, replicas, or analytics nodes are needed.
3. Use a network security group to allow TCP 3306 from vm-sub01's private IP /32
   (or its NSG). Allow the corresponding VM egress. Do not expose 3306 publicly.
4. Choose an administrator username/password and store them in a password manager.
   Record the endpoint hostname, private address, MySQL version, and certificate
   issuer/CA. Use a hostname covered by the server certificate. Supply the trusted
   CA chain with database.tls_ca if it is not in the public trust store.
5. Confirm Always Free availability before proceeding. If capacity is unavailable,
   leave the existing services running and retry provisioning later.

Official console steps:
https://docs.oracle.com/en-us/iaas/mysql-database/doc/creating-always-free-db-system.html

Connect from vm-sub01 with a MySQL client using VERIFY_IDENTITY, then check:

```sql
SELECT VERSION(), @@ngram_token_size;
CREATE DATABASE btfind CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
```

The ngram size must be 2. Every application/migration connection sets
innodb_ft_enable_stopword=OFF before indexing/searching; verify this is accepted
on the managed instance. No globally privileged startup setting is required.

Create separate TLS-required MySQL accounts. Use randomly generated passwords
and restrict the host part to the VM private address. Do not paste passwords into
shell history. The migration account needs schema DDL/DML and REFERENCES on
btfind.*. The crawler needs SELECT/INSERT/UPDATE/DELETE. The web user needs SELECT.
Grant the web user SELECT on btfind._sqlx_migrations as well (included by btfind.*).
Create accounts with REQUIRE SSL. Do not use the administrator in either service.

Run the migration with the migration account, then verify the read-only web
account can run `btfind db check`. This tests schema visibility and session setup.

## 2. Configure and build

Use Rust stable and build on a machine compatible with the VM's CPU architecture
and Linux runtime. Avoid compiling on the memory-constrained VM when possible.

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
node tests/dashboard.test.cjs
cargo build --release
```

Replace the old [database] path setting in the TOML configuration:

```toml
[database]
pool_size = 8
web_pool_size = 4
batch_size = 128
flush_interval_ms = 1000
tls_ca = "/home/opc/.config/btfind/mysql-ca.pem"
```

Omit tls_ca only if the certificate is already trusted. TLS verification is
mandatory by default. allow_insecure_local=true is solely for loopback testing.

Put a different connection URL in each service's mode-0600 environment file:

```text
BTFIND_DATABASE_URL=mysql://USER:URL_ENCODED_PASSWORD@CERTIFICATE_HOSTNAME:3306/btfind
```

Add EnvironmentFile=/home/opc/.config/btfind/crawler.env to the btfind user unit,
and the corresponding web.env to btfind-web. Preserve their existing ExecStart,
bind address, and access method. Add KillSignal=SIGINT and TimeoutStopSec=45 to
both units so the crawler receives its graceful shutdown signal. After changing
units, use systemctl --user daemon-reload. Keep admin credentials separate.

Never pass a database password as a CLI flag, commit it, or include it in logs.
The application deliberately rejects the obsolete database.path option.

## 3. Local tests and migration rehearsal

Use an isolated MySQL 8.4 container, not production:

```sh
podman run -d --name btfind-mysql-test \
  -e MYSQL_ROOT_PASSWORD=btfind-local-test \
  -p 127.0.0.1:13306:3306 docker.io/library/mysql:8.4 \
  --ngram-token-size=2 --innodb-ft-enable-stopword=OFF
export BTFIND_TEST_DATABASE_URL='mysql://root:btfind-local-test@127.0.0.1:13306/mysql'
bash scripts/test-mysql.sh
```

Wait for MySQL readiness before testing. Each integration test creates a uniquely
named btfind_test_* database on the loopback server. Remove the disposable
container after testing; never point this suite at a shared/production server.
Also rehearse on the actual MySQL version provisioned by OCI.

The source must be a standalone SQLite schema-version-4 backup. Use SQLite's
backup API or sqlite3 .backup to capture all committed WAL data. Copying just
torrents.db while it has a live WAL is unsafe. Check free disk before creating
the backup; keep enough for the source, snapshot, and any export.

With BTFIND_DATABASE_URL pointing to an empty rehearsal database:

```sh
btfind db migrate
btfind db import-sqlite --source /absolute/path/torrents-snapshot.db
btfind db validate-sqlite --source /absolute/path/torrents-snapshot.db
btfind db check
```

Import copies all ten base tables, including retry state and node identity. It
rebuilds search documents and counters. It preserves row IDs and auto-increment
progress for imported records. Each batch commits its checkpoint atomically;
rerun with the exact same immutable snapshot after interruption. A different
fingerprint or unrelated nonempty destination is rejected. MySQL DDL is not
transactional: if the initial migration fails partway, diagnose it and recreate
only the empty rehearsal schema before retrying; never mark a dirty migration
successful manually.

Validation compares every base-table row by primary key, computes SHA-256
checksums in source order, checks counts, and validates complete metadata file
counts/sizes and aggregate totals. Inconsistent source data blocks cutover.
The importer never discards or silently repairs it.

Services refuse to start while an import is incomplete. Import, validation,
reconciliation, and rebuild commands require both services stopped. An import
advisory lock excludes another importer, not already-running application writers.
Keep the snapshot unchanged throughout the import and validation.

## 4. Planned production outage

1. Verify the new binary, TLS access, parser configuration, schema migration, and
   rehearsal before stopping production. Retain a copy of the original binary,
   configuration, and user-unit definitions.
2. Stop btfind-web and btfind with systemctl --user. Check shutdown logs for
   unsaved work; confirm both processes exited. Before the old crawler receives
   SIGTERM, configure KillSignal=SIGINT as described above.
3. Create a final consistent SQLite backup and retain the original database and
   WAL together. Do not delete or manually truncate the WAL.
4. Apply schema to an empty production database, import the final snapshot, and
   validate it. Allow hours; measure duration in the rehearsal.
5. Install the new binary and MySQL configuration/environment files. Start
   btfind-web first, with its read-only account. Check /api/stats, empty browsing,
   Japanese/Chinese searches, file filtering, details, and magnet links.
6. Start btfind with its read/write account. Verify the stored node identity,
   durable job recovery, new observations, and metadata completions.
7. Measure warm request latency with five concurrent readers while crawling.
   Target p95 <500 ms for stats/details and <2 s for representative first-page
   searches. Inspect EXPLAIN plans for broad scans and check free database space.
   Do not declare cutover complete if those checks fail.

Before crawler writes resume, rollback consists of stopping the new web service
and restoring the saved binary/configuration/units with the SQLite snapshot.
After writes resume, that snapshot is stale. Stop writes and export MySQL before
deciding recovery; there is no automatic reverse migration. Reverting then would
lose post-cutover discoveries unless separately reconciled. Keep the original
snapshot until a MySQL export has been restored and checked successfully.

## 5. Backups and maintenance

Install deploy/btfind-backup.service and deploy/btfind-backup.timer as user units,
and install scripts/backup-mysql.sh at /home/opc/.local/bin/backup-mysql.sh
(mode 0700). Create a private Object Storage bucket and configure a lifecycle rule
to delete objects under btfind/ after seven days. Size the retention against the
tenancy's actual free quota before enabling the timer; seven full exports may
exceed it. If they do, reduce retention or arrange additional storage explicitly.

Grant the VM instance principal only the permissions needed to upload to that
bucket. Install the OCI CLI, mysql client tools, and gzip. Create a mode-0600
MySQL option file for a backup account with SELECT and SHOW VIEW on btfind.*:

```ini
[client]
host=CERTIFICATE_HOSTNAME
user=BACKUP_USER
password=BACKUP_PASSWORD
ssl-mode=VERIFY_IDENTITY
ssl-ca=/home/opc/.config/btfind/mysql-ca.pem
```

Create /home/opc/.config/btfind/backup.env (0600) with MYSQL_DEFAULTS_FILE,
OCI_BACKUP_BUCKET, and OCI_BACKUP_NAMESPACE. The script exports a consistent
transactional snapshot, compresses it, uploads it, and removes the temporary
local archive. Failed uploads return nonzero and are visible in the unit journal.
Observe the first successful run before enabling the daily timer.

Restore a downloaded export into a new empty local or managed MySQL database
with ngram size 2 and stopwords disabled. Use mysql with a protected option file:
`gzip -dc backup.sql.gz | mysql --defaults-extra-file=restore.cnf --init-command="SET SESSION innodb_ft_enable_stopword=OFF" btfind_restore`.
Run btfind db analyze with the maintenance account before serving traffic.
Freshly restored InnoDB full-text indexes can have stale row estimates and return
incorrect relevance or arithmetic errors until ANALYZE TABLE refreshes them.
The SQLite importer and search rebuild perform this step automatically.
Then run btfind db check, compare stats, and run multilingual search/detail smoke
checks. Never restore over the active database as a test.

Monitor service errors/timeouts, OCI storage utilization, connection counts, and
backup success. Receipts are retained to preserve retry idempotency; their table
also needs capacity monitoring. No automated receipt deletion is performed.
Run btfind db reconcile or btfind db rebuild-search only during maintenance.
Re-test search and connection behavior following OCI's automatic MySQL upgrades.

## API changes

Search and file-list responses retain results/files, limit, offset, and has_next,
but no longer return total. Clients use has_next, not page counts. Statistics
fields are unchanged and are read from transactionally maintained counters.
Each global search term needs at least two alphanumeric characters. Punctuation
and separators split terms; all terms must match. Short or punctuation-only
queries return HTTP 400; empty queries browse normally. Search ranking and
punctuation behavior are intentionally not identical to SQLite FTS5.
