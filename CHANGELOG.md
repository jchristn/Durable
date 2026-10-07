# Change Log

All notable changes to Durable are listed here, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/) loosely: each version lists what was added, changed and fixed, grouped by area, and ends with a **Breaking changes** list that names every change that can break code compiled against the previous version. Durable is in beta (0.x), so minor versions may break. Since 0.2.0 all packages share one version number and are released together. Dates are release dates (YYYY-MM-DD).

## Current Version

### v0.7.1 (2026-10-07)

`Durable.LiteGraph` now works in trimmed and Native AOT applications.

**`Durable.LiteGraph`**

- Depends on LiteGraph 10.2.0, which is AOT-compatible (10.1.0 used reflection-based System.Text.Json and `DataTable`).
- `LiteGraphBackend.Create`/`CreateAsync` no longer carry `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]`, so trimmed and AOT builds no longer warn at the call site.

**Tests**

- `Test.Aot` runs the shared repository scenario on LiteGraph (42 checks) on .NET 8 and .NET 10, with zero trim/AOT warnings from LiteGraph or its dependencies.

**Other packages**

- No code changes; republished at 0.7.1 to keep one shared version number.

**Breaking changes**

None.

## Previous Versions

### v0.7.0 (2026-10-07) - breaking

New databases: two SQL providers (Oracle, DuckDB), three wire-compatible databases on the existing providers (MariaDB on `Durable.MySql`; CockroachDB and YugabyteDB on `Durable.Postgres`), and two non-SQL backends (MongoDB, Azure Cosmos DB for NoSQL). Fifteen packages now share the version number.

**New package: `Durable.Oracle`**

- Oracle Database provider on `Oracle.ManagedDataAccess.Core` 23.26.301: `OracleDialect`, `OracleDataTypeConverter`, `OracleConnectionFactory`, `OracleRepositorySettings`, `OracleRepository<T>` (connection string, settings, shared factory, shared factory plus a configured `OracleDialect`). Generated SQL targets Oracle 19c; tested on Oracle Database 23ai Free.
- Identifiers quoted and folded to upper case (`OracleDialect(upperCaseIdentifiers: false)` keeps case); parameters bound by name (`:p0`); `OFFSET`/`FETCH` paging with LINQ's null ordering (`NULLS FIRST`/`NULLS LAST`); identity keys through `RETURNING ... INTO`; `MERGE` upsert; statement batches as PL/SQL blocks; IN lists split at 1000 items; `MINUS` for `Except`; `FROM DUAL` for table-less selects; Oracle date, interval and string functions.
- Types: `NUMBER(1)` booleans, `RAW(16)` GUIDs (RFC 4122 order), `VARCHAR2(n CHAR)`/`CLOB` strings, `CLOB` JSON, `RAW`/`BLOB` binary, `TIMESTAMP(7)`, `TIMESTAMP(7) WITH TIME ZONE`, `DATE` for `DateOnly`, `INTERVAL DAY TO SECOND` for `TimeSpan` and `TimeOnly`.
- Empty strings: Oracle stores `''` as NULL, so `OracleDialect.TreatsEmptyStringAsNull` is true, Oracle repositories lack `RepositoryCapabilities.EmptyStrings`, and string columns are declared NULL-able so non-nullable string properties can hold `""`.
- Schema: `CREATE TABLE` and the migration history table inside PL/SQL blocks that ignore ORA-00955; introspection from the `ALL_*` dictionary views (owner-qualified names supported); savepoints (release is a no-op); `DBMS_LOCK` migration lock (needs `EXECUTE` on `SYS.DBMS_LOCK`); scripts with `/` after PL/SQL blocks; DDL not transactional.
- `BulkInsert` through ODP.NET array binding. `CreateDatabaseIfNotExists` verifies connectivity only (a DBA creates pluggable databases and schemas).
- Trimming/AOT: Durable's code is warning-free; the constructors that create ODP.NET connections carry `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]` because the driver publishes with trim and AOT warnings.

**New package: `Durable.DuckDb`**

- DuckDB provider on DuckDB.NET.Data.Full 1.5.6 (engine and native libraries bundled): `DuckDbDialect`, `DuckDbDataTypeConverter`, `DuckDbConnectionFactory`, `DuckDbRepositorySettings` (`ForInMemory`, `ForSharedInMemory`, `ForFile`, `Parse`, `IsInMemory`, `AccessMode`, `Threads`, `MemoryLimit`) and `DuckDbRepository<T>`.
- In-process database lifetime: the connection factory keeps a root connection open, so a `:memory:` database is shared by every repository on the factory and released with it; file and `:memory:?cache=shared` databases stay open for the factory's lifetime.
- Native types: unsigned integers (`UTINYINT` to `UBIGINT`), `UUID`, `TIMESTAMP`/`TIMESTAMPTZ`, `DATE`, `TIME`, `INTERVAL`, `DECIMAL(38,10)`, `BLOB`, `JSON`, and `HUGEINT` for `BigInteger` provider types.
- Auto-increment keys through per-table sequences (`DEFAULT nextval`) with `INSERT ... RETURNING`; upsert through `INSERT ... ON CONFLICT`; `BulkInsert` through the DuckDB Appender, with a prepared-INSERT fallback when column types differ from the mapping.
- Optimistic concurrency control: statements outside a transaction that hit a write-write conflict are retried (`DuckDbDialect.AutocommitConflictRetries`, default 10); conflicts inside a transaction are thrown.
- Schema introspection through `information_schema` and the `duckdb_*()` functions; migration lock as a row in `durable_migration_lock`; schema sync drops and re-creates a table's indexes around column drops and NOT NULL additions, which DuckDB refuses on indexed tables.
- Documented limitations: no savepoints, no stored procedures, string lengths not stored, migrations not wrapped in a transaction, `DbCommand.CommandTimeout` ignored by the driver (Durable enforces `CommandTimeoutSeconds` itself).

**New package: `Durable.MongoDb`**

- `MongoDbBackend` (`IRepositoryBackend`) over MongoDB.Driver 3.12.0 following the backend convention: `Create`/`CreateAsync` (one `hello` round trip detects transaction support), `CreateRepository<T>`, `Owns`, `OwnsClient`, `Client`, `Database`, `SupportsTransactions`, `EnsureIndexes[Async]`, `GetStoredRows[Async]`, `Clear[Async]`, `BeginTransaction[Async]`, `QueryPlanned`, `LastQueryPlan`, `ExplainQueries`.
- `MongoDbRepository<T>`, `MongoDbRepositorySettings` (`ForClient`, `ForConnectionString`, `ForHost`, credentials, replica set, TLS, timeouts, pool sizes, `TransactionsEnabled`, `SequenceCollectionName`), `MongoDbTransaction` (client session; needs a replica set or sharded cluster), `MongoDbQueryPlan`.
- Entities are encoded to BSON from `EntityMetadata` (no driver class maps), losslessly: Decimal128 for `decimal`/`ulong` and for `DateTime`/`DateTimeOffset` ticks, standard binary GUIDs.
- Server-side push-down through a `QueryNodeVisitor` with C# semantics (comparisons, null checks, IN, ordinal and case-insensitive string matches via exact character-class regexes, AND/OR/NOT), then ordering, `Skip`/`Take`, `Count` and column aggregates when the filter is exact; everything else is evaluated by `QueryEvaluator`.
- Auto-increment keys from a counters collection, Guid/string/composite keys, version columns, soft delete, includes, indexes from `[Index]`/`[CompositeIndex]` (unique indexes enforced, null-tolerant).
- Not trim/AOT compatible (the driver is not): `Create`/`CreateAsync` carry `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]`.

**New package: `Durable.CosmosDb`**

- Azure Cosmos DB for NoSQL backend (`CosmosDbBackend`, `CosmosDbRepository<T>`, `CosmosDbRepositorySettings`, `CosmosDbQueryPlan`) on Microsoft.Azure.Cosmos 3.63.2 (plus Newtonsoft.Json 13.0.4, which the SDK needs at run time). Documents are written and read with System.Text.Json through the SDK's stream APIs.
- Settings: `ForEmulator`, `ForEndpoint`, `ForConnectionString`, `ForClient` (not owned), connection mode, `LimitToEndpoint`, database and container throughput (manual or autoscale), creation on first use, conflict retries.
- Partition keys: `/id` by default; `WithPartitionKey<T>(propertyName)` partitions an entity by a column, with single-partition queries for equality on it and cross-partition key uniqueness checks. No Cosmos-specific attribute was added to core Durable.
- Push-down to parameterized Cosmos DB SQL (comparisons, null checks, IN, string matches, AND/OR/NOT, single-key ORDER BY, OFFSET/LIMIT, COUNT, integer aggregates), point reads for key lookups, and client-side evaluation for everything else with checks that keep results identical to C#.
- Lossless values (exact shadows for longs beyond 2^53 and decimals beyond double precision, `DateTime` kind, `DateTimeOffset` offset); ETag-conditioned writes with re-check and retry, so concurrent updates are never lost; auto-increment keys from an ETag-incremented counter document.
- Transactions are not supported and reported through `Capabilities`; `Create`/`CreateAsync` carry `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]` because the SDK is not trim/AOT-compatible.

**MariaDB (`Durable.MySql`)**

- `MySqlFlavor`, `MariaDbDialect` (tested on 11.4 LTS): `VALUES()` upsert, `CHAR_LENGTH` empty-string test, `utf8mb4_nopad_bin` ordinal collation, emulated LEAD/LAG defaults, JSON as LONGTEXT, `db.system` `mariadb`.
- `MySqlRepositorySettings.Flavor`, `MySqlRepositorySettings.Parse(connectionString, flavor)`, `MySqlConnectionFactory.Flavor`, `MySqlDialect.Flavor`, `MySqlDialect.For(flavor)`; repository constructors `MySqlRepository<T>(connectionString, flavor, options?)` and `MySqlRepository<T>(connectionFactory, dialect, options?)`. `MySqlRepository<T>(connectionFactory)` uses the factory's flavor. Without a flavor nothing changes.

**CockroachDB and YugabyteDB (`Durable.Postgres`)**

- `PostgresFlavor.CockroachDb`, `CockroachDbDialect` (tested on 26.3): `INT4` integers and identity keys, `div()` integer division, `FLOAT8` cast for `Sqrt`, a migration lock table `durable_migration_locks` with takeover after `MigrationLockExpirySeconds`, non-transactional DDL, positional procedure arguments, `db.system` `cockroachdb`.
- `PostgresFlavor.YugabyteDb`, `YugabyteDbDialect` (tested on 2026.1): non-transactional DDL.
- `PostgresRepositorySettings.Flavor`, `PostgresRepositorySettings.Parse(connectionString, flavor)`, `PostgresConnectionFactory.Flavor`, `PostgresDialect.Flavor`, `PostgresDialect.For(flavor)`, protected `PostgresDialect.IndexSchemaConstraintFilter`; repository constructors `PostgresRepository<T>(connectionString, flavor, options?)` and `PostgresRepository<T>(connectionFactory, dialect, options?)`. `PostgresRepository<T>(connectionFactory)` uses the factory's flavor. PostgreSQL SQL is unchanged.

**Core (`Durable`)**

- `RepositoryCapabilities.EmptyStrings` (included in `All`): empty strings are stored and compared as values distinct from null. No call throws without it. SQL repositories lack it when the dialect stores `''` as NULL (Oracle).

**SQL engine (`Durable.Sql`)**

- `RepositoryType.Oracle` and `RepositoryType.DuckDb`.
- New `ISqlDialect` members, all with defaults on `SqlDialect` that keep existing behavior: `ConfigureCommand`, `StatementBatchPrefix`/`StatementBatchSuffix`, `SupportsMultiRowInsert`, `SingleRowFromClause`, `MaxInListItems` (IN lists split with OR/AND, include chunking), `Modulo`, `SetOperationKeyword`, `TreatsEmptyStringAsNull`, `ColumnAllowsNull` (CREATE TABLE, ADD COLUMN and schema comparison), `AppendScriptStatement` (migration script terminators), `SupportsSavepoints`, `DriverEnforcesCommandTimeout`, `AutocommitConflictRetries`, `IsRetryableConflict(Exception)`, `SupportsStringMaxLength`, `AlterTableRequiresDroppingIndexes`, `IsMigrationLockContention(Exception)`, `SupportsOffsetFunctionDefault`, `SupportsNamedProcedureArguments`, `PrepareMigrationLockSql()`.
- `InsertKeyStrategy.ReturningInto`: generated keys read from output parameters (`Create`, `CreateMany`).
- `SqlRepository<T>.Capabilities` follows `TreatsEmptyStringAsNull`.
- `ISqlTransaction.CreateSavepoint(Async)` throws `NotSupportedException` when the dialect does not support savepoints.
- The command executor enforces `SqlRepositoryOptions.CommandTimeoutSeconds` (cancel, then `TimeoutException`) for drivers that ignore `DbCommand.CommandTimeout`, and re-runs a statement that ran outside a transaction and failed with a retryable write-write conflict (only for dialects that opt in; the default is no retry).
- The migrator runs a dialect's lock setup statement before taking the lock, and treats a lock attempt that fails with a dialect-reported contention error as "not acquired".
- Schema comparison wraps column drops and NOT NULL column additions with index drop/re-create for dialects that require it, and reports a length change with a unit (`VARCHAR2(50 CHAR)` to `VARCHAR2(100 CHAR)`) as `MaxLengthMismatch` rather than `TypeMismatch`.
- `Lead`/`Lag` with a default value are emulated with a one-row-frame CASE where the database has no default argument; procedures are called positionally where named arguments are not supported.
- `Sum`/`Average` results (repository, query builder, grouped and projected queries) are converted through the data type converter, because some drivers return aggregates in types without `IConvertible` (DuckDB: `HUGEINT` as `BigInteger`).

**SQLite (`Durable.Sqlite`)**

- `SqliteDialect.SupportsStringMaxLength` is false (SQLite never stored declared lengths; this only reports it).
- Fixed: `SqliteConnectionFactory` rolls back a transaction that Microsoft.Data.Sqlite's pool left open on a native connection (Microsoft.Data.Sqlite pools a connection without checking that its transaction ended, for example after raw `BEGIN`/`SAVEPOINT` SQL or a failed `ROLLBACK`). Before, the next lease of that connection ran inside the stale transaction and `BeginTransaction` failed with "cannot start a transaction within a transaction"; this was the intermittent `ParallelBatchOperations_ShouldHandleConcurrency` failure.

**Conformance kit (`Durable.Conformance`)**

- Assertions that depend on empty strings being distinct from null moved into their own cases that require `RepositoryCapabilities.EmptyStrings` (`EqualsNullOnStrings`, `CoalesceOperatorOnStrings`, `CollectionContainsNullOnStrings`, `TrimToEmptyString`, `UpdateFieldSetsNull`, `EmptyStringsRoundTrip`). No assertion was removed; backends with the capability run all of them as before.
- The Capabilities suite gained an `EmptyStrings` case (an empty string reads back as `""` with the capability, and as `""` or null without it).

**Command-line tool (`Durable.Tool`)**

- `--provider duckdb`, `oracle` (aliases `odp`, `odpnet`), `cockroachdb` (aliases `cockroach`, `crdb`), `yugabytedb` (aliases `yugabyte`, `ysql`) and `mariadb` for every command, with scaffolding of each database's types. Help and error texts list the providers from one place.
- The tool package does not bundle DuckDB's native library (with it the package was 292 MB, over nuget.org's limit; it is now about 72 MB). With `--provider duckdb` the tool loads `libduckdb` from your build output (the `--project` or `--assembly` must reference `Durable.DuckDb`; `scaffold` uses the current project's output when built) or from `DURABLE_DUCKDB_NATIVE` (the library file or its directory), and fails with a command error that says how to fix it when the library cannot be found. SQLite's native library is still bundled.

**Tests**

- New targets `--type oracle|duckdb|mariadb|cockroachdb|yugabytedb|mongodb|cosmosdb` (docker images `gvenzl/oracle-free:23-slim-faststart`, `mariadb:11.4`, `cockroachdb/cockroach:latest-v26.3`, `yugabytedb/yugabyte:2026.1.2.0-b137`, `mongo:8` as a single-node replica set, and the Cosmos DB Linux vNext emulator; DuckDB runs in process). Every SQL suite and the conformance kit run on each SQL target; MongoDB and Cosmos DB run the conformance kit and their own backend suites through `IDocumentBackendTestTarget`.
- New suites: `Oracle.Provider`, `DuckDbProvider`, `MongoDbBackendTestSuite`, `CosmosDbBackendTestSuite`, and `durable` CLI coverage for the new provider names.
- Shared SQL suites gate cases on dialect features with `[RequiresDialect(DialectRequirement...)]`, evaluated when the suites are built: a dialect without savepoints, stored procedures or empty strings distinct from NULL reports those cases as skipped with a reason instead of passing them silently. The negative path has its own gated cases (`Savepoint_UnsupportedThrowsNotSupported`, `StoredProcedure_SqliteNotSupported`), and `GatingDialectMatchesProviderDialect` checks that the gating dialect is the provider's.
- Assertions that differ by database derive their expectation from the dialect (`ColumnAllowsNull`, `SupportsStringMaxLength`, `SupportsMultiRowInsert`, `MaxInListItems`) rather than from the database name.
- The connection-pool stress suite fills the driver pool to the workload's width before measuring memory growth, and `RapidConnectionCycling_ShouldNotLeakConnections` gained the warm-up pass the other memory measurements already had (ODP.NET keeps about 2 MB per pooled connection); the thresholds are unchanged. The Oracle test connection string turns off ODP.NET self-tuning and sets `Incr Pool Size=1`, so the pool does not grow by five connections at once during a measurement.
- The docker test sessions pull the image with retries before starting the container, so a transient registry error on a CI runner does not fail the run.
- `PublicApiConventionsTestSuite` covers `Durable.Oracle`, `Durable.DuckDb`, `Durable.MongoDb` and `Durable.CosmosDb`.
- `Test.Aot` runs a DuckDB scenario on .NET 9+ (DuckDB.NET.Data compiled in single-warn mode, like LiteDB).

**CI**

- New job "DuckDB" (in process, Linux, Windows and macOS, net8.0 and net10.0) and `databases` matrix entries for `oracle`, `mariadb`, `cockroachdb`, `yugabytedb`, `mongodb` and `cosmosdb` (net8.0 and net10.0).

**Breaking changes**

- `durable --provider mariadb` now selects `MariaDbDialect`; it used to be an alias of `mysql` (MySQL dialect). Use `--provider mysql` to keep the old behavior against a MariaDB server.
- `ISqlDialect` has new members (listed above). Dialects deriving from `SqlDialect` get behavior-preserving defaults; a class implementing `ISqlDialect` directly must add them.
- `RepositoryCapabilities.All` now includes `EmptyStrings`. A custom backend that returns `All` now claims it (correct for any store that keeps `""` distinct from null); code that stores or compares capability values numerically sees a new bit.
- Conformance kit cases that use empty strings were split into new cases that require `RepositoryCapabilities.EmptyStrings`, so test reports of a custom backend show new case names (`EqualsNullOnStrings`, ...) and a new `Capabilities.EmptyStrings` case.
- Aggregate `Sum`/`Average` results are converted through the repository's data type converter instead of `Convert.ToDecimal`; a custom `IDataTypeConverter` must convert the driver's aggregate type to `decimal`.

### v0.6.0 (2026-10-06)

Entity mapping sources: Durable can map classes that cannot carry its attributes, such as generated code, models owned by another team or package, and models already annotated for another library. The work was prompted by a fork that needed to map classes carrying its own attribute system.

**Core (`Durable`)**

- `IEntityMappingSource`: `Describes(Type)`, `GetEntityAttribute(Type)`, `GetPropertyAttributes(Type, PropertyInfo)` and `GetCompositeIndexes(Type)`. A source answers with Durable's own attribute objects constructed in code, so queries, includes, migrations, the CLI and every backend treat a source-mapped class exactly like an attributed one. A class whose source returns no `PropertyAttribute` is convention-mapped.
- `DurableMapping.Register<T>(source)` and `Register(Type, source)` register a source for one type; `DurableMapping.MappingSource` sets one for every type it `Describes`. Lookup order: per-type registration, then `MappingSource` when it describes the type, then the class's attributes. `DurableMapping.GetMappingSource(Type)` returns the source that applies, and `DurableMapping.AttributeSource` is the built-in attribute reader for sources that add to a class's attributes rather than replace them.
- `EntityMetadata.MappingSource`: the source a type's metadata was built from.
- When to use a source, and when attributes, conventions or `DurableMapping.NamingConvention` are the better fit, is covered in the README section "Mapping classes you don't own", with examples for generated classes, an adapter for another attribute system, adding to a class's own attributes, and the CLI.
- A built type's mapping cannot change: registering a different source for it, per type or through `MappingSource`, throws `InvalidOperationException` naming the type. Registering the source it already uses is allowed.
- Every mapping attribute is now read in one place (the attribute source) instead of through scattered `GetCustomAttribute` calls; behavior for attributed classes is unchanged.

**Documentation**

- On SQL Server, `SqlRepositoryOptions.CommandTimeoutSeconds` does not cover transaction commits: SqlClient issues `COMMIT` with the connection's `Command Timeout` (default 30 s). The README and the property's XML docs now say so and show how to raise it in the connection string or `AdditionalProperties`.

**Command-line tool (`Durable.Tool`)**

- `--mapping-source <type>` (and `mappingSource` in `durable.json`) for `schema diff`, `schema sync` and `migrations add`: instantiates an `IEntityMappingSource` from your assembly, registers it for the classes it describes, and discovers those classes as entities alongside `[Entity]` types. A missing, ambiguous or unusable type is a command error.

**Tests**

- `MappingSourceTestSuite` (all four SQL providers): translated metadata, CRUD, composite and auto-increment keys, reference, collection and many-to-many includes, a version conflict, soft delete, a value converter, a generated default, indexes created by `InitializeTable`, an empty schema diff, the in-memory and LiteDB backends over the same classes, the global source, and the registration rules. The classes carry only test-only attributes translated by an adapter.
- `DurableToolTestSuite` covers `--mapping-source`: discovery, schema diff, a generated migration that compiles and applies the mapped schema, and errors.
- `Test.Aot` checks that a source-mapped class (with a converter attached by the source) round-trips through SQLite under Native AOT.
- The SQL Server test connection uses a 120 s command timeout. SqlClient applies it to `COMMIT`, and a containerized server on a shared CI runner once stalled a commit in the connection-pool stress suite past the 30 s default.

**Breaking changes**

None.

### v0.5.0 (2026-10-06) - breaking

**New packages**

- `Durable.LiteDb`: a LiteDB backend (`LiteDbBackend`, `LiteDbRepository<T>`, `LiteDbRepositorySettings`, `LiteDbConnectionType`, `LiteDbTransaction`, `LiteDbQueryPlan`); depends only on `Durable` and LiteDB 5.0.21.
  - `LiteDbBackend.Create(settings?)` / `CreateAsync(settings?, token)` with `LiteDbRepositorySettings.ForInMemory()`, `ForFile(path)` or `ForDatabase(liteDatabase)` (a database you pass is never disposed; `OwnsDatabase`). Settings: `Filename`, `Password`, `ConnectionType`, `Timeout`, `ReadOnly`, `InitialSizeBytes`, `Upgrade`, `Logger`, `JsonOptions`.
  - Mapping comes from `EntityMetadata` (LiteDB's `BsonMapper` is not used): one collection per entity, a single key is `_id`, a composite key is its fields plus an `_id` sub-document; converters, JSON columns, enums, version and soft-delete columns behave as on the other backends.
  - Lossless, order-preserving encodings: `DateTime` ticks and kind, `DateTimeOffset` UTC ticks and offset, decimal scale, `ulong`, `DateOnly`, `TimeOnly`.
  - Comparisons, null checks and `IN` lists are pushed down to LiteDB and use indexes on keys, foreign keys and `[Index]` columns (`EnsureIndexes`); everything else is evaluated client-side with `QueryEvaluator<TRow>`. Plans are observable through `LastQueryPlan`, the `QueryPlanned` event (`ExplainQueries` adds LiteDB's explain output) and `ILogger`; `GetStoredRows` returns the stored BSON documents.
  - Atomic auto-increment from LiteDB sequences; writes lock the collection before reading, so optimistic-concurrency updates cannot be lost.
  - Transactions run on a dedicated thread and work across `await`; a transaction LiteDB rolled back after a failed operation is detected and rejects further use.
  - Declares `RepositoryCapabilities.All` and passes the full conformance kit.
- `Durable.LiteGraph`: a LiteGraph property-graph backend (`LiteGraphBackend`, `LiteGraphRepository<T>`, `LiteGraphRepositorySettings`, `LiteGraphTransaction`, `LiteGraphQueryPlan`, `LiteGraphRelationship`); depends on `Durable` and LiteGraph 10.1.0.
  - `LiteGraphBackend.Create(settings?)` / `CreateAsync(settings?, token)` with `LiteGraphRepositorySettings.ForInMemory()` (the default when neither `Client` nor `Filename` is set; `IsInMemory`), `ForFile(path)` (`LoadIntoMemory` optional) or `ForClient(client, tenantGuid?, graphGuid?)` (a client you pass is never disposed; `OwnsClient`). Settings also cover tenant/graph GUID or name (default "Durable"), `MaintainEdges`, `PushDownDataFilters`, `PreserveNodeSubordinates`, `MaxOperationsPerTransaction`, `TransactionTimeout` (`TimeSpan`, 1 s to 1 h), `Logger`, `JsonOptions`.
  - Not Native AOT-compatible yet: the LiteGraph library uses reflection-based System.Text.Json. Durable.LiteGraph's own code is annotated, and `Create`/`CreateAsync` carry `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]`.
  - Each row is a node labelled with the table name, with losslessly encoded column values as node data and a GUID derived from graph, table and primary key (`GetNodeGuid`).
  - Foreign keys are maintained as edges from dependent to principal: created with either side, moved when the key changes, removed with either node, back-filled when a principal arrives later; many-to-many junction rows are nodes with an edge to each side. `RebuildEdges`/`RebuildEdgesAsync(IEnumerable<EntityMetadata>?)` recompute edges.
  - Labels, keys and exact string / non-negative integer equality are pushed down; the full filter is evaluated with `QueryEvaluator<TRow>`. Plans are reported through `QueryPlanned`, `LastQueryPlan` and `ILogger`.
  - Interactive transactions with read-your-writes, committed atomically as one LiteGraph graph transaction (first committer wins), limited to `MaxOperationsPerTransaction` operations.
  - Declares `RepositoryCapabilities.All` and passes the full conformance kit.
- `Durable.Tool`: the `durable` .NET tool (net8.0/net10.0) for SQLite, PostgreSQL, MySQL and SQL Server.
  - `migrate [--target]`, `rollback --target <id|0>`, `status` / `migrations list`, `script [--from] [--to] [--output]`.
  - `migrations add <Name>`: `Up`/`Down` generated from the schema diff when a database is configured (Down only when every step is reversible), otherwise an empty template; Ids are UTC `yyyyMMddHHmmss_Name`.
  - `schema diff [--sql]`, `schema sync [--dry-run] [--allow-destructive]`, `scaffold [--tables] [--output-dir] [--namespace] [--force] [--no-singularize]`.
  - Settings from the command line, `DURABLE_PROVIDER` / `DURABLE_CONNECTION` and `durable.json`. Your project is built with `dotnet build` (`--framework` for multi-targeting), or a built `--assembly` is loaded in an isolated load context. Exit codes: 0 success, 1 command error, 2 unexpected failure. `DurableCli.RunAsync` exposes the CLI as a library entry point.

**Core (`Durable`)**

- `Durable.Query.QueryEvaluator<TRow>`: the client-side evaluator with C# semantics, promoted from `Durable.InMemory` to a public, subclassable class so any non-SQL backend can evaluate residual predicates, navigation members, collection predicates, ordering, paging and aggregates over its own row type. Hooks: `GetValue` and `FindRows` (abstract), `NormalizeValue`, `RelatedRows` and `IsSoftDeleted` (virtual). Helpers: `Bind`, `Test`, `Filter`, `Sort`, `Apply(QueryModel)`, `Aggregate`, and `GroupRows`/`GroupSource` for grouping aggregates. The in-memory backend now uses it; its behavior is unchanged.

**Non-SQL backends: one convention**

- `InMemoryBackend`, `LiteDbBackend` and `LiteGraphBackend` share one shape: `XBackend.Create(settings?)` / `CreateAsync(settings?, token)` (no public constructors); `XRepositorySettings` with `ForInMemory()` (plus `ForFile`, `ForDatabase`/`ForClient` where they apply), `IsInMemory` and `Validate()`; `backend.CreateRepository<T>(options?)` or `new XRepository<T>(backend, options?)`; a typed `repository.Backend` property.
- Backends are `IDisposable` and `IAsyncDisposable` and throw `ObjectDisposedException` afterwards; repositories never dispose their backend. A store you pass in (LiteDB database, LiteGraph client) is never disposed (`OwnsDatabase`, `OwnsClient`).
- Typed transactions: `BeginTransaction()` / `BeginTransactionAsync(token)` return `InMemoryTransaction`, `LiteDbTransaction` or `LiteGraphTransaction` (all `IAsyncDisposable`); `backend.Owns(transaction)`.
- `Clear()` / `ClearAsync(token)` empty every table and `Clear(type)` / `ClearAsync(type, token)` one table, returning the number of rows removed; auto-increment sequences restart. `GetStoredRows(type)` / `GetStoredRowsAsync` return the stored rows.
- LiteDB and LiteGraph report query plans through a `QueryPlanned` event (`EventHandler<XQueryPlan>`; plans derive from `EventArgs` and carry `Operation` and `EntityType`) and `LastQueryPlan`. A handler that throws is logged, never propagated.
- `InMemoryRepositorySettings`: `Capabilities` (default `All`) and `JsonOptions`.

**SQL engine (`Durable.Sql`) and providers**

- `ISqlDialect.TableNamesQuery()` (implemented by all four dialects) and `DatabaseSchemaReader.ReadTableNames` / `ReadTableNamesAsync`.
- `ColumnSchema.IsAutoIncrement`, read from an optional sixth column of the dialect's column schema query (all four dialects report it).
- `SqlMigrator.GenerateScript(fromId, toId)` / `GenerateScriptAsync(fromId, toId, token)`: script a range of migrations regardless of the database history, for example to upgrade another environment between two releases.
- `SqlStatement.ToInlineSql(ISqlDialect)`: render a statement with its parameters inlined as dialect-formatted literals, as migration scripts are written (for reviewable scripts and generated DDL; prefer parameters when executing statements that carry user data).

**Native AOT**

- Every library is marked `IsAotCompatible` and builds with zero trim/AOT warnings (`Durable`, `Durable.Sql`, the four SQL providers, `Durable.InMemory`, `Durable.LiteDb`, `Durable.LiteGraph`, `Durable.Conformance`); `Durable.Tool` is excluded because it loads your assemblies. `Durable.LiteGraph` is annotated, but the LiteGraph library is not AOT-compatible yet, so it does not run under Native AOT.
- No runtime code generation under AOT: accessors and row readers use reflection invokers (a 10k-row SQLite read takes about 11 ms AOT versus 12 ms JIT on .NET 10); client-side LINQ pieces run on the expression interpreter. JIT applications keep compiled delegates.
- `[DynamicallyAccessedMembers]` annotations on entity type parameters (`IRepository<T>`, `IQueryBuilder<T>`, `RepositoryBase<T>`, `SqlRepository<T>`, provider repositories, resolvers, `Select<TResult>`, `FromSql<TResult>`, `FromProcedure<TResult>`) and on `Type` arguments of `[ForeignKey]`/`[ManyToManyNavigationProperty]`, so the trimmer keeps entity members. New `EntityMetadata.RequiredMemberTypes` for annotating your own wrappers; `MemberAccessorFactory.ConstructorMemberTypes` and `MemberAccessorFactory.GetDefaultValue(Type)`.
- Types reached only through a navigation property are rooted with `EntityMetadata.For<T>()`; if one was trimmed, Durable throws an `InvalidOperationException` that names the type and the fix.
- `DurableJson` (`CreateOptions(IJsonTypeInfoResolver?)`, `Serialize`, `Deserialize`): JSON columns with a source-generated `JsonSerializerContext`, passed through the provider data type converter (`new SqliteDataTypeConverter(json)`) or the non-SQL settings' `JsonOptions`.
- AOT-safe schema overloads taking `EntityMetadata`: `SqlMigrator.DiffSchema`/`GenerateSyncScript`/`SyncSchema` (+ `Async`), `MigrationContext.EnsureSchema`/`EnsureSchemaAsync`, `SchemaDiffer.Compare`. The `Type`-collection overloads, `InitializeTables`, `ValidateTables` and `AddMigrationsFromAssembly` are `[RequiresUnreferencedCode]`; use `AddMigration(new MyMigration())`.
- The `Durable` package sets `NullabilityInfoContextSupport=true` through `buildTransitive/Durable.props`, so nullable reference annotations still decide column nullability in trimmed applications.
- `src/Test.Aot`: an end-to-end application (SQLite, in-memory and, on .NET 9+, LiteDB scenarios; migrations, schema sync, JSON, includes, transactions, timing) published with `PublishAot` with trim/AOT warnings as errors; the CI `aot` job publishes and runs it on linux-x64 for net8.0 and net10.0.

**Public API review**

The public API of the core and SQL packages was reviewed for duplicates, naming, token placement and leaked internals. Additions (removals and renames are under Breaking changes):

- Raw SQL: interpolated overloads (`FromSql($"... {value}")`, `ExecuteSql`, `ExecuteScalar`, `QueryMultiple`, + `Async`) whose holes are always parameters, and explicit `*Raw` overloads taking text plus values; one placeholder convention everywhere (`{0}`, `{1}`, `{{`/`}}` literal braces, verbatim text when there are no values), documented on `RawSql`. `ISqlQueryBuilder<T>.WhereSql(FormattableString)`; `RawSql.ToStatement(...)`.
- `ValidateTableAsync` / `ValidateTablesAsync`, returning `TableValidationResult` / `SchemaValidationResult` (`IsValid`, `Errors`, `Warnings`, `TableExists`, `Tables`); validation can run inside a transaction.
- `AmbientTransactionScope` (and `ITransactionScope`) are `IAsyncDisposable`: `await using` rolls back an uncompleted scope asynchronously. Documented that Durable does not participate in `System.Transactions` (it never reads `Transaction.Current` or enlists; a driver that auto-enlists may still enlist connections Durable opens).
- `MergeChangesResolver<T>(MergeConflictBehavior, params string[] ignoredProperties)` and the top-level `MergeConflictBehavior` (`IncomingWins`, `CurrentWins`, `ThrowException`); collections are compared element by element.
- `[VersionColumn]` infers the version type from the property (`int`/`long`/`short`/`byte` counter, `DateTime` timestamp, `Guid`, `byte[]` binary counter); a declared type that does not fit the property fails when metadata is built.
- Settings and factories: `SqliteRepositorySettings.Pooling`; all server providers share `ConnectionTimeout`, `MinPoolSize`, `MaxPoolSize`, `Pooling` (`int?`/`bool?`, documented defaults); `XConnectionFactory(XRepositorySettings, int? maxConcurrentConnections = null)` on all four providers.
- `ISqlDialect.SupportsNthValue` and `SupportsRangeFrameOffsets` (false on SQL Server, where `NthValue` and `Range(int, int)` now throw `NotSupportedException` at the call site instead of failing in the database).
- `protected SqlRepository<T>.CreateCommand(ConnectionLease, SqlStatement)` for provider bulk paths.
- `<exception>` documentation on the user-facing interfaces and thread-safety notes on public types.
- New test suites: `PublicApiConventions` (reflection checks over every Durable assembly: defaulted trailing `CancellationToken`, `Async` suffix, async twins for I/O members, no tuples, no `out`/`ref` on async-capable types), window frames, resolvers and default-value providers, `*WithQuery`, database lifecycle, provider settings and version columns.

**Fixes**

- SQLite: disposing a `SqliteConnectionFactory` no longer clears the driver's connection pool for its connection string (it raced with other factories and repositories using the same database and failed their in-flight queries with `ObjectDisposedException` 'SQLitePCL.sqlite3'). Only the private database generated for `:memory:` is released on dispose; call `SqliteConnection.ClearPool`/`ClearAllPools` to release a file or a named in-memory database.
- `UpdateField` and `BatchUpdate` (SQL and `RepositoryBase` backends) now write a fresh value to `byte[]` (`BinaryCounter`) version columns; before, held copies of the changed rows stayed "fresh".
- Raw SQL without parameters is sent verbatim, so `{` / `}` in parameterless DDL or JSON literals are no longer unescaped (`WhereRaw` without arguments included).
- Conflict resolution: an `OperationCanceledException` thrown by a resolver is no longer wrapped in `OptimisticConcurrencyException`; `ThrowExceptionResolver.ResolveConflictAsync` returns a faulted task instead of throwing synchronously, and its `ConcurrencyConflictException` carries the current, incoming and original entities.
- Native AOT: property ordering no longer relies on `MetadataToken` (unavailable under AOT), delegate types are no longer built at runtime, and decimal operators used by translated expressions are preserved from trimming.
- `QueryNormalizer` restores `char` constants compared with `char` columns. C# promotes `x.Letter == 'e'` to an `int` comparison, which matched nothing on the in-memory and other non-SQL backends.

**Continuous integration**

- GitHub Actions workflow (`.github/workflows/ci.yml`) on every push and pull request: a Release build of the solution; the Touchstone CLI runner on SQLite (with the in-memory, LiteDB and LiteGraph backends and the conformance kit) on Linux, Windows and macOS; the xUnit and NUnit adapters; PostgreSQL, MySQL and SQL Server in disposable docker containers; and a Native AOT job that publishes and runs `src/Test.Aot`; each on net8.0 and net10.0.

**Tests**

- New suites: `QueryEvaluator` (a dictionary-row evaluator), LiteDB backend (persistence, shared mode, push-down, parity with in-memory, precision, concurrency, settings, collation, disposal, transactions), LiteGraph backend (edges, traversal, persistence, round-trips, push-down parity, transactions, concurrency, settings, disposal), and the `durable` CLI (driven in-process on every provider; generated migrations and entities are compiled with Roslyn).
- The conformance kit now also runs against LiteDB and LiteGraph, with no skipped cases.
- Migration suite covers table enumeration, auto-increment detection, range scripts and inline SQL rendering.
- `ProviderRegression` suite (all four providers): chained `Where`/`Having` with `||` stay grouped, enums stored as text compare by name, `byte[]` binds and renders as a binary literal, custom-typed (value-converter) and `Guid` keys work with every by-key operation, synchronous `Execute` loads includes, empty `Contains` lists, and schema-qualified tables on PostgreSQL and SQL Server.
- SQL Server tests accept `DURABLE_TEST_USER=integrated` (or `--user integrated`) for Windows authentication, e.g. LocalDB.

**Documentation**

- README rewritten: backend comparison, package guide, supported LINQ, type mapping per provider, API overview, async/error-handling/thread-safety/dependency-injection guidance, testing with the in-memory backend, LiteDB, LiteGraph, custom backends, the non-SQL backend convention, Native AOT, CLI reference, CI, troubleshooting. CONNECTION_MGMT.md and CLAUDE.md updated for 0.5.0.

**Breaking changes**

Each item ends with how to migrate from 0.4.0.

*Repository API*

- `ReadFirstOrDefault` / `ReadFirstOrDefaultAsync` removed (identical to `ReadFirst[Async]`, which returns null when nothing matches). Rename to `ReadFirst[Async]`.
- `BatchDelete` / `BatchDeleteAsync` removed (identical to `DeleteMany[Async]`). Rename to `DeleteMany[Async]`.
- The "include query in results" switches are removed: `DurableConfiguration`, `ConfigurationSettingResult`, `ISqlTrackingConfiguration` (no longer a base of `ISqlRepository<T>`), `SqlRepository<T>.IncludeQueryInResults`, `SqlRepositoryOptions.IncludeQueryInResults`, `CreateAuto`/`CreateAutoAsync`. Call the explicit `*WithQuery` methods (`CreateWithQuery`, `ReadManyWithQuery`, `ExecuteWithQuery`, ...) or use `CaptureSql`/`LastExecutedSql`.
- `Durable.RepositoryExtensions` removed (`SelectWithQuery`, `SelectWithQueryAsync`, `SelectAsyncWithQuery`, `GetSelectQuery`). Use `repo.Query(tx).Where(p).ExecuteWithQuery()` / `ExecuteWithQueryAsync(token)` / `ExecuteAsyncEnumerableWithQuery(token)` / `.Query`, or `ReadManyWithQuery[Async]`.
- `RepositoryResultExtensions`: the `Task<...>` overloads of `AsEntity`/`AsValue`/`AsCount` are renamed `AsEntityAsync`/`AsValueAsync`/`AsCountAsync` and take a `CancellationToken`; `AsAsyncEnumerable(Task<IAsyncDurableResult<T>>)` takes an `[EnumeratorCancellation]` token. Add the `Async` suffix.

*Raw SQL (`ISqlRepository<T>`, `MigrationContext`)*

- The `@p0`, `@p1` placeholder convention is gone; placeholders are `{0}`, `{1}` everywhere (as `WhereRaw` already used), `{{`/`}}` are literal braces, and SQL without values is sent verbatim. Replace `@p0` with `{0}` in raw SQL.
- `FromSql`, `FromSql<TResult>`, `ExecuteSql`, `ExecuteScalar<TResult>`, `QueryMultiple` (+ `Async`) with `(string sql, ITransaction?, [token,] params object?[])` are replaced by interpolated forms taking a `FormattableString` and `*Raw` forms taking `(string sql, IEnumerable<object?>? parameters = null, ITransaction? transaction = null, CancellationToken token = default)`; the token is always last. `ExecuteSqlAsync("DDL")` becomes `ExecuteSqlRawAsync("DDL")`; `ExecuteScalarAsync<long>("... = @p0", null, default, id)` becomes `ExecuteScalarAsync<long>($"... = {id}")` or `ExecuteScalarRawAsync<long>("... = {0}", new object?[] { id })`; `FromSql(sql, tx, a)` becomes `FromSqlRaw(sql, new object?[] { a }, tx)`. Holes can never be identifiers: build dynamic identifiers into the text of a `*Raw` call.
- Procedures: `ExecuteProcedure[Async]` and `FromProcedure[Async]<TResult>` take `(string procedureName, IEnumerable<SqlParameterValue>? parameters = null, ITransaction? transaction = null[, CancellationToken token = default])`. `ExecuteProcedureAsync("p", tx, ct, a, b)` becomes `ExecuteProcedureAsync("p", new[] { a, b }, tx, ct)`.
- `MigrationContext.ExecuteSql`/`ExecuteScalar` (+ `Async`) take a `FormattableString`; text plus values moved to `ExecuteSqlRaw`/`ExecuteScalarRaw` (+ `Async`). `context.ExecuteSql("DDL")` becomes `context.ExecuteSqlRaw("DDL")`. The `durable` tool now generates `ExecuteSqlRaw` calls.
- `RawSql.Positional` removed; `BindPlaceholders` takes `IReadOnlyList<object?>?`. Use `RawSql.ToStatement(...)`.

*Schema validation*

- `ValidateTable(Type, out List<string> errors, out List<string> warnings)` and `ValidateTables(IEnumerable<Type>, out, out)` are replaced by `TableValidationResult ValidateTable(Type, ITransaction? = null)` and `SchemaValidationResult ValidateTables(IEnumerable<Type>, ITransaction? = null)` (+ `ValidateTableAsync`/`ValidateTablesAsync`). Use `var r = await repo.ValidateTableAsync(typeof(X)); if (!r.IsValid) ... r.Errors / r.Warnings`.

*Transactions*

- `Durable.TransactionScope` renamed `AmbientTransactionScope` (no more clash with `System.Transactions.TransactionScope`), and `TransactionScopeExtensions` renamed `AmbientTransactionScopeExtensions` (`Create`, `CreateAsync`, `Current` and `ExecuteInTransactionScope[Async]` are unchanged). Replace `TransactionScope` with `AmbientTransactionScope`; prefer `await using`.
- `ITransactionScope` now extends `IAsyncDisposable`. Custom implementations add `DisposeAsync`.
- `ISavepoint` is no longer `IDisposable` (its `Dispose` did nothing, so `using` suggested a rollback that never happened). Remove `using` around savepoints and call `Rollback[Async]` or `Release[Async]`.

*Optimistic concurrency*

- `IConcurrencyConflictResolver<T>.ResolveConflictAsync` / `TryResolveConflictAsync` take a trailing `CancellationToken token = default`. Implementations add the parameter.
- `ImprovedMergeChangesResolver<T>` removed; its behavior is folded into `MergeChangesResolver<T>`, and the nested `ConflictBehavior` enum is the top-level `Durable.MergeConflictBehavior`. `new ImprovedMergeChangesResolver<T>(ConflictBehavior.X, ...)` becomes `new MergeChangesResolver<T>(MergeConflictBehavior.X, ...)`.
- `MergeChangesResolver<T>` compares collections element by element, and `TryResolveConflict` swallows only `ConcurrencyConflictException`/`ArgumentNullException` (it swallowed every exception). Review code that relied on the old reference comparison.
- `VersionColumnType.RowVersion` renamed `BinaryCounter` (an 8-byte big-endian counter Durable maintains; not SQL Server's server-generated `rowversion`). Use `[VersionColumn(VersionColumnType.BinaryCounter)]` or just `[VersionColumn]` on a `byte[]` property.
- `[VersionColumn]` without an argument no longer means `RowVersion`: the type is inferred from the property, and `VersionColumnAttribute.Type` is `VersionColumnType?`. A declared type that does not fit the property throws `InvalidOperationException` when metadata is built (it used to fail at update time with `InvalidCastException`). Fix mismatched declarations; a bare `[VersionColumn]` on an `int` is now an integer counter.
- `VersionColumnInfo` is immutable, constructed with `VersionColumnInfo(string columnName, PropertyInfo property, VersionColumnType? declaredType)`. Replace object initializers with the constructor.

*Settings, factories and options*

- `RepositorySettings` and `RepositoryType` moved from `Durable` to the `Durable.Sql` namespace and assembly. Add `using Durable.Sql;`.
- `MySqlRepositorySettings.MinimumPoolSize`/`MaximumPoolSize` (`uint?`) renamed `MinPoolSize`/`MaxPoolSize` (`int?`), and `ConnectionTimeout` is `int?`. Rename and drop `u` suffixes.
- `PostgresRepositorySettings.CommandTimeout` removed (it duplicated `SqlRepositoryOptions.CommandTimeoutSeconds`; a parsed "Command Timeout" key is kept in `AdditionalProperties`). Use `SqlRepositoryOptions.CommandTimeoutSeconds`.
- PostgreSQL and MySQL `Parse` leave `SslMode` null when the connection string selects the driver default. Treat null as "driver default".
- `IBatchInsertConfiguration.EnablePreparedStatementReuse` / `BatchInsertConfiguration.EnablePreparedStatementReuse` removed (never read). Delete the assignment.

*Non-SQL backends (`Durable.InMemory`)*

- `new InMemoryBackend(...)` is replaced by `InMemoryBackend.Create(settings?)` / `CreateAsync(settings?, token)`; capabilities and JSON options move to `InMemoryRepositorySettings` (`Capabilities`, `JsonOptions`). `new InMemoryBackend(RepositoryCapabilities.None)` becomes `InMemoryBackend.Create(new InMemoryRepositorySettings { Capabilities = RepositoryCapabilities.None })`.
- `InMemoryRepository<T>.Store` removed; use the typed `Backend` property.
- `InMemoryBackend` is `IDisposable`/`IAsyncDisposable`; disposing discards its data and later calls throw `ObjectDisposedException`. Keep the backend alive as long as its repositories.
- `InMemoryBackend.BeginTransactionAsync` returns `Task<InMemoryTransaction>`, and `Clear(Type)` returns the number of rows removed. Source-compatible for most callers; recompile.

*SQL engine extension points*

- Engine internals are now `internal`: `SqlCommandExecutor` (and `SqlRepository<T>.Executor`), `IncludeLoader`, row materializers, `SqlExpressionTranslator`, `SqlWriteBuilder`, the select/table models, `SqlSavepoint`, the date parsers and the concrete query builders (`SqlQueryBuilder<T>` and friends). Use the interfaces (`ISqlQueryBuilder<T>`, ...); custom providers use the new `protected CreateCommand(ConnectionLease, SqlStatement)`.
- `ISqlDialect` gains `TableNamesQuery()`, `SupportsNthValue` and `SupportsRangeFrameOffsets`. Classes deriving from `SqlDialect` inherit defaults (`TableNamesQuery()` throws `NotSupportedException`, so `ReadTableNames` and `durable scaffold` need an override); classes implementing `ISqlDialect` directly must add the members.
- `ColumnSchema` keeps its 0.4.0 six-parameter constructor; the new seven-parameter constructor requires `isAutoIncrement`. No change for 0.4.0 callers.

*Dependencies*

- `Durable.Sqlite` moves to Microsoft.Data.Sqlite 10.0.12 and SQLitePCLRaw.bundle_e_sqlite3 3.0.5 (from 10.0.11 and 2.1.12). Update any direct SQLitePCLRaw 2.x references to 3.x.

### v0.4.0 (2026-10-05) - breaking

**Neutral query model and backends**

- `Durable.Query` (core): `QueryNormalizer` turns LINQ into an immutable, backend-neutral `QueryNode` tree with C# semantics (evaluated client values, enum handling, null checks, navigation and grouping nodes). The SQL engine now renders that tree; a non-SQL backend translates the same tree with a `QueryNodeVisitor<TResult>`.
- `IRepositoryBackend` + `RepositoryBase<T>` + `QueryBuilder<T>`: implement a small storage contract (query, count, aggregate, insert, replace, set-based update, delete, transactions) and get the whole `IRepository<T>` surface, including split-query Include, soft delete, query filters, optimistic concurrency and conflict resolvers.
- `RepositoryCapabilities` and `IRepository<T>.Capabilities`: backends declare what they support; unsupported operations throw `NotSupportedException` at the call site (`Include`, `GroupBy`, navigation predicates in `Where`, ...).
- New `Durable.InMemory` package: the reference non-SQL backend, with snapshot-isolation transactions and a capability mask for simulating limited backends.
- New `Durable.Conformance` package: 20 capability-gated Touchstone suites (228 cases) any `IRepository<T>` backend runs through `IConformanceTarget`/`ConformanceSuites.Build`. They run against SQLite, PostgreSQL, MySQL, SQL Server and the in-memory backend.

**String matching**

- `StringMatchMode` (`Database`, `Ordinal`, `IgnoreCase`) via `RepositoryOptions.StringMatching` or an explicit `StringComparison` argument. `Ordinal` and `IgnoreCase` return the same rows on all four databases for `==`, `!=`, ordering comparisons, `IN`, `Contains`/`StartsWith`/`EndsWith`, `Replace` and `IndexOf`. `Database` (default) keeps each collation's behavior.

**Migrations**

- `SqlMigrator`: versioned migrations (`Migration`, `MigrationContext`) recorded in a history table (default `__durable_migrations`), applied under a cross-process database lock, each in a transaction where the database supports transactional DDL; `RollbackTo` via `Down`; discovery from an assembly; reviewable scripts.
- Schema diff and sync: `DatabaseSchemaReader`, `SchemaDiffer`, `SyncSchema`/`GenerateSyncScript`. Additive changes apply automatically, destructive ones only with `AllowDestructive`; type, length, nullability and key differences are reported, never applied.

**Performance**

- Compiled, typed row readers with typed driver getters and inlined built-in conversions; direct parsing of SQLite's date format. On SQLite, 10k-row reads went from 15.7 ms to 10.9 ms (Dapper: 12.1 ms); includes are about 25% faster. New `src/Test.Benchmark` (BenchmarkDotNet; Durable vs Dapper vs ADO.NET).

**Fixes**

- PostgreSQL orders NULLs like LINQ and the other providers (`NULLS FIRST` ascending, `NULLS LAST` descending).
- `All(predicate)` treats a condition that compares NULL as false, so such a child violates `All` (C# semantics).
- Reference navigation members (`x.Author.Name`) ignore soft-deleted related rows, as Include and collection predicates do.
- Explicit `StringComparison.Ordinal`/`OrdinalIgnoreCase` arguments are now honored exactly instead of following the collation.
- SQLite: `SqliteConnectionFactory` sets `PRAGMA busy_timeout` (`BusyTimeoutMilliseconds`, default 30 s) so concurrent writers wait instead of failing with "database is locked", and `:memory:` now uses the memdb VFS instead of shared-cache mode, whose lock conflicts Microsoft.Data.Sqlite reports as `ArgumentOutOfRangeException`. For a named shared in-memory database, prefer `Data Source=file:/name?vfs=memdb` over `Mode=Memory;Cache=Shared`.

**Breaking changes**

- `ConflictResolver` moves from `ISqlRepository<T>` to `IRepository<T>`; `IRepository<T>` also gains `Capabilities`.
- `SqlFunction` is now `Durable.Query.QueryFunction`; `ExpressionEvaluator`, `GroupingSpecification`, `IncludeNode` and `KeyNormalizer` move to `Durable.Query`; `SqlExpressionTranslator.CustomTranslator` is replaced by `UseGrouping`/`GroupKeySql`.
- `ISqlDialect` gains string-matching (`OrdinalCollation`, `SupportsOrdinalLike`, `OrdinalStringMatch`, `StringCastType`), ordering (`OrderDirection`) and migration members; custom dialects deriving from `SqlDialect` get defaults.
- `IQueryBuilder.Count` is documented as applying Skip/Take, as it always did.

**Tests**

- Every build warning fixed without suppressions; tests and samples have correct nullable annotations.

### v0.3.0 (2026-10-05) - breaking

**Architecture**

- New `Durable.Sql` package: one SQL engine shared by all providers behind `ISqlDialect`. Providers shrink from ~15k lines each to ~700.
- `Durable` core is backend-neutral: `IRepository<T>`/`IQueryBuilder<T>` contain no SQL members; SQL features live on `ISqlRepository<T>`/`ISqlQueryBuilder<T>`.
- Cached per-type `EntityMetadata` with compiled accessors and constructors; ordinal-based compiled row materialization.

**Correctness**

- Every value is bound as a parameter (WHERE, HAVING, UPDATE SET, IN lists, raw `{0}` placeholders, window defaults): fixes culture-dependent numbers, double-escaped quotes, backslash corruption, LIKE wildcards without ESCAPE, and the Lead/Lag injection.
- Enums compare correctly whether stored as names or integers; `HasValue`/`.Value`, `??`, null on either side, `!=` on nullable columns (C# semantics), `!(a && b)` precedence, bool members on SQL Server, `List<T>.Contains`, empty IN lists.
- Include uses split queries: correct `Skip`/`Take` with collection includes, no row explosion, many-to-many on every provider, chunked key lists.
- `TransactionScope.CreateAsync` now makes the scope visible to the caller; ambient scopes of another provider are ignored.
- Repositories no longer dispose connection factories they were given.
- No sync-over-async; `ExecuteAsyncEnumerable` streams on every provider (including with includes); cancellation tokens flow to the driver.
- Set operations and many-to-many includes work on all four databases.
- Collection `Contains` translates on .NET 10 / C# 14, where `array.Contains(x)` binds to the span overload `MemoryExtensions.Contains`.

**Features**

- Composite primary keys (`KeyOrder`; pass `object[]` keys).
- Per-property value converters (`[ValueConverter]`, `ValueConverter<TModel, TProvider>`), explicit JSON columns (`Flags.Json`), `Flags.Integer` enums.
- Convention mapping for unattributed classes (`DurableMapping`, `[NotMapped]`, snake_case option).
- Global query filters (`AddQueryFilter`, `IgnoreQueryFilters`) and soft delete (`[SoftDelete]`).
- `BulkInsert` (SqlBulkCopy, PostgreSQL binary COPY, MySqlBulkCopy, prepared SQLite inserts); `CreateMany` returns generated keys in input order.
- Navigation predicates (`b.Author.Name == ...`, `a.Books.Any(...)`, `Count()`), grouped projections (`GroupBy().Having().Select(g => new {...})`), projections with Where/OrderBy/paging.
- `QueryMultiple`, `ExecuteProcedure`/`FromProcedure` with output parameters, `ExecuteScalar<T>`, DTO mapping by column name (snake_case to PascalCase).
- `SqlTransactionContext.Wrap` to join externally managed connections/transactions (Dapper, EF Core, ADO.NET).
- Diagnostics: `ILogger` (with slow-command warnings), OpenTelemetry `ActivitySource` "Durable", `ISqlCommandInterceptor`, `SqlCaptureScope`.

**Breaking changes**

- Custom `ConnectionPool`, `ConnectionPoolOptions`, `ISanitizer`, `SimpleChangeTracker`, `IDataSeeder` removed; drivers pool connections. `IConnectionFactory` is now `OpenConnection`/`OpenConnectionAsync` returning an open connection the caller disposes.
- Repository constructors take `(connectionString | settings | IConnectionFactory, SqlRepositoryOptions?)`; the conflict resolver is the `ConflictResolver` property.
- `Count` returns `long` everywhere; nullable annotations on all optional parameters.
- Enums are stored by name unless `Flags.Integer` is set (previously any `[Property]` without `Flags.String` stored integers).
- TimeSpan is stored as BIGINT ticks on MySQL and SQL Server.
- `IGroupedQueryBuilder.Select` returns an executable `IQueryBuilder<TResult>`; `Count()` counts groups.
- `WhereIn`/`WhereNotIn` take `(keySelector, subquery, subqueryKey)`; `WhereExists` accepts an optional correlation.
- Merge conflict resolution uses the incoming entity as the original snapshot (no change tracking).

### v0.2.0 (2026-08-15)

**Dependencies**

- Driver floors raised to the then-current majors: Microsoft.Data.Sqlite 9.0.6 to 10.0.11, Npgsql 8.0.5 to 10.0.3, MySqlConnector 2.3.7 to 2.6.2, Microsoft.Data.SqlClient 5.2.2 to 7.0.2. The version bump from 0.1.21 reflects the new dependency floors; the library API was unchanged.

**Tests**

- The test suite moved to Touchstone: tests are written once in `Test.Shared` and run by a console runner (with `--docker` for disposable PostgreSQL, MySQL and SQL Server containers) and by xUnit and NUnit adapters.
- New repository-operations suite with positive and negative cases for `FromSql`/`FromSqlAsync`, `BatchUpdate`, `UpdateField`, `UpsertMany` and `BatchDelete`.
- Test-tool updates (xunit.runner.visualstudio 4.0.0, NUnit 4.6.1, NUnit3TestAdapter 6.2.0, Microsoft.NET.Test.Sdk 18.9.0).

### v0.1.x (2025-10-10 to 2025-11-25)

The first public series (0.1.0 through 0.1.21; the provider packages were versioned independently of the core package). Patch releases fixed MySQL sanitization, enum handling, connection management and pooling, PostgreSQL issues, table initialization, date/time parsing and boolean handling. Features of the series, with later changes noted in parentheses:

- Generic architecture with clean interfaces (`IRepository<T>`, `IQueryBuilder<T>`, `IConnectionFactory`) (the `IConnectionFactory` contract changed in 0.3.0)
- Full LINQ support with expression tree parsing for type-safe queries
- Complete async/await support throughout the API
- Multi-database support: SQLite, MySQL, PostgreSQL, SQL Server
- Attribute-based entity configuration with `[Entity]` and `[Property]` attributes
- Relationship support: one-to-many, many-to-many with `[NavigationProperty]` and `[InverseNavigationProperty]`
- Connection pooling with configurable options (min/max pool size, timeouts, validation) (removed in 0.3.0; the ADO.NET drivers pool connections)
- Input sanitization (`ISanitizer`) (removed in 0.3.0; every value is bound as a parameter)
- Transaction management with explicit transactions and ambient `TransactionScope` support
- Savepoint support for nested transaction control
- Optimistic concurrency control with `[VersionColumn]` attribute
- Built-in conflict resolvers: `ClientWinsResolver`, `DatabaseWinsResolver`, `MergeChangesResolver`
- Batch operations: `CreateMany`, `UpdateMany`, `DeleteMany`, `BatchUpdate`, `BatchDelete`
- Optimized multi-row INSERT statements with configurable batching (since 0.3.0, `CreateMany` returns generated keys and `BulkInsert` is the bulk path)
- Advanced query features (on `ISqlQueryBuilder<T>` since 0.3.0):
  - Window functions (ROW_NUMBER, LAG, LEAD, SUM, etc.)
  - Common Table Expressions (CTEs) including recursive CTEs
  - Complex subqueries with `WhereIn`, `WhereExists`
  - CASE expressions for conditional logic
  - Set operations (UNION, INTERSECT, EXCEPT)
- Query builder with fluent API: `Where`, `OrderBy`, `ThenBy`, `Skip`, `Take`, `Distinct`
- Projection support with `Select` for custom result shapes
- Relationship loading with `Include` and `ThenInclude`
- Aggregate operations: `Count`, `Sum`, `Average`, `Min`, `Max`
- Raw SQL support: `FromSql`, `ExecuteSql` with parameter binding (on `ISqlRepository<T>` since 0.3.0)
- SQL capture and debugging with `ISqlCapture` interface
- Automatic query tracking with `ISqlTrackingConfiguration`
- `DurableResult<T>` objects for operations that include SQL information
- Enum storage options: string-based or integer-based (attributed enums without `Flags.String` were stored as integers; names became the default in 0.3.0)
- Custom data type converters via `IDataTypeConverter`
- In-memory testing support with SQLite
- Repository settings with connection string parsing and building:
  - `SqliteRepositorySettings` with DataSource, CacheMode, Mode
  - `MySqlRepositorySettings` with connection timeout, pooling, SSL mode
  - `PostgresRepositorySettings` with command timeout, SSL mode
  - `SqlServerRepositorySettings` with encryption, integrated security
- Constructor overloads accepting connection strings or settings objects
- Extension methods: `SelectWithQuery`, `GetSelectQuery`, `SelectAsyncWithQuery` (`GetSelectQuery` was removed in 0.3.0)
