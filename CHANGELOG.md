# Change Log

All notable changes to Durable are listed here, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/) loosely: each version lists what was added, changed and fixed, grouped by area, and ends with a **Breaking changes** list that names every change that can break code compiled against the previous version. Durable is in alpha (0.x), so minor versions may break. Since 0.2.0 all packages share one version number and are released together. Dates are release dates (YYYY-MM-DD).

## Current Version

### v0.5.0 (unreleased) - breaking

**New packages**

- `Durable.LiteDb`: a LiteDB backend (`LiteDbBackend`, `LiteDbRepository<T>`, `LiteDbRepositorySettings`, `LiteDbConnectionType`, `LiteDbTransaction`, `LiteDbQueryPlan`); depends only on `Durable` and LiteDB 5.0.21.
  - Mapping comes from `EntityMetadata` (LiteDB's `BsonMapper` is not used): one collection per entity, a single key is `_id`, a composite key is its fields plus an `_id` sub-document; converters, JSON columns, enums, version and soft-delete columns behave as on the other backends.
  - Lossless, order-preserving encodings: `DateTime` ticks and kind, `DateTimeOffset` UTC ticks and offset, decimal scale, `ulong`, `DateOnly`, `TimeOnly`.
  - Comparisons, null checks and `IN` lists are pushed down to LiteDB and use indexes on keys, foreign keys and `[Index]` columns; everything else is evaluated client-side with `QueryEvaluator<TRow>`. Plans are observable through `LastQueryPlan`, `QueryPlanned` and `ILogger`.
  - Atomic auto-increment from LiteDB sequences; writes lock the collection before reading, so optimistic-concurrency updates cannot be lost.
  - Transactions run on a dedicated thread and work across `await`; a transaction LiteDB rolled back after a failed operation is detected and rejects further use.
  - Declares `RepositoryCapabilities.All` and passes the full conformance kit.
- `Durable.LiteGraph`: a LiteGraph property-graph backend (`LiteGraphBackend`, `LiteGraphRepository<T>`, `LiteGraphBackendSettings`, `LiteGraphTransaction`, `LiteGraphQueryPlan`, `LiteGraphRelationship`); depends on `Durable` and LiteGraph 10.1.0.
  - Each row is a node labelled with the table name, with losslessly encoded column values as node data and a GUID derived from graph, table and primary key (`GetNodeGuid`).
  - Foreign keys are maintained as edges from dependent to principal: created with either side, moved when the key changes, removed with either node, back-filled when a principal arrives later; many-to-many junction rows are nodes with an edge to each side. `RebuildEdgesAsync` repairs edges.
  - Labels, keys and exact string / non-negative integer equality are pushed down; the full filter is evaluated with `QueryEvaluator<TRow>`. Plans are reported through `QueryPlanned` and `ILogger`.
  - Interactive transactions with read-your-writes, committed atomically as one LiteGraph graph transaction (first committer wins), limited to `MaxOperationsPerTransaction` operations.
  - Declares `RepositoryCapabilities.All` and passes the full conformance kit.
- `Durable.Tool`: the `durable` .NET tool (net8.0/net10.0) for SQLite, PostgreSQL, MySQL and SQL Server.
  - `migrate [--target]`, `rollback --target <id|0>`, `status` / `migrations list`, `script [--from] [--to] [--output]`.
  - `migrations add <Name>`: `Up`/`Down` generated from the schema diff when a database is configured (Down only when every step is reversible), otherwise an empty template; Ids are UTC `yyyyMMddHHmmss_Name`.
  - `schema diff [--sql]`, `schema sync [--dry-run] [--allow-destructive]`, `scaffold [--tables] [--output-dir] [--namespace] [--force] [--no-singularize]`.
  - Settings from the command line, `DURABLE_PROVIDER` / `DURABLE_CONNECTION` and `durable.json`. Your project is built with `dotnet build` (`--framework` for multi-targeting), or a built `--assembly` is loaded in an isolated load context. Exit codes: 0 success, 1 command error, 2 unexpected failure. `DurableCli.RunAsync` exposes the CLI as a library entry point.

**Core (`Durable`)**

- `Durable.Query.QueryEvaluator<TRow>`: the client-side evaluator with C# semantics, promoted from `Durable.InMemory` to a public, subclassable class so any non-SQL backend can evaluate residual predicates, navigation members, collection predicates, ordering, paging and aggregates over its own row type. Hooks: `GetValue` and `FindRows` (abstract), `NormalizeValue`, `RelatedRows` and `IsSoftDeleted` (virtual). Helpers: `Bind`, `Test`, `Filter`, `Sort`, `Apply(QueryModel)`, `Aggregate`, and `GroupRows`/`GroupSource` for grouping aggregates. The in-memory backend now uses it; its behavior is unchanged.

**SQL engine (`Durable.Sql`) and providers**

- `ISqlDialect.TableNamesQuery()` (implemented by all four dialects) and `DatabaseSchemaReader.ReadTableNames` / `ReadTableNamesAsync`.
- `ColumnSchema.IsAutoIncrement`, read from an optional sixth column of the dialect's column schema query (all four dialects report it).
- `SqlMigrator.GenerateScript(fromId, toId)` / `GenerateScriptAsync(fromId, toId, token)`: script a range of migrations regardless of the database history, for example to upgrade another environment between two releases.
- `SqlStatement.ToInlineSql(ISqlDialect)`: render a statement with its parameters inlined as dialect-formatted literals, as migration scripts are written (for reviewable scripts and generated DDL; prefer parameters when executing statements that carry user data).

**Fixes**

- `QueryNormalizer` restores `char` constants compared with `char` columns. C# promotes `x.Letter == 'e'` to an `int` comparison, which matched nothing on the in-memory and other non-SQL backends.

**Continuous integration**

- GitHub Actions workflow (`.github/workflows/ci.yml`) on every push and pull request: a Release build of the solution; the Touchstone CLI runner on SQLite (with the in-memory, LiteDB and LiteGraph backends and the conformance kit) on Linux, Windows and macOS; the xUnit and NUnit adapters; and PostgreSQL, MySQL and SQL Server in disposable docker containers; each on net8.0 and net10.0.

**Tests**

- New suites: `QueryEvaluator` (a dictionary-row evaluator), LiteDB backend (persistence, shared mode, push-down, parity with in-memory, precision, concurrency, settings, collation, disposal, transactions), LiteGraph backend (edges, traversal, persistence, round-trips, push-down parity, transactions, concurrency, settings, disposal), and the `durable` CLI (driven in-process on every provider; generated migrations and entities are compiled with Roslyn).
- The conformance kit now also runs against LiteDB and LiteGraph, with no skipped cases.
- Migration suite covers table enumeration, auto-increment detection, range scripts and inline SQL rendering.

**Documentation**

- README rewritten: backend comparison, package guide, supported LINQ, type mapping per provider, API overview, async/error-handling/thread-safety/dependency-injection guidance, testing with the in-memory backend, LiteDB, LiteGraph, custom backends, CLI reference, CI, troubleshooting.

**Native AOT**

- Native AOT: <!-- filled in when the AOT work merges -->

**Public API review**

<!-- filled in after the API review -->

**Breaking changes**

- `ISqlDialect` gains `TableNamesQuery()`. Classes deriving from `SqlDialect` inherit a default that throws `NotSupportedException` (so `ReadTableNames` and `durable scaffold` are unavailable until they override it); classes implementing `ISqlDialect` directly must add the member.
- `ColumnSchema`'s constructor gains an optional `isAutoIncrement` parameter. Source-compatible, but code compiled against 0.4.0 that calls the constructor must be recompiled.
- No other public API was removed or changed. The in-memory evaluator that moved to core (`InMemoryQueryEvaluator`) was internal; `IsSoftDeleted` is now a virtual instance method of the public `QueryEvaluator<TRow>`.

## Previous Versions

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
