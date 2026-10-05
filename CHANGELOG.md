# Change Log

## Current Version

v0.4.0 (breaking)

Neutral query model and backends
- `Durable.Query` (core): `QueryNormalizer` turns LINQ into an immutable, backend-neutral `QueryNode` tree with C# semantics (evaluated client values, enum handling, null checks, navigation and grouping nodes). The SQL engine now renders that tree; a non-SQL backend translates the same tree with a `QueryNodeVisitor<TResult>`.
- `IRepositoryBackend` + `RepositoryBase<T>` + `QueryBuilder<T>`: implement a small storage contract (query, count, aggregate, insert, replace, set-based update, delete, transactions) and get the whole `IRepository<T>` surface, including split-query Include, soft delete, query filters, optimistic concurrency and conflict resolvers.
- `RepositoryCapabilities` and `IRepository<T>.Capabilities`: backends declare what they support; unsupported operations throw `NotSupportedException` at the call site (`Include`, `GroupBy`, navigation predicates in `Where`, ...).
- New `Durable.InMemory` package: the reference non-SQL backend, with snapshot-isolation transactions and a capability mask for simulating limited backends.
- New `Durable.Conformance` package: 20 capability-gated Touchstone suites (228 cases) any `IRepository<T>` backend runs through `IConformanceTarget`/`ConformanceSuites.Build`. They run against SQLite, PostgreSQL, MySQL, SQL Server and the in-memory backend.

String matching
- `StringMatchMode` (`Database`, `Ordinal`, `IgnoreCase`) via `RepositoryOptions.StringMatching` or an explicit `StringComparison` argument. `Ordinal` and `IgnoreCase` return the same rows on all four databases for `==`, `!=`, ordering comparisons, `IN`, `Contains`/`StartsWith`/`EndsWith`, `Replace` and `IndexOf`. `Database` (default) keeps each collation's behavior.

Migrations
- `SqlMigrator`: versioned migrations (`Migration`, `MigrationContext`) recorded in a history table (default `__durable_migrations`), applied under a cross-process database lock, each in a transaction where the database supports transactional DDL; `RollbackTo` via `Down`; discovery from an assembly; reviewable scripts.
- Schema diff and sync: `DatabaseSchemaReader`, `SchemaDiffer`, `SyncSchema`/`GenerateSyncScript`. Additive changes apply automatically, destructive ones only with `AllowDestructive`; type, length, nullability and key differences are reported, never applied.

Performance
- Compiled, typed row readers with typed driver getters and inlined built-in conversions; direct parsing of SQLite's date format. On SQLite, 10k-row reads went from 15.7 ms to 10.9 ms (Dapper: 12.1 ms); includes are about 25% faster. New `src/Test.Benchmark` (BenchmarkDotNet; Durable vs Dapper vs ADO.NET).

Fixes
- PostgreSQL orders NULLs like LINQ and the other providers (`NULLS FIRST` ascending, `NULLS LAST` descending).
- `All(predicate)` treats a condition that compares NULL as false, so such a child violates `All` (C# semantics).
- Reference navigation members (`x.Author.Name`) ignore soft-deleted related rows, as Include and collection predicates do.
- Explicit `StringComparison.Ordinal`/`OrdinalIgnoreCase` arguments are now honored exactly instead of following the collation.
- SQLite: `SqliteConnectionFactory` sets `PRAGMA busy_timeout` (`BusyTimeoutMilliseconds`, default 30 s) so concurrent writers wait instead of failing with "database is locked", and `:memory:` now uses the memdb VFS instead of shared-cache mode, whose lock conflicts Microsoft.Data.Sqlite reports as `ArgumentOutOfRangeException`. For a named shared in-memory database, prefer `Data Source=file:/name?vfs=memdb` over `Mode=Memory;Cache=Shared`.

Breaking changes
- `ConflictResolver` moves from `ISqlRepository<T>` to `IRepository<T>`; `IRepository<T>` also gains `Capabilities`.
- `SqlFunction` is now `Durable.Query.QueryFunction`; `ExpressionEvaluator`, `GroupingSpecification`, `IncludeNode` and `KeyNormalizer` move to `Durable.Query`; `SqlExpressionTranslator.CustomTranslator` is replaced by `UseGrouping`/`GroupKeySql`.
- `ISqlDialect` gains string-matching (`OrdinalCollation`, `SupportsOrdinalLike`, `OrdinalStringMatch`, `StringCastType`), ordering (`OrderDirection`) and migration members; custom dialects deriving from `SqlDialect` get defaults.
- `IQueryBuilder.Count` is documented as applying Skip/Take, as it always did.

Tests
- Every build warning fixed without suppressions; tests and samples have correct nullable annotations.

## Previous Versions

v0.3.0 (breaking)

Architecture
- New `Durable.Sql` package: one SQL engine shared by all providers behind `ISqlDialect`. Providers shrink from ~15k lines each to ~700.
- `Durable` core is backend-neutral: `IRepository<T>`/`IQueryBuilder<T>` contain no SQL members; SQL features live on `ISqlRepository<T>`/`ISqlQueryBuilder<T>`.
- Cached per-type `EntityMetadata` with compiled accessors and constructors; ordinal-based compiled row materialization.

Correctness
- Every value is bound as a parameter (WHERE, HAVING, UPDATE SET, IN lists, raw `{0}` placeholders, window defaults): fixes culture-dependent numbers, double-escaped quotes, backslash corruption, LIKE wildcards without ESCAPE, and the Lead/Lag injection.
- Enums compare correctly whether stored as names or integers; `HasValue`/`.Value`, `??`, null on either side, `!=` on nullable columns (C# semantics), `!(a && b)` precedence, bool members on SQL Server, `List<T>.Contains`, empty IN lists.
- Include uses split queries: correct `Skip`/`Take` with collection includes, no row explosion, many-to-many on every provider, chunked key lists.
- `TransactionScope.CreateAsync` now makes the scope visible to the caller; ambient scopes of another provider are ignored.
- Repositories no longer dispose connection factories they were given.
- No sync-over-async; `ExecuteAsyncEnumerable` streams on every provider (including with includes); cancellation tokens flow to the driver.
- Set operations and many-to-many includes work on all four databases.
- Collection `Contains` translates on .NET 10 / C# 14, where `array.Contains(x)` binds to the span overload `MemoryExtensions.Contains`.

Features
- Composite primary keys (`KeyOrder`; pass `object[]` keys).
- Per-property value converters (`[ValueConverter]`, `ValueConverter<TModel, TProvider>`), explicit JSON columns (`Flags.Json`), `Flags.Integer` enums.
- Convention mapping for unattributed classes (`DurableMapping`, `[NotMapped]`, snake_case option).
- Global query filters (`AddQueryFilter`, `IgnoreQueryFilters`) and soft delete (`[SoftDelete]`).
- `BulkInsert` (SqlBulkCopy, PostgreSQL binary COPY, MySqlBulkCopy, prepared SQLite inserts); `CreateMany` returns generated keys in input order.
- Navigation predicates (`b.Author.Name == ...`, `a.Books.Any(...)`, `Count()`), grouped projections (`GroupBy().Having().Select(g => new {...})`), projections with Where/OrderBy/paging.
- `QueryMultiple`, `ExecuteProcedure`/`FromProcedure` with output parameters, `ExecuteScalar<T>`, DTO mapping by column name (snake_case to PascalCase).
- `SqlTransactionContext.Wrap` to join externally managed connections/transactions (Dapper, EF Core, ADO.NET).
- Diagnostics: `ILogger` (with slow-command warnings), OpenTelemetry `ActivitySource` "Durable", `ISqlCommandInterceptor`, `SqlCaptureScope`.

Breaking changes
- Custom `ConnectionPool`, `ConnectionPoolOptions`, `ISanitizer`, `SimpleChangeTracker`, `IDataSeeder` removed; drivers pool connections. `IConnectionFactory` is now `OpenConnection`/`OpenConnectionAsync` returning an open connection the caller disposes.
- Repository constructors take `(connectionString | settings | IConnectionFactory, SqlRepositoryOptions?)`; the conflict resolver is the `ConflictResolver` property.
- `Count` returns `long` everywhere; nullable annotations on all optional parameters.
- Enums are stored by name unless `Flags.Integer` is set (previously any `[Property]` without `Flags.String` stored integers).
- TimeSpan is stored as BIGINT ticks on MySQL and SQL Server.
- `IGroupedQueryBuilder.Select` returns an executable `IQueryBuilder<TResult>`; `Count()` counts groups.
- `WhereIn`/`WhereNotIn` take `(keySelector, subquery, subqueryKey)`; `WhereExists` accepts an optional correlation.
- Merge conflict resolution uses the incoming entity as the original snapshot (no change tracking).

v0.2.0 and earlier

- Initial release of Durable ORM
- Generic architecture with clean interfaces (`IRepository<T>`, `IQueryBuilder<T>`, `IConnectionFactory`)
- Full LINQ support with expression tree parsing for type-safe queries
- Complete async/await support throughout the API
- Multi-database support: SQLite, MySQL, PostgreSQL, SQL Server
- Attribute-based entity configuration with `[Entity]` and `[Property]` attributes
- Relationship support: one-to-many, many-to-many with `[NavigationProperty]` and `[InverseNavigationProperty]`
- Connection pooling with configurable options (min/max pool size, timeouts, validation)
- Transaction management with explicit transactions and ambient `TransactionScope` support
- Savepoint support for nested transaction control
- Optimistic concurrency control with `[VersionColumn]` attribute
- Built-in conflict resolvers: `ClientWinsResolver`, `DatabaseWinsResolver`, `MergeChangesResolver`
- Batch operations: `CreateMany`, `UpdateMany`, `DeleteMany`, `BatchUpdate`, `BatchDelete`
- Optimized multi-row INSERT statements with configurable batching
- Advanced query features:
  - Window functions (ROW_NUMBER, LAG, LEAD, SUM, etc.)
  - Common Table Expressions (CTEs) including recursive CTEs
  - Complex subqueries with `WhereIn`, `WhereExists`
  - CASE expressions for conditional logic
  - Set operations (UNION, INTERSECT, EXCEPT)
- Query builder with fluent API: `Where`, `OrderBy`, `ThenBy`, `Skip`, `Take`, `Distinct`
- Projection support with `Select` for custom result shapes
- Relationship loading with `Include` and `ThenInclude`
- Aggregate operations: `Count`, `Sum`, `Average`, `Min`, `Max`
- Raw SQL support: `FromSql`, `ExecuteSql` with parameter binding
- SQL capture and debugging with `ISqlCapture` interface
- Automatic query tracking with `ISqlTrackingConfiguration`
- `DurableResult<T>` objects for operations that include SQL information
- Enum storage options: string-based (default) or integer-based
- Custom data type converters via `IDataTypeConverter`
- In-memory testing support with SQLite
- Repository settings with connection string parsing and building:
  - `SqliteRepositorySettings` with DataSource, CacheMode, Mode
  - `MySqlRepositorySettings` with connection timeout, pooling, SSL mode
  - `PostgresRepositorySettings` with command timeout, SSL mode
  - `SqlServerRepositorySettings` with encryption, integrated security
- Constructor overloads accepting connection strings or settings objects
- Extension methods: `SelectWithQuery`, `GetSelectQuery`, `SelectAsyncWithQuery`
- Comprehensive test coverage across all database providers
- Extensive documentation with examples and best practices
