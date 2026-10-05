# Change Log

## Current Version

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

## Previous Versions

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
