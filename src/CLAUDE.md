# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Durable is a lightweight .NET ORM library with LINQ capabilities designed as an alternative to heavyweight ORMs like Entity Framework and nHibernate. The library emphasizes performance, simplicity, and a clean generic architecture that allows developers to build custom repository implementations without opinionated base classes.

**Key Design Principles:**
- No change tracking overhead (opt-in optimistic concurrency)
- Full LINQ expression tree parsing for type-safe queries
- Attribute-based entity configuration (no fluent API)
- Multi-database support with consistent API across providers
- Direct SQL generation with built-in capture/debugging
- Async-first design throughout

## Project Structure

```
src/
├── Durable/                    # Backend-neutral core (no SQL concepts)
│   ├── IRepository.cs         # Neutral repository interface (incl. Capabilities, ConflictResolver)
│   ├── IQueryBuilder.cs       # Neutral LINQ query builder interface
│   ├── EntityMetadata.cs      # Cached per-type mapping (columns, keys, navigations, compiled accessors)
│   ├── RepositoryCapabilities.cs # Optional features a backend supports
│   ├── Query/                 # Neutral query model (namespace Durable.Query)
│   │   ├── QueryNormalizer.cs # LINQ -> QueryNode tree with C# semantics (the one place LINQ is interpreted)
│   │   ├── *Node.cs           # Immutable query nodes; QueryNodeVisitor<TResult> translates them
│   │   ├── IRepositoryBackend.cs # Storage contract for non-SQL backends
│   │   ├── RepositoryBase.cs  # Full IRepository<T> over an IRepositoryBackend
│   │   └── QueryBuilder.cs    # Neutral IQueryBuilder<T> (QueryModel, includes, client-side Select/GroupBy)
│   └── ...                    # Attributes, transactions, resolvers, diagnostics, options
├── Durable.Sql/               # Shared SQL engine used by every SQL provider
│   ├── ISqlDialect.cs         # Everything that differs between databases
│   ├── SqlRepository.cs       # ISqlRepository<T> implementation (CRUD, upsert, bulk, schema, raw SQL)
│   ├── SqlQueryBuilder.cs     # ISqlQueryBuilder<T> implementation
│   ├── SqlExpressionTranslator.cs # Renders QueryNode trees as SQL (always parameterized)
│   ├── IncludeLoader.cs       # Split-query Include/ThenInclude loading
│   ├── SqlCommandExecutor.cs  # Connection leasing, interceptors, logging, tracing, SQL capture
│   ├── RowMaterializer.cs     # Compiled typed row readers (RowReaderCompiler, TypedRowMapper)
│   └── Migrations/            # SqlMigrator, schema reader/differ/sync, migration history and locking
├── Durable.Sqlite/            # SQLite implementation
├── Durable.MySql/             # MySQL implementation
├── Durable.Postgres/          # PostgreSQL implementation
├── Durable.SqlServer/         # SQL Server implementation
├── Durable.InMemory/          # In-memory IRepositoryBackend (reference non-SQL backend)
├── Durable.Conformance/       # Conformance kit: capability-gated suites for any IRepository<T> backend
├── Test.Shared/               # Touchstone source of truth: entities, provider glue, and all test suites
├── Test.Automated/            # Touchstone CLI runner (console); supports --docker for ephemeral DBs
├── Test.Xunit/                # Touchstone xUnit adapter (dotnet test)
├── Test.Nunit/                # Touchstone NUnit adapter (dotnet test)
├── Test.Benchmark/            # BenchmarkDotNet: Durable vs Dapper vs ADO.NET reads
└── Sample.BlogApp.*/          # Sample applications per database
```

## Architecture

### Layering

- **Durable** (core) is backend-neutral so non-SQL repositories (document stores, search engines, graph databases) can implement `IRepository<T>`/`IQueryBuilder<T>`. Do not add SQL concepts here.
- **LINQ is interpreted once**, by `QueryNormalizer` (Durable.Query), into `QueryNode` trees with C# semantics (null handling, enum conversion, string match modes, navigations, grouping). Backends translate nodes with `QueryNodeVisitor<TResult>`; never parse expression trees in a backend. New LINQ support = a normalizer change (+ node type if needed) plus a visitor method per backend.
- **Non-SQL backends** implement `IRepositoryBackend` and use `RepositoryBase<T>`; they declare `RepositoryCapabilities`, and unsupported calls must throw `NotSupportedException` at the call site (`QueryCapabilityValidator`).
- **Durable.Sql** holds the SQL engine. All SQL generation goes through `ISqlDialect`; never special-case a provider inside the engine. `RepositoryType` checks in the engine are a smell.
- **Providers** contain only a dialect (`XDialect : SqlDialect`), a converter (`XDataTypeConverter : DataTypeConverter`), a connection factory (`XConnectionFactory : ConnectionFactory`), settings, and a thin `XRepository<T> : SqlRepository<T>` (constructors, bulk insert, database creation). A new database = those five files.

### Core Abstractions (Durable project)

1. **IRepository<T>**: Primary interface for all CRUD operations
   - Read operations: `ReadFirst`, `ReadMany`, `ReadById`, `Count`, etc.
   - Write operations: `Create`, `Update`, `Delete`, `Upsert`
   - Batch operations: `CreateMany`, `UpdateMany`, `BatchUpdate`, `BatchDelete`
   - Query building: `Query()` returns `IQueryBuilder<T>`
   - Query filters: `AddQueryFilter`, soft delete via `[SoftDelete]`
   - SQL-only members (`FromSql`, `ExecuteSql`, procedures, `QueryMultiple`, `BulkInsert`, schema management, SQL capture) are on `ISqlRepository<T>` in Durable.Sql

2. **IQueryBuilder<T>**: Fluent LINQ-style query builder
   - Filtering: `Where`, `IgnoreQueryFilters` (raw/subquery filtering is on `ISqlQueryBuilder<T>`)
   - Ordering: `OrderBy`, `OrderByDescending`, `ThenBy`
   - Pagination: `Skip`, `Take`
   - Aggregation: `Count`, `Sum`, `Average`, `Min`, `Max`
   - Projection: `Select` for custom result shapes
   - Joins: `Include`, `ThenInclude` for related data
   - SQL-only (`ISqlQueryBuilder<T>`): `WhereRaw`, `WhereIn`, `WhereExists`, set operations, CTEs, window functions, `SelectCase`, `BuildSql`

3. **IConnectionFactory** (Durable.Sql): returns open connections; drivers do the pooling

4. **Attributes**: Entity configuration system
   - `[Entity("table_name")]`: Maps class to table
   - `[Property("column_name", Flags, MaxLength)]`: Maps property to column
   - `[ForeignKey(typeof(T), "PropertyName")]`: Defines foreign key relationship
   - `[NavigationProperty("ForeignKeyProperty")]`: One-to-many/one-to-one navigation
   - `[InverseNavigationProperty("ForeignKeyProperty")]`: Reverse navigation for collections
   - `[ManyToManyNavigationProperty(typeof(JoinEntity), "ThisKey", "OtherKey")]`: Many-to-many
   - `[VersionColumn(VersionColumnType)]`: Optimistic concurrency control
   - `[ValueConverter(typeof(...))]`: Per-property conversion
   - `[SoftDelete]`: Soft-delete marker column
   - `[NotMapped]`: Exclude a property from convention mapping
   - Classes without `[Property]` attributes are mapped by convention (`DurableMapping`)

### Database-Specific Implementations

Each database provider (Sqlite, MySql, Postgres, SqlServer) contains only:
- `{Provider}Dialect`: identifier quoting, paging, functions, upsert, DDL types, schema introspection, savepoints
- `{Provider}DataTypeConverter`: CLR <-> database value rules the driver does not handle natively
- `{Provider}ConnectionFactory`: opens connections (driver pooling; optional concurrency cap)
- `{Provider}RepositorySettings`: strongly-typed connection settings
- `{Provider}Repository<T>`: constructors, bulk insert, CreateDatabaseIfNotExists

**Important**: Fix SQL generation bugs once in Durable.Sql (or in a dialect hook), never by copying logic into a provider.

## Build and Test Commands

### Building the Solution

```bash
# Build all projects
dotnet build src/Durable.sln

# Build specific provider
dotnet build src/Durable.Sqlite/Durable.Sqlite.csproj
dotnet build src/Durable.MySql/Durable.MySql.csproj
dotnet build src/Durable.Postgres/Durable.Postgres.csproj
dotnet build src/Durable.SqlServer/Durable.SqlServer.csproj

# Build in Release mode for NuGet packaging
dotnet build src/Durable.sln -c Release
```

### Running Tests

Tests are defined once in **Test.Shared** as runner-agnostic Touchstone descriptors (`DurableTestSuites.All`) and executed by three runners.

```bash
# xUnit and NUnit adapters (run against SQLite by default)
dotnet test src/Test.Xunit/Test.Xunit.csproj
dotnet test src/Test.Nunit/Test.Nunit.csproj

# Touchstone CLI runner (console). Default: in-memory SQLite.
dotnet run --project src/Test.Automated/Test.Automated.csproj -c Debug -f net8.0

# Run against a specific provider using a disposable, auto-removed docker container
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type postgres --docker
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type mysql --docker
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type sqlserver --docker

# Or point at an existing server: --type <provider> --host <h> --port <p> --user <u> --pass <p> --database <db>
# Use --help to list all options.
```

**Notes**:
- The xUnit/NUnit adapters and the CLI all consume the same Touchstone suites in `Test.Shared`, so coverage stays in sync.
- Provider selection for the adapters can also be set via environment variables (`DURABLE_TEST_DB`, `DURABLE_TEST_HOST`, etc.).
- Every behavioral suite runs on all four providers; run all four before committing engine changes (Docker runs can execute in parallel).
- The Durable.Conformance kit runs against each SQL provider and, in the SQLite configuration, against the in-memory backend (full and with no capabilities). Fix behavior differences in the engine or backend, never by weakening a conformance assertion.
- The test projects target net8.0 and net10.0; run both (C# 14 changes some expression trees, e.g. `array.Contains` binds to `MemoryExtensions.Contains`).

### Creating NuGet Packages

```bash
# Pack all providers
dotnet pack src/Durable.sln -c Release

# Pack specific provider
dotnet pack src/Durable.Sqlite/Durable.Sqlite.csproj -c Release
```

Published packages:
- `Durable` (core)
- `Durable.Sql` (shared SQL engine)
- `Durable.Sqlite`
- `Durable.MySql`
- `Durable.Postgres`
- `Durable.SqlServer`
- `Durable.InMemory`
- `Durable.Conformance`

## Code Style and Conventions

**⚠️ CRITICAL - THESE RULES MUST BE FOLLOWED STRICTLY ⚠️**

### 1. File Structure and Organization

**Namespace and Using Statements**:
```csharp
// CORRECT: Namespace at top, usings INSIDE namespace block
namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using Durable;

    public class SqliteRepository<T> { }
}

// WRONG: Usings outside namespace
using System;
namespace Durable.Sqlite { }
```

- Namespace declaration must be at the top
- Using statements must be INSIDE the namespace block
- Microsoft and standard system library usings FIRST, in alphabetical order
- Third-party usings AFTER system usings, in alphabetical order

**File Organization**:
- Limit each file to exactly ONE class or exactly ONE enum
- Do NOT nest multiple classes or enums in a single file

### 2. Region Organization

**For files 500+ lines**, organize classes with these five regions in order:

```csharp
public class ExampleRepository<T>
{
    #region Public-Members
    // Public properties and fields
    #endregion

    #region Private-Members
    // Private fields (must start with underscore: _PascalCase)
    #endregion

    #region Constructors-and-Factories
    // Constructors and factory methods
    #endregion

    #region Public-Methods
    // Public methods
    #endregion

    #region Private-Methods
    // Private methods
    #endregion
}
```

- Extra line break before and after region statements (unless next to opening/closing brace)
- **Regions are NOT required for files under 500 lines**

### 3. Naming Conventions

**Private Members**:
- MUST start with underscore and use PascalCase: `_FooBar`
- NOT camelCase: ~~`_fooBar`~~

**Variable Declarations**:
- NEVER use `var` - always use explicit types
```csharp
// CORRECT
List<Person> people = new List<Person>();
string connectionString = "Data Source=test.db";

// WRONG
var people = new List<Person>();
var connectionString = "Data Source=test.db";
```

**Tuples**:
- Do NOT use tuples unless absolutely, absolutely necessary
- Tuples are strongly discouraged

### 4. Documentation Requirements

**XML Documentation**:
- ALL public members, constructors, and public methods MUST have XML documentation
- NO documentation on private members or private methods
- Document exceptions with `/// <exception>` tags
- Document default values, minimum values, and maximum values for configurable properties
- Document nullability in XML comments
- Document thread safety guarantees in XML comments

```csharp
/// <summary>
/// Gets or sets the maximum number of connections in the pool.
/// Default: 100. Minimum: 1. Maximum: 1000.
/// </summary>
/// <exception cref="ArgumentOutOfRangeException">
/// Thrown when value is less than 1 or greater than 1000.
/// </exception>
public int MaxPoolSize
{
    get => _MaxPoolSize;
    set
    {
        if (value < 1 || value > 1000)
            throw new ArgumentOutOfRangeException(nameof(value), "MaxPoolSize must be between 1 and 1000");
        _MaxPoolSize = value;
    }
}
```

### 5. Public Members and Properties

**Backing Variables**:
- Public members SHOULD have explicit getters and setters using backing variables when value requires range or null validation
- Avoid auto-properties when validation is needed

```csharp
// CORRECT: With validation
private int _MaxPoolSize = 100;
public int MaxPoolSize
{
    get => _MaxPoolSize;
    set
    {
        if (value < 1) throw new ArgumentOutOfRangeException(nameof(value));
        _MaxPoolSize = value;
    }
}

// ACCEPTABLE: No validation needed
public string ConnectionString { get; set; }
```

### 6. Async/Await Patterns

**ConfigureAwait**:
- Use `.ConfigureAwait(false)` where appropriate (library code)

**CancellationToken**:
- Every async method MUST accept a `CancellationToken` as a parameter
- Exception: If the class has `CancellationToken` or `CancellationTokenSource` as a class member
- Check cancellation at appropriate points using `token.ThrowIfCancellationRequested()`

**IEnumerable Variants**:
- When implementing a method that returns `IEnumerable<T>`, also create an async variant with `CancellationToken`

```csharp
// Sync version
public IEnumerable<T> ReadMany(Expression<Func<T, bool>> predicate)
{
    // Implementation
}

// Async version - REQUIRED
public async IAsyncEnumerable<T> ReadManyAsync(
    Expression<Func<T, bool>> predicate,
    [EnumeratorCancellation] CancellationToken token = default)
{
    token.ThrowIfCancellationRequested();
    // Implementation with await
}
```

### 7. Error Handling and Validation

**Input Validation**:
- Validate input parameters with guard clauses at method start
- Use `ArgumentNullException.ThrowIfNull(parameter)` for .NET 6+
- For older versions, use manual null checks
- Proactively identify and eliminate situations where null might cause exceptions

**Exception Handling**:
- Use specific exception types rather than generic `Exception`
- Always include meaningful error messages with context
- Consider using custom exception types for domain-specific errors
- Use exception filters when appropriate: `catch (SqlException ex) when (ex.Number == 2601)`

```csharp
public async Task<T> CreateAsync(T entity, CancellationToken token = default)
{
    ArgumentNullException.ThrowIfNull(entity);
    if (string.IsNullOrWhiteSpace(_TableName))
        throw new InvalidOperationException("Table name cannot be null or empty");

    token.ThrowIfCancellationRequested();

    try
    {
        // Implementation
    }
    catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
    {
        throw new InvalidOperationException($"Constraint violation on table {_TableName}", ex);
    }
}
```

### 8. Nullable Reference Types

- Nullable reference types MUST be enabled in all projects: `<Nullable>enable</Nullable>`
- Use `?` for nullable value types: `int?`, `DateTime?`
- Use `?` for nullable reference types: `string?`, `Person?`
- Consider using the Result pattern or Option/Maybe types for methods that can fail

### 9. Resource Management

**IDisposable Pattern**:
- Implement `IDisposable`/`IAsyncDisposable` when holding unmanaged resources or disposable objects
- Use `using` statements or `using` declarations for `IDisposable` objects
- Follow the full Dispose pattern with `protected virtual void Dispose(bool disposing)`
- Always call `base.Dispose()` in derived classes

```csharp
public class Repository<T> : IDisposable
{
    private bool _Disposed = false;

    protected virtual void Dispose(bool disposing)
    {
        if (_Disposed) return;

        if (disposing)
        {
            // Dispose managed resources
            _ConnectionFactory?.Dispose();
        }

        _Disposed = true;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}
```

### 10. Thread Safety

- Document thread safety guarantees in XML comments
- Use `Interlocked` operations for simple atomic operations
- Prefer `ReaderWriterLockSlim` over `lock` for read-heavy scenarios

### 11. LINQ and Performance Best Practices

- Use `.Any()` instead of `.Count() > 0` for existence checks
- Be aware of multiple enumeration issues - consider `.ToList()` when needed
- Use `.FirstOrDefault()` with null checks rather than `.First()` when element might not exist
- Prefer LINQ methods over manual loops when readability is not compromised

```csharp
// CORRECT
if (entities.Any()) { }
Person? first = entities.FirstOrDefault();
if (first != null) { }

// WRONG
if (entities.Count() > 0) { }
Person first = entities.First(); // Throws if empty
```

### 12. Configurable Values

- Avoid using constant values for things that a developer may later want to configure
- Instead use a public member with a backing private member set to a reasonable default

```csharp
// CORRECT
private int _DefaultTimeout = 30;
public int DefaultTimeout
{
    get => _DefaultTimeout;
    set => _DefaultTimeout = value;
}

// WRONG
private const int DefaultTimeout = 30;
```

### 13. Library Code Rules

**NO Console Output**:
- Ensure NO `Console.WriteLine` statements are added to library code
- Console output is ONLY acceptable in test projects and sample applications
- Use logging frameworks or return diagnostic information through proper channels

### 14. SQL and Manual String Construction

**Manual SQL Construction**:
- This codebase intentionally uses manually prepared strings for SQL statements
- This is by design for performance and control
- Assume this approach is correct - do not suggest query builders or ORMs

### 15. Working with Opaque Classes

- Do NOT make assumptions about what class members or methods exist on a class that is opaque to you
- ASK for the implementation if you need to understand what members/methods are available

## Working with Entity Relationships

### Defining Relationships

**One-to-Many**:
```csharp
[Entity("books")]
public class Book
{
    [Property("author_id")]
    [ForeignKey(typeof(Author), "Id")]
    public int AuthorId { get; set; }

    [NavigationProperty("AuthorId")]
    public Author Author { get; set; }
}

[Entity("authors")]
public class Author
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [InverseNavigationProperty("AuthorId")]
    public List<Book> Books { get; set; } = new List<Book>();
}
```

**Many-to-Many**:
```csharp
[ManyToManyNavigationProperty(typeof(JoinEntity), "ThisForeignKey", "OtherForeignKey")]
public List<RelatedEntity> RelatedEntities { get; set; }
```

### Loading Related Data

The ORM supports eager loading via `Include()` and `ThenInclude()`:
- `Include()` loads a single level of navigation properties (`x => x.A.B` is shorthand for Include + ThenInclude)
- `ThenInclude()` loads nested relationships
- Multiple `Include()` calls load sibling relationships

Implementation (`IncludeLoader`) uses split queries: the root query runs (with its paging), then one query per navigation loads related rows by key (`IN` lists chunked to the dialect's parameter limit; many-to-many joins the junction table).

## SQL Generation and Expression Parsing

`SqlExpressionTranslator` (Durable.Sql) converts LINQ expressions to SQL for every provider:
- Client-side values are always bound as parameters (converted with the target column's rules); never inline values into SQL text
- Dialect-specific pieces (functions, LIKE escaping, boolean literals, concatenation) come from `ISqlDialect`
- Supports complex expressions: `p => p.Age > 30 && p.Name.StartsWith("John")`, navigation subqueries, collection Contains

**Manual SQL Construction**: SQL text is built by hand (no external query builder) for performance and control, but values are always parameters.

## Testing Strategy

Tests use the **Touchstone** framework: each case is authored once in `Test.Shared` and surfaced identically to the CLI runner (Test.Automated), the xUnit adapter (Test.Xunit), and the NUnit adapter (Test.Nunit). Provider-agnostic behavioral suites (`IRepositoryProvider`-based) run against whichever provider is configured (SQLite by default, or MySQL/PostgreSQL/SQL Server via docker or an external server). Coverage includes:
- CRUD, querying, ordering, pagination, aggregation
- Data-type round-tripping
- Include/Join and relationship loading
- Optimistic concurrency and conflict resolution
- Batch insert/update/delete
- Schema management and indexes
- Connection-pool stress
- Group-by / having, projections, complex expression translation
- String matching modes (Ordinal / IgnoreCase identical on all databases)
- Migrations (introspection, diff/sync, versioned migrations, locking, scripts)
- Neutral query model (QueryNormalizer unit suite), in-memory backend, SQL/in-memory parity, conformance kit
- Transactions (commit/rollback, sync + async)
- Negative / edge cases (not-found, empty sets, single-result violations)
- SQLite-specific unit suites (data-type converter, repository settings, initialization)

Test entities and the four `IRepositoryProvider` implementations live in `Test.Shared`.

## Common Patterns

### Connections
Drivers pool connections; Durable does not. Share one `{Provider}ConnectionFactory` across repositories; repositories never dispose a factory they were given. Optional `maxConcurrentConnections` caps open connections. See CONNECTION_MGMT.md.

### Optimistic Concurrency
Version columns track concurrent updates:
- `VersionColumnType.Integer`: Auto-incremented
- `VersionColumnType.RowVersion`: Binary timestamp (SQL Server)
- `VersionColumnType.Timestamp`: DateTime-based
- `VersionColumnType.Guid`: Unique per update

Conflict resolvers: `ClientWinsResolver`, `DatabaseWinsResolver`, `MergeChangesResolver`, `ImprovedMergeChangesResolver`

### SQL Capture
SQL repositories implement `ISqlCapture` for debugging (plus `ILogger`, `ISqlCommandInterceptor` and the "Durable" OpenTelemetry ActivitySource via `SqlRepositoryOptions`):
```csharp
repository.CaptureSql = true;
// Execute operations
string sql = repository.LastExecutedSql;
string sqlWithParams = repository.LastExecutedSqlWithParameters;
```

## Important Implementation Notes

1. **No assumptions about opaque classes**: If you don't see a class implementation, ask before assuming what members/methods exist.

2. **Primary keys are required**: All entities must have a property marked with `Flags.PrimaryKey` (several for composite keys, ordered by `KeyOrder`) or a convention key (`Id` / `{Type}Id`).

3. **Enums storage**: By default stored as strings. Use `Flags.Integer` to store as integers.

4. **Nullable properties**: Use `int?`, `DateTime?`, `string?` for nullable columns.

5. **Transaction scope**: Supports both explicit transactions (`ITransaction`) and ambient transactions (`TransactionScope`).

6. **Batch operations**: `CreateMany` returns generated keys (one statement per row batched into a single command); `BulkInsert` uses the database's bulk path without key write-back. Batch sizes via `SqlRepositoryOptions.BatchConfiguration`.

7. **Repository settings**: Each provider has a `{Provider}RepositorySettings` class for strongly-typed configuration instead of connection strings.

## Key Interfaces for Extension

When adding new features, these are the primary extension points:
- `IRepository<T>`: Add new repository operations
- `IQueryBuilder<T>`: Add new query capabilities
- `IRepositoryBackend` / `RepositoryBase<T>`: Add a non-SQL backend (prove it with `Durable.Conformance`)
- `QueryNodeVisitor<TResult>`: Translate the neutral query tree for a backend
- `ISqlDialect`: Add a new SQL database
- `IConnectionFactory`: Add new connection management strategies
- `IConcurrencyConflictResolver<T>`: Custom conflict resolution
- `IValueConverter`: Per-property conversion
- `IDataTypeConverter`: Database-wide type conversion between .NET and database types
- `ISqlCommandInterceptor`: Observe or modify commands
