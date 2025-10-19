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
├── Durable/                    # Core abstractions and interfaces
│   ├── IRepository.cs         # Main repository interface
│   ├── IQueryBuilder.cs       # LINQ query builder interface
│   ├── IConnectionFactory.cs  # Connection management abstraction
│   └── ...                    # Supporting types, attributes, exceptions
├── Durable.Sqlite/            # SQLite implementation
├── Durable.MySql/             # MySQL implementation
├── Durable.Postgres/          # PostgreSQL implementation
├── Durable.SqlServer/         # SQL Server implementation
├── Test.Sqlite/               # SQLite tests
├── Test.MySql/                # MySQL tests
├── Test.Postgres/             # PostgreSQL tests
├── Test.SqlServer/            # SQL Server tests
├── Test.Shared/               # Shared test utilities and entities
└── Sample.BlogApp.*/          # Sample applications per database
```

## Architecture

### Core Abstractions (Durable project)

1. **IRepository<T>**: Primary interface for all CRUD operations
   - Read operations: `ReadFirst`, `ReadMany`, `ReadById`, `Count`, etc.
   - Write operations: `Create`, `Update`, `Delete`, `Upsert`
   - Batch operations: `CreateMany`, `UpdateMany`, `BatchUpdate`, `BatchDelete`
   - Query building: `Query()` returns `IQueryBuilder<T>`
   - Raw SQL: `FromSql`, `ExecuteSql`

2. **IQueryBuilder<T>**: Fluent LINQ-style query builder
   - Filtering: `Where`, `WhereRaw`, `WhereIn`, `WhereExists`
   - Ordering: `OrderBy`, `OrderByDescending`, `ThenBy`
   - Pagination: `Skip`, `Take`
   - Aggregation: `Count`, `Sum`, `Average`, `Min`, `Max`
   - Projection: `Select` for custom result shapes
   - Joins: `Include`, `ThenInclude` for related data
   - Advanced: Window functions, CTEs, set operations

3. **IConnectionFactory**: Connection pooling and management abstraction

4. **Attributes**: Entity configuration system
   - `[Entity("table_name")]`: Maps class to table
   - `[Property("column_name", Flags, MaxLength)]`: Maps property to column
   - `[ForeignKey(typeof(T), "PropertyName")]`: Defines foreign key relationship
   - `[NavigationProperty("ForeignKeyProperty")]`: One-to-many/one-to-one navigation
   - `[InverseNavigationProperty("ForeignKeyProperty")]`: Reverse navigation for collections
   - `[ManyToManyNavigationProperty(typeof(JoinEntity), "ThisKey", "OtherKey")]`: Many-to-many
   - `[VersionColumn(VersionColumnType)]`: Optimistic concurrency control

### Database-Specific Implementations

Each database provider (Sqlite, MySql, Postgres, SqlServer) contains:
- `{Provider}Repository<T>`: Concrete implementation of `IRepository<T>`
- `{Provider}QueryBuilder<T>`: Database-specific query builder
- `ExpressionParser`: Converts LINQ expressions to SQL WHERE clauses
- `EntityMapper`: Maps database readers to entity objects
- `{Provider}ConnectionFactory`: Connection management with pooling
- `{Provider}Sanitizer`: SQL identifier sanitization (table/column names)
- Supporting classes: `JoinBuilder`, `IncludeProcessor`, `CollectionLoader`, etc.

**Important**: Each provider has its own SQL generation logic to handle database-specific syntax (e.g., parameter prefixes, identifier quoting, pagination, data types).

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

```bash
# Run all tests
dotnet test src/Durable.sln

# Run tests for a specific database provider
dotnet test src/Test.Sqlite/Test.Sqlite.csproj
dotnet test src/Test.MySql/Test.MySql.csproj
dotnet test src/Test.Postgres/Test.Postgres.csproj
dotnet test src/Test.SqlServer/Test.SqlServer.csproj

# Run integration tests (test projects can also be executed as console apps)
cd src/Test.Sqlite
dotnet run
```

**Note**: Test projects are dual-purpose - they can run as xUnit test suites via `dotnet test` OR as console applications via `dotnet run` for interactive/integration testing.

### Creating NuGet Packages

```bash
# Pack all providers
dotnet pack src/Durable.sln -c Release

# Pack specific provider
dotnet pack src/Durable.Sqlite/Durable.Sqlite.csproj -c Release
```

Published packages:
- `Durable.Sqlite`
- `Durable.MySql`
- `Durable.Postgres`
- `Durable.SqlServer`

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
- `Include()` loads a single level of navigation properties
- `ThenInclude()` loads nested relationships
- Multiple `Include()` calls load sibling relationships

Implementation uses SQL JOINs and entity materialization in `EntityMapper` classes.

## SQL Generation and Expression Parsing

Each provider has its own `ExpressionParser` that converts LINQ expressions to SQL:
- Handles property access, constants, method calls, binary operations
- Supports complex expressions: `p => p.Age > 30 && p.Name.StartsWith("John")`
- Database-specific handling for string methods, null checks, etc.

**Manual SQL Construction**: The codebase intentionally uses manual string building for SQL queries rather than parameterized query builders. This is by design for performance and control.

## Testing Strategy

Test projects use xUnit and are organized by database provider. They include:
- Unit tests for individual operations
- Integration tests for complex scenarios
- Concurrency tests for optimistic locking
- Relationship/Include tests
- Transaction tests
- Sanitization tests

Test entities are defined in `Test.Shared` and reused across all provider tests.

## Common Patterns

### Connection Pooling
All implementations support connection pooling via `ConnectionPool` class:
```csharp
ConnectionPoolOptions options = new ConnectionPoolOptions
{
    MinPoolSize = 5,
    MaxPoolSize = 100,
    ConnectionTimeout = TimeSpan.FromSeconds(30)
};
```

### Optimistic Concurrency
Version columns track concurrent updates:
- `VersionColumnType.Integer`: Auto-incremented
- `VersionColumnType.RowVersion`: Binary timestamp (SQL Server)
- `VersionColumnType.Timestamp`: DateTime-based
- `VersionColumnType.Guid`: Unique per update

Conflict resolvers: `ClientWinsResolver`, `DatabaseWinsResolver`, `MergeChangesResolver`, `ImprovedMergeChangesResolver`

### SQL Capture
Repositories implement `ISqlCapture` for debugging:
```csharp
repository.CaptureSql = true;
// Execute operations
string sql = repository.LastExecutedSql;
string sqlWithParams = repository.LastExecutedSqlWithParameters;
```

## Important Implementation Notes

1. **No assumptions about opaque classes**: If you don't see a class implementation, ask before assuming what members/methods exist.

2. **Primary keys are required**: All entities must have a property marked with `Flags.PrimaryKey`.

3. **Enums storage**: By default stored as strings. Use `Flags.Integer` (or any non-String flag) to store as integers.

4. **Nullable properties**: Use `int?`, `DateTime?`, `string?` for nullable columns.

5. **Transaction scope**: Supports both explicit transactions (`ITransaction`) and ambient transactions (`TransactionScope`).

6. **Batch operations**: Optimized multi-row inserts with configurable batch sizes via `IBatchInsertConfiguration`.

7. **Repository settings**: Each provider has a `{Provider}RepositorySettings` class for strongly-typed configuration instead of connection strings.

## Key Interfaces for Extension

When adding new features, these are the primary extension points:
- `IRepository<T>`: Add new repository operations
- `IQueryBuilder<T>`: Add new query capabilities
- `IConnectionFactory`: Add new connection management strategies
- `IConcurrencyConflictResolver<T>`: Custom conflict resolution
- `IDataTypeConverter`: Custom type conversion between .NET and database types
