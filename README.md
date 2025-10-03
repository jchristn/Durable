# Durable ORM

A lightweight .NET ORM library with LINQ capabilities, designed with a clean, generic architecture that allows developers to build custom repository implementations without being constrained by opinionated base classes.

## Features

- **Generic Architecture**: Clean interfaces with no database-specific opinions
- **LINQ Support**: Full expression tree parsing for type-safe queries
- **Async/Await**: Complete async support throughout the API
- **Transaction Support**: Built-in transaction management with ambient transaction support
- **Connection Pooling**: Efficient connection management
- **Batch Operations**: Optimized bulk insert, update, and delete operations
- **Query Builder**: Fluent API for complex query construction
- **Change Tracking**: Built-in optimistic concurrency control
- **Extensible**: Easy to extend with custom data type converters and conflict resolvers

## Requirements

- **.NET 8.0** or later
- **Database versions:**
  - SQLite 3.8+ (via Microsoft.Data.Sqlite 9.0+)
  - MySQL 5.7+ / MariaDB 10.2+ (via MySqlConnector 2.3+)
  - PostgreSQL 12+ (via Npgsql 8.0+)
  - SQL Server 2016+ (via Microsoft.Data.SqlClient 5.2+)

## Core Architecture

The Durable ORM is built around three main interfaces:

### IRepository&lt;T&gt;
The primary interface for all CRUD operations. It provides:
- **Read Operations**: `ReadFirst`, `ReadMany`, `ReadById`, `Count`, etc.
- **Create Operations**: `Create`, `CreateMany`
- **Update Operations**: `Update`, `UpdateMany`, `BatchUpdate`
- **Delete Operations**: `Delete`, `DeleteMany`, `BatchDelete`
- **Upsert Operations**: `Upsert`, `UpsertMany`
- **Query Building**: `Query()` method for complex queries
- **Raw SQL**: `FromSql`, `ExecuteSql` for custom SQL execution

### IQueryBuilder&lt;T&gt;
A fluent query builder that supports:
- **Filtering**: `Where`, `WhereRaw`, `WhereIn`, `WhereExists`
- **Ordering**: `OrderBy`, `OrderByDescending`, `ThenBy`
- **Pagination**: `Skip`, `Take`
- **Aggregation**: `Count`, `Sum`, `Average`, `Min`, `Max`
- **Projection**: `Select` for custom result shapes
- **Joins & Includes**: `Include`, `ThenInclude` for related data
- **Set Operations**: `Union`, `Intersect`, `Except`
- **Advanced Features**: Window functions, CTEs, subqueries

### IConnectionFactory
Abstraction for database connection management with pooling support.

## Getting Started

### 1. Define Your Entities

Entities require two attributes: `[Entity]` for the table name and `[Property]` for column mappings.

```csharp
using Durable;

[Entity("people")]
public class Person
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [Property("first_name", Flags.String, 64)]
    public string FirstName { get; set; }

    [Property("last_name", Flags.String, 64)]
    public string LastName { get; set; }

    [Property("email", Flags.String, 128)]
    public string Email { get; set; }

    [Property("age")]
    public int Age { get; set; }

    [Property("salary")]
    public decimal Salary { get; set; }

    [Property("status")]  // Stored as string by default (e.g., "Active")
    public Status Status { get; set; }
}

public enum Status
{
    Active,
    Inactive,
    Pending
}
```

**Enum Storage:**

```csharp
// String storage (default) - stores "Active", "Inactive", etc.
[Property("status")]
public Status Status { get; set; }

// Integer storage - stores 0, 1, 2, etc.
[Property("status", Flags.Integer)]  // or any flag except Flags.String
public Status StatusAsInt { get; set; }
```

### 2. Define Relationships

Durable supports three types of relationships using attributes:

#### One-to-Many

```csharp
[Entity("books")]
public class Book
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

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

    [Property("name", Flags.String, 100)]
    public string Name { get; set; }

    [InverseNavigationProperty("AuthorId")]
    public List<Book> Books { get; set; } = new List<Book>();
}
```

#### Many-to-Many

```csharp
[Entity("author_categories")]
public class AuthorCategory
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [Property("author_id")]
    [ForeignKey(typeof(Author), "Id")]
    public int AuthorId { get; set; }

    [Property("category_id")]
    [ForeignKey(typeof(Category), "Id")]
    public int CategoryId { get; set; }
}

[Entity("authors")]
public class Author
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [ManyToManyNavigationProperty(typeof(AuthorCategory), "AuthorId", "CategoryId")]
    public List<Category> Categories { get; set; } = new List<Category>();
}

[Entity("categories")]
public class Category
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [ManyToManyNavigationProperty(typeof(AuthorCategory), "CategoryId", "AuthorId")]
    public List<Author> Authors { get; set; } = new List<Author>();
}
```

#### Loading Related Data

```csharp
// Load single navigation property
var books = await repository.Query()
    .Include(b => b.Author)
    .ExecuteAsync();

// Load nested relationships
var books = await repository.Query()
    .Include(b => b.Author)
    .ThenInclude<Author, Company>(a => a.Company)
    .ExecuteAsync();

// Load multiple relationships
var books = await repository.Query()
    .Include(b => b.Author)
    .Include(b => b.Publisher)
    .ExecuteAsync();
```

### 3. Optimistic Concurrency

Add a version column to detect concurrent updates:

```csharp
[Entity("authors")]
public class Author
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [Property("name", Flags.String, 100)]
    public string Name { get; set; }

    [Property("version")]
    [VersionColumn(VersionColumnType.Integer)]
    public int Version { get; set; } = 1;
}
```

**Version Types:**
- `Integer` - Auto-incremented counter
- `RowVersion` - Binary timestamp (SQL Server)
- `Timestamp` - DateTime-based
- `Guid` - Unique identifier per update

**Conflict Handling:**

```csharp
// Default: Throws OptimisticConcurrencyException
try
{
    await repository.UpdateAsync(author);
}
catch (OptimisticConcurrencyException ex)
{
    // Handle conflict: ex.ExpectedVersion vs ex.ActualVersion
}

// With conflict resolver
var resolver = new ClientWinsResolver<Author>();  // Client wins
// or new DatabaseWinsResolver<Author>();          // Database wins
// or new MergeChangesResolver<Author>();          // Merge changes

var repository = new SqliteRepository<Author>(connectionString,
    conflictResolver: resolver);
```

### 4. Database Provider Setup

Durable provides implementations for SQLite, MySQL, PostgreSQL, and SQL Server:

```csharp
// SQLite
using Durable.Sqlite;
var repo = new SqliteRepository<Person>("Data Source=myapp.db");

// MySQL
using Durable.MySql;
var repo = new MySqlRepository<Person>("Server=localhost;Database=mydb;User=root;Password=pass;");

// PostgreSQL
using Durable.Postgres;
var repo = new PostgresRepository<Person>("Host=localhost;Database=mydb;Username=postgres;Password=pass;");

// SQL Server
using Durable.SqlServer;
var repo = new SqlServerRepository<Person>("Server=localhost;Database=mydb;Trusted_Connection=true;");
```

**Connection Pooling:**

```csharp
using Durable.Sqlite;

// Default pooling (MinPoolSize: 5, MaxPoolSize: 100)
var factory = new SqliteConnectionFactory("Data Source=myapp.db");
var repository = new SqliteRepository<Person>(factory);

// Configure pool options
var customFactory = "Data Source=myapp.db".CreateFactory(options =>
{
    options.MinPoolSize = 10;              // Minimum connections (default: 5)
    options.MaxPoolSize = 200;             // Maximum connections (default: 100)
    options.ConnectionTimeout = TimeSpan.FromSeconds(60);  // Wait timeout (default: 30s)
    options.IdleTimeout = TimeSpan.FromMinutes(15);        // Idle before cleanup (default: 10m)
    options.ValidateConnections = true;    // Validate before use (default: true)
});
var repository = new SqliteRepository<Person>(customFactory);
```

### 5. Basic CRUD Operations

```csharp
// Create
var person = new Person 
{ 
    FirstName = "John", 
    LastName = "Doe", 
    Email = "john.doe@example.com",
    Age = 30,
    Salary = 75000m
};

var createdPerson = await repository.CreateAsync(person);
Console.WriteLine($"Created person with ID: {createdPerson.Id}");

// Read
var foundPerson = await repository.ReadByIdAsync(createdPerson.Id);
var allPeople = await repository.ReadAllAsync().ToListAsync();
var adults = await repository.ReadManyAsync(p => p.Age >= 18).ToListAsync();

// Update
foundPerson.Salary = 80000m;
await repository.UpdateAsync(foundPerson);

// Delete
await repository.DeleteByIdAsync(foundPerson.Id);
```

### 6. Advanced Querying

```csharp
// Complex filtering with LINQ
var highEarners = await repository
    .Query()
    .Where(p => p.Salary > 100000)
    .Where(p => p.Age >= 25)
    .OrderByDescending(p => p.Salary)
    .Take(10)
    .ExecuteAsync();

// Projection to custom types
var summary = await repository
    .Query()
    .Where(p => p.Department == "Engineering")
    .Select(p => new { p.FirstName, p.LastName, p.Salary })
    .ExecuteAsync();

// Get query with SQL
var result = await repository
    .Query()
    .Where(p => p.Age > 30)
    .ExecuteWithQueryAsync();

Console.WriteLine($"SQL: {result.Query}");
foreach (var person in result.Results)
{
    Console.WriteLine(person.Name);
}
```

### 7. Advanced Query Features

#### Window Functions

```csharp
// Add row numbers partitioned by department
var rankedEmployees = await repository
    .Query()
    .WithWindowFunction("ROW_NUMBER()")
    .RowNumber("employee_rank")
    .PartitionBy(p => p.Department)
    .OrderByDescending(p => p.Salary)
    .ExecuteAsync();

// Calculate running salary totals by department
var runningTotals = await repository
    .Query()
    .WithWindowFunction("SUM")
    .Sum(p => p.Salary, "running_total")
    .PartitionBy(p => p.Department)
    .OrderBy(p => p.Age)
    .RowsUnboundedPreceding()
    .ExecuteAsync();

// Find salary differences between adjacent employees
var salaryComparisons = await repository
    .Query()
    .WithWindowFunction("LAG")
    .Lag(p => p.Salary, 1, 0, "previous_salary")
    .PartitionBy(p => p.Department)
    .OrderBy(p => p.Salary)
    .ExecuteAsync();
```

#### Common Table Expressions (CTEs)

```csharp
// Simple CTE for complex filtering
var seniorEmployees = await repository
    .Query()
    .WithCte("senior_staff", 
        "SELECT * FROM people WHERE age >= 40 AND salary > 80000")
    .FromRaw("senior_staff")
    .Where(p => p.Department == "Engineering")
    .ExecuteAsync();

// Recursive CTE for hierarchical data (if you have manager relationships)
var orgChart = await repository
    .Query()
    .WithRecursiveCte("org_hierarchy",
        // Anchor: Top-level managers
        "SELECT id, first_name, last_name, manager_id, 1 as level FROM people WHERE manager_id IS NULL",
        // Recursive: Each level down
        "SELECT p.id, p.first_name, p.last_name, p.manager_id, oh.level + 1 FROM people p INNER JOIN org_hierarchy oh ON p.manager_id = oh.id")
    .FromRaw("org_hierarchy")
    .OrderBy(p => p.Id) // Assuming level and id are available in result
    .ExecuteAsync();
```

#### Complex Subqueries

```csharp
// WHERE IN with subquery
var topPerformers = await repository
    .Query()
    .WhereIn(p => p.Department,
        repository.Query()
            .Where(p => p.Salary > 90000)
            .Select(p => new { Department = p.Department })
            .Distinct())
    .ExecuteAsync();

// WHERE EXISTS for correlated subqueries
var employeesInLargeDepartments = await repository
    .Query()
    .WhereExists(
        repository.Query()
            .Where(other => other.Department == p.Department) // Correlated condition
            .Skip(10) // Departments with more than 10 people
            .Take(1))
    .ExecuteAsync();

// Complex subquery with aggregation
var aboveAverageSalary = await repository
    .Query()
    .WhereInRaw(p => p.Salary, 
        "SELECT salary FROM people WHERE salary > (SELECT AVG(salary) FROM people)")
    .ExecuteAsync();
```

#### CASE Expressions

```csharp
// Conditional selection with CASE expressions
var categorizedEmployees = await repository
    .Query()
    .SelectCase()
    .When(p => p.Salary >= 100000, "Senior")
    .When(p => p.Salary >= 70000, "Mid-Level")
    .When(p => p.Salary >= 40000, "Junior")
    .Else("Entry-Level")
    .EndCase("salary_category")
    .ExecuteAsync();

// Complex CASE with multiple conditions
var employeeStatus = await repository
    .Query()
    .SelectCase()
    .When(p => p.Age >= 60 && p.Department == "Management", "Executive")
    .WhenRaw("salary > (SELECT AVG(salary) FROM people) AND age < 30", "High Potential")
    .When(p => p.Department == "Engineering" && p.Salary > 80000, "Senior Engineer")
    .Else("Standard")
    .EndCase("employee_classification")
    .Where(p => p.Age >= 25)
    .ExecuteAsync();
```

#### Set Operations

```csharp
// Union queries for combining different criteria
var engineersQuery = repository.Query()
    .Where(p => p.Department == "Engineering");

var highEarnersQuery = repository.Query()
    .Where(p => p.Salary > 100000);

var combined = await engineersQuery
    .Union(highEarnersQuery)
    .OrderBy(p => p.LastName)
    .ExecuteAsync();

// Intersect to find overlap
var seniorEngineers = await engineersQuery
    .Intersect(highEarnersQuery)
    .ExecuteAsync();

// Except to find differences
var nonEngineerHighEarners = await highEarnersQuery
    .Except(engineersQuery)
    .ExecuteAsync();
```

### 8. Batch Operations

```csharp
// Batch insert
var people = new List<Person>
{
    new Person { FirstName = "Alice", LastName = "Smith", Age = 28, Salary = 65000m },
    new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Salary = 75000m },
    new Person { FirstName = "Carol", LastName = "Williams", Age = 42, Salary = 85000m }
};

var createdPeople = await repository.CreateManyAsync(people);

// Batch update using expressions
int updatedCount = await repository.BatchUpdateAsync(
    p => p.Department == "Engineering",
    p => new Person { Salary = p.Salary * 1.1m } // 10% raise
);

// Batch delete
int deletedCount = await repository.BatchDeleteAsync(p => p.Age < 18);
```

### 9. Transaction Management

```csharp
// Explicit transactions
using var transaction = await repository.BeginTransactionAsync();
try
{
    var person1 = await repository.CreateAsync(new Person { ... }, transaction);
    var person2 = await repository.CreateAsync(new Person { ... }, transaction);
    
    await transaction.CommitAsync();
}
catch
{
    await transaction.RollbackAsync();
    throw;
}

// Ambient transactions using TransactionScope
using var scope = new TransactionScope();
try
{
    await repository.CreateAsync(new Person { ... });
    await repository.CreateAsync(new Person { ... });
    
    scope.Complete();
}
catch
{
    // Automatic rollback
    throw;
}
```

**Savepoints:**

```csharp
using var transaction = await repository.BeginTransactionAsync();
try
{
    await repository.CreateAsync(person1, transaction);

    // Create savepoint
    var savepoint = await transaction.CreateSavepointAsync("sp1");
    try
    {
        await repository.CreateAsync(person2, transaction);
        await savepoint.ReleaseAsync();  // Success - release savepoint
    }
    catch
    {
        await savepoint.RollbackAsync();  // Rollback to savepoint only
        throw;
    }

    await transaction.CommitAsync();
}
catch
{
    await transaction.RollbackAsync();
    throw;
}
```

### 10. Raw SQL Support

```csharp
// Execute raw queries
var customResults = await repository
    .FromSqlAsync("SELECT * FROM people WHERE salary BETWEEN @min AND @max", 50000, 100000)
    .ToListAsync();

// Execute non-query SQL
int affectedRows = await repository
    .ExecuteSqlAsync("UPDATE people SET salary = salary * 1.05 WHERE department = @dept", "Engineering");

// Map to custom types
var summaries = await repository
    .FromSqlAsync<PersonSummary>("SELECT first_name, last_name, salary FROM people WHERE age > @age", 30)
    .ToListAsync();
```

### 11. SQL Capture and Debugging

The Durable ORM provides built-in SQL capture capabilities for debugging and monitoring executed queries. Repositories that implement the `ISqlCapture` interface can track and expose the actual SQL statements being executed.

#### Basic SQL Capture

```csharp
// Enable SQL capture on a repository that supports it
if (repository is ISqlCapture sqlCapture)
{
    sqlCapture.CaptureSql = true;

    // Execute some operations
    var people = await repository.ReadManyAsync(p => p.Age > 25).ToListAsync();
    var person = await repository.ReadByIdAsync(1);

    // Get the last executed SQL
    Console.WriteLine("Last SQL: " + sqlCapture.LastExecutedSql);
    Console.WriteLine("Last SQL with parameters: " + sqlCapture.LastExecutedSqlWithParameters);
}
```

#### Automatic Query Results with SQL

For repositories that implement `ISqlTrackingConfiguration`, you can configure automatic inclusion of SQL in query results:

```csharp
// Enable automatic query inclusion
if (repository is ISqlTrackingConfiguration trackingConfig)
{
    trackingConfig.IncludeQueryInResults = true;

    // Now all operations return results with SQL information
    var result = await repository
        .Query()
        .Where(p => p.Salary > 75000)
        .ExecuteWithQueryAsync();

    Console.WriteLine($"Executed SQL: {result.Query}");
    foreach (var person in result.Results)
    {
        Console.WriteLine($"{person.FirstName} {person.LastName}");
    }
}
```

#### Working with DurableResult Objects

When SQL tracking is enabled, repository operations return `IDurableResult<T>` objects that contain both the query results and the executed SQL:

```csharp
// Query with automatic SQL capture
var durableResult = await repository.SelectWithQueryAsync(p => p.Department == "Engineering");

// Access the results
foreach (var engineer in durableResult.Result)
{
    Console.WriteLine($"{engineer.FirstName} - {engineer.Salary:C}");
}

// Access the SQL that was executed
Console.WriteLine($"Query executed: {durableResult.Query}");

// Convert back to regular enumerable for backward compatibility
IEnumerable<Person> regularResults = durableResult.AsEnumerable();
```

#### Async Enumerable with SQL Tracking

```csharp
// Get async enumerable with query information
var asyncResult = repository.SelectAsyncWithQuery(p => p.Age >= 30);

Console.WriteLine($"Starting to process query: {asyncResult.Query}");

await foreach (var person in asyncResult.Result)
{
    Console.WriteLine($"Processing: {person.FirstName} {person.LastName}");
}
```

#### Performance Considerations

- SQL capture is **disabled by default** for optimal performance
- Only enable SQL capture during development or when debugging is needed
- The `LastExecutedSqlWithParameters` property performs parameter substitution, which has additional overhead
- When `IncludeQueryInResults` is enabled, all repository operations return wrapped result objects

## Testing

**In-Memory Testing (SQLite):**

```csharp
using Durable.Sqlite;
using Xunit;

public class RepositoryTests
{
    [Fact]
    public async Task CreatePerson_ReturnsWithId()
    {
        // Use in-memory database
        const string connStr = "Data Source=TestDB;Mode=Memory;Cache=Shared";

        // Keep connection alive for in-memory database
        using var keepAlive = new SqliteConnection(connStr);
        keepAlive.Open();

        var repo = new SqliteRepository<Person>(connStr);

        // Create table
        await repo.ExecuteSqlAsync(@"
            CREATE TABLE people (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT,
                last_name TEXT,
                age INTEGER
            )");

        var person = new Person { FirstName = "John", LastName = "Doe", Age = 30 };
        var created = await repo.CreateAsync(person);

        Assert.True(created.Id > 0);
    }
}
```

**Transaction Testing:**

```csharp
[Fact]
public async Task Transaction_RollbackOnError()
{
    using var transaction = await repo.BeginTransactionAsync();

    await repo.CreateAsync(person1, transaction);

    // Verify within transaction
    var count = repo.Count(transaction: transaction);
    Assert.Equal(1, count);

    await transaction.RollbackAsync();

    // Verify rollback
    Assert.Equal(0, repo.Count());
}
```

**Concurrency Testing:**

```csharp
[Fact]
public void OptimisticConcurrency_ThrowsException()
{
    var created = repo.Create(authorWithVersion);

    var copy1 = repo.ReadById(created.Id);
    var copy2 = repo.ReadById(created.Id);

    copy1.Name = "Update 1";
    repo.Update(copy1);  // Success - version increments

    copy2.Name = "Update 2";
    Assert.Throws<OptimisticConcurrencyException>(() => repo.Update(copy2));
}
```

## Building Your Own Repository Implementation

The generic architecture makes it easy to create repository implementations for different databases:

### 1. Implement IConnectionFactory

```csharp
public class MyConnectionFactory : IConnectionFactory
{
    private readonly string _connectionString;
    
    public MyConnectionFactory(string connectionString)
    {
        _connectionString = connectionString;
    }
    
    public IDbConnection GetConnection()
    {
        return new MyDatabaseConnection(_connectionString);
    }
    
    public async Task<IDbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = new MyDatabaseConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
    
    // Implement pooling, disposal, etc.
}
```

### 2. Implement IRepository&lt;T&gt;

```csharp
public class MyRepository<T> : IRepository<T> where T : class, new()
{
    private readonly IConnectionFactory _connectionFactory;
    private readonly string _tableName;
    private readonly Dictionary<string, PropertyInfo> _columnMappings;
    
    public MyRepository(IConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
        _tableName = GetTableName<T>();
        _columnMappings = GetColumnMappings<T>();
    }
    
    public async Task<T> ReadByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default)
    {
        using var connection = await _connectionFactory.GetConnectionAsync(token);
        using var command = connection.CreateCommand();
        
        command.CommandText = $"SELECT * FROM {_tableName} WHERE id = @id";
        command.Parameters.Add(new Parameter("@id", id));
        
        using var reader = await command.ExecuteReaderAsync(token);
        if (await reader.ReadAsync(token))
        {
            return MapFromReader<T>(reader);
        }
        return null;
    }
    
    // Implement other repository methods...
}
```

### 3. Implement IQueryBuilder&lt;T&gt;

```csharp
public class MyQueryBuilder<T> : IQueryBuilder<T> where T : class, new()
{
    private readonly MyRepository<T> _repository;
    private readonly List<string> _whereClauses = new();
    private readonly List<string> _orderByClauses = new();
    private int? _skipCount;
    private int? _takeCount;
    
    public MyQueryBuilder(MyRepository<T> repository)
    {
        _repository = repository;
    }
    
    public IQueryBuilder<T> Where(Expression<Func<T, bool>> predicate)
    {
        var sql = ConvertExpressionToSql(predicate);
        _whereClauses.Add(sql);
        return this;
    }
    
    public IQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        var columnName = GetColumnName(keySelector);
        _orderByClauses.Add($"{columnName} ASC");
        return this;
    }
    
    public async Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default)
    {
        var sql = BuildSql();
        return await _repository.FromSqlAsync(sql, token);
    }
    
    private string BuildSql()
    {
        var sql = $"SELECT * FROM {_repository.TableName}";
        
        if (_whereClauses.Any())
            sql += " WHERE " + string.Join(" AND ", _whereClauses);
            
        if (_orderByClauses.Any())
            sql += " ORDER BY " + string.Join(", ", _orderByClauses);
            
        if (_skipCount.HasValue)
            sql += $" OFFSET {_skipCount}";
            
        if (_takeCount.HasValue)
            sql += $" LIMIT {_takeCount}";
            
        return sql;
    }
    
    // Implement other query builder methods...
}
```

## Configuration Options

### Batch Insert Configuration

```csharp
var batchConfig = new BatchInsertConfiguration
{
    MaxRowsPerBatch = 1000,
    MaxParametersPerStatement = 2000,
    EnableMultiRowInsert = true,
    EnablePreparedStatementReuse = true
};

var repository = new SqliteRepository<Person>(connectionString, batchConfig);
```

### Custom Data Type Converters

```csharp
public class MyDataTypeConverter : IDataTypeConverter
{
    public object ConvertToDatabase(object value, Type targetType, PropertyInfo property)
    {
        // Convert .NET types to database-specific types
        if (value is DateTime dateTime)
            return dateTime.ToString("yyyy-MM-dd HH:mm:ss");
            
        return value;
    }
    
    public object ConvertFromDatabase(object value, Type targetType, PropertyInfo property)
    {
        // Convert database types to .NET types
        if (targetType == typeof(DateTime) && value is string dateString)
            return DateTime.Parse(dateString);
            
        return Convert.ChangeType(value, targetType);
    }
}

var repository = new SqliteRepository<Person>(connectionString, dataTypeConverter: new MyDataTypeConverter());
```

### Concurrency Conflict Resolution

```csharp
// Built-in conflict resolvers
var clientWinsResolver = new ClientWinsResolver<Person>();
var databaseWinsResolver = new DatabaseWinsResolver<Person>();
var mergeResolver = new MergeChangesResolver<Person>();

var repository = new SqliteRepository<Person>(connectionString, conflictResolver: mergeResolver);

// Advanced merge resolver with configurable conflict behavior
var advancedMergeResolver = new ImprovedMergeChangesResolver<Person>(ConflictBehavior.IncomingWins);
// Supports ConflictBehavior options: IncomingWins, CurrentWins, ThrowException, and property ignoring

// Custom conflict resolver
public class CustomConflictResolver<T> : IConcurrencyConflictResolver<T>
{
    public ConflictResolutionStrategy DefaultStrategy => ConflictResolutionStrategy.Custom;
    
    public bool TryResolveConflict(T currentEntity, T incomingEntity, T originalEntity, 
        ConflictResolutionStrategy strategy, out T resolvedEntity)
    {
        // Implement custom conflict resolution logic
        resolvedEntity = incomingEntity;
        return true;
    }
}
```

## Error Handling

**Concurrency Exceptions:**

```csharp
// OptimisticConcurrencyException - Version mismatch detected
try
{
    await repository.UpdateAsync(entity);
}
catch (OptimisticConcurrencyException ex)
{
    Console.WriteLine($"Expected: {ex.ExpectedVersion}, Actual: {ex.ActualVersion}");
    // Retry with fresh entity or use conflict resolver
}

// ConcurrencyConflictException - Conflict during resolution
catch (ConcurrencyConflictException ex)
{
    var current = ex.CurrentEntity;
    var incoming = ex.IncomingEntity;
    var original = ex.OriginalEntity;
    // Handle unresolvable conflict
}
```

**Standard Exceptions:**

```csharp
// ArgumentNullException - Null parameters
try
{
    await repository.CreateAsync(null);
}
catch (ArgumentNullException ex) { }

// InvalidOperationException - Invalid state (e.g., transaction already completed)
```

**Database Exceptions:**

Database-specific exceptions (SqlException, MySqlException, NpgsqlException, SqliteException) are passed through from the underlying providers.

## Extension Methods

The repository includes helpful extension methods:

```csharp
// Get query with results
var result = await repository.SelectWithQueryAsync(p => p.Age > 25);
Console.WriteLine($"SQL: {result.Query}");

// Get just the SQL query
string sql = repository.GetSelectQuery(p => p.Salary > 50000);

// Async enumerable with query
var asyncResult = repository.SelectAsyncWithQuery(p => p.Department == "Engineering");
await foreach (var person in asyncResult.Results)
{
    Console.WriteLine(person.Name);
}
```

## Best Practices

1. **Use Connection Pooling**: Always use a connection factory with pooling for production applications
2. **Leverage Transactions**: Use transactions for multi-step operations to ensure data consistency
3. **Batch Operations**: Use batch methods for bulk operations to improve performance
4. **Async/Await**: Prefer async methods for all database operations
5. **Expression Trees**: Use LINQ expressions instead of raw SQL when possible for type safety
6. **Dispose Resources**: Properly dispose of repositories and transactions using `using` statements

## Query Performance

**Pagination:**
```csharp
// Good - limits database load
var page = await repo.Query()
    .Where(p => p.IsActive)
    .OrderBy(p => p.Id)
    .Skip(pageNum * pageSize)
    .Take(pageSize)
    .ExecuteAsync();

// Bad - loads everything then pages in memory
var all = await repo.ReadManyAsync(p => p.IsActive).ToListAsync();
var page = all.Skip(pageNum * pageSize).Take(pageSize);
```

**Batch Operations:**
```csharp
// Good - single statement with optimized batching
await repo.CreateManyAsync(1000_items);
await repo.BatchUpdateAsync(p => p.Status == "Pending", p => new { Status = "Active" });

// Bad - N queries
foreach (var item in items) await repo.CreateAsync(item);
```

**Avoid N+1 Queries:**
```csharp
// Good - single query with joins
var books = await repo.Query()
    .Include(b => b.Author)
    .Include(b => b.Publisher)
    .ExecuteAsync();

// Bad - N+1 queries
var books = await repo.ReadAllAsync();
foreach (var book in books)
{
    book.Author = await authorRepo.ReadByIdAsync(book.AuthorId);  // N queries!
}
```

**Select Only What You Need:**
```csharp
// Good - projection reduces data transfer
var summary = await repo.Query()
    .Select(p => new { p.Id, p.Name })
    .ExecuteAsync();

// Bad - fetches all columns
var all = await repo.ReadAllAsync();
var names = all.Select(p => p.Name);
```

**Connection Pooling:**
```csharp
// Good - reuses connections
var factory = connectionString.CreateFactory(opt => opt.MaxPoolSize = 100);
var repo = new SqliteRepository<Person>(factory);

// Bad - creates new connection each time
var repo = new SqliteRepository<Person>(connectionString);
```

**Use Exists for Checks:**
```csharp
// Good - stops at first match
bool hasActive = await repo.ExistsAsync(p => p.IsActive);

// Bad - counts all matching rows
bool hasActive = await repo.CountAsync(p => p.IsActive) > 0;
```

## License

[Specify your license here]

## Contributing

[Specify contribution guidelines here]
