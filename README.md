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
}
```

### 2. Create Repository Implementation

The included SQLite implementation demonstrates how to implement the interfaces:

```csharp
using Durable;
using Durable.Sqlite;

// Using the built-in SQLite implementation
var repository = new SqliteRepository<Person>("Data Source=myapp.db");

// Or using connection factory for pooling
var connectionFactory = new SqliteConnectionFactory("Data Source=myapp.db");
var repository = new SqliteRepository<Person>(connectionFactory);
```

### 3. Basic CRUD Operations

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

### 4. Advanced Querying

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

### 5. Advanced Query Features

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

### 6. Batch Operations

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

### 7. Transaction Management

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

### 8. Raw SQL Support

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

## Performance Tips

- Use `Take()` and `Skip()` for pagination to limit result sets
- Leverage batch operations for bulk inserts/updates
- Use `ReadManyAsync()` with streaming for large result sets
- Enable prepared statement reuse for repeated operations
- Consider using raw SQL for complex queries that don't translate well to LINQ

## License

[Specify your license here]

## Contributing

[Specify contribution guidelines here]
