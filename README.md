<div align="center">
  <img src="https://github.com/jchristn/Durable/blob/main/assets/logo.png" width="182" height="182">
</div>

# Durable ORM

[![NuGet Durable.MySql](https://img.shields.io/nuget/v/Durable.MySql.svg?label=Durable.MySql)](https://www.nuget.org/packages/Durable.MySql/)
[![NuGet Durable.Postgres](https://img.shields.io/nuget/v/Durable.Postgres.svg?label=Durable.Postgres)](https://www.nuget.org/packages/Durable.Postgres/)
[![NuGet Durable.Sqlite](https://img.shields.io/nuget/v/Durable.Sqlite.svg?label=Durable.Sqlite)](https://www.nuget.org/packages/Durable.Sqlite/)
[![NuGet Durable.SqlServer](https://img.shields.io/nuget/v/Durable.SqlServer.svg?label=Durable.SqlServer)](https://www.nuget.org/packages/Durable.SqlServer/)

_**IMPORTANT** Durable is in ALPHA.  We appreciate your patience, feedback, and willingness to test this library in its early stages.  We welcome feedback, issues, and constructive criticism in the [Issues](https://github.com/jchristn/durable/issues) and [Discussions](https://github.com/jchristn/durable/discussions)_

A lightweight .NET ORM library with LINQ capabilities, designed with a clean, generic architecture that allows developers to build custom repository implementations without being constrained by opinionated base classes.

## Quick Start - Hello World

Here's a complete working example using SQLite:

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Durable;
using Durable.Sqlite;

// 1. Define your entity
[Entity("people")]
public class Person
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [Property("first_name", Flags.String, 64)]
    public string FirstName { get; set; }

    [Property("last_name", Flags.String, 64)]
    public string LastName { get; set; }

    [Property("birthday")]
    public DateTime Birthday { get; set; }
}

// 2. Use the repository
public class Program
{
    public static async Task Main()
    {
        // Create repository (file-based database)
        SqliteRepository<Person> repo = new SqliteRepository<Person>("Data Source=myapp.db");

        // Initialize the table (creates if not exists)
        repo.InitializeTable(typeof(Person));

        // Create five records
        List<Person> people = new List<Person>
        {
            new Person { FirstName = "Alice",   LastName = "Smith",    Birthday = new DateTime(1990, 3, 15) },
            new Person { FirstName = "Bob",     LastName = "Johnson",  Birthday = new DateTime(1985, 7, 22) },
            new Person { FirstName = "Carol",   LastName = "Williams", Birthday = new DateTime(1992, 11, 8) },
            new Person { FirstName = "David",   LastName = "Brown",    Birthday = new DateTime(1988, 1, 30) },
            new Person { FirstName = "Eve",     LastName = "Davis",    Birthday = new DateTime(1995, 5, 12) }
        };

        IEnumerable<Person> created = await repo.CreateManyAsync(people);
        Console.WriteLine("Created 5 records:");
        foreach (Person p in created)
        {
            Console.WriteLine($"  {p.Id}: {p.FirstName} {p.LastName} - {p.Birthday:yyyy-MM-dd}");
        }

        // Retrieve and display all records
        Console.WriteLine("\nAll records:");
        IEnumerable<Person> all = repo.ReadAll().ToList();
        foreach (Person p in all)
        {
            Console.WriteLine($"  {p.Id}: {p.FirstName} {p.LastName} - {p.Birthday:yyyy-MM-dd}");
        }

        // Modify all records (add 1 year to birthday)
        Console.WriteLine("\nUpdating birthdays...");
        foreach (Person p in all)
        {
            p.Birthday = p.Birthday.AddYears(1);
            await repo.UpdateAsync(p);
        }

        // Display modified records
        Console.WriteLine("\nModified records:");
        foreach (Person p in repo.ReadAll())
        {
            Console.WriteLine($"  {p.Id}: {p.FirstName} {p.LastName} - {p.Birthday:yyyy-MM-dd}");
        }

        // Delete all records
        int deleted = repo.DeleteAll();
        Console.WriteLine($"\nDeleted {deleted} records.");
    }
}
```

## Why Durable?

**Durable** was built to address the limitations and overhead that come with heavyweight ORMs. While frameworks like Entity Framework and nHibernate are powerful, they often introduce unnecessary complexity, performance overhead, and lock you into their opinionated ways of doing things.

### Key Benefits

- **No configuration overhead**: No DbContext, no migrations system, no complex model builder configurations
- **Attributes instead of fluent API**: Simple, declarative entity definitions with `[Entity]` and `[Property]` attributes
- **No change tracking overhead**: Durable doesn't track every property change on every entity by default
- **True LINQ support**: Full expression tree parsing for type-safe queries
- **Multi-database support**: SQLite, MySQL, PostgreSQL, SQL Server - same API
- **Async from the ground up**: Every operation has async support

## Requirements

- **.NET 8.0** or later
- **Database versions:**
  - SQLite 3.8+ (via Microsoft.Data.Sqlite 9.0+)
  - MySQL 5.7+ / MariaDB 10.2+ (via MySqlConnector 2.3+)
  - PostgreSQL 12+ (via Npgsql 8.0+)
  - SQL Server 2016+ (via Microsoft.Data.SqlClient 5.2+)

## Installation

```bash
# SQLite
dotnet add package Durable.Sqlite

# MySQL
dotnet add package Durable.MySql

# PostgreSQL
dotnet add package Durable.Postgres

# SQL Server
dotnet add package Durable.SqlServer
```

## Database Provider Setup

### SQLite

```csharp
using Durable.Sqlite;

// Using connection string
SqliteRepository<Person> repo = new SqliteRepository<Person>("Data Source=myapp.db");

// Using settings object
SqliteRepositorySettings settings = new SqliteRepositorySettings
{
    DataSource = "myapp.db",
    Mode = SqliteOpenMode.ReadWriteCreate,
    CacheMode = SqliteCacheMode.Shared
};
SqliteRepository<Person> repo = new SqliteRepository<Person>(settings);
```

### MySQL

```bash
# Quick start with Docker
docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=mydb mysql:8
```

```csharp
using Durable.MySql;

// Using connection string
MySqlRepository<Person> repo = new MySqlRepository<Person>(
    "Server=localhost;Database=mydb;User=root;Password=password;");

// Using settings object
MySqlRepositorySettings settings = new MySqlRepositorySettings
{
    Hostname = "localhost",
    Database = "mydb",
    Username = "root",
    Password = "password",
    Port = 3306,
    SslMode = MySqlSslMode.Preferred
};
MySqlRepository<Person> repo = new MySqlRepository<Person>(settings);
```

### PostgreSQL

```bash
# Quick start with Docker
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_DB=mydb postgres:16
```

```csharp
using Durable.Postgres;

// Using connection string
PostgresRepository<Person> repo = new PostgresRepository<Person>(
    "Host=localhost;Database=mydb;Username=postgres;Password=password;");

// Using settings object
PostgresRepositorySettings settings = new PostgresRepositorySettings
{
    Hostname = "localhost",
    Database = "mydb",
    Username = "postgres",
    Password = "password",
    Port = 5432,
    SslMode = SslMode.Prefer
};
PostgresRepository<Person> repo = new PostgresRepository<Person>(settings);
```

### SQL Server

```bash
# Quick start with Docker
docker run -d -p 1433:1433 -e ACCEPT_EULA=Y -e SA_PASSWORD=YourStrong@Passw0rd mcr.microsoft.com/mssql/server:2022-latest
```

```csharp
using Durable.SqlServer;

// Using connection string
SqlServerRepository<Person> repo = new SqlServerRepository<Person>(
    "Server=localhost;Database=mydb;User Id=sa;Password=YourStrong@Passw0rd;TrustServerCertificate=true;");

// Using settings object
SqlServerRepositorySettings settings = new SqlServerRepositorySettings
{
    Hostname = "localhost",
    Database = "mydb",
    Username = "sa",
    Password = "YourStrong@Passw0rd",
    TrustServerCertificate = true,
    Encrypt = false
};
SqlServerRepository<Person> repo = new SqlServerRepository<Person>(settings);
```

## Defining Entities

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

    // Nullable value types
    [Property("birth_date")]
    public DateTime? BirthDate { get; set; }

    // Enum stored as string by default
    [Property("status")]
    public Status Status { get; set; }

    // Enum stored as integer
    [Property("priority", Flags.Integer)]
    public Priority Priority { get; set; }
}

public enum Status { Active, Inactive, Pending }
public enum Priority { Low, Medium, High }
```

## Basic CRUD Operations

```csharp
// Create
Person person = new Person
{
    FirstName = "John",
    LastName = "Doe",
    Email = "john.doe@example.com",
    Age = 30,
    Salary = 75000m
};
Person created = await repo.CreateAsync(person);
Console.WriteLine($"Created with ID: {created.Id}");

// Read
Person found = await repo.ReadByIdAsync(created.Id);
IEnumerable<Person> adults = repo.ReadMany(p => p.Age >= 18).ToList();
IEnumerable<Person> all = repo.ReadAll().ToList();

// Update
found.Salary = 80000m;
await repo.UpdateAsync(found);

// Delete
await repo.DeleteByIdAsync(found.Id);
// Or delete by predicate
int deleted = repo.DeleteMany(p => p.Age < 18);
```

## Query Builder

```csharp
// Complex filtering with LINQ
IEnumerable<Person> results = repo
    .Query()
    .Where(p => p.Salary > 100000)
    .Where(p => p.Age >= 25)
    .OrderByDescending(p => p.Salary)
    .Take(10)
    .Execute();

// Async execution
IEnumerable<Person> results = await repo
    .Query()
    .Where(p => p.Status == Status.Active)
    .OrderBy(p => p.LastName)
    .ExecuteAsync();

// Get executed SQL for debugging
IDurableResult<Person> result = await repo
    .Query()
    .Where(p => p.Age > 30)
    .ExecuteWithQueryAsync();

Console.WriteLine($"SQL: {result.Query}");
foreach (Person p in result.Result)
{
    Console.WriteLine(p.FirstName);
}
```

## Relationships

### One-to-Many

```csharp
[Entity("books")]
public class Book
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [Property("title", Flags.String, 200)]
    public string Title { get; set; }

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

// Loading related data
IEnumerable<Book> books = repo.Query()
    .Include(b => b.Author)
    .Execute();
```

## Transactions

```csharp
// Explicit transactions
ITransaction transaction = await repo.BeginTransactionAsync();
try
{
    await repo.CreateAsync(person1, transaction);
    await repo.CreateAsync(person2, transaction);
    await transaction.CommitAsync();
}
catch
{
    await transaction.RollbackAsync();
    throw;
}
```

## Connection Pooling

```csharp
using Durable.Sqlite;

// Create factory with custom pool options
SqliteConnectionFactory factory = "Data Source=myapp.db".CreateFactory(options =>
{
    options.MinPoolSize = 5;
    options.MaxPoolSize = 100;
    options.ConnectionTimeout = TimeSpan.FromSeconds(30);
    options.IdleTimeout = TimeSpan.FromMinutes(10);
    options.ValidateConnections = true;
});

SqliteRepository<Person> repo = new SqliteRepository<Person>(factory);
```

## Optimistic Concurrency

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

// Conflict handling
try
{
    await repo.UpdateAsync(author);
}
catch (OptimisticConcurrencyException ex)
{
    Console.WriteLine($"Expected: {ex.ExpectedVersion}, Actual: {ex.ActualVersion}");
}
```

## SQL Capture for Debugging

```csharp
SqliteRepository<Person> repo = new SqliteRepository<Person>(connectionString);
repo.CaptureSql = true;

IEnumerable<Person> results = repo.ReadMany(p => p.Age > 25).ToList();

Console.WriteLine($"SQL: {repo.LastExecutedSql}");
Console.WriteLine($"SQL with params: {repo.LastExecutedSqlWithParameters}");
```

## Raw SQL

```csharp
// Execute raw queries
IEnumerable<Person> results = repo
    .FromSql("SELECT * FROM people WHERE salary BETWEEN @p0 AND @p1", null, 50000, 100000)
    .ToList();

// Execute non-query SQL
int affected = await repo.ExecuteSqlAsync(
    "UPDATE people SET salary = salary * 1.05 WHERE department = @p0",
    null, default, "Engineering");
```

## Table Initialization

```csharp
// Create table if not exists
repo.InitializeTable(typeof(Person));

// Initialize multiple tables
repo.InitializeTables(new[] { typeof(Person), typeof(Author), typeof(Book) });

// Validate entity definition without creating table
bool isValid = repo.ValidateTable(typeof(Person), out List<string> errors, out List<string> warnings);
```

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Contributors

Special thanks to the following contributors:

- [@joshclopton](https://github.com/JoshClopton) - Josh Clopton
- [@jchristn](https://github.com/jchristn) - Joel Christner

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Getting Started with Development

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run the tests (`dotnet test src/Durable.sln`)
5. Commit your changes (`git commit -m 'Add some amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Code Style

Please follow the existing code style and conventions outlined in [CLAUDE.md](src/CLAUDE.md).

### Running Tests

```bash
# Run all tests
dotnet test src/Durable.sln

# Run tests for a specific database
dotnet test src/Test.Sqlite/Test.Sqlite.csproj
dotnet test src/Test.MySql/Test.MySql.csproj
dotnet test src/Test.Postgres/Test.Postgres.csproj
dotnet test src/Test.SqlServer/Test.SqlServer.csproj
```
