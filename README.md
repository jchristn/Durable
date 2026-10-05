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

**Durable** sits between Dapper and Entity Framework: typed LINQ queries, CRUD, relationships, optimistic concurrency and schema creation, without a DbContext, change tracking or a migrations system, and with SQL you can always see.

### Key Benefits

- **No configuration overhead**: no DbContext, no model builder; attributes when you want control, conventions when you don't
- **Parameterized, predictable SQL**: every value is a parameter; `BuildSql()`, `CaptureSql` and tracing show exactly what runs
- **No change tracking**: entities are plain objects; opt-in optimistic concurrency with version columns
- **One engine, four databases**: SQLite, MySQL, PostgreSQL and SQL Server share a single SQL engine (`Durable.Sql`) behind a small dialect interface, so behavior and fixes are identical across providers
- **Fast materialization**: per-type metadata and compiled accessors are cached; rows map by ordinal
- **Async from the ground up**: true streaming with `IAsyncEnumerable`, cancellation everywhere
- **Backend-neutral core**: `IRepository<T>` and `IQueryBuilder<T>` in the `Durable` package contain no SQL concepts, leaving room for non-SQL backends

### Packages

| Package | Contents |
|---|---|
| `Durable` | Backend-neutral contracts: `IRepository<T>`, `IQueryBuilder<T>`, attributes, `EntityMetadata`, transactions, conflict resolvers, diagnostics |
| `Durable.Sql` | Shared SQL engine: `ISqlRepository<T>`, `ISqlQueryBuilder<T>`, `ISqlDialect`, LINQ-to-SQL translation, includes, executor, interceptors |
| `Durable.Sqlite` / `Durable.MySql` / `Durable.Postgres` / `Durable.SqlServer` | Dialect, connection factory and repository for each database |

## Requirements

- **.NET 8.0** or later (tested on .NET 8 and .NET 10)
- **Database versions:**
  - SQLite 3.35+ (bundled with Microsoft.Data.Sqlite)
  - MySQL 8.0.31+ (via MySqlConnector 2.6)
  - PostgreSQL 12+ (via Npgsql 10)
  - SQL Server 2017+ (via Microsoft.Data.SqlClient 7)

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

Map a class with `[Entity]` and `[Property]`:

```csharp
using Durable;

[Entity("people")]
public class Person
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
    public int Id { get; set; }

    [Property("first_name", Flags.String, 64)]
    public string FirstName { get; set; }

    [Property("email", Flags.String, 128)]
    public string? Email { get; set; }

    [Property("salary")]
    public decimal Salary { get; set; }

    [Property("birth_date")]
    public DateTime? BirthDate { get; set; }

    // Enums are stored by name by default...
    [Property("status")]
    public Status Status { get; set; }

    // ...or as integers with Flags.Integer
    [Property("priority", Flags.Integer)]
    public Priority Priority { get; set; }

    // Collections and complex objects are stored as JSON (jsonb on PostgreSQL)
    [Property("tags", Flags.Json)]
    public List<string> Tags { get; set; } = new List<string>();
}
```

### Conventions

A class with no `[Property]` attributes maps every scalar read/write property by name. `Id` (or `{TypeName}Id`) is the key and is auto-increment when it is an integer. Use `[NotMapped]` to skip a property. `DurableMapping.NamingConvention = NamingConvention.SnakeCase` maps `FirstName` to `first_name`.

```csharp
public class Note            // table "Note", columns Id, Title, CreatedUtc
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public DateTime CreatedUtc { get; set; }
    [NotMapped] public string Preview => Title.Length > 20 ? Title[..20] : Title;
}
```

### Composite keys

Mark several properties as `Flags.PrimaryKey` and order them with `KeyOrder`. Key arguments take an `object[]`:

```csharp
[Entity("enrollments")]
public class Enrollment
{
    [Property("student_id", Flags.PrimaryKey, KeyOrder = 0)] public int StudentId { get; set; }
    [Property("course_id", Flags.PrimaryKey, KeyOrder = 1)] public int CourseId { get; set; }
    [Property("grade")] public string? Grade { get; set; }
}

Enrollment? e = await repo.ReadByIdAsync(new object[] { 42, 7 });
```

### Value converters

```csharp
public class CsvListConverter : ValueConverter<List<string>, string>
{
    public override string ConvertToProvider(List<string> value) => string.Join(",", value);
    public override List<string> ConvertFromProvider(string value) => value.Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
}

[Property("labels")]
[ValueConverter(typeof(CsvListConverter))]
public List<string> Labels { get; set; } = new();
```

Converters also apply to values compared against the column in `Where` predicates.

## Basic CRUD Operations

```csharp
Person created = await repo.CreateAsync(new Person { FirstName = "John", Salary = 75000m });
Person? found = await repo.ReadByIdAsync(created.Id);
List<Person> adults = repo.ReadMany(p => p.Salary > 50000).ToList();   // streamed

found!.Salary = 80000m;
await repo.UpdateAsync(found);

await repo.DeleteByIdAsync(found.Id);
int deleted = await repo.DeleteManyAsync(p => p.Salary < 1000);

// Set-based updates in one statement
await repo.BatchUpdateAsync(p => p.Status == Status.Pending, p => new Person { Salary = p.Salary * 1.05m });
await repo.UpdateFieldAsync(p => p.Email == null, p => p.Status, Status.Inactive);

// Inserts with generated keys written back, in input order
IEnumerable<Person> inserted = await repo.CreateManyAsync(people);

// Fastest path, no key write-back: SqlBulkCopy / PostgreSQL COPY / MySqlBulkCopy / prepared SQLite inserts
long rows = await repo.BulkInsertAsync(manyPeople);

// Native upsert (ON CONFLICT / ON DUPLICATE KEY / MERGE)
await repo.UpsertAsync(person);
```

## Query Builder

```csharp
List<Person> page = (await repo.Query()
    .Where(p => p.Salary > 100000 && p.Email != null)
    .Where(p => p.FirstName.StartsWith("Jo"))
    .OrderByDescending(p => p.Salary)
    .Skip(20).Take(10)
    .ExecuteAsync()).ToList();

// Streaming
await foreach (Person p in repo.Query().Where(p => p.Status == Status.Active).ExecuteAsyncEnumerable()) { }

// Projection computed in SQL; Where/OrderBy apply to projected members
List<PersonSummary> summaries = (await repo.Query()
    .Select(p => new PersonSummary { Name = p.FirstName + " " + p.LastName, Monthly = p.Salary / 12 })
    .Where(s => s.Monthly > 5000)
    .OrderBy(s => s.Name)
    .ExecuteAsync()).ToList();

// Grouping with HAVING, computed by the database
List<DepartmentStats> stats = (await repo.Query()
    .GroupBy(p => p.Department)
    .Having(g => g.Count() > 2)
    .Select(g => new DepartmentStats { Department = g.Key, Headcount = g.Count(), Payroll = g.Sum(p => p.Salary) })
    .ExecuteAsync()).ToList();

// Navigation predicates become subqueries
List<Author> prolific = (await authors.Query().Where(a => a.Books.Count() > 3).ExecuteAsync()).ToList();
List<Book> byAcme = (await books.Query().Where(b => b.Author.Company.Name == "Acme").ExecuteAsync()).ToList();
```

Supported in predicates: comparisons (with C# null semantics), `&&`/`||`/`!`, arithmetic, string concatenation, `??`, ternaries, enums, `HasValue`/`.Value`, `Contains`/`StartsWith`/`EndsWith` (wildcards escaped), case-insensitive `Equals`/`Contains` via `StringComparison`, `ToUpper`/`ToLower`/`Trim`/`Substring`/`Replace`/`IndexOf`/`Length`, `string.IsNullOrEmpty`, collection `Contains` (IN), date parts and `Add*` methods, `Math` functions, `Between`/`In`/`NotIn` helpers, and `Any`/`All`/`Count` over collection navigations.

Null comparisons follow C# semantics (`x.A != x.B` and `!(x.N > 1)` include rows where a nullable operand is NULL). String equality, `LIKE` and `Replace` follow the database collation (for example, case- and accent-insensitive on MySQL's default collation), as in EF Core.

`ISqlQueryBuilder<T>` adds `Union`/`UnionAll`/`Intersect`/`Except`, `WhereIn`/`WhereExists` subqueries, `WhereRaw("col = {0}", value)` (placeholders are parameters), CTEs, window functions and `SelectCase()`.

## Relationships

```csharp
[Entity("books")]
public class Book
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)] public int Id { get; set; }
    [Property("author_id")] [ForeignKey(typeof(Author), "Id")] public int AuthorId { get; set; }
    [NavigationProperty("AuthorId")] public Author? Author { get; set; }
}

[Entity("authors")]
public class Author
{
    [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)] public int Id { get; set; }
    [InverseNavigationProperty("AuthorId")] public List<Book> Books { get; set; } = new();
    [ManyToManyNavigationProperty(typeof(AuthorCategory), "AuthorId", "CategoryId")] public List<Category> Categories { get; set; } = new();
}

List<Author> withBooks = (await authors.Query()
    .Include(a => a.Books).ThenInclude<Book, Company?>(b => b.Publisher)
    .Include(a => a.Categories)
    .OrderBy(a => a.Name).Take(20)          // 20 authors, each with all of their books
    .ExecuteAsync()).ToList();
```

Includes load with one query per navigation, keyed by the parent rows. There is no cartesian explosion, and `Skip`/`Take` count parent rows only.

## Query Filters and Soft Delete

```csharp
repo.AddQueryFilter(o => o.TenantId == tenantContext.TenantId);  // evaluated per query

[Property("deleted_utc")] [SoftDelete] public DateTime? DeletedUtc { get; set; }
await repo.DeleteAsync(order);                                   // sets deleted_utc instead of deleting
List<Order> everything = (await repo.Query().IgnoreQueryFilters().ExecuteAsync()).ToList();
```

## Transactions

```csharp
using ISqlTransaction tx = await repo.BeginTransactionAsync();
await repo.CreateAsync(order, tx);
await lines.CreateManyAsync(orderLines, tx);
ISavepoint sp = await tx.CreateSavepointAsync();
await tx.CommitAsync();                       // dispose without commit rolls back

// Ambient scope across awaits
using (TransactionScope scope = await TransactionScope.CreateAsync(repo))
{
    await repo.CreateAsync(a);                // joins the scope automatically
    await scope.CompleteAsync();
}

// Join a transaction opened by Dapper/EF/ADO.NET
await repo.CreateAsync(entity, SqlTransactionContext.Wrap(connection, transaction, PostgresDialect.Default));
```

## Connections

Durable uses each driver's own connection pooling. Share one factory across repositories and dispose it at shutdown. Repositories never dispose a factory they were given. See [CONNECTION_MGMT.md](CONNECTION_MGMT.md).

```csharp
PostgresConnectionFactory factory = new PostgresConnectionFactory(connectionString, maxConcurrentConnections: 50);
PostgresRepository<Person> people = new PostgresRepository<Person>(factory);
```

## Optimistic Concurrency

```csharp
[Property("version")]
[VersionColumn(VersionColumnType.Integer)]
public int Version { get; set; } = 1;

try { await repo.UpdateAsync(author); }
catch (OptimisticConcurrencyException) { /* reload and retry */ }

repo.ConflictResolver = new ClientWinsResolver<Author>();   // or DatabaseWinsResolver, MergeChangesResolver
```

## Diagnostics

```csharp
SqlRepositoryOptions options = new SqlRepositoryOptions
{
    Logger = loggerFactory.CreateLogger("Durable"),   // Debug per command, Warning when slow, Error on failure
    SlowCommandThreshold = TimeSpan.FromMilliseconds(200),
    CommandTimeoutSeconds = 30
};
options.Interceptors.Add(new MyCommandInterceptor());   // ISqlCommandInterceptor
SqliteRepository<Person> repo = new SqliteRepository<Person>(connectionString, options);

// OpenTelemetry
builder.Services.AddOpenTelemetry().WithTracing(t => t.AddSource(DurableDiagnostics.ActivitySourceName));

// SQL capture
repo.CaptureSql = true;
List<Person> rows = repo.ReadMany(p => p.Salary > 25).ToList();
Console.WriteLine(repo.LastExecutedSql);                 // parameterized SQL
Console.WriteLine(repo.LastExecutedSqlWithParameters);   // with values, for debugging only
string sql = repo.Query().Where(p => p.Salary > 25).BuildSql();
```

## Raw SQL, Procedures and Multiple Result Sets

```csharp
List<Person> rows = repo.FromSql("SELECT * FROM people WHERE salary BETWEEN @p0 AND @p1", null, 50000, 100000).ToList();
List<TopEarner> dtos = repo.FromSql<TopEarner>("SELECT first_name, salary FROM people ORDER BY salary DESC").ToList(); // snake_case -> PascalCase
long total = repo.ExecuteScalar<long>("SELECT COUNT(*) FROM people");
int affected = await repo.ExecuteSqlAsync("UPDATE people SET salary = salary * 1.05 WHERE department = @p0", null, default, "Engineering");

using SqlMultipleResultReader multi = repo.QueryMultiple("SELECT * FROM people; SELECT COUNT(*) FROM people");
List<Person> people = multi.Read<Person>();
long count = multi.Read<long>()[0];

List<Person> fromProc = repo.FromProcedure<Person>("get_people_by_department", null, new SqlParameterValue("@department", "Sales"));
```

## Table Initialization

```csharp
repo.InitializeTable(typeof(Person));                    // CREATE TABLE if missing, indexes, column validation
repo.InitializeTables(new[] { typeof(Person), typeof(Author), typeof(Book) });
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

Tests are written once in `src/Test.Shared` (Touchstone) and run by three runners.

```bash
# xUnit / NUnit adapters (in-memory SQLite by default)
dotnet test src/Test.Xunit/Test.Xunit.csproj
dotnet test src/Test.Nunit/Test.Nunit.csproj

# CLI runner; --docker starts a disposable database container
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type postgres --docker
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type mysql --docker
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type sqlserver --docker

# Use --help for options to target an existing server (--host, --port, --user, --pass, --database)
```

Engine changes should pass on all four databases, on both net8.0 and net10.0.
