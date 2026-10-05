# Test.Benchmark

BenchmarkDotNet comparison of Durable's read path against Dapper and hand-written ADO.NET.

Every implementation opens a pooled connection per operation (as Durable does) and fully materializes its result.
Results are grouped by scenario; the hand-written ADO.NET version is the baseline (ratio 1.00) of each group.

| Category       | What it measures                                                                                   |
|----------------|----------------------------------------------------------------------------------------------------|
| `ReadById`     | One row by primary key: `ReadById(id)` and `Query().Where(x => x.Id == id).Execute()`               |
| `ReadAll`      | All 10,000 rows, synchronous (`ReadAll()`)                                                          |
| `ReadAllAsync` | All 10,000 rows, asynchronous (`Query().ExecuteAsyncEnumerable()`)                                  |
| `Filtered100`  | 100 rows selected with a predicate (`ReadMany(x => x.CustomerId == c)`)                             |
| `Dto`          | 10,000 rows into a DTO via `FromSql<TDto>` and a `Select` projection                                |
| `Include`      | 100 parents with 10 children each via `Include` (split query; the baselines run the same two queries) |

The `bench_orders` entity mixes int and long columns, a string, a nullable string, `DateTime`, `decimal`, `bool`,
an enum stored as a string, a `Guid` and a nullable int.

## Running

Always run in Release:

```bash
# everything
dotnet run -c Release --project src/Test.Benchmark -- --filter '*'

# one scenario
dotnet run -c Release --project src/Test.Benchmark -- --filter '*ReadById*'

# quick, noisier run while iterating
dotnet run -c Release --project src/Test.Benchmark -- --filter '*' --job short
```

SQLite works out of the box: the database is a file in a fresh temporary directory, created and seeded once per
provider in `GlobalSetup` and deleted in `GlobalCleanup`.

To also benchmark PostgreSQL, set `DURABLE_BENCH_POSTGRES` to a connection string. The benchmark drops and recreates
the `bench_orders`, `bench_authors` and `bench_posts` tables in that database.

```bash
docker run --rm -d --name durable-bench-pg -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:16
export DURABLE_BENCH_POSTGRES="Host=localhost;Port=5432;Username=postgres;Password=password;Database=postgres"
dotnet run -c Release --project src/Test.Benchmark -- --filter '*'
```

Results are written to `BenchmarkDotNet.Artifacts/results` (use `--artifacts <dir>` to change the location).
