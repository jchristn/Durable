# mongodb (Durable.MongoDb)

Branch `feature/v0.7.0-mongodb`. New package `Durable.MongoDb` (MongoDB.Driver 3.12.0), a non-SQL backend following
the backend convention. Tested against `mongo:8` (8.x) as a single-node replica set and as a standalone server.
`--type mongodb`.

## README sections

### 1. New section "MongoDB Backend" (after "LiteDB Backend", before "LiteGraph Backend"; add it to the table of contents)

````markdown
## MongoDB Backend

`Durable.MongoDb` stores entities in [MongoDB](https://www.mongodb.com/) collections through the official driver
(MongoDB.Driver 3.12.0). Durable encodes documents itself from its entity metadata; the driver's reflection class maps
are never used.

```csharp
using Durable.MongoDb;

await using MongoDbBackend store = await MongoDbBackend.CreateAsync(
    MongoDbRepositorySettings.ForConnectionString("mongodb://localhost:27017/?directConnection=true", "library"));
// Host settings: MongoDbRepositorySettings.ForHost("db.example.com", 27017, "library") with Username, Password, UseTls, ...
// Existing client (never disposed by Durable): MongoDbRepositorySettings.ForClient(myMongoClient, "library")

MongoDbRepository<Author> authors = store.CreateRepository<Author>();
MongoDbRepository<Book> books = store.CreateRepository<Book>();

Author ada = await authors.CreateAsync(new Author { Name = "Ada" });
await using (MongoDbTransaction tx = await store.BeginTransactionAsync())   // needs a replica set (SupportsTransactions)
{
    await books.CreateAsync(new Book { Title = "Notes", Year = 1843, AuthorId = ada.Id }, tx);
    await books.CreateAsync(new Book { Title = "Sketch", Year = 1842, AuthorId = ada.Id }, tx);
    await tx.CommitAsync();
}

store.QueryPlanned += (sender, plan) => Console.WriteLine($"plan: {plan}");
List<Book> recent = books.Query()
    .Where(b => b.AuthorId == ada.Id && b.Title.StartsWith("N"))
    .OrderByDescending(b => b.Year)
    .Take(10)
    .Execute()
    .ToList();
Console.WriteLine($"{recent.Count} book(s)");

int firstYear = await books.Query().Where(b => b.AuthorId == ada.Id).MinAsync(b => b.Year);   // $group on the server
List<Book> sketches = books.ReadMany(b => b.Title.ToUpper() == "SKETCH").ToList();            // ToUpper runs client-side
```

```text
plan: Query books WHERE { "author_id" : { "$eq" : 1 } } AND { "title" : { "$regularExpression" : { "pattern" : "^N", "options" : "" } } } SORT { "year" : -1, "_id" : 1 } [paging in MongoDB] | exact | read 1
1 book(s)
plan: Aggregate books WHERE { "author_id" : { "$eq" : 1 } } PIPELINE { "pipeline" : [{ "$match" : { "$and" : [{ "author_id" : { "$eq" : 1 } }, { "year" : { "$ne" : null } }] } }, { "$group" : { "_id" : null, "v" : { "$min" : "$year" } } }] } [paging in MongoDB] | exact
plan: Query books WHERE (all documents) | client: WHERE (UPPER(books.title) = 'SKETCH') | read 2
```

The filter, the sort and `Take` ran in MongoDB (using the `author_id` index); `Min` ran as an aggregation pipeline;
`ToUpper` has no exact MongoDB equivalent, so that predicate ran client-side. `LastQueryPlan` holds the most recent plan,
and `ExplainQueries = true` adds MongoDB's winning plan (`explain`, query planner verbosity) to finds outside
transactions.

| Setting (`MongoDbRepositorySettings`) | Default |
|---|---|
| `Client` | None; an existing `IMongoClient` (`ForClient(client, database)`), never disposed by Durable |
| `ConnectionString` | None (`ForConnectionString(cs, database?)`; the database defaults to the one in the URL, else `durable`) |
| `DatabaseName` | `durable` (letters, digits, `_`, `-`; at most 63 characters) |
| `Hostname`, `Port` | `localhost`, 27017 (`ForHost(host, port, database)`) |
| `Username`, `Password`, `AuthenticationDatabase` | None, None, `admin` |
| `ReplicaSetName`, `DirectConnection`, `UseTls`, `ApplicationName` | None, false, false, None |
| `ConnectionTimeout`, `ServerSelectionTimeout` | 30 s, 30 s |
| `MinPoolSize`, `MaxPoolSize` | 0, 100 |
| `TransactionsEnabled` | null: detected when the backend is created (replica set or sharded cluster) |
| `SequenceCollectionName` | `durable_sequences` |
| `Logger`, `JsonOptions` | None; Durable's default JSON options |

When `Client` or `ConnectionString` is set, the host and connection options are not used. `Create`/`CreateAsync`
contact the server once (a `hello` command) to detect transaction support unless `TransactionsEnabled` is set.

- **Storage**: one collection per entity (`[Entity]` name); each column is a document field named after the column. A
  single primary key is `_id`; a composite key is stored as its fields plus an `_id` sub-document of the key parts.
  Values round-trip exactly: `decimal` (scale kept) and `ulong` as Decimal128, `DateTime` as Decimal128 ticks plus kind
  (the BSON date type keeps only milliseconds), `DateTimeOffset` as Decimal128 UTC ticks plus offset, `TimeSpan` and
  `TimeOnly` as Int64 ticks, `DateOnly` as an Int32 day number, `Guid` as standard binary (subtype 4), enums as names (or
  Int64 with `Flags.Integer`).
- **Keys**: auto-increment needs a single integer primary key; values come from a counter document per collection in
  `durable_sequences` (an atomic `findOneAndUpdate`, taken outside transactions like a SQL sequence, so concurrent creates
  never share a key and a rolled-back insert leaves a gap). Guid and string keys are stored as given. `Clear(type)`
  restarts the sequence.
- **Indexes**: foreign keys, composite key parts, `[Index]` (grouped by name) and `[CompositeIndex]` get MongoDB indexes
  on the first insert outside a transaction, or up front with `EnsureIndexes(type)` / `EnsureIndexesAsync`. Unique
  indexes are enforced (a violation throws `InvalidOperationException`) and, as in SQL, ignore rows whose indexed columns
  are null (partial indexes on the fields' BSON types).
- **Queries**: comparisons, null checks, `IN` lists, `StartsWith`/`EndsWith`/`Contains`, `string.IsNullOrEmpty` and
  case-insensitive equality between a column and a value, combined with AND/OR/NOT, are pushed down as an exact MongoDB
  filter. When the whole filter is pushed down, ordering by columns, `Skip`/`Take`, `Count` and `Sum`/`Average`/`Min`/`Max`
  of a column (integers and decimals for `Sum`/`Average`, computed as Decimal128) run on the server too. Everything else
  (functions such as `ToUpper` or `Length`, arithmetic, navigation members, collection predicates, ordering by
  expressions, double sums) runs client-side with C# semantics over the documents MongoDB returns, so push-down never
  changes results.
- **Strings**: every command uses the simple (binary) collation, so a collection's default collation never applies and
  `StringMatchMode.Database` behaves as `Ordinal`. Case-insensitive matches are pushed down as regular expressions in which
  each letter is a character class of exactly the characters `StringComparison.OrdinalIgnoreCase` treats as equal (never
  the regex `i` option, whose Unicode folding differs), so results equal the other backends'.
- **Writes**: a conditional update (optimistic concurrency) or delete whose filter is pushed down exactly is one
  `updateMany`/`deleteMany`; otherwise each document is written with a filter on its complete previous content and
  re-checked when it changed concurrently, so updates are never lost. Changing a primary key replaces the document
  (atomic only inside a transaction).
- **Transactions**: client sessions; they need a replica set or a sharded cluster (a single-node replica set is enough).
  On a standalone server `SupportsTransactions` is false, `Capabilities` omits `Transactions`, `BeginTransaction` throws
  `NotSupportedException` and `CreateMany` is not atomic. MongoDB does not make writers wait: when two transactions write
  the same document, the second write fails at once with a write conflict (`MongoException` labeled
  `TransientTransactionError`); retry the transaction. If a server operation fails inside a transaction, MongoDB aborts it
  and further use throws `InvalidOperationException`; a duplicate primary key that Durable detects before writing does
  not abort it. `transaction.Session` exposes the client session for your own driver calls.
- **Reset**: `Clear()` drops every collection of the database (use a database dedicated to Durable); `Clear(type)` drops
  one collection, restarts its sequence and recreates it with its indexes.
- **Native AOT**: not supported. The MongoDB driver produces trim and AOT warnings, so `MongoDbBackend.Create` and
  `CreateAsync` carry `[RequiresUnreferencedCode]` and `[RequiresDynamicCode]`; Durable's own code in the package is
  annotated and warning-free.
- Capabilities: all on a replica set or sharded cluster; all except `Transactions` on a standalone server. Passes the full
  conformance kit (transaction cases are gated on a standalone server).
````

### 2. "Non-SQL Backend Conventions" table: add a MongoDB column (after LiteDB)

Column values, row by row (also change "The three non-SQL backends" to "The four non-SQL backends" and the trailing
sentence to "Plan objects (`LiteDbQueryPlan`, `MongoDbQueryPlan`, `LiteGraphQueryPlan`) ..."):

| Row | MongoDB |
|---|---|
| Backend type | `MongoDbBackend` |
| Create | `MongoDbBackend.Create(settings?)` / `CreateAsync(...)` (contacts the server once) |
| Settings | `MongoDbRepositorySettings` |
| Factory methods | `ForClient(client, database)`, `ForConnectionString(cs, database?)`, `ForHost(host, port, database)` (no `ForInMemory`: MongoDB is a server) |
| Settings members | `IsInMemory` (always false), `Validate()`, `JsonOptions`, `Logger`, `ToClientSettings()`, ... |
| Repositories | `backend.CreateRepository<T>(options?)` or `new MongoDbRepository<T>(backend, options?)` |
| Typed `repository.Backend` | `MongoDbBackend` |
| Ownership | Owns the client it created (`OwnsClient`); one you pass is never disposed |
| Disposal | same |
| Transactions | `MongoDbTransaction` (replica set or sharded cluster; `SupportsTransactions`) |
| Reset | same (`Clear()` drops every collection of the database) |
| Inspect storage | same (BSON documents) |
| Query plans | `QueryPlanned` event, `LastQueryPlan` (`ExplainQueries` adds MongoDB's winning plan) |
| Native AOT | Not supported (driver) |

## README table rows

### Badges table (top of README)

```markdown
| Durable.MongoDb | [![NuGet](https://img.shields.io/nuget/v/Durable.MongoDb.svg)](https://www.nuget.org/packages/Durable.MongoDb/) | [![Downloads](https://img.shields.io/nuget/dt/Durable.MongoDb.svg)](https://www.nuget.org/packages/Durable.MongoDb/) |
```

### Packages

```markdown
| `Durable.MongoDb` | MongoDB (document database server) backend | Durable, MongoDB.Driver 3.12.0 |
```

### Which package do I need?

```markdown
| Store entities in MongoDB (a replica set for transactions) | `Durable.MongoDb` |
```

### Requirements: databases table

```markdown
| MongoDB | 4.4 (tested with 8.0; multi-document transactions need a replica set or sharded cluster) | MongoDB.Driver 3.12.0 | Implicit collection creation inside transactions (`hello` topology detection falls back to `isMaster` on older servers) |
```

Also: the CI sentence becomes "... and `mcr.microsoft.com/mssql/server:2022-latest`, plus `mongo:8` (single-node replica set) for MongoDB."
and the Requirements bullet "Every library except `Durable.LiteGraph` ..." becomes "Every library except `Durable.LiteGraph`
and `Durable.MongoDb` (whose LiteGraph and MongoDB driver dependencies are not AOT-compatible) and the `Durable.Tool`
executable works in trimmed and Native AOT applications".

### Installation

```bash
dotnet add package Durable.MongoDb      # MongoDB backend
```

### Backends Compared (new column "MongoDB", after LiteDB)

| Row | MongoDB |
|---|---|
| Repository type | `MongoDbRepository<T>` |
| Storage | Server (database per backend) |
| `Capabilities` | All (no `Transactions` on a standalone server) |
| Full LINQ, Include, grouping, projections, aggregates | Yes |
| Where queries run | Filters, ordering, paging, counts and column aggregates pushed to MongoDB when exact, rest client-side |
| Transactions | Client sessions (replica set or sharded cluster); write conflicts fail fast |
| Savepoints | No |
| Upsert, `BatchUpdate`, `UpdateField` | Yes |
| `StringMatchMode.Database` behaves as | Ordinal (simple collation on every command) |
| Raw SQL, `QueryMultiple` | - |
| Stored procedures | - |
| `BulkInsert` | - |
| Set operations, CTEs, window functions | - |
| `InitializeTable`, migrations, `durable` CLI | Not needed (indexes from `[Index]`/`[CompositeIndex]` via `EnsureIndexes`) |
| SQL capture, interceptors, OpenTelemetry | Query plans (`QueryPlanned`, `LastQueryPlan`, `ExplainQueries`) |
| Native AOT | Not supported (driver) |
| Best for | Document data on a MongoDB server or Atlas |

### Feature matrix / capabilities (for any capability table the integration builds)

| Capability | MongoDB (replica set / sharded) | MongoDB (standalone) |
|---|---|---|
| Transactions | Yes | No (gated: `NotSupportedException`) |
| Include, ManyToMany, NavigationPredicates | Yes | Yes |
| Grouping, Projection, Distinct | Yes (client-side above the backend) | Yes |
| Aggregates | Yes (Sum/Average of integer and decimal columns, Min/Max of columns on the server) | Yes |
| Functions | Yes (client-side) | Yes |
| StringMatchModes | Yes (exact, server-side regex) | Yes |
| CompositeKeys, OptimisticConcurrency, BatchUpdate, Upsert | Yes | Yes |

### Thread Safety table

Add `MongoDbBackend` to the row "`InMemoryBackend`, `LiteDbBackend`, `LiteGraphBackend`" and `MongoDB` to the
"`RepositoryBase<T>` repositories (In-Memory, LiteDB, LiteGraph)" row. `MongoDbTransaction`: do not use one transaction
from several threads at once (operations are serialized).

### Dependency Injection table

Add `MongoDbBackend` to the singleton row ("Hold the data (or the database handle) and serve every entity type").

### Transactions section

In "the non-SQL backends' `BeginTransactionAsync` returns their own type (`InMemoryTransaction`, `LiteDbTransaction`,
`LiteGraphTransaction`)" add `MongoDbTransaction`.

### Error Handling table

Append to the `InvalidOperationException` row: "duplicate key or unique index violation on the MongoDB backend; use of a
MongoDB transaction after a server operation inside it failed". Add a row:

```markdown
| `MongoException` (MongoDB.Driver) | MongoDB backend: connection or authentication failures, write conflicts between transactions (`TransientTransactionError` label: retry the transaction), server errors |
```

### Async, Streaming and Cancellation

Add: "- MongoDB queries read all matching documents before returning the first entity (the server still filters, sorts
and pages)."

### Troubleshooting and FAQ

```markdown
| MongoDB: `BeginTransaction` throws `NotSupportedException` | The server is standalone; multi-document transactions need a replica set or a sharded cluster. Start `mongod --replSet rs0` and run `rs.initiate()` once (a single node is enough), or set `TransactionsEnabled = false` to opt out explicitly. |
| MongoDB in Docker: the client times out although the port is published | The replica set advertises the container's host name, which the host cannot resolve. Connect with `directConnection=true` (`DirectConnection = true` in the settings), or initiate the set with a host name the client can reach. |
| MongoDB: a transaction fails with a write conflict (`TransientTransactionError`) | Another transaction wrote the same document first; MongoDB does not wait for it. Retry the whole transaction. |
| MongoDB: queries sort emoji differently from `OrderBy` in C# | MongoDB orders strings by UTF-8 bytes (code points); C# ordinal order compares UTF-16 units. They differ only between supplementary characters (emoji and other characters above U+FFFF) and U+E000 to U+FFFF. Filters are unaffected; only server-side sort order of such strings. |
```

### Continuous Integration and Tests

CI `databases` matrix runs `mongodb` (both frameworks): `dotnet run --project src/Test.Automated -c Release -f <tfm> -- --type mongodb --docker`
(a `mongo:8` single-node replica set, `--memory 1g`, no authentication). It runs the conformance kit, `MongoDbBackendTestSuite`
and the backend-neutral suites.

## CHANGELOG

### Durable.MongoDb (new package)

- `MongoDbBackend` (`IRepositoryBackend`) over MongoDB.Driver 3.12.0: `Create`/`CreateAsync(MongoDbRepositorySettings?)`
  (one `hello` round trip to detect transaction support), `CreateRepository<T>`, `Owns`, `OwnsClient`, `Client`,
  `Database`, `DatabaseName`, `SupportsTransactions`, `Capabilities`, `JsonOptions`, `EnsureIndexes[Async]`,
  `GetStoredRows[Async]`, `Clear[Async]()`, `Clear[Async](Type)`, `BeginTransaction[Async]`, `QueryPlanned`,
  `LastQueryPlan`, `ExplainQueries`; `IDisposable` + `IAsyncDisposable`.
- `MongoDbRepository<T>` (`RepositoryBase<T>`) with a typed `Backend`; accepts ambient transactions of its own backend.
- `MongoDbRepositorySettings`: `ForClient(client, database)`, `ForConnectionString(cs, database?)`, `ForHost(host, port,
  database)`, connection string or host, port, credentials, replica set, direct connection, TLS, timeouts, pool sizes,
  application name, `TransactionsEnabled`, `SequenceCollectionName`, `Logger`, `JsonOptions`, `Validate()`,
  `ToClientSettings()`.
- `MongoDbTransaction` (client session; `Session` exposed). Requires a replica set or sharded cluster; on a standalone
  server the backend reports no `Transactions` capability.
- `MongoDbQueryPlan` (`EventArgs`): pushed filters, pushed sort, aggregation pipeline, exactness, client-side residue,
  paging push-down, documents read, optional `explain` winning plan.
- Entities are encoded to `BsonDocument` from `EntityMetadata` (no driver class maps), losslessly: Decimal128 for
  `decimal`/`ulong` and for `DateTime`/`DateTimeOffset` ticks (kind and offset kept), standard binary GUIDs.
- Server-side push-down through a `QueryNodeVisitor` with C# semantics: comparisons, null checks, IN, string matches
  (ordinal and case-insensitive via exact character-class regexes), AND/OR/NOT; then ordering, Skip/Take, Count and
  Sum/Average/Min/Max of a column when the filter is exact. Everything else is evaluated by `QueryEvaluator`.
- Auto-increment keys from a counters collection (`findOneAndUpdate`), Guid/string/composite keys, version columns
  (filtered `updateMany`), soft delete, includes, indexes from `[Index]`/`[CompositeIndex]` (unique indexes enforced,
  null-tolerant).
- Not trim/AOT compatible (the driver is not): `Create`/`CreateAsync` carry `[RequiresUnreferencedCode]` and
  `[RequiresDynamicCode]`.

### Tests

- `MongoDbTestTarget` (`IDocumentBackendTestTarget`) and `MongoDbConformanceTarget`: the full Durable.Conformance kit
  against MongoDB (`--type mongodb`); `DURABLE_TEST_MONGODB_STANDALONE=1` declares a standalone server (transaction
  cases gated).
- `MongoDbBackendTestSuite`: data shared across backends, push-down verified through query plans (including `explain`
  index use), parity with the in-memory backend in both string modes (non-ASCII, Kelvin sign, dotless i, emoji, regex
  metacharacters), exact round-trips, unique and composite indexes, concurrent creates, optimistic concurrency and
  computed set-based updates without lost updates, transactions (commit, rollback, abandoned, ambient, abort after a
  failed operation), ownership and disposal, settings, unsupported mappings, synchronous use.
- Docker: `--type mongodb --docker` starts `mongo:8` as a single-node replica set (`--replSet rs0`, `--memory 1g`) and
  initiates it in the readiness probe. CI runs it on net8.0 and net10.0.
- `PublicApiConventionsTestSuite` checks the Durable.MongoDb assembly.

## CLAUDE.md

Project tree (after `Durable.LiteDb/`):

```
├── Durable.MongoDb/           # MongoDB IRepositoryBackend (document server; not AOT-capable: MongoDB driver)
```

Published packages list: add `- `Durable.MongoDb`` after `Durable.LiteDb` (12 or more packages with the other v0.7.0
additions).

Backend convention line: "**Backend convention** (InMemory, LiteDb, LiteGraph, MongoDb; follow it for any new backend):"
and the factory line gains "`ForClient(client, ...)`/`ForConnectionString(...)`/`ForHost(...)` for servers (MongoDb has
no `ForInMemory`; `IsInMemory` is false)".

Notes (Testing / Native AOT):

- Document backends: MongoDB runs with `--type mongodb --docker` (single-node replica set, `directConnection=true`);
  `DURABLE_TEST_MONGODB_STANDALONE=1` declares a standalone server so transaction cases are gated.
- Durable.MongoDb is annotated, but the MongoDB driver is not trim/AOT compatible; `MongoDbBackend.Create`/`CreateAsync`
  carry `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]`. Do not add it to Test.Aot.
- Durable.MongoDb pushes filters down only where MongoDB matches C# exactly (simple collation on every command;
  case-insensitive matching via character-class regexes built from `OrdinalIgnoreCase`, never the regex `i` option).

## Limitations and capability gates

- **Transactions** (gated by capability): absent on a standalone server (`SupportsTransactions` false,
  `Capabilities` without `Transactions`, conformance transaction cases skipped with the capability reason). README
  wording: see the Transactions bullet of the MongoDB section.
- **Native AOT**: not supported (driver warnings IL2104/IL3053 from MongoDB.Driver and MongoDB.Bson); entry points
  annotated.
- **String sort order**: server-side sorting and `Min`/`Max` of strings follow MongoDB's code point order, which differs
  from C# ordinal order only between supplementary characters and U+E000 to U+FFFF (filters are exact: ordinal range
  comparisons are pushed only when the value has no character at or above U+D800).
- **Sort ties on DateTime/DateTimeOffset**: two values that are equal in C# but differ in `Kind` (or offset) sort by kind
  (offset) on the server instead of by input order.
- **Key changes** (`BatchUpdate` assigning a primary key) replace documents and are atomic only inside a transaction.
- **Write conflicts** between transactions fail fast instead of waiting (MongoDB semantics).
- `Clear()` drops every collection of the configured database.
