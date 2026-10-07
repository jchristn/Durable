# docs-fragments/cosmosdb.md (Durable.CosmosDb, agent "cosmosdb")

Package: `Durable.CosmosDb` 0.7.0. SDK: `Microsoft.Azure.Cosmos` 3.63.2 plus `Newtonsoft.Json` 13.0.4 (the SDK needs it
at run time but no longer brings it in; Durable never uses it for entities). Tested against the Linux "vNext" emulator
`mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview` (build 20260908.1, multi-arch amd64 + arm64,
`--protocol http`, gateway mode). Test `--type` name: `cosmosdb` (alias `cosmos`).

## README sections

### Insert after the "LiteDB Backend" section (before "LiteGraph Backend")

~~~~markdown
## Cosmos DB Backend

`Durable.CosmosDb` stores entities as JSON documents in [Azure Cosmos DB for NoSQL](https://learn.microsoft.com/azure/cosmos-db/nosql/), one container per entity, through the official `Microsoft.Azure.Cosmos` SDK. Documents are written and read by Durable itself (System.Text.Json over the SDK's stream APIs), never by the SDK's reflection serializer.

```csharp
using Durable.CosmosDb;

CosmosDbRepositorySettings settings = CosmosDbRepositorySettings.ForEndpoint(endpoint, accountKey, "shop")
    .WithPartitionKey<Order>(nameof(Order.CustomerId));
// Local emulator: CosmosDbRepositorySettings.ForEmulator("http://localhost:8081/", "shop")
// Connection string: CosmosDbRepositorySettings.ForConnectionString(connectionString, "shop")
// Existing CosmosClient (never disposed by Durable): CosmosDbRepositorySettings.ForClient(client, "shop")

await using CosmosDbBackend backend = await CosmosDbBackend.CreateAsync(settings);   // creates the database if missing
CosmosDbRepository<Order> orders = backend.CreateRepository<Order>();                 // the container is created on first use

Order created = await orders.CreateAsync(new Order { CustomerId = "c-42", Total = 19.99m, Placed = DateTime.UtcNow });

List<Order> recent = (await orders.Query()
    .Where(o => o.CustomerId == "c-42" && o.Total > 10m)
    .OrderByDescending(o => o.Placed)
    .Take(20)
    .ExecuteAsync()).ToList();

CosmosDbQueryPlan? plan = backend.LastQueryPlan;   // Cosmos DB SQL text, parameters, partition, RU charge
Console.WriteLine(plan);
```

```text
Query orders: SELECT * FROM c WHERE (IS_STRING(c["customer_id"]) AND c["customer_id"] = @p0) AND (IS_NUMBER(c["total"]) AND c["total"] > @p1) ORDER BY c["placed"] DESC OFFSET 0 LIMIT 20 {"@p0":"c-42","@p1":10} [partition ["c-42"]] | exact | read 1 | 2 RU
```

The whole query ran inside one partition (request charges depend on the account and document size). `ClientSide` is set when part of a query runs in C#.

| Setting (`CosmosDbRepositorySettings`) | Default |
|---|---|
| `Client` / `Endpoint` + `AccountKey` / `ConnectionString` | Exactly one is required (`ForClient`, `ForEndpoint`, `ForConnectionString`, `ForEmulator`); a client you pass is never disposed (`OwnsClient` is false) |
| `DatabaseName` | `"durable"` |
| `ConnectionMode`, `LimitToEndpoint`, `RequestTimeout`, `ApplicationName`, `AcceptAnyServerCertificate` | SDK defaults (Direct mode), false, SDK default, none, false. Only for a client Durable creates; `ForEmulator` sets Gateway, `LimitToEndpoint` and `AcceptAnyServerCertificate` |
| `CreateIfNotExists` | true (database on create, containers on first use) |
| `DatabaseThroughput` / `DatabaseAutoscaleMaxThroughput` | None (serverless or per-container throughput); manual >= 400 RU/s in steps of 100, autoscale >= 1000 in steps of 1000 |
| `ContainerThroughput` / `ContainerAutoscaleMaxThroughput` | None (shared database throughput or serverless) |
| `PartitionKeys` (`WithPartitionKey<T>(propertyName)`) | Empty: every entity is partitioned by `/id` |
| `SequenceContainerName` | `"durable_sequences"` (auto-increment counters) |
| `MaxConflictRetries` | 100 (ETag conflict retries per write) |
| `MaxConcurrentDeletes` | 8 (`Clear(type)`) |
| `Logger`, `JsonOptions` | None; Durable's default JSON options |

- **Storage**: one container per entity (`[Entity]` name), each column a document property. The document `id` is the primary key as text (composite keys joined with `|`, with `~`, `%`, `/`, `\`, `?`, `#` and `|` escaped as `~XX`); a single string key column named `id` is the `id` itself (such keys cannot contain `/`, `\`, `?`, `#` or a percent escape), and a column named `id` that is not (for example an `int` key) is stored as `durable_id`. Values round-trip exactly: `DateTime` as fixed-width ISO 8601 text with its kind (`Z`, none, or the local offset), `DateTimeOffset` as its UTC instant plus the offset, `DateOnly` as `yyyy-MM-dd`, `TimeSpan`/`TimeOnly` as ticks, `Guid` as text, byte arrays as base64, `NaN`/infinities as strings. Cosmos DB may keep numbers only as doubles, so longs beyond plus or minus 2^53 and decimals a double cannot round-trip also keep an exact copy in the document's `_durable` object (as does a non-zero `DateTimeOffset` offset); reads use it. Decimal scale (trailing zeros) is not preserved.
- **Queries**: comparisons, null checks, `IN` lists, `StartsWith`/`EndsWith`/`Contains` and `IsNullOrEmpty` between a column and values, combined with AND/OR/NOT, become a parameterized Cosmos DB SQL query with C# null semantics (`IS_DEFINED`/`IS_NULL`, type-guarded so negation is safe). A single-key `OrderBy`, `Skip`/`Take` (`OFFSET`/`LIMIT`), `Count`, and `Sum`/`Average`/`Min`/`Max` of integer (and, for `Min`/`Max`, string and date) columns run in Cosmos DB when the whole filter was pushed; key lookups are point reads; equality on the partition key scopes the query to one partition. Everything else (functions, arithmetic, navigations, multi-key ordering, grouping) runs client-side with C# semantics, so push-down never changes results: values Cosmos DB could compare or order differently from C# (decimals beyond double precision, longs beyond 2^53, strings with characters above U+D800, non-finite doubles) are detected and re-checked client-side. Unordered queries return documents in primary key order.
- **Strings**: Cosmos DB compares ordinally and case-sensitively, so `StringMatchMode.Database` behaves as `Ordinal`. `IgnoreCase` matches use Cosmos DB's case-insensitive `STARTSWITH`/`CONTAINS`/`ENDSWITH`/`STRINGEQUALS` for ASCII patterns without `i`/`s` (whose case folding differs from `OrdinalIgnoreCase`) and are re-checked client-side; other patterns are evaluated client-side.
- **Writes and concurrency**: every document write is atomic. Updates, deletes and version-checked updates read the matching documents and write each one with an ETag condition (`If-Match`); when another writer got there first, the document is read again, re-checked and written again, so concurrent `Update`, `UpdateField` and `BatchUpdate` calls never lose updates, also across processes. Auto-increment keys come from a counter document per container (ETag-conditioned increment), so they are unique across processes; `Clear(type)` restarts them.
- **Transactions**: not supported (`Capabilities` lacks `Transactions`). Cosmos DB transactional batches cover only one logical partition, and Durable does not fake atomicity across documents. A write that matches several documents is not atomic as a whole, and neither is changing a document's key or partition key value (Durable creates the new document, then deletes the old one).
- **Partition keys**: see [Choosing a partition key](#choosing-a-cosmos-db-partition-key).
- **Limits**: `[Index]` needs nothing (Cosmos DB indexes every property) and `[Index(IsUnique = true)]` is not enforced (Cosmos DB unique keys apply only within a logical partition and only at container creation). Container names must be 1 to 255 characters without `/`, `\`, `#` or `?`. Partition key columns must be strings, integers, Guids, dates or booleans.
- **Native AOT**: not supported. The Cosmos DB SDK uses Newtonsoft.Json and reflection internally (229 trim/AOT warnings when published with `PublishAot`), so `CosmosDbBackend.Create`/`CreateAsync` carry `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]`. Durable.CosmosDb itself is annotated and warning-free.
- Capabilities: all except `Transactions`. Passes the conformance kit (the 10 transaction cases are skipped by capability).

### Choosing a Cosmos DB partition key

By default every entity is partitioned by its document `id`: each document is its own logical partition. That spreads storage and throughput evenly, needs no design work, and makes every `ReadById` a 1 RU point read, but every query that does not name a key fans out across partitions (higher RU and latency as the container grows).

Partition an entity by one of its properties when most queries filter on it (a tenant, customer or device id):

```csharp
settings.WithPartitionKey<Order>(nameof(Order.CustomerId));   // container path /customer_id
```

| | `/id` (default) | Property (`WithPartitionKey`) |
|---|---|---|
| `Where(o => o.CustomerId == "c-42" ...)` | Cross-partition query | Single-partition query (`plan.PartitionKey` is set) |
| `ReadById` | Point read | Point read if the property is part of the primary key, otherwise a cross-partition query by `id` |
| Key uniqueness | Enforced by Cosmos DB | Cosmos DB ids are unique only within a partition; Durable checks other partitions with an extra query on insert |
| Changing the property's value | n/a | Moves the document (create, then delete; not atomic) |
| Hot partitions | None | Possible if one value gets most of the traffic |

The path is fixed when the container is created: a container that already exists with another path is rejected with `InvalidOperationException` (Durable never re-partitions data). Use a composite primary key whose first part is the partition property (for example `TenantId` + `LineNo`) to get point reads and single-partition queries at once.

### Cosmos DB emulator

The tests run against the Linux emulator, which works on arm64 and x64:

```bash
docker run --rm -p 8081:8081 mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview --protocol http
```

```csharp
await using CosmosDbBackend backend = await CosmosDbBackend.CreateAsync(CosmosDbRepositorySettings.ForEmulator("http://localhost:8081/"));
```

`ForEmulator` uses the well-known emulator key, gateway mode (the only mode the emulator supports) and `LimitToEndpoint` (required when the emulator is reached through a mapped port). Behavior the emulator cannot prove, and how Durable handles it:

- Real Cosmos DB may compare and return JSON numbers as doubles; the emulator keeps 64-bit integers exact. Durable's exact shadows and widened comparisons are correct either way, but only the exact path is exercised by the tests.
- Multi-property `ORDER BY` works on the emulator without a composite index but fails on the real service without one; Durable never pushes it down (it orders client-side).
- The emulator's gateway URL-decodes ids that contain percent escapes such as `%2F`, so point reads of such ids fail; Durable escapes ids with `~` and rejects such `id` keys.
- Direct connection mode, throughput limits (429 retries) and multi-region behavior are not exercised.
~~~~

### Add to "Non-SQL Backend Conventions" (new column "Cosmos DB" after LiteGraph)

Rows, in table order:

- Backend type: `CosmosDbBackend`
- Create: `CosmosDbBackend.Create(settings?)` / `CreateAsync(settings?, token)` (null settings: `ForEmulator()`)
- Settings: `CosmosDbRepositorySettings`
- Factory methods: `ForEmulator(endpoint?, database?)`, `ForEndpoint(endpoint, key, database?)`, `ForConnectionString(cs, database?)`, `ForClient(client, database?)` (no in-memory mode)
- Settings members: `IsInMemory` (always false), `Validate()`, `JsonOptions`, `Logger`, `PartitionKeys`, throughput, ...
- Repositories: `backend.CreateRepository<T>(options?)` or `new CosmosDbRepository<T>(backend, options?)`
- Typed `repository.Backend`: `CosmosDbBackend`
- Ownership: Owns the client it created (`OwnsClient`); one you pass is never disposed
- Disposal: same
- Transactions: Not supported (`BeginTransaction`/`BeginTransactionAsync` throw `NotSupportedException`; `Owns` is always false)
- Reset: `Clear()` / `ClearAsync(token)` delete every container of the database; `Clear(type)` / `ClearAsync(type, token)` delete the documents and restart the counter
- Inspect storage: same (`JsonObject` documents, including system properties)
- Query plans: `QueryPlanned` event, `LastQueryPlan` (Cosmos DB SQL, parameters, partition, point read, RU charge)
- Native AOT: Not supported (SDK)

## README table rows

### Top badge table (after Durable.LiteGraph)

```markdown
| Durable.CosmosDb | [![NuGet](https://img.shields.io/nuget/v/Durable.CosmosDb.svg)](https://www.nuget.org/packages/Durable.CosmosDb/) | [![Downloads](https://img.shields.io/nuget/dt/Durable.CosmosDb.svg)](https://www.nuget.org/packages/Durable.CosmosDb/) |
```

### Packages (after Durable.LiteGraph)

```markdown
| `Durable.CosmosDb` | Azure Cosmos DB for NoSQL backend | Durable, Microsoft.Azure.Cosmos 3.63.2, Newtonsoft.Json 13.0.4 |
```

### Which package do I need?

```markdown
| Store entities in Azure Cosmos DB for NoSQL | `Durable.CosmosDb` |
```

### Requirements / databases

```markdown
| Azure Cosmos DB for NoSQL | Service (any API version the SDK supports); Linux emulator `vnext-preview` for tests | Microsoft.Azure.Cosmos 3.63.2 | |
```

CI sentence: add "and the Cosmos DB Linux emulator (`mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview`)".

Requirements bullet on AOT: "Every library except `Durable.LiteGraph` and `Durable.CosmosDb` (whose LiteGraph and Cosmos DB SDK dependencies are not AOT-compatible) and the `Durable.Tool` executable works in trimmed and Native AOT applications".

### Installation

```bash
dotnet add package Durable.CosmosDb     # Azure Cosmos DB for NoSQL backend
```

### Backends Compared (new column "Cosmos DB")

| Row | Cosmos DB |
|---|---|
| Repository type | `CosmosDbRepository<T>` |
| Storage | Azure Cosmos DB for NoSQL (one container per entity) |
| `Capabilities` | All except `Transactions` |
| Full LINQ, Include, grouping, projections, aggregates | Yes |
| Where queries run | Comparisons/IN/null/string matches, single-key ORDER BY, paging, counts and integer aggregates pushed to Cosmos DB SQL; rest client-side |
| Transactions | No (per-document atomic writes; ETag-checked updates) |
| Savepoints | No |
| Upsert, `BatchUpdate`, `UpdateField` | Yes |
| `StringMatchMode.Database` behaves as | Ordinal |
| Raw SQL, `QueryMultiple` | - |
| Stored procedures | - |
| `BulkInsert` | - |
| Set operations, CTEs, window functions | - |
| `InitializeTable`, migrations, `durable` CLI | Not needed (containers created on first use) |
| SQL capture, interceptors, OpenTelemetry | Query plans (`QueryPlanned`, `LastQueryPlan`, with RU charge) |
| Native AOT | Not supported (Cosmos DB SDK) |
| Best for | Globally distributed, elastic document storage on Azure |

### Thread Safety table

Add `CosmosDbBackend` to the `InMemoryBackend`, `LiteDbBackend`, `LiteGraphBackend` row, and `CosmosDbRepository<T>` (Cosmos DB) to the `RepositoryBase<T>` repositories row.

### Dependency Injection table

Add `CosmosDbBackend` to the singleton row (it holds the `CosmosClient`, which Microsoft recommends as a singleton).

### Error Handling (`InvalidOperationException` row)

Append: "duplicate key or an id Cosmos DB cannot hold on the Cosmos DB backend; a Cosmos DB container partitioned by another path; a document that kept changing for `MaxConflictRetries` attempts". `CosmosException` (from the SDK) surfaces for account-level failures (wrong key, throttling after the SDK's retries, missing permissions).

### Troubleshooting and FAQ

```markdown
| Cosmos DB: "container ... is partitioned by '/id', but Durable maps it to '/tenant_id'" | The container was created before the entity got a partition key (or with another one). Partition key paths cannot change: migrate the data to a new container (another `[Entity]` name) or remove the `WithPartitionKey` setting. |
| Cosmos DB: queries are slow or expensive (high RU in `plan.RequestCharge`) | They fan out across partitions or run client-side. Filter on the partition key (`WithPartitionKey`), keep filters to pushed-down forms (check `LastQueryPlan.Exact` and `ClientSide`), and avoid multi-key `OrderBy` on large containers (it orders client-side). |
| Cosmos DB emulator: connection refused or requests to another port | Use `ForEmulator(endpoint)` (gateway mode, `LimitToEndpoint`); start the vNext emulator with `--protocol http` or pass an `https` endpoint (`AcceptAnyServerCertificate` accepts its self-signed certificate). |
| Cosmos DB: `NotSupportedException` from `BeginTransaction` | The Cosmos DB backend has no transactions; group related changes in one document, or use a backend with transactions. |
```

### Continuous Integration and Tests

- Databases job row: "PostgreSQL, MySQL, SQL Server, ... and the Cosmos DB emulator in disposable docker containers, net8.0 and net10.0".
- Local run line: `dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type cosmosdb --docker`
- Existing account: `--type cosmosdb --host https://myaccount.documents.azure.com:443/ --pass <account key> --database <db>` (the host may be a full endpoint URI; the password defaults to the emulator key).

## CHANGELOG

### Durable.CosmosDb (new package)

- New Azure Cosmos DB for NoSQL backend (`CosmosDbBackend`, `CosmosDbRepository<T>`, `CosmosDbRepositorySettings`, `CosmosDbQueryPlan`) following the non-SQL backend convention, on Microsoft.Azure.Cosmos 3.63.2. Documents are written and read with System.Text.Json through the SDK's stream APIs, never the SDK's reflection serializer.
- Settings: `ForEmulator`, `ForEndpoint`, `ForConnectionString`, `ForClient` (not owned), database name, connection mode, `LimitToEndpoint`, request timeout, database and container throughput (manual or autoscale), creation on first use, `JsonOptions`, `Logger`, conflict retries.
- Partition keys: `/id` by default (one logical partition per document); `WithPartitionKey<T>(propertyName)` / `PartitionKeys` partition an entity by a column, with single-partition queries for equality on it, point reads when it is part of the key, cross-partition key uniqueness checks and document moves when its value changes. No Cosmos-specific attribute was added to core Durable.
- Push-down: parameterized Cosmos DB SQL for comparisons, null checks, IN lists, string matches (case-insensitive variants for safe patterns), `IsNullOrEmpty`, AND/OR/NOT (type-guarded, C# null semantics), single-key ORDER BY, OFFSET/LIMIT, COUNT, integer SUM/AVG and MIN/MAX; point reads for key lookups; client-side evaluation for everything else with checks that keep results identical to C# (decimal and long precision probes, code point versus UTF-16 ordering, non-finite doubles).
- Lossless values: exact shadows for longs beyond 2^53 and decimals beyond double precision, DateTime ticks and kind, DateTimeOffset offset, NaN and infinities.
- Concurrency: ETag-conditioned replace and delete with re-check and retry (no lost updates for version-checked, field and batch updates); auto-increment keys from an ETag-incremented counter document per container.
- Transactions are not supported and reported through `Capabilities`; `Create`/`CreateAsync` are marked `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]` because the SDK is not trim/AOT-compatible.

### Tests and CI

- `--type cosmosdb --docker` runs the conformance kit and `CosmosDbBackendTestSuite` (persistence, document shape, push-down via query plans, in-memory parity, precision, concurrency, partition keys, key changes, settings, ownership and disposal, includes) against the Linux vNext emulator; CI runs it on net8.0 and net10.0.
- `PublicApiConventionsTestSuite` covers `Durable.CosmosDb`.

## CLAUDE.md

Project tree (after the `Durable.LiteGraph/` line):

```
├── Durable.CosmosDb/          # Azure Cosmos DB for NoSQL IRepositoryBackend (not AOT-capable: Cosmos DB SDK)
```

Layering, backend convention bullet: change "(InMemory, LiteDb, LiteGraph; follow it for any new backend)" to include CosmosDb, and add to the settings bullet: "`ForClient(client, ...)` / `ForEndpoint` / `ForEmulator` for Cosmos DB (`IsInMemory` is always false there)". Add after the transactions bullet: "Backends without transactions (Cosmos DB) omit `Transactions` from `Capabilities`; their `BeginTransaction[Async]` throw `NotSupportedException` and `Owns` returns false."

Testing notes, conformance bullet: append "Document backends (`--type mongodb|cosmosdb --docker`) run the kit through `IDocumentBackendTestTarget`; Cosmos DB uses the Linux vNext emulator (`--protocol http`, gateway mode)."

Native AOT section, LiteGraph bullet: append "Durable.CosmosDb is in the same position (the Cosmos DB SDK uses Newtonsoft.Json and reflection): `CosmosDbBackend.Create`/`CreateAsync` carry `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]`; do not add it to Test.Aot."

Published packages list: add `Durable.CosmosDb` (and bump the count).

Key interfaces: no change.

## Limitations and capability gates

| Gate / limitation | Why | README wording |
|---|---|---|
| `RepositoryCapabilities.Transactions` absent (10 conformance transaction cases skipped) | Cosmos DB transactional batches cover one logical partition only; Durable does not fake atomicity | "Transactions: not supported" bullet above |
| Multi-document writes and key/partition changes not atomic | Same | Same bullet |
| `[Index(IsUnique = true)]` not enforced | Unique keys are per logical partition and fixed at container creation | "Limits" bullet |
| Multi-key ORDER BY, functions, navigations, grouping run client-side | Composite indexes would be required on the real service; functions differ in casing/length semantics | "Queries" bullet |
| Decimal scale not preserved | JSON numbers carry no scale | "Storage" bullet |
| Native AOT | SDK not trim-compatible | "Native AOT" bullet |
| Emulator test gaps (double-only number semantics, composite-index enforcement, Direct mode, throttling) | vNext emulator behaves differently or does not offer them | "Cosmos DB emulator" subsection |
