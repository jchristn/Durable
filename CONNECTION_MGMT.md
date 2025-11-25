# Durable Connection Management

This document describes exactly how Durable manages database connections, when connections are assigned and released, developer responsibilities, library automatic handling, and possible root causes for connection pool timeout exceptions.

## Architecture Overview

Durable implements a two-tier connection pooling strategy:

1. **Durable's `ConnectionPool`**: A custom connection pool that wraps and manages connections from the underlying ADO.NET provider
2. **ADO.NET Provider Pool**: The underlying database driver's native connection pool (e.g., MySqlConnector's pool, Npgsql's pool)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Application Code                            │
│          repository.Query().Where(...).Execute()                    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Repository / QueryBuilder                       │
│   Uses IConnectionFactory to get connections for operations         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    IConnectionFactory                               │
│       MySqlConnectionFactory / SqliteConnectionFactory / etc.       │
│              (wraps ConnectionPool)                                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      ConnectionPool                                 │
│  - SemaphoreSlim controls MaxPoolSize concurrent connections        │
│  - ConcurrentQueue<PooledConnection> for available connections      │
│  - ConcurrentBag<PooledConnection> tracks all connections           │
│  - Returns PooledConnectionHandle that auto-returns on Dispose      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ADO.NET Provider Connection Pool                       │
│        MySqlConnector / Npgsql / Microsoft.Data.Sqlite              │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Components

### ConnectionPoolOptions

Default configuration values:

| Option | Default | Description |
|--------|---------|-------------|
| `MinPoolSize` | 5 | Minimum connections to maintain in the pool |
| `MaxPoolSize` | 100 | Maximum concurrent connections allowed |
| `ConnectionTimeout` | 30 seconds | Time to wait for an available connection |
| `IdleTimeout` | 10 minutes | How long unused connections stay in pool |
| `ValidateConnections` | true | Validate connections before returning them |

### ConnectionPool Class

**Location**: `src/Durable/ConnectionPool.cs`

The `ConnectionPool` class manages the connection lifecycle:

1. **Semaphore-Based Limiting**: Uses `SemaphoreSlim` initialized to `MaxPoolSize` to strictly control concurrent connection usage
2. **Connection Tracking**:
   - `_AvailableConnections` (ConcurrentQueue): Connections ready to be reused
   - `_AllConnections` (ConcurrentBag): All connections ever created (for cleanup)
3. **Cleanup Timer**: Runs every 60 seconds to remove idle connections exceeding `IdleTimeout`

### PooledConnectionHandle Class

**Location**: `src/Durable/PooledConnectionHandle.cs`

A wrapper around `DbConnection` that **automatically returns the connection to the pool when disposed**. This is critical for proper resource management.

```csharp
// The PooledConnectionHandle.Dispose() method:
protected override void Dispose(bool disposing)
{
    if (disposing)
    {
        lock (_Lock)
        {
            if (!_Returned && _InnerConnection != null)
            {
                _Returned = true;
                DbConnection conn = _InnerConnection;
                _InnerConnection = null;
                _Pool.ReturnConnection(conn);  // Returns to pool AND releases semaphore
            }
        }
    }
    base.Dispose(disposing);
}
```

## Connection Lifecycle

### 1. Connection Acquisition (`GetConnection` / `GetConnectionAsync`)

When you request a connection, the following happens:

```
1. Check if pool is disposed → throw ObjectDisposedException
2. Wait on semaphore (up to ConnectionTimeout)
   └─ If timeout → throw TimeoutException
3. Try to dequeue from _AvailableConnections
   ├─ If connection found and valid:
   │   ├─ Reopen if closed
   │   └─ Return wrapped in PooledConnectionHandle
   └─ If no connection or invalid:
       ├─ Create new connection via factory function
       ├─ Open the connection
       ├─ Add to _AllConnections
       └─ Return wrapped in PooledConnectionHandle
```

**Critical Point**: The semaphore is acquired BEFORE getting a connection. If `_Semaphore.Wait()` times out, you get:
```
System.TimeoutException: Timeout waiting for available connection from pool
```

### 2. Connection Release (`ReturnConnection` / `ReturnConnectionAsync`)

When a connection is returned to the pool:

```
1. Unwrap PooledConnectionHandle if necessary
2. Find the PooledConnection in _AllConnections
3. Mark as not in use (IsInUse = false)
4. Close the connection (returns to ADO.NET pool)
5. If connection still valid → Enqueue to _AvailableConnections
6. Release the semaphore → _Semaphore.Release()
```

**Critical Point**: The semaphore is ONLY released when a connection is returned. If connections are not properly returned, the semaphore count decreases permanently until no new connections can be acquired.

### 3. Connection Disposal (When Connection is Disposed)

The `PooledConnectionHandle` ensures automatic cleanup:

```csharp
// This is safe:
using DbConnection connection = connectionFactory.GetConnection();
// Connection is automatically returned to pool when leaving this scope

// This is also safe:
await using DbConnection connection = await connectionFactory.GetConnectionAsync(token);
// Connection is automatically returned to pool when leaving this scope
```

## How Repository and QueryBuilder Use Connections

### Pattern 1: Using Statement (Safe - Automatic Return)

This is the predominant pattern in the codebase:

```csharp
// From MySqlQueryBuilder.ExecuteSqlInternal()
private IEnumerable<TEntity> ExecuteSqlInternal(string sql)
{
    if (_Transaction != null)
    {
        // Transaction owns the connection - do NOT dispose
        return ExecuteWithConnection(_Transaction.Connection, sql);
    }
    else
    {
        // Safe: using statement ensures connection return
        using DbConnection connection = _Repository._ConnectionFactory.GetConnection();
        return ExecuteWithConnection(connection, sql);
    }
}
```

### Pattern 2: Try/Finally (Safe - Manual Return)

Used in some aggregate operations:

```csharp
// From MySqlQueryBuilder.ExecuteScalarInternal()
private TResult ExecuteScalarInternal<TResult>(string sql)
{
    if (_Transaction != null)
    {
        return ExecuteScalarWithConnection<TResult>(_Transaction.Connection, sql);
    }
    else
    {
        DbConnection connection = _Repository._ConnectionFactory.GetConnection();
        try
        {
            return ExecuteScalarWithConnection<TResult>(connection, sql);
        }
        finally
        {
            _Repository._ConnectionFactory.ReturnConnection(connection);  // Explicit return
        }
    }
}
```

### Pattern 3: Transaction (Connection Returned on Transaction Dispose)

Transactions hold a connection for their lifetime:

```csharp
// Connection is acquired when transaction begins
ITransaction transaction = repository.BeginTransaction();
try
{
    // All operations use the transaction's connection
    repository.Create(entity1, transaction);
    repository.Update(entity2, transaction);
    transaction.Commit();
}
finally
{
    // MySqlRepositoryTransaction.Dispose() returns the connection to pool
    transaction.Dispose();
}
```

## What the Library Handles Automatically

1. **Connection Wrapping**: All connections returned from `GetConnection()` are wrapped in `PooledConnectionHandle`
2. **Automatic Return on Dispose**: When `PooledConnectionHandle.Dispose()` is called, the connection is returned to the pool
3. **Idle Connection Cleanup**: A background timer removes connections that have been idle longer than `IdleTimeout`
4. **Connection Validation**: Before returning a cached connection, it validates the connection is still usable
5. **Connection State Management**: Opens connections if they are closed when retrieved from the pool
6. **Semaphore Management**: Automatically acquires and releases semaphore slots

## Developer/User Responsibilities

**In theory, Durable's design should NOT require any manual connection management from developers.** The library wraps all connections in `PooledConnectionHandle` which automatically returns connections when disposed. All repository methods use `using` statements or `try/finally` blocks to ensure proper cleanup.

However, there are some edge cases and best practices:

### 1. Transactions (MUST be disposed)

Transactions hold connections and MUST be disposed:

```csharp
// CORRECT: Transaction properly disposed
using ITransaction transaction = await repository.BeginTransactionAsync();
try
{
    await repository.CreateAsync(entity, transaction);
    await transaction.CommitAsync();
}
catch
{
    await transaction.RollbackAsync();
    throw;
}
// Transaction disposed here, connection returned to pool

// WRONG: Transaction leak!
ITransaction transaction = repository.BeginTransaction();
repository.Create(entity, transaction);
transaction.Commit();
// Transaction never disposed - connection never returned!
```

### 2. Direct Connection Access (if using GetConnection directly)

If you call `GetConnection()` or `GetConnectionAsync()` directly on a repository or connection factory, you MUST dispose the result:

```csharp
// CORRECT
using DbConnection connection = repository.GetConnection();
// Use connection

// WRONG
DbConnection connection = repository.GetConnection();
// Forgot to dispose - semaphore slot permanently lost!
```

### 3. Standard Repository Operations (NO manual cleanup needed)

For normal repository operations, no manual cleanup is required:

```csharp
// These are all safe - library handles connection lifecycle internally
Person person = await repository.ReadFirstAsync(p => p.Id == 1);
await repository.CreateAsync(newPerson);
int count = await repository.CountAsync();
IEnumerable<Person> people = await repository.Query().Where(p => p.Age > 30).ExecuteAsync();
```

### 4. Don't Hold Transactions During Long Operations

```csharp
// AVOID: Holding connection during external call
using ITransaction transaction = repository.BeginTransaction();
T entity = repository.ReadFirst(e => e.Id == id, transaction);
await ExternalApiCall(entity);  // Long running - connection held entire time!
repository.Update(entity, transaction);
transaction.Commit();

// BETTER: Release connection between operations
T entity = repository.ReadFirst(e => e.Id == id);  // Connection acquired and released
await ExternalApiCall(entity);  // No connection held
using ITransaction transaction = repository.BeginTransaction();
repository.Update(entity, transaction);
transaction.Commit();
```

### 5. Configure Appropriate Pool Size

If your application has high concurrency needs:

```csharp
ConnectionPoolOptions options = new ConnectionPoolOptions
{
    MinPoolSize = 10,
    MaxPoolSize = 200,  // Increase if you have many concurrent operations
    ConnectionTimeout = TimeSpan.FromSeconds(60),  // Increase timeout if needed
    IdleTimeout = TimeSpan.FromMinutes(5)
};

IConnectionFactory factory = new MySqlConnectionFactory(connectionString, options);
```

## Possible Root Causes for TimeoutException

The exception you're seeing:
```
System.TimeoutException: Timeout waiting for available connection from pool
```

This occurs when `_Semaphore.Wait(ConnectionTimeout)` times out. Here are the possible causes:

### 1. Connection Leaks (Most Common)

**Cause**: Connections are acquired but never returned to the pool.

**How it happens**:
- Forgetting to dispose transactions
- Acquiring connections directly without using statements
- Exceptions that bypass cleanup code
- Async methods that don't await properly (connection disposal occurs before method completes)

**Detection**: Monitor your semaphore count over time. If it continuously decreases, you have a leak.

### 2. Long-Running Operations Holding Connections

**Cause**: Operations hold connections for extended periods, preventing other operations from getting connections.

**Example scenarios**:
- Long-running transactions that span external API calls
- Operations that read large result sets without streaming
- Sync-over-async patterns that block threads holding connections

### 3. Connection Pool Too Small for Concurrency

**Cause**: `MaxPoolSize` is smaller than peak concurrent connection demand.

**Your situation**: Looking at your error, you have multiple services (WebhookService processing task, WebhookService cleanup task) all trying to acquire connections simultaneously. If you have:
- 10 concurrent webhook processors
- 5 cleanup tasks
- Default `MaxPoolSize = 100`

And each operation takes 1 second, you should be fine. But if operations take longer or you have more concurrent workers, the pool can exhaust.

### 4. Async Operations Not Using ConfigureAwait(false)

**Cause**: In library code without `ConfigureAwait(false)`, async continuations may deadlock on the synchronization context, preventing connection release.

**Note**: The Durable library does use `ConfigureAwait(false)` consistently, but if your application code doesn't, it could cause issues.

### 5. Exception During Connection Return

**Cause**: If an exception occurs during `ReturnConnection()`, the semaphore might not be released.

**Current code analysis**: The `ReturnConnection` method in `ConnectionPool.cs` does handle this:
```csharp
public void ReturnConnection(DbConnection connection)
{
    // ... validation and processing ...
    _Semaphore.Release();  // Always called at the end
}
```

However, if `connection.Close()` throws, the semaphore is released but the connection might be in a bad state.

### 6. Mixed Sync and Async Patterns

**Cause**: Mixing synchronous and asynchronous connection usage can cause thread pool exhaustion, which then starves the semaphore wait.

**Example**:
```csharp
// This can cause problems in high-concurrency scenarios
Task.Run(() => {
    using ITransaction tx = repository.BeginTransaction();  // Sync call in Task.Run
    // ...
});
```

## Diagnostic Steps

### Step 1: Add Connection Pool Monitoring

Create a monitoring wrapper or add logging to track:
- Number of connections acquired
- Number of connections returned
- Time held per connection
- Current semaphore count

### Step 2: Audit Transaction Usage

Search your codebase for:
- `BeginTransaction()` without matching `Dispose()` or `using`
- `BeginTransactionAsync()` without proper `await using` or disposal

### Step 3: Check Concurrent Operation Count

In your WebhookService scenario:
- How many concurrent ProcessingTask instances run?
- How many concurrent CleanupTask instances run?
- What's the `MaxPoolSize` configured?

### Step 4: Add Connection Timeout Logging

Before operations that can timeout, log:
- Operation name
- Current timestamp
- Expected duration

### Step 5: Review Long-Running Operations

Examine operations that:
- Make external HTTP calls within a transaction
- Process large datasets within a single connection scope
- Have retry logic that doesn't dispose connections on failure

## Summary

| Aspect | Detail |
|--------|--------|
| **Connection Acquisition** | Semaphore wait → Dequeue from pool or create new → Wrap in PooledConnectionHandle |
| **Connection Release** | Dispose PooledConnectionHandle → ReturnConnection → Release semaphore |
| **Automatic Handling** | Wrapping, validation, idle cleanup, state management |
| **Developer Must** | Dispose connections/transactions, configure appropriate pool size |
| **TimeoutException Cause** | Semaphore exhausted due to leaks, long holds, or insufficient pool size |
