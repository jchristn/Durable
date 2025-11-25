namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Stress test suite for connection pool management.
    /// Tests connection pooling behavior under high load to ensure proper resource management,
    /// no connection leakage, and no memory issues.
    /// </summary>
    public class ConnectionPoolStressTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionPoolStressTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public ConnectionPoolStressTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests sequential query execution to verify connections are properly returned to the pool.
        /// </summary>
        [Fact]
        public async Task SequentialQueries_ShouldReuseConnections()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person testPerson = new Person
            {
                FirstName = "Test",
                LastName = "User",
                Age = 30,
                Email = "test@example.com",
                Salary = 50000m,
                Department = "Testing"
            };

            await repository.CreateAsync(testPerson);

            int queryCount = 5000;
            long initialMemory = GC.GetTotalMemory(true);

            for (int i = 0; i < queryCount; i++)
            {
                Person? person = await repository.ReadFirstAsync(p => p.Email == "test@example.com");
                Assert.NotNull(person);
            }

            long finalMemory = GC.GetTotalMemory(true);
            long memoryGrowth = finalMemory - initialMemory;

            Assert.True(memoryGrowth < 10_000_000,
                $"Memory grew by {memoryGrowth:N0} bytes, indicating possible connection leakage");
        }

        /// <summary>
        /// Tests concurrent query execution to verify thread-safe connection pool behavior.
        /// </summary>
        [Fact]
        public async Task ConcurrentQueries_ShouldHandleMultipleThreads()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] testPeople = new Person[100];
            for (int i = 0; i < testPeople.Length; i++)
            {
                testPeople[i] = new Person
                {
                    FirstName = $"Person{i}",
                    LastName = $"Test{i}",
                    Age = 20 + (i % 50),
                    Email = $"person{i}@example.com",
                    Salary = 50000m + (i * 1000),
                    Department = i % 2 == 0 ? "Engineering" : "Marketing"
                };
            }

            await repository.CreateManyAsync(testPeople);

            int queriesPerThread = 500;
            int threadCount = 10;
            int totalQueries = queriesPerThread * threadCount;

            long initialMemory = GC.GetTotalMemory(true);
            Stopwatch stopwatch = Stopwatch.StartNew();

            Task[] tasks = new Task[threadCount];
            for (int t = 0; t < threadCount; t++)
            {
                int threadId = t;
                tasks[t] = Task.Run(async () =>
                {
                    for (int i = 0; i < queriesPerThread; i++)
                    {
                        Person[] people = (await repository.Query()
                            .Where(p => p.Age > 25)
                            .ExecuteAsync())
                            .ToArray();

                        Assert.NotEmpty(people);
                    }
                });
            }

            await Task.WhenAll(tasks);

            stopwatch.Stop();

            long finalMemory = GC.GetTotalMemory(true);
            long memoryGrowth = finalMemory - initialMemory;

            Console.WriteLine($"  Completed {totalQueries:N0} concurrent queries in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Average: {stopwatch.ElapsedMilliseconds / (double)totalQueries:F2}ms per query");
            Console.WriteLine($"  Memory growth: {memoryGrowth:N0} bytes");

            Assert.True(memoryGrowth < 20_000_000,
                $"Memory grew by {memoryGrowth:N0} bytes, indicating possible connection leakage");
        }

        /// <summary>
        /// Tests mixed read and write operations under load.
        /// </summary>
        [Fact]
        public async Task MixedReadWriteOperations_ShouldHandleHighLoad()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int operationCount = 2000;
            long initialMemory = GC.GetTotalMemory(true);

            for (int i = 0; i < operationCount; i++)
            {
                if (i % 4 == 0)
                {
                    Person person = new Person
                    {
                        FirstName = $"Person{i}",
                        LastName = $"Test{i}",
                        Age = 20 + (i % 60),
                        Email = $"person{i}@example.com",
                        Salary = 50000m,
                        Department = "Testing"
                    };

                    await repository.CreateAsync(person);
                }
                else if (i % 4 == 1)
                {
                    int count = await repository.CountAsync(p => p.Department == "Testing");
                    Assert.True(count >= 0);
                }
                else if (i % 4 == 2)
                {
                    Person[] people = (await repository.Query()
                        .Where(p => p.Age > 30)
                        .Take(10)
                        .ExecuteAsync())
                        .ToArray();

                    Assert.NotNull(people);
                }
                else
                {
                    Person? person = await repository.ReadFirstAsync(p => p.Department == "Testing");
                    if (person != null)
                    {
                        person.Age += 1;
                        await repository.UpdateAsync(person);
                    }
                }
            }

            long finalMemory = GC.GetTotalMemory(true);
            long memoryGrowth = finalMemory - initialMemory;

            Console.WriteLine($"  Completed {operationCount:N0} mixed operations");
            Console.WriteLine($"  Memory growth: {memoryGrowth:N0} bytes");

            Assert.True(memoryGrowth < 15_000_000,
                $"Memory grew by {memoryGrowth:N0} bytes, indicating possible connection leakage");
        }

        /// <summary>
        /// Tests rapid connection acquisition and release patterns.
        /// </summary>
        [Fact]
        public async Task RapidConnectionCycling_ShouldNotLeakConnections()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person testPerson = new Person
            {
                FirstName = "Test",
                LastName = "User",
                Age = 30,
                Email = "rapid@example.com",
                Salary = 50000m,
                Department = "Testing"
            };

            await repository.CreateAsync(testPerson);

            int cycleCount = 3000;
            long initialMemory = GC.GetTotalMemory(true);

            for (int i = 0; i < cycleCount; i++)
            {
                Person? person = await repository.ReadByIdAsync(1);

                if (i % 100 == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                }
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            long finalMemory = GC.GetTotalMemory(true);
            long memoryGrowth = finalMemory - initialMemory;

            Console.WriteLine($"  Completed {cycleCount:N0} rapid connection cycles");
            Console.WriteLine($"  Memory growth after forced GC: {memoryGrowth:N0} bytes");

            Assert.True(memoryGrowth < 5_000_000,
                $"Memory grew by {memoryGrowth:N0} bytes after forced GC, indicating connection leakage");
        }

        /// <summary>
        /// Tests connection pool behavior with complex queries.
        /// </summary>
        [Fact]
        public async Task ComplexQueries_ShouldMaintainPoolIntegrity()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] testPeople = new Person[500];
            for (int i = 0; i < testPeople.Length; i++)
            {
                testPeople[i] = new Person
                {
                    FirstName = $"Person{i}",
                    LastName = $"Test{i}",
                    Age = 20 + (i % 60),
                    Email = $"person{i}@example.com",
                    Salary = 40000m + (i * 100),
                    Department = i % 3 == 0 ? "Engineering" : (i % 3 == 1 ? "Marketing" : "Sales")
                };
            }

            await repository.CreateManyAsync(testPeople);

            int queryCount = 1000;
            long initialMemory = GC.GetTotalMemory(true);

            for (int i = 0; i < queryCount; i++)
            {
                Person[] results = (await repository.Query()
                    .Where(p => p.Age > 25 && p.Age < 50)
                    .Where(p => p.Salary > 45000m)
                    .OrderBy(p => p.LastName)
                    .Skip(10)
                    .Take(20)
                    .ExecuteAsync())
                    .ToArray();

                Assert.NotNull(results);
            }

            long finalMemory = GC.GetTotalMemory(true);
            long memoryGrowth = finalMemory - initialMemory;

            Console.WriteLine($"  Completed {queryCount:N0} complex queries");
            Console.WriteLine($"  Memory growth: {memoryGrowth:N0} bytes");

            Assert.True(memoryGrowth < 10_000_000,
                $"Memory grew by {memoryGrowth:N0} bytes, indicating possible connection leakage");
        }

        /// <summary>
        /// Tests connection pool behavior with transaction-heavy workload.
        /// </summary>
        [Fact]
        public async Task HighVolumeTransactions_ShouldReleaseConnections()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int transactionCount = 1000;
            long initialMemory = GC.GetTotalMemory(true);

            for (int i = 0; i < transactionCount; i++)
            {
                Person person = new Person
                {
                    FirstName = $"Trans{i}",
                    LastName = $"Test{i}",
                    Age = 25 + (i % 40),
                    Email = $"trans{i}@example.com",
                    Salary = 50000m,
                    Department = "Transactions"
                };

                Person created = await repository.CreateAsync(person);
                Assert.NotNull(created);
                Assert.True(created.Id > 0);

                Person? retrieved = await repository.ReadByIdAsync(created.Id);
                Assert.NotNull(retrieved);

                retrieved.Age += 1;
                Person updated = await repository.UpdateAsync(retrieved);
                Assert.NotNull(updated);
            }

            long finalMemory = GC.GetTotalMemory(true);
            long memoryGrowth = finalMemory - initialMemory;

            Console.WriteLine($"  Completed {transactionCount:N0} transaction cycles");
            Console.WriteLine($"  Memory growth: {memoryGrowth:N0} bytes");

            Assert.True(memoryGrowth < 15_000_000,
                $"Memory grew by {memoryGrowth:N0} bytes, indicating possible connection leakage");
        }

        /// <summary>
        /// Tests connection pool with parallel batch operations.
        /// </summary>
        [Fact]
        public async Task ParallelBatchOperations_ShouldHandleConcurrency()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int batchCount = 50;
            int recordsPerBatch = 100;
            long initialMemory = GC.GetTotalMemory(true);

            Task[] tasks = new Task[batchCount];

            for (int b = 0; b < batchCount; b++)
            {
                int batchId = b;
                tasks[b] = Task.Run(async () =>
                {
                    string departmentName = "Batch" + batchId.ToString();
                    Person[] batch = new Person[recordsPerBatch];
                    for (int i = 0; i < recordsPerBatch; i++)
                    {
                        batch[i] = new Person
                        {
                            FirstName = "Batch" + batchId.ToString() + "Person" + i.ToString(),
                            LastName = "Test" + i.ToString(),
                            Age = 25,
                            Email = "batch" + batchId.ToString() + "person" + i.ToString() + "@example.com",
                            Salary = 50000m,
                            Department = departmentName
                        };
                    }

                    await repository.CreateManyAsync(batch);

                    int count = await repository.CountAsync(p => p.Department == departmentName);
                    Assert.Equal(recordsPerBatch, count);
                });
            }

            await Task.WhenAll(tasks);

            long finalMemory = GC.GetTotalMemory(true);
            long memoryGrowth = finalMemory - initialMemory;

            int totalRecords = await repository.CountAsync();

            Console.WriteLine($"  Created {totalRecords:N0} records in {batchCount} parallel batches");
            Console.WriteLine($"  Memory growth: {memoryGrowth:N0} bytes");

            Assert.Equal(batchCount * recordsPerBatch, totalRecords);
            Assert.True(memoryGrowth < 25_000_000,
                $"Memory grew by {memoryGrowth:N0} bytes, indicating possible connection leakage");
        }

        /// <summary>
        /// Tests sustained load over extended period to detect slow leaks.
        /// </summary>
        [Fact]
        public async Task SustainedLoad_ShouldNotLeakMemory()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] initialData = new Person[200];
            for (int i = 0; i < initialData.Length; i++)
            {
                initialData[i] = new Person
                {
                    FirstName = $"Sustained{i}",
                    LastName = $"Test{i}",
                    Age = 25 + (i % 40),
                    Email = $"sustained{i}@example.com",
                    Salary = 50000m,
                    Department = i % 2 == 0 ? "Engineering" : "Marketing"
                };
            }

            await repository.CreateManyAsync(initialData);

            int iterations = 10;
            int queriesPerIteration = 500;
            List<long> memorySnapshots = new List<long>();

            for (int iteration = 0; iteration < iterations; iteration++)
            {
                for (int q = 0; q < queriesPerIteration; q++)
                {
                    Person[] people = (await repository.Query()
                        .Where(p => p.Age > 30)
                        .ExecuteAsync())
                        .ToArray();

                    Assert.NotEmpty(people);
                }

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                long memory = GC.GetTotalMemory(false);
                memorySnapshots.Add(memory);
                Console.WriteLine($"  Iteration {iteration + 1}/{iterations}: {memory:N0} bytes");
            }

            long firstIterationMemory = memorySnapshots[0];
            long lastIterationMemory = memorySnapshots[memorySnapshots.Count - 1];
            long memoryGrowth = lastIterationMemory - firstIterationMemory;

            Console.WriteLine($"  Total sustained operations: {iterations * queriesPerIteration:N0}");
            Console.WriteLine($"  Memory growth from first to last iteration: {memoryGrowth:N0} bytes");

            Assert.True(memoryGrowth < 5_000_000,
                $"Memory grew by {memoryGrowth:N0} bytes over {iterations} iterations, indicating slow memory leak");
        }

        /// <summary>
        /// Tests extreme concurrent load that exceeds typical pool capacity.
        /// This test launches many more concurrent operations than the default pool size
        /// to verify the pool handles backpressure correctly.
        /// </summary>
        [Fact]
        public async Task ExtremeConcurrentLoad_ShouldHandleBackpressure()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            // Create test data
            for (int i = 0; i < 50; i++)
            {
                await repository.CreateAsync(new Person
                {
                    FirstName = $"Extreme{i}",
                    LastName = "Test",
                    Age = 25 + (i % 40),
                    Email = $"extreme{i}@test.com",
                    Salary = 50000m,
                    Department = "ExtremeTest"
                });
            }

            // Launch 200 concurrent operations - far more than typical pool size
            int concurrentOps = 200;
            int successCount = 0;
            int errorCount = 0;

            Stopwatch stopwatch = Stopwatch.StartNew();

            Task[] tasks = new Task[concurrentOps];
            for (int i = 0; i < concurrentOps; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    try
                    {
                        Person[] results = (await repository.Query()
                            .Where(p => p.Department == "ExtremeTest")
                            .Take(10)
                            .ExecuteAsync())
                            .ToArray();
                        Interlocked.Increment(ref successCount);
                    }
                    catch
                    {
                        Interlocked.Increment(ref errorCount);
                    }
                });
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();

            Console.WriteLine($"  {concurrentOps} concurrent operations: {successCount} success, {errorCount} errors");
            Console.WriteLine($"  Completed in {stopwatch.ElapsedMilliseconds}ms");

            // Most operations should succeed even under extreme load
            Assert.True(successCount > concurrentOps * 0.9,
                $"Expected >90% success rate under extreme load, got {successCount}/{concurrentOps}");
        }

        /// <summary>
        /// Tests that transactions don't starve regular queries under load.
        /// Long-running transactions hold connections, but regular queries should still complete.
        /// </summary>
        [Fact]
        public async Task TransactionsUnderLoad_ShouldNotStarveQueries()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            // Create test data
            for (int i = 0; i < 20; i++)
            {
                await repository.CreateAsync(new Person
                {
                    FirstName = $"TransLoad{i}",
                    LastName = "Test",
                    Age = 30,
                    Email = $"transload{i}@test.com",
                    Salary = 50000m,
                    Department = "TransLoadTest"
                });
            }

            int transactionCount = 10;
            int queryCount = 50;
            int transactionHoldTimeMs = 500;

            int transactionSuccess = 0;
            int querySuccess = 0;

            // Start long-running transactions
            Task[] transactionTasks = new Task[transactionCount];
            for (int t = 0; t < transactionCount; t++)
            {
                transactionTasks[t] = Task.Run(async () =>
                {
                    ITransaction? transaction = null;
                    try
                    {
                        transaction = await repository.BeginTransactionAsync();
                        int count = await repository.CountAsync(p => p.Department == "TransLoadTest", transaction);
                        await Task.Delay(transactionHoldTimeMs);
                        await transaction.CommitAsync();
                        Interlocked.Increment(ref transactionSuccess);
                    }
                    catch
                    {
                        if (transaction != null)
                        {
                            try { await transaction.RollbackAsync(); } catch { }
                        }
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                });
            }

            // Give transactions time to start
            await Task.Delay(50);

            // Run concurrent queries while transactions are active
            Task[] queryTasks = new Task[queryCount];
            for (int q = 0; q < queryCount; q++)
            {
                queryTasks[q] = Task.Run(async () =>
                {
                    try
                    {
                        int count = await repository.CountAsync();
                        Interlocked.Increment(ref querySuccess);
                    }
                    catch
                    {
                        // Query failed
                    }
                });
            }

            await Task.WhenAll(queryTasks);
            await Task.WhenAll(transactionTasks);

            Console.WriteLine($"  Transactions: {transactionSuccess}/{transactionCount} succeeded");
            Console.WriteLine($"  Concurrent queries: {querySuccess}/{queryCount} succeeded");

            // Queries should not be completely starved
            Assert.True(querySuccess > queryCount * 0.5,
                $"Expected >50% query success while transactions active, got {querySuccess}/{queryCount}");
        }

        /// <summary>
        /// Tests recovery after many exceptions - connections should be properly returned.
        /// </summary>
        [Fact]
        public async Task ManyExceptions_ShouldNotLeakConnections()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            // Cause 100 exceptions with invalid SQL
            int exceptionCount = 100;
            for (int i = 0; i < exceptionCount; i++)
            {
                try
                {
                    await repository.ExecuteSqlAsync("SELECT * FROM nonexistent_table_xyz_999");
                }
                catch
                {
                    // Expected
                }
            }

            // Pool should still be functional
            int successCount = 0;
            for (int i = 0; i < 20; i++)
            {
                try
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"AfterException{i}",
                        LastName = "Test",
                        Age = 30,
                        Email = $"afterexception{i}@test.com",
                        Salary = 50000m,
                        Department = "ExceptionRecovery"
                    });
                    successCount++;
                }
                catch
                {
                    // Failed
                }
            }

            Console.WriteLine($"  After {exceptionCount} exceptions: {successCount}/20 operations succeeded");

            Assert.Equal(20, successCount);

            int count = await repository.CountAsync(p => p.Department == "ExceptionRecovery");
            Assert.Equal(20, count);
        }

        /// <summary>
        /// Tests concurrent transaction rollbacks to ensure connections are properly returned.
        /// </summary>
        [Fact]
        public async Task ConcurrentRollbacks_ShouldReturnConnections()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int rollbackCount = 30;
            int successfulRollbacks = 0;

            Task[] tasks = new Task[rollbackCount];
            for (int t = 0; t < rollbackCount; t++)
            {
                int transId = t;
                tasks[t] = Task.Run(async () =>
                {
                    ITransaction? transaction = null;
                    try
                    {
                        transaction = await repository.BeginTransactionAsync();
                        await repository.CreateAsync(new Person
                        {
                            FirstName = $"Rollback{transId}",
                            LastName = "Test",
                            Age = 30,
                            Email = $"rollback{transId}@test.com",
                            Salary = 50000m,
                            Department = "RollbackTest"
                        }, transaction);

                        // Intentionally rollback
                        await transaction.RollbackAsync();
                        Interlocked.Increment(ref successfulRollbacks);
                    }
                    catch
                    {
                        if (transaction != null)
                        {
                            try { await transaction.RollbackAsync(); } catch { }
                        }
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                });
            }

            await Task.WhenAll(tasks);

            Console.WriteLine($"  {successfulRollbacks}/{rollbackCount} rollbacks completed");

            // Verify data was actually rolled back
            int count = await repository.CountAsync(p => p.Department == "RollbackTest");
            Assert.Equal(0, count);

            // Pool should still work after all rollbacks
            await repository.CreateAsync(new Person
            {
                FirstName = "AfterRollbacks",
                LastName = "Test",
                Age = 30,
                Email = "afterrollbacks@test.com",
                Salary = 50000m,
                Department = "AfterRollback"
            });

            count = await repository.CountAsync(p => p.Department == "AfterRollback");
            Assert.Equal(1, count);
        }

        /// <summary>
        /// Tests mixed create, read, update operations under high concurrency.
        /// </summary>
        [Fact]
        public async Task HighConcurrencyMixedOperations_ShouldComplete()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int threadCount = 20;
            int opsPerThread = 30;
            int createCount = 0;
            int readCount = 0;
            int updateCount = 0;

            Task[] tasks = new Task[threadCount];
            for (int t = 0; t < threadCount; t++)
            {
                int threadId = t;
                tasks[t] = Task.Run(async () =>
                {
                    for (int i = 0; i < opsPerThread; i++)
                    {
                        int op = (threadId + i) % 3;
                        try
                        {
                            switch (op)
                            {
                                case 0: // Create
                                    await repository.CreateAsync(new Person
                                    {
                                        FirstName = $"Mixed{threadId}_{i}",
                                        LastName = "Test",
                                        Age = 30,
                                        Email = $"mixed{threadId}_{i}@test.com",
                                        Salary = 50000m,
                                        Department = "MixedOps"
                                    });
                                    Interlocked.Increment(ref createCount);
                                    break;

                                case 1: // Read
                                    Person[] people = (await repository.Query()
                                        .Where(p => p.Department == "MixedOps")
                                        .Take(5)
                                        .ExecuteAsync())
                                        .ToArray();
                                    Interlocked.Increment(ref readCount);
                                    break;

                                case 2: // Update (find and update)
                                    Person? person = await repository.ReadFirstAsync(p => p.Department == "MixedOps");
                                    if (person != null)
                                    {
                                        person.Age = person.Age + 1;
                                        await repository.UpdateAsync(person);
                                        Interlocked.Increment(ref updateCount);
                                    }
                                    break;
                            }
                        }
                        catch
                        {
                            // Operation failed - continue
                        }
                    }
                });
            }

            await Task.WhenAll(tasks);

            Console.WriteLine($"  Creates: {createCount}, Reads: {readCount}, Updates: {updateCount}");
            Console.WriteLine($"  Total operations: {createCount + readCount + updateCount}");

            int totalOps = createCount + readCount + updateCount;
            int expectedMinOps = threadCount * opsPerThread / 2; // At least 50% should succeed

            Assert.True(totalOps >= expectedMinOps,
                $"Expected at least {expectedMinOps} operations, got {totalOps}");
        }

        /// <summary>
        /// Tests that large result sets don't cause connection issues.
        /// </summary>
        [Fact]
        public async Task LargeResultSets_ShouldNotExhaustPool()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            // Create many records
            int recordCount = 1000;
            int batchSize = 50;

            for (int batch = 0; batch < recordCount / batchSize; batch++)
            {
                Person[] people = new Person[batchSize];
                for (int i = 0; i < batchSize; i++)
                {
                    int idx = batch * batchSize + i;
                    people[i] = new Person
                    {
                        FirstName = $"Large{idx}",
                        LastName = $"Result{idx}",
                        Age = 25 + (idx % 50),
                        Email = $"large{idx}@test.com",
                        Salary = 50000m,
                        Department = "LargeResultTest"
                    };
                }
                await repository.CreateManyAsync(people);
            }

            // Read all records multiple times concurrently
            int concurrentReads = 10;
            int successCount = 0;

            Stopwatch stopwatch = Stopwatch.StartNew();

            Task[] tasks = new Task[concurrentReads];
            for (int i = 0; i < concurrentReads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    try
                    {
                        Person[] results = (await repository.Query()
                            .Where(p => p.Department == "LargeResultTest")
                            .ExecuteAsync())
                            .ToArray();

                        if (results.Length == recordCount)
                        {
                            Interlocked.Increment(ref successCount);
                        }
                    }
                    catch
                    {
                        // Failed
                    }
                });
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();

            Console.WriteLine($"  {concurrentReads} concurrent reads of {recordCount} records each");
            Console.WriteLine($"  Success: {successCount}, Time: {stopwatch.ElapsedMilliseconds}ms");

            Assert.Equal(concurrentReads, successCount);
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
