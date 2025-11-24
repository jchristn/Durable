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
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
