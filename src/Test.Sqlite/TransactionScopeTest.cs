namespace Test.Sqlite
{
    using Durable;
    using Durable.Sqlite;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Test.Shared;

    internal class TransactionScopeTest
    {
        private readonly SqliteRepository<Person> _repository;

        public TransactionScopeTest()
        {
            string connectionString = "Data Source=InMemoryDemo;Mode=Memory;Cache=Shared";

            // Configure connection pool with higher timeout and larger pool for concurrent tests
            SqliteConnectionFactory factory = connectionString.CreateFactory(options =>
            {
                options.ConnectionTimeout = TimeSpan.FromMinutes(2); // Increase timeout to 2 minutes
                options.MaxPoolSize = 50; // Increase max pool size for concurrent operations
                options.MinPoolSize = 10; // Keep more connections ready
            });

            _repository = new SqliteRepository<Person>(factory);
        }

        public TransactionScopeTest(SqliteRepository<Person> repository)
        {
            _repository = repository;
        }

        public void TestBasicTransactionScope()
        {
            Console.WriteLine("=== Testing Basic Transaction Scope ===");
            
            // Test successful transaction
            _repository.ExecuteInTransactionScope(() =>
            {
                Person person1 = new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john@test.com", Salary = 75000, Department = "IT" };
                Person person2 = new Person { FirstName = "Jane", LastName = "Smith", Age = 25, Email = "jane@test.com", Salary = 65000, Department = "HR" };
                
                _repository.Create(person1);  // Uses ambient transaction
                _repository.Create(person2);  // Uses ambient transaction
                
                Console.WriteLine($"Created person 1 with ID: {person1.Id}");
                Console.WriteLine($"Created person 2 with ID: {person2.Id}");
            });
            
            int count = _repository.Count();
            Console.WriteLine($"Total persons after successful transaction: {count}");
            
            // Test rollback transaction
            try
            {
                _repository.ExecuteInTransactionScope(() =>
                {
                    Person person3 = new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Email = "bob@test.com", Salary = 80000, Department = "Sales" };
                    _repository.Create(person3);
                    Console.WriteLine($"Created person 3 with ID: {person3.Id}");
                    
                    // Force an exception to trigger rollback
                    throw new InvalidOperationException("Simulated error");
                });
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"Caught expected exception: {ex.Message}");
            }
            
            int finalCount = _repository.Count();
            Console.WriteLine($"Total persons after rollback: {finalCount}");
            Console.WriteLine();
        }

        public async Task TestAsyncTransactionScope()
        {
            Console.WriteLine("=== Testing Async Transaction Scope ===");
            
            string result = await _repository.ExecuteInTransactionScopeAsync(async () =>
            {
                Person person4 = new Person { FirstName = "Alice", LastName = "Wilson", Age = 28, Email = "alice@test.com", Salary = 70000, Department = "Marketing" };
                Person person5 = new Person { FirstName = "Charlie", LastName = "Brown", Age = 32, Email = "charlie@test.com", Salary = 85000, Department = "Finance" };
                
                await _repository.CreateAsync(person4);
                await _repository.CreateAsync(person5);
                
                Console.WriteLine($"Created person 4 with ID: {person4.Id}");
                Console.WriteLine($"Created person 5 with ID: {person5.Id}");
                
                return $"Successfully created {2} persons";
            });
            
            Console.WriteLine($"Operation result: {result}");
            
            int count = _repository.Count();
            Console.WriteLine($"Total persons after async transaction: {count}");
            Console.WriteLine();
        }

        public void TestNestedTransactionWithSavepoints()
        {
            Console.WriteLine("=== Testing Nested Transactions with Savepoints ===");
            
            using ITransaction transaction = _repository.BeginTransaction();
            try
            {
                // Create first person in main transaction
                Person person6 = new Person { FirstName = "David", LastName = "Lee", Age = 40, Email = "david@test.com", Salary = 90000, Department = "Operations" };
                _repository.Create(person6, transaction);
                Console.WriteLine($"Created person 6 with ID: {person6.Id} in main transaction");
                
                // Execute operation with savepoint
                transaction.ExecuteWithSavepoint(() =>
                {
                    Person person7 = new Person { FirstName = "Eva", LastName = "Garcia", Age = 26, Email = "eva@test.com", Salary = 68000, Department = "HR" };
                    _repository.Create(person7, transaction);
                    Console.WriteLine($"Created person 7 with ID: {person7.Id} in savepoint");
                    
                    // This will succeed and release the savepoint
                });
                
                // Try another operation with savepoint that will fail
                try
                {
                    transaction.ExecuteWithSavepoint(() =>
                    {
                        Person person8 = new Person { FirstName = "Frank", LastName = "Miller", Age = 45, Email = "frank@test.com", Salary = 95000, Department = "Legal" };
                        _repository.Create(person8, transaction);
                        Console.WriteLine($"Created person 8 with ID: {person8.Id} in failing savepoint");
                        
                        throw new InvalidOperationException("Savepoint operation failed");
                    });
                }
                catch (InvalidOperationException ex)
                {
                    Console.WriteLine($"Savepoint rolled back due to: {ex.Message}");
                }
                
                transaction.Commit();
                Console.WriteLine("Main transaction committed successfully");
            }
            catch
            {
                transaction.Rollback();
                Console.WriteLine("Main transaction rolled back");
                throw;
            }
            
            int finalCount = _repository.Count();
            Console.WriteLine($"Total persons after nested transaction test: {finalCount}");
            Console.WriteLine();
        }

        public void TestAmbientTransactionScope()
        {
            Console.WriteLine("=== Testing Ambient Transaction Scope ===");

            using (TransactionScope scope1 = TransactionScope.Create(_repository))
            {
                // These operations will use the ambient transaction
                Person person9 = new Person { FirstName = "Grace", LastName = "Taylor", Age = 29, Email = "grace@test.com", Salary = 72000, Department = "Marketing" };
                _repository.Create(person9);  // No transaction parameter needed
                Console.WriteLine($"Created person 9 with ID: {person9.Id} using ambient transaction");

                // Nested scope with same transaction
                using (TransactionScope scope2 = TransactionScope.Create(scope1.Transaction))
                {
                    Person person10 = new Person { FirstName = "Henry", LastName = "Davis", Age = 33, Email = "henry@test.com", Salary = 78000, Department = "IT" };
                    _repository.Create(person10);  // Still uses the same transaction
                    Console.WriteLine($"Created person 10 with ID: {person10.Id} using nested ambient transaction");

                    scope2.Complete();
                }

                scope1.Complete();
            } // Transaction scope is disposed here, making the transaction invalid

            // Now it's safe to call Count() as no ambient transaction is active
            int finalCount = _repository.Count();
            Console.WriteLine($"Total persons after ambient transaction test: {finalCount}");
            Console.WriteLine();
        }

        public void TestConcurrentTransactionScopes()
        {
            Console.WriteLine("=== Testing Concurrent Transaction Scopes ===");
            
            List<Task> tasks = new List<Task>();
            int successCount = 0;
            object lockObject = new object();
            
            // Create 10 concurrent tasks that each create persons in transaction scopes
            for (int i = 0; i < 10; i++)
            {
                int taskId = i;
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        _repository.ExecuteInTransactionScope(() =>
                        {
                            Person person = new Person 
                            { 
                                FirstName = $"Concurrent{taskId}", 
                                LastName = "Test", 
                                Age = 20 + taskId,
                                Email = $"concurrent{taskId}@test.com",
                                Salary = 50000 + taskId * 1000,
                                Department = "Testing"
                            };
                            _repository.Create(person);
                            
                            // Simulate some work
                            Thread.Sleep(10);
                            
                            Console.WriteLine($"Task {taskId}: Created person with ID {person.Id}");
                        });
                        
                        lock (lockObject)
                        {
                            successCount++;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Task {taskId} failed: {ex.Message}");
                    }
                }));
            }
            
            Task.WaitAll(tasks.ToArray());
            Console.WriteLine($"Concurrent test completed: {successCount}/10 tasks successful");
            Console.WriteLine();
        }
        
        public void TestDeepNestedSavepoints()
        {
            Console.WriteLine("=== Testing Deep Nested Savepoints ===");
            
            using ITransaction transaction = _repository.BeginTransaction();
            try
            {
                // Create a person in the main transaction
                Person mainPerson = new Person { FirstName = "Main", LastName = "Transaction", Age = 30, Email = "main@test.com", Salary = 85000, Department = "Management" };
                _repository.Create(mainPerson, transaction);
                Console.WriteLine($"Created main person with ID: {mainPerson.Id}");
                
                // Create nested savepoints (depth of 5)
                CreateNestedSavepoints(transaction, 1, 5);
                
                transaction.Commit();
                Console.WriteLine("Deep nested savepoint test completed successfully");
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                Console.WriteLine($"Deep nested test failed: {ex.Message}");
                throw;
            }
            
            Console.WriteLine();
        }
        
        private void CreateNestedSavepoints(ITransaction transaction, int currentDepth, int maxDepth)
        {
            if (currentDepth > maxDepth)
                return;
                
            transaction.ExecuteWithSavepoint(() =>
            {
                Person person = new Person 
                { 
                    FirstName = $"Nested{currentDepth}", 
                    LastName = "Savepoint", 
                    Age = 25 + currentDepth,
                    Email = $"nested{currentDepth}@test.com",
                    Salary = 60000 + currentDepth * 5000,
                    Department = "Nested"
                };
                _repository.Create(person, transaction);
                Console.WriteLine($"Created nested person at depth {currentDepth} with ID: {person.Id}");
                
                // Recursively create deeper savepoints
                CreateNestedSavepoints(transaction, currentDepth + 1, maxDepth);
                
            }, $"nested_sp_{currentDepth}");
        }

        public void TestMemoryLeakPrevention()
        {
            Console.WriteLine("=== Testing Memory Leak Prevention ===");
            
            int initialCount = _repository.Count();
            
            // Create many nested scopes to test for memory leaks
            for (int i = 0; i < 100; i++)
            {
                using (TransactionScope scope1 = TransactionScope.Create(_repository))
                {
                    using (TransactionScope scope2 = TransactionScope.Create(scope1.Transaction))
                    {
                        using (TransactionScope scope3 = TransactionScope.Create(scope2.Transaction))
                        {
                            Person person = new Person 
                            { 
                                FirstName = $"Memory{i}", 
                                LastName = "Test", 
                                Age = 25,
                                Email = $"memory{i}@test.com",
                                Salary = 55000,
                                Department = "Memory"
                            };
                            _repository.Create(person);
                            
                            // Only complete every 10th transaction to test rollback scenarios
                            if (i % 10 == 0)
                            {
                                scope3.Complete();
                                scope2.Complete();
                                scope1.Complete();
                            }
                        }
                    }
                }
                
                // Force garbage collection periodically
                if (i % 25 == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
            
            int finalCount = _repository.Count();
            int expectedCount = initialCount + 10; // Only 10 transactions should have committed
            
            Console.WriteLine($"Initial count: {initialCount}, Final count: {finalCount}, Expected: {expectedCount}");
            if (finalCount == expectedCount)
            {
                Console.WriteLine("✓ Memory leak prevention test passed");
            }
            else
            {
                Console.WriteLine("✗ Memory leak prevention test failed");
            }
            
            Console.WriteLine();
        }

        public void RunAllTests()
        {
            try
            {
                TestBasicTransactionScope();
                TestAsyncTransactionScope().Wait();
                TestNestedTransactionWithSavepoints();
                TestAmbientTransactionScope();
                TestConcurrentTransactionScopes();
                TestDeepNestedSavepoints();
                TestMemoryLeakPrevention();

                Console.WriteLine("=== All Tests Completed Successfully ===");
                IEnumerable<Person> allPersons = _repository.ReadAll();
                Console.WriteLine("Final state of database:");
                foreach (Person person in allPersons)
                {
                    Console.WriteLine($"  ID: {person.Id}, Name: {person.FirstName} {person.LastName}, Age: {person.Age}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Test failed with exception: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                throw; // Re-throw the exception so the test runner knows the test failed
            }
        }

        public void Dispose()
        {
            _repository?.Dispose();
        }
    }
}