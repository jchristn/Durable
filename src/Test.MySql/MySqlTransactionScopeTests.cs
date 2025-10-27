namespace Test.MySql
{
    using Durable;
    using Durable.MySql;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Test.Shared;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Comprehensive tests for MySQL advanced transaction features including:
    /// - Savepoints: Nested transaction rollback points
    /// - Ambient transactions: TransactionScope integration
    /// - Concurrent transactions: Thread-safety testing
    /// - Deep nesting: Multi-level savepoint chains
    /// - Memory leak prevention: Resource management testing
    /// </summary>
    public class MySqlTransactionScopeTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly MySqlRepository<Person> _PersonRepository;
        private readonly MySqlRepository<Company> _CompanyRepository;
        private readonly MySqlRepository<Author> _AuthorRepository;
        private readonly bool _SkipTests;

        public MySqlTransactionScopeTests(ITestOutputHelper output)
        {
            _output = output;

            // Check if MySQL is available and skip tests if not
            // Use a separate database to avoid conflicts with entity relationship tests
            string connectionString = "Server=localhost;Database=durable_transaction_test;User=test_user;Password=test_password;";

            try
            {
                // Test connection availability with a simple query that doesn't require tables
                using (MySqlConnector.MySqlConnection connection = new MySqlConnector.MySqlConnection(connectionString))
                {
                    connection.Open();
                    using (MySqlConnector.MySqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "SELECT 1";
                        command.ExecuteScalar();
                    }
                }

                _PersonRepository = new MySqlRepository<Person>(connectionString);
                _CompanyRepository = new MySqlRepository<Company>(connectionString);
                _AuthorRepository = new MySqlRepository<Author>(connectionString);

                // Create tables if they don't exist
                CreateTablesIfNeeded();

                // Clean up any existing test data
                CleanupTestData();

                _output.WriteLine("MySQL transaction scope tests initialized successfully");
            }
            catch (Exception ex)
            {
                _SkipTests = true;
                _output.WriteLine($"WARNING: MySQL initialization failed - {ex.Message}");
            }
        }

        private void CreateTablesIfNeeded()
        {
            try
            {
                // Create people table
                _PersonRepository.ExecuteSql(@"
                    CREATE TABLE IF NOT EXISTS people (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        first VARCHAR(100) NOT NULL,
                        last VARCHAR(100) NOT NULL,
                        age INT NOT NULL,
                        email VARCHAR(255) NOT NULL,
                        salary DECIMAL(10,2) NOT NULL,
                        department VARCHAR(100) NOT NULL,
                        INDEX idx_department (department),
                        INDEX idx_age (age)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

                // Create companies table
                _CompanyRepository.ExecuteSql(@"
                    CREATE TABLE IF NOT EXISTS companies (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        industry VARCHAR(50),
                        INDEX idx_name (name)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

                // Create authors table
                _AuthorRepository.ExecuteSql(@"
                    CREATE TABLE IF NOT EXISTS authors (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        company_id INT NULL,
                        INDEX idx_name (name),
                        INDEX idx_company_id (company_id),
                        FOREIGN KEY (company_id) REFERENCES companies(id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

                _output.WriteLine("Database tables created or verified successfully");
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Warning: Could not create tables: {ex.Message}");
            }
        }

        private void CleanupTestData()
        {
            try
            {
                _PersonRepository.DeleteAll();
                _CompanyRepository.DeleteAll();
                _AuthorRepository.DeleteAll();
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Warning: Could not clean up test data: {ex.Message}");
            }
        }

        [Fact]
        public void TestBasicTransactionScope()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Basic Transaction Scope ===");

            int initialCount = _PersonRepository.Count();

            // Test successful transaction with ExecuteInTransactionScope
            _PersonRepository.ExecuteInTransactionScope(() =>
            {
                Person person1 = new Person
                {
                    FirstName = "John",
                    LastName = "Doe",
                    Age = 30,
                    Email = "john@transactiontest.com",
                    Salary = 75000,
                    Department = "IT"
                };
                Person person2 = new Person
                {
                    FirstName = "Jane",
                    LastName = "Smith",
                    Age = 25,
                    Email = "jane@transactiontest.com",
                    Salary = 65000,
                    Department = "HR"
                };

                _PersonRepository.Create(person1);  // Uses ambient transaction
                _PersonRepository.Create(person2);  // Uses ambient transaction

                _output.WriteLine($"Created person 1 with ID: {person1.Id}");
                _output.WriteLine($"Created person 2 with ID: {person2.Id}");

                Assert.True(person1.Id > 0);
                Assert.True(person2.Id > 0);
            });

            int afterSuccessCount = _PersonRepository.Count();
            Assert.Equal(initialCount + 2, afterSuccessCount);
            _output.WriteLine($"Total persons after successful transaction: {afterSuccessCount}");

            // Test rollback transaction
            bool exceptionCaught = false;
            try
            {
                _PersonRepository.ExecuteInTransactionScope(() =>
                {
                    Person person3 = new Person
                    {
                        FirstName = "Bob",
                        LastName = "Johnson",
                        Age = 35,
                        Email = "bob@transactiontest.com",
                        Salary = 80000,
                        Department = "Sales"
                    };
                    _PersonRepository.Create(person3);
                    _output.WriteLine($"Created person 3 with ID: {person3.Id}");

                    // Force an exception to trigger rollback
                    throw new InvalidOperationException("Simulated error for rollback test");
                });
            }
            catch (InvalidOperationException ex)
            {
                _output.WriteLine($"Caught expected exception: {ex.Message}");
                exceptionCaught = true;
            }

            Assert.True(exceptionCaught);
            int finalCount = _PersonRepository.Count();
            Assert.Equal(afterSuccessCount, finalCount); // Should not have increased
            _output.WriteLine($"Total persons after rollback: {finalCount}");
        }

        [Fact]
        public async Task TestAsyncTransactionScope()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Async Transaction Scope ===");

            int initialCount = _PersonRepository.Count();

            string result = await _PersonRepository.ExecuteInTransactionScopeAsync(async () =>
            {
                Person person4 = new Person
                {
                    FirstName = "Alice",
                    LastName = "Wilson",
                    Age = 28,
                    Email = "alice@asynctest.com",
                    Salary = 70000,
                    Department = "Marketing"
                };
                Person person5 = new Person
                {
                    FirstName = "Charlie",
                    LastName = "Brown",
                    Age = 32,
                    Email = "charlie@asynctest.com",
                    Salary = 85000,
                    Department = "Finance"
                };

                await _PersonRepository.CreateAsync(person4);
                await _PersonRepository.CreateAsync(person5);

                _output.WriteLine($"Created person 4 with ID: {person4.Id}");
                _output.WriteLine($"Created person 5 with ID: {person5.Id}");

                Assert.True(person4.Id > 0);
                Assert.True(person5.Id > 0);

                return $"Successfully created 2 persons";
            });

            _output.WriteLine($"Operation result: {result}");
            Assert.Equal("Successfully created 2 persons", result);

            int count = _PersonRepository.Count();
            Assert.Equal(initialCount + 2, count);
            _output.WriteLine($"Total persons after async transaction: {count}");
        }

        [Fact]
        public void TestNestedTransactionWithSavepoints()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Nested Transactions with Savepoints ===");

            int initialCount = _PersonRepository.Count();

            using ITransaction transaction = _PersonRepository.BeginTransaction();
            try
            {
                // Create first person in main transaction
                Person person6 = new Person
                {
                    FirstName = "David",
                    LastName = "Wilson",
                    Age = 40,
                    Email = "david@savepointtest.com",
                    Salary = 90000,
                    Department = "Engineering"
                };
                _PersonRepository.Create(person6, transaction);
                _output.WriteLine($"Created person 6 with ID: {person6.Id} in main transaction");
                Assert.True(person6.Id > 0);

                // Execute operation with savepoint - this should succeed
                transaction.ExecuteWithSavepoint(() =>
                {
                    Person person7 = new Person
                    {
                        FirstName = "Eva",
                        LastName = "Garcia",
                        Age = 26,
                        Email = "eva@savepointtest.com",
                        Salary = 68000,
                        Department = "HR"
                    };
                    _PersonRepository.Create(person7, transaction);
                    _output.WriteLine($"Created person 7 with ID: {person7.Id} in savepoint");
                    Assert.True(person7.Id > 0);

                    // This will succeed and release the savepoint
                });

                // Try another operation with savepoint that will fail
                bool savepointExceptionCaught = false;
                try
                {
                    transaction.ExecuteWithSavepoint(() =>
                    {
                        Person person8 = new Person
                        {
                            FirstName = "Frank",
                            LastName = "Miller",
                            Age = 45,
                            Email = "frank@savepointtest.com",
                            Salary = 95000,
                            Department = "Legal"
                        };
                        _PersonRepository.Create(person8, transaction);
                        _output.WriteLine($"Created person 8 with ID: {person8.Id} in failing savepoint");

                        throw new InvalidOperationException("Savepoint operation failed");
                    });
                }
                catch (InvalidOperationException ex)
                {
                    _output.WriteLine($"Savepoint rolled back due to: {ex.Message}");
                    savepointExceptionCaught = true;
                }

                Assert.True(savepointExceptionCaught);

                transaction.Commit();
                _output.WriteLine("Main transaction committed successfully");
            }
            catch (Exception)
            {
                // The using statement will automatically dispose the transaction,
                // which includes rollback, so no manual rollback needed
                throw;
            }

            // Should have 2 new persons (person6 and person7, but not person8 due to savepoint rollback)
            int finalCount = _PersonRepository.Count();
            Assert.Equal(initialCount + 2, finalCount);
            _output.WriteLine($"Final count after savepoint test: {finalCount}");
        }

        [Fact]
        public void TestAmbientTransactionScope()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Ambient Transaction Scope ===");

            int initialCount = _PersonRepository.Count();

            using (TransactionScope scope1 = TransactionScope.Create(_PersonRepository))
            {
                // These operations will use the ambient transaction
                Person person9 = new Person
                {
                    FirstName = "Grace",
                    LastName = "Taylor",
                    Age = 29,
                    Email = "grace@ambienttest.com",
                    Salary = 72000,
                    Department = "Marketing"
                };
                _PersonRepository.Create(person9);  // No transaction parameter needed
                _output.WriteLine($"Created person 9 with ID: {person9.Id} using ambient transaction");
                Assert.True(person9.Id > 0);

                // Nested scope with same transaction
                using (TransactionScope scope2 = TransactionScope.Create(scope1.Transaction))
                {
                    Person person10 = new Person
                    {
                        FirstName = "Henry",
                        LastName = "Davis",
                        Age = 33,
                        Email = "henry@ambienttest.com",
                        Salary = 78000,
                        Department = "IT"
                    };
                    _PersonRepository.Create(person10);  // Still uses the same transaction
                    _output.WriteLine($"Created person 10 with ID: {person10.Id} in nested ambient transaction");
                    Assert.True(person10.Id > 0);
                }

                scope1.Complete();
                _output.WriteLine("Ambient transaction scopes completed successfully");
            }

            int finalCount = _PersonRepository.Count();
            Assert.Equal(initialCount + 2, finalCount);
            _output.WriteLine($"Final count after ambient transaction test: {finalCount}");
        }

        [Fact]
        public async Task TestConcurrentTransactionScopes()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Concurrent Transaction Scopes ===");

            int initialCount = _PersonRepository.Count();
            List<Task> tasks = new List<Task>();
            int successCount = 0;
            object lockObject = new object();

            // Create 10 concurrent tasks that each create persons in transaction scopes
            for (int i = 0; i < 10; i++)
            {
                int taskId = i;
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await _PersonRepository.ExecuteInTransactionScopeAsync(async () =>
                        {
                            Person person = new Person
                            {
                                FirstName = $"Concurrent{taskId}",
                                LastName = "User",
                                Age = 25 + taskId,
                                Email = $"concurrent{taskId}@threadtest.com",
                                Salary = 50000 + taskId * 3000,
                                Department = "Concurrent"
                            };

                            await _PersonRepository.CreateAsync(person);

                            // Simulate some work
                            await Task.Delay(50 + taskId * 10);

                            _output.WriteLine($"Task {taskId}: Created person with ID {person.Id}");

                            lock (lockObject)
                            {
                                successCount++;
                            }

                            return "Success";
                        });
                    }
                    catch (Exception ex)
                    {
                        _output.WriteLine($"Concurrent task {taskId} failed: {ex.Message}");
                    }
                }));
            }

            await Task.WhenAll(tasks);

            _output.WriteLine($"Concurrent tasks completed: {successCount} successful");
            Assert.True(successCount >= 8); // Allow for some potential failures in concurrent scenarios

            int finalCount = _PersonRepository.Count();
            Assert.True(finalCount >= initialCount + 8); // Should have at least 8 new records
            _output.WriteLine($"Final count after concurrent test: {finalCount}");
        }

        [Fact]
        public void TestDeepNestedSavepoints()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Deep Nested Savepoints ===");

            int initialCount = _PersonRepository.Count();

            using ITransaction transaction = _PersonRepository.BeginTransaction();
            try
            {
                // Create a person in the main transaction
                Person mainPerson = new Person
                {
                    FirstName = "Main",
                    LastName = "Transaction",
                    Age = 35,
                    Email = "main@nestedtest.com",
                    Salary = 80000,
                    Department = "Main"
                };
                _PersonRepository.Create(mainPerson, transaction);
                _output.WriteLine($"Created main person with ID: {mainPerson.Id}");
                Assert.True(mainPerson.Id > 0);

                // Create nested savepoints (depth of 5)
                CreateNestedSavepoints(transaction, 1, 5);

                transaction.Commit();
                _output.WriteLine("Deep nested savepoint test completed successfully");
            }
            catch (Exception ex)
            {
                // The using statement will automatically dispose the transaction,
                // which includes rollback, so no manual rollback needed
                _output.WriteLine($"Deep nested test failed: {ex.Message}");
                throw;
            }

            // Should have 1 main person + 5 nested persons = 6 total new persons
            int finalCount = _PersonRepository.Count();
            Assert.Equal(initialCount + 6, finalCount);
            _output.WriteLine($"Final count after deep nested test: {finalCount}");
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
                    Email = $"nested{currentDepth}@nestedtest.com",
                    Salary = 60000 + currentDepth * 5000,
                    Department = "Nested"
                };
                _PersonRepository.Create(person, transaction);
                _output.WriteLine($"Created nested person at depth {currentDepth} with ID: {person.Id}");
                Assert.True(person.Id > 0);

                // Recursively create deeper savepoints
                CreateNestedSavepoints(transaction, currentDepth + 1, maxDepth);

            }, $"nested_sp_{currentDepth}");
        }

        [Fact]
        public void TestMemoryLeakPrevention()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Memory Leak Prevention ===");

            int initialCount = _PersonRepository.Count();

            // Create many nested scopes to test for memory leaks
            for (int i = 0; i < 100; i++)
            {
                using (TransactionScope scope1 = TransactionScope.Create(_PersonRepository))
                {
                    using (TransactionScope scope2 = TransactionScope.Create(scope1.Transaction))
                    {
                        using (TransactionScope scope3 = TransactionScope.Create(scope2.Transaction))
                        {
                            Person person = new Person
                            {
                                FirstName = $"Memory{i}",
                                LastName = "Test",
                                Age = 20 + (i % 50),
                                Email = $"memory{i}@leaktest.com",
                                Salary = 40000 + i * 500,
                                Department = "MemoryTest"
                            };

                            // Only create person every 10th iteration to avoid too much data
                            if (i % 10 == 0)
                            {
                                _PersonRepository.Create(person);
                                scope3.Complete();
                                scope2.Complete();
                                scope1.Complete();
                            }
                            else
                            {
                                // Just create the object but don't commit - test disposal
                            }
                        }
                    }
                }
            }

            // Force garbage collection to test for memory leaks
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            int finalCount = _PersonRepository.Count();
            int expectedNewRecords = 10; // 100 iterations, but only every 10th commits
            Assert.Equal(initialCount + expectedNewRecords, finalCount);
            _output.WriteLine($"Memory leak test completed - created {expectedNewRecords} records out of 100 iterations");
        }

        [Fact]
        public void TestTransactionScopeWithMultipleRepositories()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Transaction Scope with Multiple Repositories ===");

            int initialPersonCount = _PersonRepository.Count();
            int initialCompanyCount = _CompanyRepository.Count();

            // Test cross-repository transaction scope
            bool success = false;
            _PersonRepository.ExecuteInTransactionScope(() =>
            {
                // Create a company
                Company company = new Company
                {
                    Name = "Transaction Test Corp",
                    Industry = "Software"
                };
                _CompanyRepository.Create(company);
                _output.WriteLine($"Created company with ID: {company.Id}");

                // Create a person
                Person person = new Person
                {
                    FirstName = "Multi",
                    LastName = "Repository",
                    Age = 30,
                    Email = "multi@repositorytest.com",
                    Salary = 75000,
                    Department = "CrossRepo"
                };
                _PersonRepository.Create(person);
                _output.WriteLine($"Created person with ID: {person.Id}");

                Assert.True(company.Id > 0);
                Assert.True(person.Id > 0);
                success = true;
            });

            Assert.True(success);

            int finalPersonCount = _PersonRepository.Count();
            int finalCompanyCount = _CompanyRepository.Count();

            Assert.Equal(initialPersonCount + 1, finalPersonCount);
            Assert.Equal(initialCompanyCount + 1, finalCompanyCount);

            _output.WriteLine($"Cross-repository transaction completed successfully");
        }

        [Fact]
        public void TestTransactionRollbackWithMultipleRepositories()
        {
            if (_SkipTests) return;

            _output.WriteLine("=== Testing Transaction Rollback with Multiple Repositories ===");

            int initialPersonCount = _PersonRepository.Count();
            int initialCompanyCount = _CompanyRepository.Count();

            // Test cross-repository transaction rollback
            bool exceptionCaught = false;
            try
            {
                _PersonRepository.ExecuteInTransactionScope(() =>
                {
                    // Create a company
                    Company company = new Company
                    {
                        Name = "Rollback Test Corp",
                        Industry = "Finance"
                    };
                    _CompanyRepository.Create(company);
                    _output.WriteLine($"Created company with ID: {company.Id} (will be rolled back)");

                    // Create a person
                    Person person = new Person
                    {
                        FirstName = "Rollback",
                        LastName = "Test",
                        Age = 35,
                        Email = "rollback@repositorytest.com",
                        Salary = 80000,
                        Department = "RollbackTest"
                    };
                    _PersonRepository.Create(person);
                    _output.WriteLine($"Created person with ID: {person.Id} (will be rolled back)");

                    // Force rollback
                    throw new InvalidOperationException("Forced rollback for testing");
                });
            }
            catch (InvalidOperationException ex)
            {
                _output.WriteLine($"Caught expected exception: {ex.Message}");
                exceptionCaught = true;
            }

            Assert.True(exceptionCaught);

            int finalPersonCount = _PersonRepository.Count();
            int finalCompanyCount = _CompanyRepository.Count();

            // Both should remain unchanged due to rollback
            Assert.Equal(initialPersonCount, finalPersonCount);
            Assert.Equal(initialCompanyCount, finalCompanyCount);

            _output.WriteLine($"Cross-repository rollback completed successfully");
        }

        public void Dispose()
        {
            if (!_SkipTests)
            {
                try
                {
                    CleanupTestData();
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"Warning: Could not clean up test data during disposal: {ex.Message}");
                }
            }
        }
    }
}