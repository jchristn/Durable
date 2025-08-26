using System;
using System.Threading.Tasks;
using Durable;
using Durable.Sqlite;
using Test.Shared;

namespace TransactionScopeDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== Transaction Scope Demo ===\n");

            // Setup file-based database for testing
            string connectionString = "Data Source=transaction_demo.db";
            
            // Delete existing database if it exists
            if (System.IO.File.Exists("transaction_demo.db"))
            {
                System.IO.File.Delete("transaction_demo.db");
            }
            
            var repository = new SqliteRepository<Person>(connectionString);

            // Create table
            try
            {
                repository.ExecuteSql(@"
                    CREATE TABLE people (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        first VARCHAR(64) NOT NULL,
                        last VARCHAR(64) NOT NULL,
                        age INTEGER NOT NULL,
                        email VARCHAR(128) NOT NULL,
                        salary DECIMAL(10,2) NOT NULL,
                        department VARCHAR(32) NOT NULL
                    );
                ");
                Console.WriteLine("Database and table created successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Table creation error: {ex.Message}");
            }

            try
            {
                // Test 1: Basic Transaction Scope
                Console.WriteLine("=== Test 1: Basic Transaction Scope ===");
                repository.ExecuteInTransactionScope(() =>
                {
                    var person1 = new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john@test.com", Salary = 50000, Department = "IT" };
                    var person2 = new Person { FirstName = "Jane", LastName = "Smith", Age = 25, Email = "jane@test.com", Salary = 45000, Department = "HR" };
                    
                    repository.Create(person1);  // Uses ambient transaction
                    repository.Create(person2);  // Uses ambient transaction
                    
                    Console.WriteLine($"Created person 1 with ID: {person1.Id}");
                    Console.WriteLine($"Created person 2 with ID: {person2.Id}");
                });
                
                Console.WriteLine($"Total persons after successful transaction: {repository.Count()}");

                // Test 2: Transaction Rollback
                Console.WriteLine("\n=== Test 2: Transaction Rollback ===");
                try
                {
                    repository.ExecuteInTransactionScope(() =>
                    {
                        var person3 = new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Email = "bob@test.com", Salary = 55000, Department = "Sales" };
                        repository.Create(person3);
                        Console.WriteLine($"Created person 3 with ID: {person3.Id}");
                        
                        // Force an exception to trigger rollback
                        throw new InvalidOperationException("Simulated error");
                    });
                }
                catch (InvalidOperationException ex)
                {
                    Console.WriteLine($"Caught expected exception: {ex.Message}");
                }
                
                Console.WriteLine($"Total persons after rollback: {repository.Count()}");

                // Give SQLite a moment to release locks
                await Task.Delay(100);

                // Test 3: Async Transaction Scope
                Console.WriteLine("\n=== Test 3: Async Transaction Scope ===");
                var result = await repository.ExecuteInTransactionScopeAsync(async () =>
                {
                    var person4 = new Person { FirstName = "Alice", LastName = "Wilson", Age = 28, Email = "alice@test.com", Salary = 52000, Department = "Marketing" };
                    var person5 = new Person { FirstName = "Charlie", LastName = "Brown", Age = 32, Email = "charlie@test.com", Salary = 58000, Department = "Finance" };
                    
                    await repository.CreateAsync(person4);
                    await repository.CreateAsync(person5);
                    
                    Console.WriteLine($"Created person 4 with ID: {person4.Id}");
                    Console.WriteLine($"Created person 5 with ID: {person5.Id}");
                    
                    return "Successfully created 2 persons";
                });
                
                Console.WriteLine($"Operation result: {result}");
                Console.WriteLine($"Total persons after async transaction: {repository.Count()}");

                // Test 4: Nested Transaction with Savepoints
                Console.WriteLine("\n=== Test 4: Nested Transactions with Savepoints ===");
                using var transaction = repository.BeginTransaction();
                try
                {
                    // Create first person in main transaction
                    var person6 = new Person { FirstName = "David", LastName = "Lee", Age = 40, Email = "david@test.com", Salary = 65000, Department = "Engineering" };
                    repository.Create(person6, transaction);
                    Console.WriteLine($"Created person 6 with ID: {person6.Id} in main transaction");
                    
                    // Execute operation with savepoint
                    transaction.ExecuteWithSavepoint(() =>
                    {
                        var person7 = new Person { FirstName = "Eva", LastName = "Garcia", Age = 26, Email = "eva@test.com", Salary = 48000, Department = "Design" };
                        repository.Create(person7, transaction);
                        Console.WriteLine($"Created person 7 with ID: {person7.Id} in savepoint");
                    });
                    
                    // Try another operation with savepoint that will fail
                    try
                    {
                        transaction.ExecuteWithSavepoint(() =>
                        {
                            var person8 = new Person { FirstName = "Frank", LastName = "Miller", Age = 45, Email = "frank@test.com", Salary = 70000, Department = "Operations" };
                            repository.Create(person8, transaction);
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
                
                Console.WriteLine($"Total persons after nested transaction test: {repository.Count()}");

                // Test 5: Ambient Transaction Scope
                Console.WriteLine("\n=== Test 5: Ambient Transaction Scope ===");
                using var scope1 = TransactionScope.Create(repository);
                
                // These operations will use the ambient transaction
                var person9 = new Person { FirstName = "Grace", LastName = "Taylor", Age = 29, Email = "grace@test.com", Salary = 53000, Department = "Legal" };
                repository.Create(person9);  // No transaction parameter needed
                Console.WriteLine($"Created person 9 with ID: {person9.Id} using ambient transaction");
                
                // Nested scope with same transaction
                using (var scope2 = TransactionScope.Create(scope1.Transaction))
                {
                    var person10 = new Person { FirstName = "Henry", LastName = "Davis", Age = 33, Email = "henry@test.com", Salary = 60000, Department = "Support" };
                    repository.Create(person10);  // Still uses the same transaction
                    Console.WriteLine($"Created person 10 with ID: {person10.Id} using nested ambient transaction");
                    
                    scope2.Complete();
                }
                
                scope1.Complete();
                
                Console.WriteLine($"Total persons after ambient transaction test: {repository.Count()}");

                // Final Results
                Console.WriteLine("\n=== Final Results ===");
                var allPersons = repository.ReadAll();
                Console.WriteLine("Final state of database:");
                foreach (var person in allPersons)
                {
                    Console.WriteLine($"  ID: {person.Id}, Name: {person.FirstName} {person.LastName}, Age: {person.Age}");
                }
                
                Console.WriteLine("\n✅ All transaction scope tests completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Test failed with exception: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
            finally
            {
                repository?.Dispose();
            }
        }
    }
}