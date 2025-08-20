using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Durable;
using Durable.Sqlite;
using Microsoft.Data.Sqlite;
using Test.Shared;

namespace Test.Sqlite
{
    public static class BatchInsertTest
    {
        public static async Task RunBatchInsertTests()
        {
            Console.WriteLine("\n=== BATCH INSERT OPTIMIZATION TESTS ===");
            
            // Test with different batch configurations
            await TestConfiguration("Default Configuration", BatchInsertConfiguration.Default);
            await TestConfiguration("Small Batch Configuration", BatchInsertConfiguration.SmallBatch);
            await TestConfiguration("Large Batch Configuration", BatchInsertConfiguration.LargeBatch);
            await TestConfiguration("Compatible (No Optimization)", BatchInsertConfiguration.Compatible);
            
            // Performance comparison
            await PerformanceComparison();
        }
        
        private static async Task TestConfiguration(string configName, IBatchInsertConfiguration config)
        {
            Console.WriteLine($"\n--- Testing {configName} ---");
            Console.WriteLine($"MaxRowsPerBatch: {config.MaxRowsPerBatch}");
            Console.WriteLine($"EnableMultiRowInsert: {config.EnableMultiRowInsert}");
            Console.WriteLine($"EnablePreparedStatementReuse: {config.EnablePreparedStatementReuse}");
            
            var connectionString = $"Data Source=BatchTest{configName.Replace(" ", "")};Mode=Memory;Cache=Shared";
            
            // Keep one connection open to maintain the in-memory database
            using var keepAliveConnection = new SqliteConnection(connectionString);
            keepAliveConnection.Open();
            
            using var repository = new SqliteRepository<Person>(connectionString, config);
            
            // Create table
            await repository.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first TEXT NOT NULL,
                    last TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT NOT NULL,
                    salary REAL NOT NULL,
                    department TEXT NOT NULL
                );");
            
            // Test small batch
            var smallBatch = GenerateTestPeople(50);
            var sw = Stopwatch.StartNew();
            var createdSmall = await repository.CreateManyAsync(smallBatch);
            sw.Stop();
            
            var countSmall = await repository.CountAsync();
            Console.WriteLine($"Small batch (50): {sw.ElapsedMilliseconds}ms, Count: {countSmall}");
            
            // Test medium batch
            await repository.DeleteAllAsync();
            var mediumBatch = GenerateTestPeople(500);
            sw.Restart();
            var createdMedium = await repository.CreateManyAsync(mediumBatch);
            sw.Stop();
            
            var countMedium = await repository.CountAsync();
            Console.WriteLine($"Medium batch (500): {sw.ElapsedMilliseconds}ms, Count: {countMedium}");
            
            // Test large batch
            await repository.DeleteAllAsync();
            var largeBatch = GenerateTestPeople(2000);
            sw.Restart();
            var createdLarge = await repository.CreateManyAsync(largeBatch);
            sw.Stop();
            
            var countLarge = await repository.CountAsync();
            Console.WriteLine($"Large batch (2000): {sw.ElapsedMilliseconds}ms, Count: {countLarge}");
            
            // Verify data integrity
            var firstPerson = createdLarge.First();
            var retrievedPerson = await repository.ReadByIdAsync(firstPerson.Id);
            var dataIntegrityOk = retrievedPerson != null && 
                                 retrievedPerson.FirstName == firstPerson.FirstName &&
                                 retrievedPerson.LastName == firstPerson.LastName;
            Console.WriteLine($"Data integrity check: {(dataIntegrityOk ? "PASS" : "FAIL")}");
            
            // Test with transaction
            await repository.DeleteAllAsync();
            using var transaction = await repository.BeginTransactionAsync();
            try
            {
                var transactionBatch = GenerateTestPeople(100);
                sw.Restart();
                await repository.CreateManyAsync(transactionBatch, transaction);
                await transaction.CommitAsync();
                sw.Stop();
                
                var transactionCount = await repository.CountAsync();
                Console.WriteLine($"Transaction batch (100): {sw.ElapsedMilliseconds}ms, Count: {transactionCount}");
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
        
        private static async Task PerformanceComparison()
        {
            Console.WriteLine("\n--- Performance Comparison ---");
            const int testSize = 1000;
            
            // Test optimized version
            const string optimizedConnectionString = "Data Source=OptimizedPerfTest;Mode=Memory;Cache=Shared";
            using var optimizedKeepAlive = new SqliteConnection(optimizedConnectionString);
            optimizedKeepAlive.Open();
            
            using var optimizedRepo = new SqliteRepository<Person>(optimizedConnectionString, BatchInsertConfiguration.LargeBatch);
            await SetupDatabase(optimizedRepo);
            
            var testData = GenerateTestPeople(testSize);
            var sw = Stopwatch.StartNew();
            await optimizedRepo.CreateManyAsync(testData);
            sw.Stop();
            var optimizedTime = sw.ElapsedMilliseconds;
            
            // Test compatible (non-optimized) version
            const string compatibleConnectionString = "Data Source=CompatiblePerfTest;Mode=Memory;Cache=Shared";
            using var compatibleKeepAlive = new SqliteConnection(compatibleConnectionString);
            compatibleKeepAlive.Open();
            
            using var compatibleRepo = new SqliteRepository<Person>(compatibleConnectionString, BatchInsertConfiguration.Compatible);
            await SetupDatabase(compatibleRepo);
            
            testData = GenerateTestPeople(testSize); // Fresh data
            sw.Restart();
            await compatibleRepo.CreateManyAsync(testData);
            sw.Stop();
            var compatibleTime = sw.ElapsedMilliseconds;
            
            Console.WriteLine($"Optimized (multi-row): {optimizedTime}ms");
            Console.WriteLine($"Compatible (individual): {compatibleTime}ms");
            
            if (compatibleTime > 0)
            {
                var improvement = ((double)(compatibleTime - optimizedTime) / compatibleTime) * 100;
                Console.WriteLine($"Performance improvement: {improvement:F1}%");
            }
        }
        
        private static async Task SetupDatabase(SqliteRepository<Person> repository)
        {
            await repository.ExecuteSqlAsync(@"
                DROP TABLE IF EXISTS people;
                CREATE TABLE people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first TEXT NOT NULL,
                    last TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT NOT NULL,
                    salary REAL NOT NULL,
                    department TEXT NOT NULL
                );");
        }
        
        private static List<Person> GenerateTestPeople(int count)
        {
            var departments = new[] { "IT", "HR", "Finance", "Sales", "Marketing", "Operations" };
            var firstNames = new[] { "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank" };
            var lastNames = new[] { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis" };
            var random = new Random(42); // Fixed seed for reproducible tests
            
            var people = new List<Person>(count);
            for (int i = 0; i < count; i++)
            {
                people.Add(new Person
                {
                    FirstName = firstNames[random.Next(firstNames.Length)],
                    LastName = lastNames[random.Next(lastNames.Length)],
                    Age = random.Next(18, 65),
                    Email = $"person{i}@test.com",
                    Salary = (decimal)(random.NextDouble() * 80000 + 30000),
                    Department = departments[random.Next(departments.Length)]
                });
            }
            return people;
        }
    }
}