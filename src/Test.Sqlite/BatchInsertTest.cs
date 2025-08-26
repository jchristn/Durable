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
            
            string connectionString = $"Data Source=BatchTest{configName.Replace(" ", "")};Mode=Memory;Cache=Shared";
            
            // Keep one connection open to maintain the in-memory database
            using SqliteConnection keepAliveConnection = new SqliteConnection(connectionString);
            keepAliveConnection.Open();
            
            using SqliteRepository<Person> repository = new SqliteRepository<Person>(connectionString, config);
            
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
            List<Person> smallBatch = GenerateTestPeople(50);
            Stopwatch sw = Stopwatch.StartNew();
            IEnumerable<Person> createdSmall = await repository.CreateManyAsync(smallBatch);
            sw.Stop();
            
            int countSmall = await repository.CountAsync();
            Console.WriteLine($"Small batch (50): {sw.ElapsedMilliseconds}ms, Count: {countSmall}");
            
            // Test medium batch
            await repository.DeleteAllAsync();
            List<Person> mediumBatch = GenerateTestPeople(500);
            sw.Restart();
            IEnumerable<Person> createdMedium = await repository.CreateManyAsync(mediumBatch);
            sw.Stop();
            
            int countMedium = await repository.CountAsync();
            Console.WriteLine($"Medium batch (500): {sw.ElapsedMilliseconds}ms, Count: {countMedium}");
            
            // Test large batch
            await repository.DeleteAllAsync();
            List<Person> largeBatch = GenerateTestPeople(2000);
            sw.Restart();
            IEnumerable<Person> createdLarge = await repository.CreateManyAsync(largeBatch);
            sw.Stop();
            
            int countLarge = await repository.CountAsync();
            Console.WriteLine($"Large batch (2000): {sw.ElapsedMilliseconds}ms, Count: {countLarge}");
            
            // Verify data integrity
            Person firstPerson = createdLarge.First();
            Person retrievedPerson = await repository.ReadByIdAsync(firstPerson.Id);
            bool dataIntegrityOk = retrievedPerson != null && 
                                 retrievedPerson.FirstName == firstPerson.FirstName &&
                                 retrievedPerson.LastName == firstPerson.LastName;
            Console.WriteLine($"Data integrity check: {(dataIntegrityOk ? "PASS" : "FAIL")}");
            
            // Test with transaction
            await repository.DeleteAllAsync();
            using ITransaction transaction = await repository.BeginTransactionAsync();
            try
            {
                List<Person> transactionBatch = GenerateTestPeople(100);
                sw.Restart();
                await repository.CreateManyAsync(transactionBatch, transaction);
                await transaction.CommitAsync();
                sw.Stop();
                
                int transactionCount = await repository.CountAsync();
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
            using SqliteConnection optimizedKeepAlive = new SqliteConnection(optimizedConnectionString);
            optimizedKeepAlive.Open();
            
            using SqliteRepository<Person> optimizedRepo = new SqliteRepository<Person>(optimizedConnectionString, BatchInsertConfiguration.LargeBatch);
            await SetupDatabase(optimizedRepo);
            
            List<Person> testData = GenerateTestPeople(testSize);
            Stopwatch sw = Stopwatch.StartNew();
            await optimizedRepo.CreateManyAsync(testData);
            sw.Stop();
            long optimizedTime = sw.ElapsedMilliseconds;
            
            // Test compatible (non-optimized) version
            const string compatibleConnectionString = "Data Source=CompatiblePerfTest;Mode=Memory;Cache=Shared";
            using SqliteConnection compatibleKeepAlive = new SqliteConnection(compatibleConnectionString);
            compatibleKeepAlive.Open();
            
            using SqliteRepository<Person> compatibleRepo = new SqliteRepository<Person>(compatibleConnectionString, BatchInsertConfiguration.Compatible);
            await SetupDatabase(compatibleRepo);
            
            testData = GenerateTestPeople(testSize); // Fresh data
            sw.Restart();
            await compatibleRepo.CreateManyAsync(testData);
            sw.Stop();
            long compatibleTime = sw.ElapsedMilliseconds;
            
            Console.WriteLine($"Optimized (multi-row): {optimizedTime}ms");
            Console.WriteLine($"Compatible (individual): {compatibleTime}ms");
            
            if (compatibleTime > 0)
            {
                double improvement = ((double)(compatibleTime - optimizedTime) / compatibleTime) * 100;
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
            string[] departments = new[] { "IT", "HR", "Finance", "Sales", "Marketing", "Operations" };
            string[] firstNames = new[] { "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank" };
            string[] lastNames = new[] { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis" };
            Random random = new Random(42); // Fixed seed for reproducible tests
            
            List<Person> people = new List<Person>(count);
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