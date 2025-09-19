namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;
    
    /// <summary>
    /// Provides simple batch insert and basic functionality tests.
    /// </summary>
    public static class SimpleTest
    {
        /// <summary>
        /// Runs simple batch insert functionality tests.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static async Task RunSimpleTest()
        {
            Console.WriteLine("=== SIMPLE BATCH INSERT TEST ===");
            
            const string connectionString = "Data Source=TestDB;Mode=Memory;Cache=Shared";
            
            // Keep one connection open to maintain the in-memory database
            using SqliteConnection keepAliveConnection = new SqliteConnection(connectionString);
            keepAliveConnection.Open();
            
            using SqliteRepository<Person> repository = new SqliteRepository<Person>(connectionString, BatchInsertConfiguration.Default);
            
            Console.WriteLine("Creating table...");
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
            Console.WriteLine("Table created successfully");
            
            Console.WriteLine("Inserting test data...");
            Person testPerson = new Person
            {
                FirstName = "Test",
                LastName = "User",
                Age = 30,
                Email = "test@example.com",
                Salary = 50000,
                Department = "IT"
            };
            
            Person created = await repository.CreateAsync(testPerson);
            Console.WriteLine($"Single insert successful: {created}");
            
            Console.WriteLine("Testing batch insert...");
            Person[] people = new[]
            {
                new Person { FirstName = "John", LastName = "Doe", Age = 25, Email = "john@test.com", Salary = 60000, Department = "IT" },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane@test.com", Salary = 65000, Department = "HR" }
            };
            
            IEnumerable<Person> batchCreated = await repository.CreateManyAsync(people);
            List<Person> batchCreatedList = batchCreated.ToList();
            Console.WriteLine($"Batch insert successful: {batchCreatedList.Count} records");
            
            int count = await repository.CountAsync();
            Console.WriteLine($"Total records: {count}");
        }
    }
}