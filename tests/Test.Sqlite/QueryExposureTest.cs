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
    /// Tests query exposure functionality and SQL query generation.
    /// </summary>
    public static class QueryExposureTest
    {
        /// <summary>
        /// Runs tests for query exposure and SQL generation functionality.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static async Task RunQueryExposureTest()
        {
            Console.WriteLine("=== QUERY EXPOSURE TEST ===");
            
            const string connectionString = "Data Source=QueryTestDB;Mode=Memory;Cache=Shared";
            
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
            
            Console.WriteLine("Inserting test data...");
            Person[] testPeople = new[]
            {
                new Person { FirstName = "John", LastName = "Doe", Age = 25, Email = "john@test.com", Salary = 60000, Department = "IT" },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane@test.com", Salary = 65000, Department = "HR" },
                new Person { FirstName = "Bob", LastName = "Johnson", Age = 30, Email = "bob@test.com", Salary = 70000, Department = "IT" }
            };
            
            await repository.CreateManyAsync(testPeople);
            Console.WriteLine($"Inserted {testPeople.Length} test records");
            
            Console.WriteLine("\n=== Testing Query Exposure Methods ===");
            
            // Test 1: Using DurableResult approach with ExecuteWithQuery
            Console.WriteLine("\n1. Testing ExecuteWithQuery (DurableResult approach):");
            IQueryBuilder<Person> queryBuilder = repository.Query().Where(p => p.Age > 25);
            IDurableResult<Person> durableResult = queryBuilder.ExecuteWithQuery();
            
            Console.WriteLine($"Query: {durableResult.Query}");
            Console.WriteLine($"Results: {durableResult.Result.Count()} records");
            foreach (Person person in durableResult.Result)
            {
                Console.WriteLine($"  - {person.FirstName} {person.LastName}, Age: {person.Age}");
            }
            
            // Test 2: Using Query property directly
            Console.WriteLine("\n2. Testing Query property:");
            IQueryBuilder<Person> queryWithOrderBy = repository.Query()
                .Where(p => p.Department == "IT")
                .OrderBy(p => p.Age)
                .Take(10);
            
            Console.WriteLine($"Query: {queryWithOrderBy.Query}");
            IEnumerable<Person> results = queryWithOrderBy.Execute();
            Console.WriteLine($"Results: {results.Count()} records");
            
            // Test 3: Using extension method
            Console.WriteLine("\n3. Testing extension method SelectWithQuery:");
            IDurableResult<Person> extensionResult = repository.SelectWithQuery(p => p.Salary > 60000);
            Console.WriteLine($"Query: {extensionResult.Query}");
            Console.WriteLine($"Results: {extensionResult.Result.Count()} records");
            
            // Test 4: Using extension method to get query only
            Console.WriteLine("\n4. Testing extension method GetSelectQuery:");
            string queryOnly = repository.GetSelectQuery(p => p.FirstName.Contains("J"));
            Console.WriteLine($"Query only: {queryOnly}");
            
            // Test 5: Async version
            Console.WriteLine("\n5. Testing async version:");
            IDurableResult<Person> asyncResult = await repository.SelectWithQueryAsync(p => p.Age < 30);
            Console.WriteLine($"Async Query: {asyncResult.Query}");
            Console.WriteLine($"Async Results: {asyncResult.Result.Count()} records");
            
            Console.WriteLine("\n=== Query Exposure Test Completed Successfully ===");
        }
    }
}