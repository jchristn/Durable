using System;
using Durable.Sqlite;
using Test.Shared;
using Microsoft.Data.Sqlite;

namespace SimpleDemo
{
    class ParameterCaptureTest
    {
        public static void TestParameterCapture()
        {
            Console.WriteLine("\n=== Parameter Value Capture Test ===");

            // Use SQLite file database
            string connectionString = "Data Source=sql_capture_test.db";

            // Create repository
            var repository = new SqliteRepository<Person>(connectionString);
            repository.CaptureSql = true;

            // Test parameterized queries
            Console.WriteLine("\n--- Testing Parameter Value Capture ---");

            // Test 1: ReadById with parameter
            var person = repository.ReadById(1);
            Console.WriteLine($"ReadById SQL: {repository.LastExecutedSql}");
            Console.WriteLine($"ReadById SQL with parameters: {repository.LastExecutedSqlWithParameters}");

            // Test 2: Create with parameters
            var newPerson = new Person
            {
                FirstName = "Test'Quote",
                LastName = "O'Connor",
                Age = 25,
                Email = "test@example.com",
                Salary = 50000,
                Department = "Test Dept"
            };

            var created = repository.Create(newPerson);
            Console.WriteLine($"\nCreate SQL: {repository.LastExecutedSql}");
            Console.WriteLine($"Create SQL with parameters: {repository.LastExecutedSqlWithParameters}");

            // Test 3: Update with parameters
            created.Salary = 55000;
            created.Department = "Updated Dept";
            repository.Update(created);
            Console.WriteLine($"\nUpdate SQL: {repository.LastExecutedSql}");
            Console.WriteLine($"Update SQL with parameters: {repository.LastExecutedSqlWithParameters}");

            // Test 4: Exists with parameters
            bool exists = repository.ExistsById(created.Id);
            Console.WriteLine($"\nExistsById SQL: {repository.LastExecutedSql}");
            Console.WriteLine($"ExistsById SQL with parameters: {repository.LastExecutedSqlWithParameters}");

            // Test 5: Raw SQL with parameters
            var results = repository.FromSql("SELECT * FROM people WHERE age > @p0 AND department = @p1", null, 20, "Test Dept");
            Console.WriteLine($"\nFromSql SQL: {repository.LastExecutedSql}");
            Console.WriteLine($"FromSql SQL with parameters: {repository.LastExecutedSqlWithParameters}");

            Console.WriteLine("\n--- Parameter Capture Test Complete ---");
        }
    }
}