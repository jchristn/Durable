using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Durable.Sqlite;
using Microsoft.Data.Sqlite;
using Test.Shared;

namespace Test.Sqlite
{
    public static class SimpleGroupByTest
    {
        public static async Task RunSimpleGroupByTest()
        {
            Console.WriteLine("=== SIMPLE GROUP BY TEST ===");
            
            const string connectionString = "Data Source=SimpleGroupByTest;Mode=Memory;Cache=Shared";
            
            // Keep one connection open to maintain the in-memory database
            using SqliteConnection keepAliveConnection = new SqliteConnection(connectionString);
            keepAliveConnection.Open();
            
            try
            {
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
                
                // Insert test data
                Console.WriteLine("Inserting test data...");
                List<Person> testPeople = new List<Person>
                {
                    new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john@example.com", Salary = 50000, Department = "IT" },
                    new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane@example.com", Salary = 55000, Department = "IT" },
                    new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Email = "bob@example.com", Salary = 60000, Department = "HR" },
                    new Person { FirstName = "Alice", LastName = "Williams", Age = 32, Email = "alice@example.com", Salary = 58000, Department = "HR" },
                    new Person { FirstName = "Charlie", LastName = "Brown", Age = 27, Email = "charlie@example.com", Salary = 48000, Department = "Sales" }
                };
                
                foreach (Person person in testPeople)
                {
                    await repository.CreateAsync(person);
                }
                Console.WriteLine($"Inserted {testPeople.Count} test records");
                
                // Test 1: Simple GROUP BY with basic execution
                Console.WriteLine("\nTest 1: Simple Group by Department");
                Console.WriteLine("-----------------------------------");
                try
                {
                    IEnumerable<IGrouping<string, Person>> groups = await repository.Query()
                        .GroupBy(p => p.Department)
                        .ExecuteAsync();
                    
                    Console.WriteLine($"Successfully created {groups.Count()} groups");
                    foreach (IGrouping<string, Person> group in groups)
                    {
                        Console.WriteLine($"Department: {group.Key}, Count: {group.Count()}");
                        foreach (Person person in group)
                        {
                            Console.WriteLine($"  - {person.FirstName} {person.LastName}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR in Test 1: {ex.Message}");
                    Console.WriteLine($"Stack trace: {ex.StackTrace}");
                }
                
                // Test 2: GROUP BY with WHERE clause
                Console.WriteLine("\nTest 2: Group by Department with WHERE clause");
                Console.WriteLine("----------------------------------------------");
                try
                {
                    IEnumerable<IGrouping<string, Person>> filteredGroups = await repository.Query()
                        .Where(p => p.Salary > 50000)
                        .GroupBy(p => p.Department)
                        .ExecuteAsync();
                    
                    Console.WriteLine($"Successfully created {filteredGroups.Count()} filtered groups");
                    foreach (IGrouping<string, Person> group in filteredGroups)
                    {
                        Console.WriteLine($"Department: {group.Key}, Count: {group.Count()}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR in Test 2: {ex.Message}");
                    Console.WriteLine($"Stack trace: {ex.StackTrace}");
                }
                
                // Test 3: Aggregate functions
                Console.WriteLine("\nTest 3: Aggregate Functions");
                Console.WriteLine("----------------------------");
                try
                {
                    int totalCount = await repository.Query()
                        .GroupBy(p => p.Department)
                        .CountAsync();
                    Console.WriteLine($"Total count: {totalCount}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR in Test 3: {ex.Message}");
                    Console.WriteLine($"Stack trace: {ex.StackTrace}");
                }
                
                Console.WriteLine("\n=== SIMPLE GROUP BY TEST COMPLETED ===");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"FATAL ERROR: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }
    }
}