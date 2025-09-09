namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;
    
    public static class QuickGroupByTest
    {
        public static async Task RunQuickTest()
        {
            Console.WriteLine("=== QUICK GROUP BY TEST ===");
            
            const string connectionString = "Data Source=QuickTest;Mode=Memory;Cache=Shared";
            
            // Keep connection alive for in-memory database
            using SqliteConnection keepAlive = new SqliteConnection(connectionString);
            keepAlive.Open();
            
            try
            {
                using SqliteRepository<Person> repo = new SqliteRepository<Person>(connectionString, BatchInsertConfiguration.Default);
                
                // Create table
                await repo.ExecuteSqlAsync(@"
                    CREATE TABLE IF NOT EXISTS people (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        first TEXT NOT NULL,
                        last TEXT NOT NULL,
                        age INTEGER NOT NULL,
                        email TEXT NOT NULL,
                        salary REAL NOT NULL,
                        department TEXT NOT NULL
                    );");
                
                // Insert test data
                List<Person> testData = new List<Person>
                {
                    new Person { FirstName = "Alice", LastName = "Smith", Age = 30, Email = "alice@test.com", Salary = 75000, Department = "IT" },
                    new Person { FirstName = "Bob", LastName = "Jones", Age = 25, Email = "bob@test.com", Salary = 65000, Department = "IT" },
                    new Person { FirstName = "Carol", LastName = "Davis", Age = 35, Email = "carol@test.com", Salary = 80000, Department = "HR" },
                    new Person { FirstName = "David", LastName = "Wilson", Age = 28, Email = "david@test.com", Salary = 70000, Department = "IT" },
                    new Person { FirstName = "Eve", LastName = "Brown", Age = 32, Email = "eve@test.com", Salary = 85000, Department = "HR" },
                    new Person { FirstName = "Frank", LastName = "Miller", Age = 45, Email = "frank@test.com", Salary = 95000, Department = "Finance" }
                };
                
                foreach (Person person in testData)
                {
                    await repo.CreateAsync(person);
                }
                
                Console.WriteLine($"✓ Created {testData.Count} test records");
                
                // Test 1: Basic GroupBy
                Console.WriteLine("\n--- Test 1: Basic GroupBy ---");
                IEnumerable<IGrouping<string, Person>> groups = await repo.Query()
                    .GroupBy(p => p.Department)
                    .ExecuteAsync();
                
                foreach (IGrouping<string, Person> group in groups)
                {
                    Console.WriteLine($"Department: {group.Key} ({group.Count()} people)");
                    foreach (Person person in group)
                    {
                        Console.WriteLine($"  - {person.FirstName} {person.LastName} (${person.Salary:N0})");
                    }
                }
                
                // Test 2: GroupBy with WHERE
                Console.WriteLine("\n--- Test 2: GroupBy with WHERE ---");
                IEnumerable<IGrouping<string, Person>> seniorGroups = await repo.Query()
                    .Where(p => p.Age >= 30)
                    .GroupBy(p => p.Department)
                    .ExecuteAsync();
                
                foreach (IGrouping<string, Person> group in seniorGroups)
                {
                    Console.WriteLine($"Senior {group.Key}: {group.Count()} people");
                }
                
                // Test 3: HAVING clause
                Console.WriteLine("\n--- Test 3: HAVING clause ---");
                IEnumerable<IGrouping<string, Person>> largeDepts = await repo.Query()
                    .GroupBy(p => p.Department)
                    .Having(g => g.Count() > 1)
                    .ExecuteAsync();
                
                foreach (IGrouping<string, Person> group in largeDepts)
                {
                    Console.WriteLine($"Large Department: {group.Key} ({group.Count()} people)");
                }
                
                // Test 4: Aggregate methods
                Console.WriteLine("\n--- Test 4: Aggregate methods ---");
                int totalPeople = await repo.Query().GroupBy(p => p.Department).CountAsync();
                decimal totalSalaries = await repo.Query().GroupBy(p => p.Department).SumAsync(p => p.Salary);
                decimal avgSalary = await repo.Query().GroupBy(p => p.Department).AverageAsync(p => p.Salary);
                
                Console.WriteLine($"Total people: {totalPeople}");
                Console.WriteLine($"Total salaries: ${totalSalaries:N0}");
                Console.WriteLine($"Average salary: ${avgSalary:N0}");
                
                Console.WriteLine("\n✅ Quick GroupBy test completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Test failed: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }
    }
}