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
    
    public static class GroupByTest
    {
        public static async Task RunGroupByTests()
        {
            Console.WriteLine("=== GROUP BY TESTS ===");
            
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
            
            // Insert test data
            Console.WriteLine("Inserting test data...");
            List<Person> testPeople = new List<Person>
            {
                new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john@example.com", Salary = 50000, Department = "IT" },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane@example.com", Salary = 55000, Department = "IT" },
                new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Email = "bob@example.com", Salary = 60000, Department = "HR" },
                new Person { FirstName = "Alice", LastName = "Williams", Age = 32, Email = "alice@example.com", Salary = 58000, Department = "HR" },
                new Person { FirstName = "Charlie", LastName = "Brown", Age = 27, Email = "charlie@example.com", Salary = 48000, Department = "Sales" },
                new Person { FirstName = "David", LastName = "Davis", Age = 40, Email = "david@example.com", Salary = 65000, Department = "IT" },
                new Person { FirstName = "Eve", LastName = "Wilson", Age = 29, Email = "eve@example.com", Salary = 52000, Department = "Sales" },
                new Person { FirstName = "Frank", LastName = "Moore", Age = 45, Email = "frank@example.com", Salary = 70000, Department = "HR" },
                new Person { FirstName = "Grace", LastName = "Taylor", Age = 26, Email = "grace@example.com", Salary = 45000, Department = "Sales" },
                new Person { FirstName = "Henry", LastName = "Anderson", Age = 33, Email = "henry@example.com", Salary = 62000, Department = "IT" }
            };
            
            foreach (Person person in testPeople)
            {
                await repository.CreateAsync(person);
            }
            Console.WriteLine($"Inserted {testPeople.Count} test records");
            
            // Test 1: Simple GROUP BY with Count
            Console.WriteLine("\nTest 1: Group by Department with Count");
            Console.WriteLine("---------------------------------------");
            IEnumerable<IGrouping<string, Person>> groupsByDept = await repository.Query()
                .GroupBy(p => p.Department)
                .ExecuteAsync();
            
            foreach (IGrouping<string, Person> group in groupsByDept)
            {
                Console.WriteLine($"Department: {group.Key}, Count: {group.Count()}");
                foreach (Person person in group)
                {
                    Console.WriteLine($"  - {person.FirstName} {person.LastName}");
                }
            }
            
            // Test 2: GROUP BY with WHERE clause
            Console.WriteLine("\nTest 2: Group by Department where Age > 30");
            Console.WriteLine("-------------------------------------------");
            IEnumerable<IGrouping<string, Person>> filteredGroups = await repository.Query()
                .Where(p => p.Age > 30)
                .GroupBy(p => p.Department)
                .ExecuteAsync();
            
            foreach (IGrouping<string, Person> group in filteredGroups)
            {
                Console.WriteLine($"Department: {group.Key}, Count: {group.Count()}");
            }
            
            // Test 3: GROUP BY with HAVING clause
            Console.WriteLine("\nTest 3: Group by Department having Count > 2");
            Console.WriteLine("---------------------------------------------");
            IEnumerable<IGrouping<string, Person>> havingGroups = await repository.Query()
                .GroupBy(p => p.Department)
                .Having(g => g.Count() > 2)
                .ExecuteAsync();
            
            foreach (IGrouping<string, Person> group in havingGroups)
            {
                Console.WriteLine($"Department: {group.Key}, Count: {group.Count()}");
            }
            
            // Test 4: Aggregate functions - Sum
            Console.WriteLine("\nTest 4: Total Salary by Department");
            Console.WriteLine("-----------------------------------");
            IGroupedQueryBuilder<Person, string> salaryByDept = repository.Query()
                .GroupBy(p => p.Department);
            
            decimal totalITSalary = await salaryByDept.SumAsync(p => p.Salary);
            Console.WriteLine($"Total salary across all groups: ${totalITSalary:N2}");
            
            // Test 5: Aggregate functions - Average
            Console.WriteLine("\nTest 5: Average Age by Department");
            Console.WriteLine("----------------------------------");
            IEnumerable<IGrouping<string, Person>> ageGroups = await repository.Query()
                .GroupBy(p => p.Department)
                .ExecuteAsync();
            
            foreach (IGrouping<string, Person> group in ageGroups)
            {
                double avgAge = group.Average(p => p.Age);
                Console.WriteLine($"Department: {group.Key}, Average Age: {avgAge:F1}");
            }
            
            // Test 6: Aggregate functions - Min/Max
            Console.WriteLine("\nTest 6: Min and Max Salary by Department");
            Console.WriteLine("-----------------------------------------");
            foreach (IGrouping<string, Person> group in ageGroups)
            {
                decimal minSalary = group.Min(p => p.Salary);
                decimal maxSalary = group.Max(p => p.Salary);
                Console.WriteLine($"Department: {group.Key}, Min Salary: ${minSalary:N2}, Max Salary: ${maxSalary:N2}");
            }
            
            // Test 7: Complex HAVING with multiple conditions
            Console.WriteLine("\nTest 7: Departments with Count > 1 AND Average Salary > 50000");
            Console.WriteLine("--------------------------------------------------------------");
            IEnumerable<IGrouping<string, Person>> complexGroups = await repository.Query()
                .GroupBy(p => p.Department)
                .Having(g => g.Count() > 1 && g.Average(p => p.Salary) > 50000)
                .ExecuteAsync();
            
            foreach (IGrouping<string, Person> group in complexGroups)
            {
                decimal avgSalary = group.Average(p => p.Salary);
                Console.WriteLine($"Department: {group.Key}, Count: {group.Count()}, Avg Salary: ${avgSalary:N2}");
            }
            
            // Test 8: GROUP BY with multiple aggregate results
            Console.WriteLine("\nTest 8: Department Statistics");
            Console.WriteLine("------------------------------");
            IEnumerable<IGrouping<string, Person>> deptStats = await repository.Query()
                .GroupBy(p => p.Department)
                .ExecuteAsync();
            
            foreach (IGrouping<string, Person> group in deptStats)
            {
                int count = group.Count();
                decimal totalSalary = group.Sum(p => p.Salary);
                decimal avgSalary = group.Average(p => p.Salary);
                int minAge = group.Min(p => p.Age);
                int maxAge = group.Max(p => p.Age);
                
                Console.WriteLine($"Department: {group.Key}");
                Console.WriteLine($"  Employees: {count}");
                Console.WriteLine($"  Total Salary: ${totalSalary:N2}");
                Console.WriteLine($"  Average Salary: ${avgSalary:N2}");
                Console.WriteLine($"  Age Range: {minAge} - {maxAge}");
            }
            
            // Test 9: Async aggregate methods
            Console.WriteLine("\nTest 9: Testing Async Aggregate Methods");
            Console.WriteLine("----------------------------------------");
            IGroupedQueryBuilder<Person, string> itGroup = repository.Query()
                .Where(p => p.Department == "IT")
                .GroupBy(p => p.Department);
            
            int itCount = await itGroup.CountAsync();
            decimal itSum = await itGroup.SumAsync(p => p.Salary);
            decimal itAvg = await itGroup.AverageAsync(p => p.Salary);
            decimal itMax = await itGroup.MaxAsync(p => p.Salary);
            decimal itMin = await itGroup.MinAsync(p => p.Salary);
            
            Console.WriteLine($"IT Department Statistics (Async):");
            Console.WriteLine($"  Count: {itCount}");
            Console.WriteLine($"  Total Salary: ${itSum:N2}");
            Console.WriteLine($"  Average Salary: ${itAvg:N2}");
            Console.WriteLine($"  Max Salary: ${itMax:N2}");
            Console.WriteLine($"  Min Salary: ${itMin:N2}");
            
            Console.WriteLine("\n=== GROUP BY TESTS COMPLETED SUCCESSFULLY ===");
        }
    }
}