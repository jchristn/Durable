namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;
    
    public static class TestSelectFunctionality
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Testing Select/Projection Functionality...");
            string connectionString = "Data Source=select_test.db";
            
            try
            {
                // Setup
                CreateDatabase(connectionString);
                SqliteRepository<Person> repository = new SqliteRepository<Person>(connectionString);
                
                // Insert test data
                InsertTestData(repository);
                
                // Test 1: Simple projection
                Console.WriteLine("\n1. Testing simple projection...");
                List<PersonName> names = repository.Query()
                    .Select(p => new PersonName 
                    { 
                        FirstName = p.FirstName,
                        LastName = p.LastName 
                    })
                    .Execute()
                    .ToList();
                
                Console.WriteLine($"   Retrieved {names.Count} names");
                Console.WriteLine($"   First name: {names.First().FirstName} {names.First().LastName}");
                
                // Test 2: Check generated SQL
                Console.WriteLine("\n2. Testing SQL generation...");
                string sqlQuery = repository.Query()
                    .Where(p => p.Department == "IT")
                    .Select(p => new PersonSummary
                    {
                        FirstName = p.FirstName,
                        LastName = p.LastName,
                        Email = p.Email,
                        Salary = p.Salary
                    })
                    .Query;
                    
                Console.WriteLine($"   Generated SQL: {sqlQuery}");
                
                // Test 3: Full projection with filtering and ordering
                Console.WriteLine("\n3. Testing full projection with filtering and ordering...");
                List<PersonSummary> summaries = repository.Query()
                    .Where(p => p.Salary > 60000)
                    .Select(p => new PersonSummary
                    {
                        FirstName = p.FirstName,
                        LastName = p.LastName,
                        Email = p.Email,
                        Salary = p.Salary
                    })
                    .OrderBy(s => s.LastName)
                    .Take(3)
                    .Execute()
                    .ToList();
                
                Console.WriteLine($"   Retrieved {summaries.Count} high-salary people:");
                foreach (PersonSummary summary in summaries)
                {
                    Console.WriteLine($"     {summary.FirstName} {summary.LastName} - ${summary.Salary:N0}");
                }
                
                Console.WriteLine("\n✅ All Select/Projection tests passed!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ Test failed: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }
        
        private static void CreateDatabase(string connectionString)
        {
            using (SqliteConnection connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                string dropTable = "DROP TABLE IF EXISTS people;";
                using (SqliteCommand dropCommand = new SqliteCommand(dropTable, connection))
                {
                    dropCommand.ExecuteNonQuery();
                }

                string createTable = @"
                CREATE TABLE people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first TEXT NOT NULL,
                    last TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT,
                    salary REAL,
                    department TEXT
                );";

                using (SqliteCommand createCommand = new SqliteCommand(createTable, connection))
                {
                    createCommand.ExecuteNonQuery();
                }
            }
        }
        
        private static void InsertTestData(SqliteRepository<Person> repository)
        {
            List<Person> people = new List<Person>
            {
                new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john@example.com", Salary = 75000, Department = "IT" },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane@example.com", Salary = 65000, Department = "HR" },
                new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Email = "bob@example.com", Salary = 55000, Department = "Finance" },
                new Person { FirstName = "Alice", LastName = "Williams", Age = 32, Email = "alice@example.com", Salary = 85000, Department = "IT" },
                new Person { FirstName = "Charlie", LastName = "Brown", Age = 45, Email = "charlie@example.com", Salary = 70000, Department = "Sales" }
            };

            foreach (Person person in people)
            {
                repository.Create(person);
            }
            
            Console.WriteLine($"Inserted {people.Count} test records");
        }
    }
}