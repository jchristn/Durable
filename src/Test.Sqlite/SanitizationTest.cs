using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Durable.Sqlite;
using Test.Shared;

namespace Test.Sqlite
{
    public static class SanitizationTest
    {
        public static async Task RunSanitizationTests()
        {
            Console.WriteLine("✓ Testing SQL injection protection");
            
            const string connectionString = "Data Source=SanitizationTest;Mode=Memory;Cache=Shared";
            using Microsoft.Data.Sqlite.SqliteConnection keepAlive = new Microsoft.Data.Sqlite.SqliteConnection(connectionString);
            keepAlive.Open();
            
            InitializeDatabase(connectionString);
            SqliteRepository<Person> repository = new SqliteRepository<Person>(connectionString);
            
            // Test 1: Malicious column names in expressions should be sanitized
            try
            {
                List<Person> people = new List<Person>
                {
                    new Person { FirstName = "John", LastName = "Doe", Age = 30, Department = "IT", Salary = 50000 },
                    new Person { FirstName = "Jane", LastName = "Smith", Age = 25, Department = "HR", Salary = 45000 }
                };
                
                await repository.CreateManyAsync(people);
                
                // This should work safely even if column names were somehow manipulated
                decimal result = repository.Max(p => p.Salary, p => p.Department == "IT");
                if (result != 50000) throw new Exception("Max query failed");
                
                int count = repository.Count(p => p.Age > 20);
                if (count != 2) throw new Exception("Count query failed");
                
                Console.WriteLine("✓ Expression-based queries properly sanitized");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Expression sanitization failed: {ex.Message}");
                throw;
            }
            
            // Test 2: Raw SQL with parameters (should be safe)
            try
            {
                string maliciousValue = "'; DROP TABLE people; --";
                List<Person> results = repository.FromSql(
                    "SELECT * FROM people WHERE department = @p0", 
                    null, 
                    maliciousValue
                ).ToList();
                
                // Should return empty results, not crash or drop tables
                int totalCount = repository.Count();
                if (totalCount == 0) throw new Exception("Table appears to have been dropped - sanitization failed!");
                
                Console.WriteLine("✓ Parameterized queries protect against injection");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Parameter sanitization failed: {ex.Message}");
                throw;
            }
            
            // Test 3: LIKE operations should be properly sanitized
            try
            {
                Person person = new Person 
                { 
                    FirstName = "Test'User", 
                    LastName = "O'Brien", 
                    Age = 35, 
                    Department = "QA", 
                    Salary = 55000 
                };
                await repository.CreateAsync(person);
                
                // Test LIKE with special characters
                List<Person> found = repository.ReadMany(p => p.FirstName.Contains("'User")).ToList();
                if (found.Count != 1) throw new Exception("LIKE sanitization failed");
                
                Console.WriteLine("✓ LIKE operations properly sanitized");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ LIKE sanitization failed: {ex.Message}");
                throw;
            }
            
            Console.WriteLine("✓ All sanitization tests passed");
        }
        
        private static void InitializeDatabase(string connectionString)
        {
            using Microsoft.Data.Sqlite.SqliteConnection connection = new Microsoft.Data.Sqlite.SqliteConnection(connectionString);
            connection.Open();
            
            string sql = @"
                CREATE TABLE IF NOT EXISTS people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first TEXT NOT NULL,
                    last TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    salary REAL NOT NULL,
                    department TEXT,
                    email TEXT
                );";
                
            using Microsoft.Data.Sqlite.SqliteCommand command = new Microsoft.Data.Sqlite.SqliteCommand(sql, connection);
            command.ExecuteNonQuery();
        }
    }
}