namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;

    public static class SelectTest
    {
        public static void RunSelectTests()
        {
            string connectionString = "Data Source=select_test.db";
            SqliteRepository<Person> repository = new SqliteRepository<Person>(connectionString);

            try
            {
                // Setup database
                CreateTestTable(connectionString);
                InsertTestData(repository);

                Console.WriteLine("=== SELECT/PROJECTION TESTS ===\n");

                // Test 1: Basic projection to new type
                Test1_BasicProjection(repository);

                // Test 2: Simple SQL generation test
                Test2_SqlGeneration(repository);

                // Test 3: With filtering and ordering
                Test3_FilteringAndOrdering(repository);

                Console.WriteLine("\n✅ All Select/Projection tests passed!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Select test failed: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private static void Test1_BasicProjection(SqliteRepository<Person> repository)
        {
            Console.WriteLine("Test 1: Basic projection to PersonSummary");
            
            IEnumerable<PersonSummary> summaries = repository
                .Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .Execute();

            List<PersonSummary> results = summaries.ToList();
            Console.WriteLine($"  Retrieved {results.Count} projected results");
            
            // Verify we got data back
            if (results.Count == 0)
                throw new Exception("No results returned from basic projection");

            // Verify properties are populated
            PersonSummary first = results.First();
            if (string.IsNullOrEmpty(first.FirstName) || string.IsNullOrEmpty(first.LastName))
                throw new Exception("Properties not properly mapped in projection");

            Console.WriteLine($"  Sample result: {first.FirstName} {first.LastName} - {first.Email}");
            Console.WriteLine("  ✅ Basic projection test passed");
        }

        private static void Test2_SqlGeneration(SqliteRepository<Person> repository)
        {
            Console.WriteLine("\nTest 2: SQL generation for projection");
            
            string sql = repository
                .Query()
                .Where(p => p.Department == "IT")
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .Query;

            Console.WriteLine($"  Generated SQL: {sql}");
            
            // Verify SQL contains expected elements
            if (!sql.Contains("SELECT") || !sql.Contains("FROM"))
                throw new Exception("Generated SQL missing basic SELECT/FROM structure");

            if (sql.Contains("SELECT *"))
                throw new Exception("SQL should not contain SELECT *, should list specific columns");

            if (!sql.Contains("first") || !sql.Contains("last") || !sql.Contains("email"))
                throw new Exception("SQL should contain projected column names");

            Console.WriteLine("  ✅ SQL generation test passed");
        }

        private static void Test3_FilteringAndOrdering(SqliteRepository<Person> repository)
        {
            Console.WriteLine("\nTest 3: Projection with filtering and ordering");

            IEnumerable<PersonSummary> results = repository
                .Query()
                .Where(p => p.Salary > 60000)
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .OrderBy(s => s.LastName)
                .Take(3)
                .Execute();

            List<PersonSummary> resultList = results.ToList();
            Console.WriteLine($"  Retrieved {resultList.Count} filtered and ordered results");

            if (resultList.Count == 0)
                throw new Exception("No results returned from filtered projection");

            // Verify ordering (should be sorted by last name)
            for (int i = 1; i < resultList.Count; i++)
            {
                if (string.Compare(resultList[i-1].LastName, resultList[i].LastName, StringComparison.OrdinalIgnoreCase) > 0)
                    throw new Exception($"Results not properly ordered: {resultList[i-1].LastName} comes before {resultList[i].LastName}");
            }

            foreach (PersonSummary person in resultList)
            {
                Console.WriteLine($"    {person.LastName}, {person.FirstName}");
            }

            Console.WriteLine("  ✅ Filtering and ordering test passed");
        }

        private static void CreateTestTable(string connectionString)
        {
            using SqliteConnection connection = new SqliteConnection(connectionString);
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
            
            using SqliteCommand createCommand = new SqliteCommand(createTable, connection);
            createCommand.ExecuteNonQuery();
        }

        private static void InsertTestData(SqliteRepository<Person> repository)
        {
            List<Person> people = new List<Person>
            {
                new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john@example.com", Salary = 75000, Department = "IT" },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane@example.com", Salary = 65000, Department = "HR" },
                new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Email = "bob@example.com", Salary = 55000, Department = "Finance" },
                new Person { FirstName = "Alice", LastName = "Williams", Age = 32, Email = "alice@example.com", Salary = 85000, Department = "IT" },
                new Person { FirstName = "Charlie", LastName = "Brown", Age = 45, Email = "charlie@example.com", Salary = 70000, Department = "IT" }
            };

            foreach (Person person in people)
            {
                repository.Create(person);
            }
        }
    }
}