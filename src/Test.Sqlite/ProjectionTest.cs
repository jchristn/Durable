namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;

    public class ProjectionTest
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private string ConnectionString = "Data Source=projection_test.db";
        private SqliteRepository<Person> PersonRepository;

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        public void Run()
        {
            // Initialize repository
            PersonRepository = new SqliteRepository<Person>(ConnectionString);

            // Create table
            CreateTable();

            // Insert test data
            InsertTestData();

            // Test 1: Select specific fields into a new type
            TestSelectSpecificFields();

            // Test 2: Select with anonymous type
            TestSelectAnonymousType();

            // Test 3: Select with member initialization
            TestSelectMemberInit();

            // Test 4: Select with ordering
            TestSelectWithOrdering();

            // Test 5: Select with filtering
            TestSelectWithFiltering();

            // Test 6: Select distinct values
            TestSelectDistinct();

            // Test 7: Select with pagination
            TestSelectWithPagination();

            // Test 8: Async operations
            TestAsyncOperations().Wait();

            Console.WriteLine("\nAll projection tests completed successfully!");
        }

        #endregion

        #region Private-Methods

        private void CreateTable()
        {
            using SqliteConnection connection = new SqliteConnection(ConnectionString);
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

        private void InsertTestData()
        {
            List<Person> people = new List<Person>
            {
                new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john@example.com", Salary = 60000, Department = "IT" },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane@example.com", Salary = 65000, Department = "HR" },
                new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Email = "bob@example.com", Salary = 70000, Department = "IT" },
                new Person { FirstName = "Alice", LastName = "Williams", Age = 32, Email = "alice@example.com", Salary = 75000, Department = "Sales" },
                new Person { FirstName = "Charlie", LastName = "Brown", Age = 45, Email = "charlie@example.com", Salary = 80000, Department = "IT" },
                new Person { FirstName = "Diana", LastName = "Davis", Age = 27, Email = "diana@example.com", Salary = 55000, Department = "HR" },
                new Person { FirstName = "Eve", LastName = "Miller", Age = 38, Email = "eve@example.com", Salary = 90000, Department = "Sales" },
                new Person { FirstName = "Frank", LastName = "Wilson", Age = 41, Email = "frank@example.com", Salary = 85000, Department = "IT" }
            };

            foreach (Person person in people)
            {
                PersonRepository.Create(person);
            }

            Console.WriteLine($"Inserted {people.Count} test records");
        }

        private void TestSelectSpecificFields()
        {
            Console.WriteLine("\n=== Test 1: Select specific fields ===");

            IEnumerable<PersonSummary> summaries = PersonRepository
                .Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .Execute();

            foreach (PersonSummary summary in summaries)
            {
                Console.WriteLine($"  {summary.FirstName} {summary.LastName} - {summary.Email}");
            }
        }

        private void TestSelectAnonymousType()
        {
            Console.WriteLine("\n=== Test 2: Select with anonymous type ===");
            
            // Note: Anonymous types would need special handling in the actual implementation
            // For this test, we'll use a concrete type
            IEnumerable<PersonSummary> results = PersonRepository
                .Query()
                .Where(p => p.Department == "IT")
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .Execute();

            foreach (PersonSummary result in results)
            {
                Console.WriteLine($"  IT Employee: {result.FirstName} {result.LastName}");
            }
        }

        private void TestSelectMemberInit()
        {
            Console.WriteLine("\n=== Test 3: Select with member initialization ===");

            IEnumerable<DepartmentInfo> deptInfo = PersonRepository
                .Query()
                .Select(p => new DepartmentInfo
                {
                    Department = p.Department,
                    Salary = p.Salary
                })
                .Execute();

            foreach (DepartmentInfo info in deptInfo)
            {
                Console.WriteLine($"  Department: {info.Department}, Salary: {info.Salary:C}");
            }
        }

        private void TestSelectWithOrdering()
        {
            Console.WriteLine("\n=== Test 4: Select with ordering ===");

            IEnumerable<PersonSummary> ordered = PersonRepository
                .Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .OrderBy(s => s.LastName)
                .ThenBy(s => s.FirstName)
                .Execute();

            foreach (PersonSummary person in ordered)
            {
                Console.WriteLine($"  {person.LastName}, {person.FirstName}");
            }
        }

        private void TestSelectWithFiltering()
        {
            Console.WriteLine("\n=== Test 5: Select with filtering (WHERE before SELECT) ===");

            IEnumerable<PersonSummary> filtered = PersonRepository
                .Query()
                .Where(p => p.Age > 30)
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .Execute();

            Console.WriteLine("  People over 30:");
            foreach (PersonSummary person in filtered)
            {
                Console.WriteLine($"    {person.FirstName} {person.LastName}");
            }
        }

        private void TestSelectDistinct()
        {
            Console.WriteLine("\n=== Test 6: Select distinct values ===");

            IEnumerable<DepartmentInfo> distinctDepts = PersonRepository
                .Query()
                .Select(p => new DepartmentInfo
                {
                    Department = p.Department,
                    Salary = p.Salary
                })
                .Distinct()
                .Execute();

            foreach (DepartmentInfo dept in distinctDepts)
            {
                Console.WriteLine($"  Department: {dept.Department}");
            }
        }

        private void TestSelectWithPagination()
        {
            Console.WriteLine("\n=== Test 7: Select with pagination ===");

            IEnumerable<PersonSummary> page1 = PersonRepository
                .Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .OrderBy(s => s.LastName)
                .Take(3)
                .Execute();

            Console.WriteLine("  First 3 records:");
            foreach (PersonSummary person in page1)
            {
                Console.WriteLine($"    {person.FirstName} {person.LastName}");
            }

            IEnumerable<PersonSummary> page2 = PersonRepository
                .Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .OrderBy(s => s.LastName)
                .Skip(3)
                .Take(3)
                .Execute();

            Console.WriteLine("  Next 3 records:");
            foreach (PersonSummary person in page2)
            {
                Console.WriteLine($"    {person.FirstName} {person.LastName}");
            }
        }

        private async System.Threading.Tasks.Task TestAsyncOperations()
        {
            Console.WriteLine("\n=== Test 8: Async operations ===");

            IEnumerable<PersonSummary> asyncResults = await PersonRepository
                .Query()
                .Where(p => p.Salary > 60000)
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .ExecuteAsync();

            Console.WriteLine("  High earners (async):");
            foreach (PersonSummary person in asyncResults)
            {
                Console.WriteLine($"    {person.FirstName} {person.LastName}");
            }

            // Test with query exposure
            IDurableResult<PersonSummary> resultWithQuery = await PersonRepository
                .Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email
                })
                .Take(2)
                .ExecuteWithQueryAsync();

            Console.WriteLine($"\n  Generated SQL: {resultWithQuery.Query}");
            Console.WriteLine("  Results:");
            foreach (PersonSummary person in resultWithQuery.Results)
            {
                Console.WriteLine($"    {person.FirstName} {person.LastName}");
            }
        }

        #endregion
    }
}