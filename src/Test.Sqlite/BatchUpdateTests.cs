namespace Test.Sqlite
{
    using System;
    using System.Linq.Expressions;
    using Xunit;
    using Durable.Sqlite;
    using System.Collections.Generic;
    using System.Reflection;
    using Microsoft.Data.Sqlite;
    using System.Threading.Tasks;
    using System.Linq;
    using Durable;
    
    public class BatchUpdateTests : IDisposable
    {
        private readonly SqliteRepository<TestPerson> Repository;
        private readonly Dictionary<string, PropertyInfo> ColumnMappings;

        public BatchUpdateTests()
        {
            string connectionString = "Data Source=batch_update_test.db";
            Repository = new SqliteRepository<TestPerson>(connectionString);
            
            ColumnMappings = typeof(TestPerson).GetProperties()
                .ToDictionary(p => p.Name, p => p);
        }

        private async Task CreateTableAsync()
        {
            await Repository.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS TestPerson (
                    Id INTEGER PRIMARY KEY,
                    FirstName TEXT NOT NULL,
                    LastName TEXT NOT NULL,
                    Salary REAL NOT NULL,
                    Age INTEGER NOT NULL,
                    LastModified TEXT,
                    Department TEXT,
                    Bonus REAL NOT NULL,
                    Status TEXT,
                    YearsOfService INTEGER NOT NULL,
                    Email TEXT
                );");
        }

        private async Task InsertTestDataAsync()
        {
            await CreateTableAsync();
            
            // Clear existing data
            await Repository.ExecuteSqlAsync("DELETE FROM TestPerson");

            TestPerson[] people = new[]
            {
                new TestPerson { Id = 1, FirstName = "John", LastName = "Doe", Salary = 50000, Age = 30, Department = "IT", Bonus = 5000, Status = "Active", YearsOfService = 5, Email = "john@example.com" },
                new TestPerson { Id = 2, FirstName = "Jane", LastName = "Smith", Salary = 60000, Age = 35, Department = "HR", Bonus = 6000, Status = "Active", YearsOfService = 7, Email = "jane@example.com" },
                new TestPerson { Id = 3, FirstName = "Bob", LastName = "Johnson", Salary = 45000, Age = 28, Department = "IT", Bonus = 4000, Status = "Inactive", YearsOfService = 3, Email = "bob@example.com" },
                new TestPerson { Id = 4, FirstName = "Alice", LastName = "Brown", Salary = 70000, Age = 42, Department = "Finance", Bonus = 8000, Status = "Active", YearsOfService = 10, Email = "alice@example.com" }
            };

            await Repository.CreateManyAsync(people);
        }

        [Fact]
        public async Task TestSimpleFieldToFieldOperation()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                Salary = p.Salary * 1.1m
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("Salary = (Salary * 1.1)", result);
        }

        [Fact]
        public async Task TestMultipleFieldCalculations()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                Salary = p.Salary * 1.1m,
                Bonus = p.Salary * 0.2m,
                Age = p.Age + 1
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("Salary = (Salary * 1.1)", result);
            Assert.Contains("Bonus = (Salary * 0.2)", result);
            Assert.Contains("Age = (Age + 1)", result);
        }

        [Fact]
        public async Task TestConditionalUpdate()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                Salary = p.YearsOfService > 5 ? p.Salary * 1.15m : p.Salary * 1.05m,
                Status = p.Age > 30 ? "Senior" : "Junior"
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("CASE WHEN", result);
            Assert.Contains("YearsOfService > 5", result);
            Assert.Contains("Salary * 1.15", result);
            Assert.Contains("Salary * 1.05", result);
            Assert.Contains("Age > 30", result);
            Assert.Contains("'Senior'", result);
            Assert.Contains("'Junior'", result);
        }

        [Fact]
        public async Task TestStringManipulation()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                FirstName = p.FirstName.ToUpper(),
                LastName = p.LastName.ToLower(),
                Email = p.FirstName.ToLower() + "." + p.LastName.ToLower() + "@newdomain.com"
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("FirstName = UPPER(FirstName)", result);
            Assert.Contains("LastName = LOWER(LastName)", result);
            Assert.Contains("Email = (((LOWER(FirstName) || '.') || LOWER(LastName)) || '@newdomain.com')", result);
        }

        [Fact]
        public async Task TestMathFunctions()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                Salary = Math.Round(p.Salary * 1.1m, 2),
                Bonus = Math.Max(p.Bonus, 5000)
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("ROUND((Salary * 1.1), 2)", result);
            Assert.Contains("MAX(Bonus, 5000)", result);
        }

        [Fact]
        public async Task TestComplexFieldToFieldOperations()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                Salary = p.Salary + p.Bonus,
                Bonus = (p.Salary + p.Bonus) * 0.1m
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("Salary = (Salary + Bonus)", result);
            Assert.Contains("Bonus = ((Salary + Bonus) * 0.1)", result);
        }

        [Fact]
        public async Task TestDateTimeOperations()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                LastModified = DateTime.Now
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("LastModified = datetime('now')", result);
        }

        [Fact]
        public async Task TestBatchUpdateIntegration()
        {
            await InsertTestDataAsync();

            List<TestPerson> initialPeople = Repository.ReadMany(p => p.Department == "IT").ToList();
            
            int rowsAffected = await Repository.BatchUpdateAsync(
                p => p.Department == "IT",
                p => new TestPerson
                {
                    Salary = p.Salary * 1.1m,
                    Bonus = p.Salary * 0.15m
                }
            );
            
            Assert.Equal(2, rowsAffected);

            List<TestPerson> updatedPeople = Repository.ReadMany(p => p.Department == "IT").ToList();

            foreach (TestPerson person in updatedPeople)
            {
                TestPerson originalPerson = initialPeople.First(p => p.Id == person.Id);
                Assert.Equal(Math.Round(originalPerson.Salary * 1.1m, 2), Math.Round(person.Salary, 2));
                Assert.Equal(Math.Round(originalPerson.Salary * 0.15m, 2), Math.Round(person.Bonus, 2));
            }
        }

        [Fact]
        public async Task TestConditionalBatchUpdate()
        {
            await InsertTestDataAsync();
            
            int rowsAffected = await Repository.BatchUpdateAsync(
                p => p.Status == "Active",
                p => new TestPerson
                {
                    Salary = p.YearsOfService > 5 ? p.Salary * 1.20m : p.Salary * 1.10m,
                    Status = p.Age > 35 ? "Senior" : p.Status
                }
            );
            
            Assert.Equal(3, rowsAffected);
        }

        [Fact]
        public async Task TestCoalesceOperator()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                Department = p.Department ?? "Unknown"
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("Department = COALESCE(Department, 'Unknown')", result);
        }

        [Fact]
        public async Task TestSubstringOperations()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                FirstName = p.FirstName.Substring(0, 3),
                Email = p.Email.Replace("@example.com", "@newdomain.com")
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("FirstName = SUBSTR(FirstName, 0 + 1, 3)", result);
            Assert.Contains("Email = REPLACE(Email, '@example.com', '@newdomain.com')", result);
        }

        [Fact]
        public async Task TestComplexConditionalWithMultipleConditions()
        {
            await InsertTestDataAsync();
            
            ExpressionParser<TestPerson> parser = new ExpressionParser<TestPerson>(ColumnMappings);
            
            Expression<Func<TestPerson, TestPerson>> updateExpr = p => new TestPerson
            {
                Salary = (p.YearsOfService > 10 && p.Department == "Finance") 
                    ? p.Salary * 1.25m 
                    : (p.YearsOfService > 5 
                        ? p.Salary * 1.15m 
                        : p.Salary * 1.05m)
            };
            
            string result = parser.ParseUpdateExpression(updateExpr);
            
            Assert.Contains("CASE WHEN", result);
            Assert.Contains("YearsOfService > 10", result);
            Assert.Contains("Department = 'Finance'", result);
            Assert.Contains("AND", result);
        }

        public void Dispose()
        {
            Repository?.Dispose();
        }
    }
}