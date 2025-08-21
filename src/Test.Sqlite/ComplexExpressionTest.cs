namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using Durable.Sqlite;
    using Test.Shared;

    public class ComplexExpressionTest
    {
        private SqliteRepository<Person> _Repository;

        public ComplexExpressionTest()
        {
            // Initialize repository for testing
            var connectionFactory = new SqliteConnectionFactory("Data Source=:memory:");
            _Repository = new SqliteRepository<Person>(connectionFactory);
        }

        public void RunAllTests()
        {
            Console.WriteLine("=== Running Complex Expression Tests ===");
            
            try
            {
                TestInOperator();
                TestBetweenOperator();
                TestComplexBooleanLogic();
                TestDateTimeOperations();
                TestMathOperations();
                TestCaseWhenExpressions();
                TestStringOperations();
                TestNullChecks();
                
                Console.WriteLine("✅ All complex expression tests passed!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Test failed: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        private void TestInOperator()
        {
            Console.WriteLine("Testing IN operator...");
            
            // Test with array
            var departments = new[] { "IT", "HR", "Finance" };
            var query1 = _Repository.Query()
                .Where(p => departments.Contains(p.Department))
                .BuildSql();
            
            Console.WriteLine($"IN with array: {query1}");
            
            // Test with list
            var ageList = new List<int> { 25, 30, 35 };
            var query2 = _Repository.Query()
                .Where(p => ageList.Contains(p.Age))
                .BuildSql();
            
            Console.WriteLine($"IN with list: {query2}");
            
            // Test extension method (if supported)
            var query3 = _Repository.Query()
                .Where(p => p.Age.In(25, 30, 35))
                .BuildSql();
            
            Console.WriteLine($"IN extension method: {query3}");
        }

        private void TestBetweenOperator()
        {
            Console.WriteLine("Testing BETWEEN operator...");
            
            // Test Between extension method
            var query1 = _Repository.Query()
                .Where(p => p.Age.Between(25, 65))
                .BuildSql();
            
            Console.WriteLine($"BETWEEN ages: {query1}");
            
            var query2 = _Repository.Query()
                .Where(p => p.Salary.Between(50000m, 100000m))
                .BuildSql();
            
            Console.WriteLine($"BETWEEN salaries: {query2}");
        }

        private void TestComplexBooleanLogic()
        {
            Console.WriteLine("Testing complex boolean logic...");
            
            // Complex nested AND/OR
            var query1 = _Repository.Query()
                .Where(p => (p.Age > 30 && p.Department == "IT") || 
                           (p.Age < 25 && p.Salary > 70000))
                .BuildSql();
            
            Console.WriteLine($"Complex AND/OR: {query1}");
            
            // NOT operator
            var query2 = _Repository.Query()
                .Where(p => !(p.Age < 18 || p.Age > 65))
                .BuildSql();
            
            Console.WriteLine($"NOT operator: {query2}");
        }

        private void TestDateTimeOperations()
        {
            Console.WriteLine("Testing DateTime operations...");
            
            // Note: Person doesn't have DateTime fields, so we'll test with expressions
            // These would work if Person had a CreatedDate field
            
            /*
            var query1 = _Repository.Query()
                .Where(p => p.CreatedDate.Year == 2024)
                .BuildSql();
            
            Console.WriteLine($"DateTime Year: {query1}");
            
            var query2 = _Repository.Query()
                .Where(p => p.CreatedDate.AddDays(30) > DateTime.Now)
                .BuildSql();
            
            Console.WriteLine($"DateTime AddDays: {query2}");
            */
            
            Console.WriteLine("DateTime operations require DateTime fields in the model");
        }

        private void TestMathOperations()
        {
            Console.WriteLine("Testing Math operations...");
            
            // Math operations in WHERE clause
            var query1 = _Repository.Query()
                .Where(p => p.Salary * 0.1m > 5000)
                .BuildSql();
            
            Console.WriteLine($"Math multiplication: {query1}");
            
            var query2 = _Repository.Query()
                .Where(p => p.Age + 5 > 30)
                .BuildSql();
            
            Console.WriteLine($"Math addition: {query2}");
            
            var query3 = _Repository.Query()
                .Where(p => p.Salary / 12 > 5000)
                .BuildSql();
            
            Console.WriteLine($"Math division: {query3}");
        }

        private void TestCaseWhenExpressions()
        {
            Console.WriteLine("Testing CASE WHEN expressions...");
            
            // Conditional expressions (ternary operator)
            var query1 = _Repository.Query()
                .Where(p => (p.Age > 65 ? "Senior" : "Regular") == "Senior")
                .BuildSql();
            
            Console.WriteLine($"CASE WHEN (ternary): {query1}");
        }

        private void TestStringOperations()
        {
            Console.WriteLine("Testing String operations...");
            
            // String methods
            var query1 = _Repository.Query()
                .Where(p => p.FirstName.ToUpper().Contains("JOHN"))
                .BuildSql();
            
            Console.WriteLine($"String ToUpper + Contains: {query1}");
            
            var query2 = _Repository.Query()
                .Where(p => p.Email.ToLower().EndsWith("@company.com"))
                .BuildSql();
            
            Console.WriteLine($"String ToLower + EndsWith: {query2}");
            
            var query3 = _Repository.Query()
                .Where(p => p.FirstName.Trim().Length > 3)
                .BuildSql();
            
            Console.WriteLine($"String Trim + Length: {query3}");
        }

        private void TestNullChecks()
        {
            Console.WriteLine("Testing NULL checks...");
            
            // Standard null comparison
            var query1 = _Repository.Query()
                .Where(p => p.Email != null)
                .BuildSql();
            
            Console.WriteLine($"Standard not null: {query1}");
            
            // Extension method null checks (if supported)
            var query2 = _Repository.Query()
                .Where(p => p.Email.IsNotNullOrEmpty())
                .BuildSql();
            
            Console.WriteLine($"IsNotNullOrEmpty: {query2}");
            
            var query3 = _Repository.Query()
                .Where(p => p.FirstName.IsNotNullOrWhiteSpace())
                .BuildSql();
            
            Console.WriteLine($"IsNotNullOrWhiteSpace: {query3}");
        }
    }
}