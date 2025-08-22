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
            Console.WriteLine("✓ Testing IN operator");
            
            // Test with array
            var departments = new[] { "IT", "HR", "Finance" };
            var query1 = _Repository.Query()
                .Where(p => departments.Contains(p.Department))
                .BuildSql();
            
            // Test with list
            var ageList = new List<int> { 25, 30, 35 };
            var query2 = _Repository.Query()
                .Where(p => ageList.Contains(p.Age))
                .BuildSql();
            
            // Test extension method (if supported)
            var query3 = _Repository.Query()
                .Where(p => p.Age.In(25, 30, 35))
                .BuildSql();
            
            // Validate queries contain expected SQL
            if (!query1.Contains("IN (") || !query2.Contains("IN (") || !query3.Contains("IN ("))
                throw new Exception("IN operator tests failed - generated SQL missing IN clause");
        }

        private void TestBetweenOperator()
        {
            Console.WriteLine("✓ Testing BETWEEN operator");
            
            // Test Between extension method
            var query1 = _Repository.Query()
                .Where(p => p.Age.Between(25, 65))
                .BuildSql();
            
            var query2 = _Repository.Query()
                .Where(p => p.Salary.Between(50000m, 100000m))
                .BuildSql();
            
            // Validate queries contain expected SQL
            if (!query1.Contains("BETWEEN") || !query2.Contains("BETWEEN"))
                throw new Exception("BETWEEN operator tests failed - generated SQL missing BETWEEN clause");
        }

        private void TestComplexBooleanLogic()
        {
            Console.WriteLine("✓ Testing complex boolean logic");
            
            // Complex nested AND/OR
            var query1 = _Repository.Query()
                .Where(p => (p.Age > 30 && p.Department == "IT") || 
                           (p.Age < 25 && p.Salary > 70000))
                .BuildSql();
            
            // NOT operator
            var query2 = _Repository.Query()
                .Where(p => !(p.Age < 18 || p.Age > 65))
                .BuildSql();
            
            // Validate queries contain expected SQL
            if (!query1.Contains("AND") || !query1.Contains("OR") || !query2.Contains("NOT"))
                throw new Exception("Boolean logic tests failed - generated SQL missing expected operators");
        }

        private void TestDateTimeOperations()
        {
            Console.WriteLine("✓ DateTime operations (skipped - requires DateTime fields)");
            
            // Note: Person doesn't have DateTime fields, so we'll test with expressions
            // These would work if Person had a CreatedDate field
            
            /*
            var query1 = _Repository.Query()
                .Where(p => p.CreatedDate.Year == 2024)
                .BuildSql();
            
            var query2 = _Repository.Query()
                .Where(p => p.CreatedDate.AddDays(30) > DateTime.Now)
                .BuildSql();
            */
        }

        private void TestMathOperations()
        {
            Console.WriteLine("✓ Testing Math operations");
            
            // Math operations in WHERE clause
            var query1 = _Repository.Query()
                .Where(p => p.Salary * 0.1m > 5000)
                .BuildSql();
            
            var query2 = _Repository.Query()
                .Where(p => p.Age + 5 > 30)
                .BuildSql();
            
            var query3 = _Repository.Query()
                .Where(p => p.Salary / 12 > 5000)
                .BuildSql();
            
            // Validate queries contain expected SQL operators
            if (!query1.Contains("*") || !query2.Contains("+") || !query3.Contains("/"))
                throw new Exception("Math operations tests failed - generated SQL missing math operators");
        }

        private void TestCaseWhenExpressions()
        {
            Console.WriteLine("✓ Testing CASE WHEN expressions");
            
            // Conditional expressions (ternary operator)
            var query1 = _Repository.Query()
                .Where(p => (p.Age > 65 ? "Senior" : "Regular") == "Senior")
                .BuildSql();
            
            // Validate query contains expected SQL
            if (!query1.Contains("CASE WHEN"))
                throw new Exception("CASE WHEN tests failed - generated SQL missing CASE WHEN clause");
        }

        private void TestStringOperations()
        {
            Console.WriteLine("✓ Testing String operations");
            
            // String methods
            var query1 = _Repository.Query()
                .Where(p => p.FirstName.ToUpper().Contains("JOHN"))
                .BuildSql();
            
            var query2 = _Repository.Query()
                .Where(p => p.Email.ToLower().EndsWith("@company.com"))
                .BuildSql();
            
            var query3 = _Repository.Query()
                .Where(p => p.FirstName.Trim().Length > 3)
                .BuildSql();
            
            // Validate queries contain expected SQL functions
            if (!query1.Contains("UPPER") || !query2.Contains("LOWER") || !query3.Contains("TRIM") || !query3.Contains("LENGTH"))
                throw new Exception("String operations tests failed - generated SQL missing expected functions");
        }

        private void TestNullChecks()
        {
            Console.WriteLine("✓ Testing NULL checks");
            
            // Standard null comparison
            var query1 = _Repository.Query()
                .Where(p => p.Email != null)
                .BuildSql();
            
            // Extension method null checks (if supported)
            var query2 = _Repository.Query()
                .Where(p => p.Email.IsNotNullOrEmpty())
                .BuildSql();
            
            var query3 = _Repository.Query()
                .Where(p => p.FirstName.IsNotNullOrWhiteSpace())
                .BuildSql();
            
            // Validate queries contain expected SQL
            if (!query1.Contains("IS NOT NULL") || !query2.Contains("IS NOT NULL") || !query3.Contains("IS NOT NULL"))
                throw new Exception("NULL checks tests failed - generated SQL missing NULL check clauses");
        }
    }
}