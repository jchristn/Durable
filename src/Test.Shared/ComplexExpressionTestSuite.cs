namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Coverage for complex predicate translation: boolean logic, range checks, string operations,
    /// membership, and null checks. Executed identically across all database providers.
    /// </summary>
    public class ComplexExpressionTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ComplexExpressionTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public ComplexExpressionTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Conjunction (AND) narrows results.
        /// </summary>
        [Fact]
        public async Task AndConditionNarrowsResults()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query()
                .Where(p => p.Age >= 30 && p.Salary > 60000m)
                .ExecuteAsync()).ToList();
            Assert.All(results, p => Assert.True(p.Age >= 30 && p.Salary > 60000m));
        }

        /// <summary>
        /// Disjunction (OR) broadens results.
        /// </summary>
        [Fact]
        public async Task OrConditionBroadensResults()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query()
                .Where(p => p.Department == "IT" || p.Department == "HR")
                .ExecuteAsync()).ToList();
            Assert.NotEmpty(results);
            Assert.All(results, p => Assert.True(p.Department == "IT" || p.Department == "HR"));
        }

        /// <summary>
        /// Range comparisons behave like BETWEEN.
        /// </summary>
        [Fact]
        public async Task RangeConditionFiltersInclusiveBounds()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query()
                .Where(p => p.Age >= 30 && p.Age <= 40)
                .ExecuteAsync()).ToList();
            Assert.All(results, p => Assert.True(p.Age >= 30 && p.Age <= 40));
        }

        /// <summary>
        /// String StartsWith translates to a prefix match.
        /// </summary>
        [Fact]
        public async Task StringStartsWithFilters()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query()
                .Where(p => p.FirstName.StartsWith("A"))
                .ExecuteAsync()).ToList();
            Assert.All(results, p => Assert.StartsWith("A", p.FirstName));
        }

        /// <summary>
        /// String Contains translates to a substring match.
        /// </summary>
        [Fact]
        public async Task StringContainsFilters()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query()
                .Where(p => p.Email!.Contains("company.com"))
                .ExecuteAsync()).ToList();
            Assert.All(results, p => Assert.Contains("company.com", p.Email));
        }

        /// <summary>
        /// Negation excludes matching rows.
        /// </summary>
        [Fact]
        public async Task NotConditionExcludesRows()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query()
                .Where(p => p.Department != "IT")
                .ExecuteAsync()).ToList();
            Assert.All(results, p => Assert.NotEqual("IT", p.Department));
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<Person>> SeedAsync()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "Alice", LastName = "Anderson", Age = 28, Email = "alice@company.com", Salary = 72000m, Department = "IT" },
                new Person { FirstName = "Aaron", LastName = "Adams", Age = 34, Email = "aaron@company.com", Salary = 81000m, Department = "IT" },
                new Person { FirstName = "Bob", LastName = "Baker", Age = 45, Email = "bob@company.com", Salary = 68000m, Department = "HR" },
                new Person { FirstName = "Carol", LastName = "Chen", Age = 39, Email = "carol@company.com", Salary = 90000m, Department = "Finance" },
                new Person { FirstName = "Dave", LastName = "Davis", Age = 52, Email = "dave@company.com", Salary = 55000m, Department = "Sales" }
            };

            await repository.CreateManyAsync(people);
            return repository;
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
