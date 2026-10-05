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
    /// Group-by coverage: grouping keys, filtering before grouping, HAVING clauses, and grouped aggregates.
    /// Executed identically across all database providers.
    /// </summary>
    public class GroupByTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupByTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public GroupByTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// GroupBy produces one group per distinct key and every member carries that key.
        /// </summary>
        [Fact]
        public async Task GroupByProducesGroupsPerKey()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<IGrouping<string?, Person>> groups = (await repository.Query()
                .GroupBy(p => p.Department)
                .ExecuteAsync()).ToList();

            Assert.True(groups.Count >= 3);
            foreach (IGrouping<string?, Person> group in groups)
            {
                Assert.True(group.Count() > 0);
                Assert.All(group, p => Assert.Equal(group.Key, p.Department));
            }
        }

        /// <summary>
        /// A WHERE clause filters rows before grouping.
        /// </summary>
        [Fact]
        public async Task GroupByWithWhereFiltersFirst()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<IGrouping<string?, Person>> groups = (await repository.Query()
                .Where(p => p.Age > 30)
                .GroupBy(p => p.Department)
                .ExecuteAsync()).ToList();

            foreach (IGrouping<string?, Person> group in groups)
            {
                Assert.All(group, p => Assert.True(p.Age > 30));
            }
        }

        /// <summary>
        /// HAVING filters groups by an aggregate condition, keeping only the qualifying group keys.
        /// Seed data: IT (3), HR (2), Sales (3); HAVING COUNT(*) &gt; 2 keeps IT and Sales.
        /// </summary>
        [Fact]
        public async Task GroupByWithHavingFiltersGroups()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<IGrouping<string?, Person>> groups = (await repository.Query()
                .GroupBy(p => p.Department)
                .Having(g => g.Count() > 2)
                .ExecuteAsync()).ToList();

            List<string?> keys = groups.Select(g => g.Key).ToList();
            Assert.Contains("IT", keys);
            Assert.Contains("Sales", keys);
            Assert.DoesNotContain("HR", keys);
        }

        /// <summary>
        /// Grouped aggregate helpers compute across the grouped set.
        /// </summary>
        [Fact]
        public async Task GroupedAggregatesComputeValues()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            IGroupedQueryBuilder<Person, string?> grouped = repository.Query().GroupBy(p => p.Department);

            decimal totalSalary = await grouped.SumAsync(p => p.Salary);
            Assert.True(totalSalary > 0);

            List<IGrouping<string?, Person>> groups = (await repository.Query()
                .GroupBy(p => p.Department)
                .ExecuteAsync()).ToList();

            foreach (IGrouping<string?, Person> group in groups)
            {
                decimal sum = group.Sum(p => p.Salary);
                decimal avg = group.Average(p => p.Salary);
                decimal min = group.Min(p => p.Salary);
                decimal max = group.Max(p => p.Salary);

                Assert.True(sum > 0);
                Assert.True(avg > 0);
                Assert.True(max >= min);
            }
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<Person>> SeedAsync()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john.doe@company.com", Salary = 75000m, Department = "IT" },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane.smith@company.com", Salary = 72000m, Department = "IT" },
                new Person { FirstName = "David", LastName = "Johnson", Age = 35, Email = "david.johnson@company.com", Salary = 85000m, Department = "IT" },
                new Person { FirstName = "Michael", LastName = "Brown", Age = 42, Email = "michael.brown@company.com", Salary = 65000m, Department = "HR" },
                new Person { FirstName = "Emily", LastName = "Davis", Age = 38, Email = "emily.davis@company.com", Salary = 68000m, Department = "HR" },
                new Person { FirstName = "Lisa", LastName = "Garcia", Age = 29, Email = "lisa.garcia@company.com", Salary = 55000m, Department = "Sales" },
                new Person { FirstName = "Mark", LastName = "Rodriguez", Age = 33, Email = "mark.rodriguez@company.com", Salary = 58000m, Department = "Sales" },
                new Person { FirstName = "Amanda", LastName = "Martinez", Age = 27, Email = "amanda.martinez@company.com", Salary = 52000m, Department = "Sales" }
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
