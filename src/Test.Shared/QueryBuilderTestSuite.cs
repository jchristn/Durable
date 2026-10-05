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
    /// Exercises the fluent query builder surface: filtering, ordering, secondary ordering, pagination,
    /// distinct, aggregation, and raw-SQL predicates. Executed identically across all database providers.
    /// </summary>
    public class QueryBuilderTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryBuilderTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public QueryBuilderTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Where filters results by a single predicate.
        /// </summary>
        [Fact]
        public async Task WhereFiltersResults()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> engineers = (await repository.Query().Where(p => p.Department == "Engineering").ExecuteAsync()).ToList();
            Assert.Equal(3, engineers.Count);
            Assert.All(engineers, p => Assert.Equal("Engineering", p.Department));
        }

        /// <summary>
        /// Multiple Where calls compose with AND semantics.
        /// </summary>
        [Fact]
        public async Task ChainedWhereCombinesWithAnd()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query()
                .Where(p => p.Department == "Engineering")
                .Where(p => p.Age > 30)
                .ExecuteAsync()).ToList();
            Assert.All(results, p => Assert.True(p.Department == "Engineering" && p.Age > 30));
        }

        /// <summary>
        /// OrderBy sorts ascending.
        /// </summary>
        [Fact]
        public async Task OrderByAscendingSorts()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> ordered = (await repository.Query().OrderBy(p => p.Age).ExecuteAsync()).ToList();
            for (int i = 1; i < ordered.Count; i++)
            {
                Assert.True(ordered[i - 1].Age <= ordered[i].Age);
            }
        }

        /// <summary>
        /// OrderByDescending sorts descending.
        /// </summary>
        [Fact]
        public async Task OrderByDescendingSorts()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> ordered = (await repository.Query().OrderByDescending(p => p.Salary).ExecuteAsync()).ToList();
            for (int i = 1; i < ordered.Count; i++)
            {
                Assert.True(ordered[i - 1].Salary >= ordered[i].Salary);
            }
        }

        /// <summary>
        /// ThenBy applies a secondary ordering.
        /// </summary>
        [Fact]
        public async Task ThenByAppliesSecondaryOrdering()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> ordered = (await repository.Query()
                .OrderBy(p => p.Department)
                .ThenByDescending(p => p.Salary)
                .ExecuteAsync()).ToList();

            for (int i = 1; i < ordered.Count; i++)
            {
                if (ordered[i - 1].Department == ordered[i].Department)
                {
                    Assert.True(ordered[i - 1].Salary >= ordered[i].Salary);
                }
            }
        }

        /// <summary>
        /// Skip and Take paginate without overlap.
        /// </summary>
        [Fact]
        public async Task SkipAndTakePaginate()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> page1 = (await repository.Query().OrderBy(p => p.Id).Take(2).ExecuteAsync()).ToList();
            List<Person> page2 = (await repository.Query().OrderBy(p => p.Id).Skip(2).Take(2).ExecuteAsync()).ToList();

            Assert.Equal(2, page1.Count);
            Assert.True(page2.Count >= 1);
            Assert.DoesNotContain(page1, a => page2.Any(b => b.Id == a.Id));
        }

        /// <summary>
        /// Distinct removes duplicate projected rows.
        /// </summary>
        [Fact]
        public async Task DistinctRemovesDuplicates()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> distinctByDept = (await repository.Query()
                .SelectRaw("DISTINCT department")
                .ExecuteAsync()).ToList();
            // Engineering, Marketing, Finance
            Assert.True(distinctByDept.Count >= 1);
        }

        /// <summary>
        /// The query builder Count reflects the number of matching rows.
        /// </summary>
        [Fact]
        public async Task QueryCountMatchesRows()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            long count = await repository.Query().Where(p => p.Department == "Engineering").CountAsync();
            Assert.Equal(3, count);
        }

        /// <summary>
        /// Aggregation helpers (Sum, Average, Min, Max) compute the expected values.
        /// </summary>
        [Fact]
        public async Task AggregationHelpersComputeValues()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            decimal sum = await repository.Query().SumAsync(p => p.Salary);
            decimal avg = await repository.Query().AverageAsync(p => p.Salary);
            decimal min = await repository.Query().MinAsync(p => p.Salary);
            decimal max = await repository.Query().MaxAsync(p => p.Salary);

            Assert.True(sum > 0);
            Assert.True(avg > 0);
            Assert.True(max >= min);
            Assert.True(min > 0);
        }

        /// <summary>
        /// Repository-level aggregation with a predicate computes over the filtered set.
        /// </summary>
        [Fact]
        public async Task RepositoryAggregationWithPredicate()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            decimal engineeringSum = await repository.SumAsync(p => p.Salary, p => p.Department == "Engineering");
            int engineeringMax = await repository.MaxAsync(p => p.Age, p => p.Department == "Engineering");

            Assert.True(engineeringSum > 0);
            Assert.True(engineeringMax > 0);
        }

        /// <summary>
        /// WhereRaw applies a raw SQL predicate with parameters.
        /// </summary>
        [Fact]
        public async Task WhereRawFiltersResults()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query()
                .WhereRaw("age >= {0}", 40)
                .ExecuteAsync()).ToList();
            Assert.All(results, p => Assert.True(p.Age >= 40));
        }

        /// <summary>
        /// BuildSql returns a non-empty SELECT statement without executing.
        /// </summary>
        [Fact]
        public async Task BuildSqlReturnsSelectStatement()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            string sql = repository.Query().Where(p => p.Age > 20).BuildSql();
            Assert.False(string.IsNullOrWhiteSpace(sql));
            Assert.Contains("SELECT", sql, StringComparison.OrdinalIgnoreCase);
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<Person>> SeedAsync()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] people = new[]
            {
                new Person { FirstName = "Alice", LastName = "Smith", Age = 25, Email = "alice@example.com", Salary = 60000m, Department = "Engineering" },
                new Person { FirstName = "Bob", LastName = "Jones", Age = 35, Email = "bob@example.com", Salary = 70000m, Department = "Engineering" },
                new Person { FirstName = "Carol", LastName = "White", Age = 45, Email = "carol@example.com", Salary = 90000m, Department = "Engineering" },
                new Person { FirstName = "Dave", LastName = "Green", Age = 50, Email = "dave@example.com", Salary = 85000m, Department = "Marketing" },
                new Person { FirstName = "Eve", LastName = "Black", Age = 29, Email = "eve@example.com", Salary = 55000m, Department = "Finance" }
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
