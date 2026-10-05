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
    /// Projection coverage: mapping entities into result types, computed members, and projection combined with
    /// filtering, ordering, and pagination. Executed identically across all database providers.
    /// </summary>
    public class ProjectionTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ProjectionTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public ProjectionTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Select maps each entity into the projected result type with populated fields.
        /// </summary>
        [Fact]
        public async Task SelectProjectsIntoResultType()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<PersonSummary> summaries = (await repository.Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email,
                    Salary = p.Salary
                })
                .ExecuteAsync()).ToList();

            Assert.True(summaries.Count >= 5);
            foreach (PersonSummary summary in summaries)
            {
                Assert.False(string.IsNullOrEmpty(summary.FirstName));
                Assert.False(string.IsNullOrEmpty(summary.LastName));
                Assert.True(summary.Salary > 0);
            }
        }

        /// <summary>
        /// Select can project a subset of columns into a different result type.
        /// </summary>
        [Fact]
        public async Task SelectProjectsColumnSubset()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<DepartmentInfo> info = (await repository.Query()
                .Select(p => new DepartmentInfo
                {
                    Department = p.Department,
                    Salary = p.Salary
                })
                .ExecuteAsync()).ToList();

            Assert.True(info.Count >= 5);
            Assert.All(info, i =>
            {
                Assert.False(string.IsNullOrEmpty(i.Department));
                Assert.True(i.Salary > 0);
            });
        }

        /// <summary>
        /// A WHERE clause filters the source rows before projection.
        /// </summary>
        [Fact]
        public async Task SelectWithFilterProjectsSubset()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<DepartmentInfo> itInfo = (await repository.Query()
                .Where(p => p.Department == "IT")
                .Select(p => new DepartmentInfo
                {
                    Department = p.Department,
                    Salary = p.Salary,
                    EmployeeName = p.FirstName + " " + p.LastName
                })
                .ExecuteAsync()).ToList();

            Assert.NotEmpty(itInfo);
            Assert.All(itInfo, i => Assert.Equal("IT", i.Department));
        }

        /// <summary>
        /// Projection composes with ordering and pagination.
        /// </summary>
        [Fact]
        public async Task SelectWithOrderingAndPagination()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<PersonSummary> page = (await repository.Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email,
                    Salary = p.Salary
                })
                .OrderByDescending(s => s.Salary)
                .Take(3)
                .ExecuteAsync()).ToList();

            Assert.True(page.Count <= 3);
            for (int i = 1; i < page.Count; i++)
            {
                Assert.True(page[i - 1].Salary >= page[i].Salary);
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
                new Person { FirstName = "Alice", LastName = "Anderson", Age = 28, Email = "alice.anderson@company.com", Salary = 72000m, Department = "IT" },
                new Person { FirstName = "Bob", LastName = "Baker", Age = 32, Email = "bob.baker@company.com", Salary = 78000m, Department = "IT" },
                new Person { FirstName = "Carol", LastName = "Chen", Age = 29, Email = "carol.chen@company.com", Salary = 75000m, Department = "IT" },
                new Person { FirstName = "David", LastName = "Davis", Age = 45, Email = "david.davis@company.com", Salary = 68000m, Department = "HR" },
                new Person { FirstName = "Emma", LastName = "Evans", Age = 38, Email = "emma.evans@company.com", Salary = 65000m, Department = "HR" },
                new Person { FirstName = "Frank", LastName = "Foster", Age = 26, Email = "frank.foster@company.com", Salary = 55000m, Department = "Sales" }
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
