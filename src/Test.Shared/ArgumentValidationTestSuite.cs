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
    /// Negative and edge-case coverage for the repository surface: not-found lookups, empty result sets,
    /// single-result violations, and no-op mutations. Executed identically across all database providers.
    /// </summary>
    public class ArgumentValidationTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentValidationTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public ArgumentValidationTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// ReadById returns null for an identifier that does not exist.
        /// </summary>
        [Fact]
        public async Task ReadByIdReturnsNullWhenNotFound()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            Person? result = await repository.ReadByIdAsync(999999);
            Assert.Null(result);
        }

        /// <summary>
        /// ReadFirstOrDefault returns null when no entity matches the predicate.
        /// </summary>
        [Fact]
        public async Task ReadFirstOrDefaultReturnsNullWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            Person? result = await repository.ReadFirstOrDefaultAsync(p => p.Department == "NoSuchDepartment");
            Assert.Null(result);
        }

        /// <summary>
        /// Count returns zero when no rows exist.
        /// </summary>
        [Fact]
        public async Task CountReturnsZeroWhenEmpty()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");
            long count = await repository.CountAsync();
            Assert.Equal(0, count);
        }

        /// <summary>
        /// Count with a predicate returns zero when nothing matches.
        /// </summary>
        [Fact]
        public async Task CountWithPredicateReturnsZeroWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            long count = await repository.CountAsync(p => p.Age > 1000);
            Assert.Equal(0, count);
        }

        /// <summary>
        /// ReadMany returns an empty sequence (never null) when nothing matches.
        /// </summary>
        [Fact]
        public async Task ReadManyReturnsEmptyWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            List<Person> results = (await repository.Query().Where(p => p.Age > 1000).ExecuteAsync()).ToList();
            Assert.Empty(results);
        }

        /// <summary>
        /// Exists returns false when no entity matches the predicate.
        /// </summary>
        [Fact]
        public async Task ExistsReturnsFalseWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            bool exists = await repository.ExistsAsync(p => p.Email == "nobody@nowhere.test");
            Assert.False(exists);
        }

        /// <summary>
        /// ExistsById returns false for a non-existent identifier.
        /// </summary>
        [Fact]
        public async Task ExistsByIdReturnsFalseWhenNotFound()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            bool exists = await repository.ExistsByIdAsync(999999);
            Assert.False(exists);
        }

        /// <summary>
        /// DeleteById returns false when the identifier does not exist.
        /// </summary>
        [Fact]
        public async Task DeleteByIdReturnsFalseWhenNotFound()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            bool deleted = await repository.DeleteByIdAsync(999999);
            Assert.False(deleted);
        }

        /// <summary>
        /// DeleteMany returns zero when the predicate matches nothing.
        /// </summary>
        [Fact]
        public async Task DeleteManyReturnsZeroWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            int deleted = await repository.DeleteManyAsync(p => p.Department == "NoSuchDepartment");
            Assert.Equal(0, deleted);
        }

        /// <summary>
        /// ReadSingle throws when more than one entity matches the predicate.
        /// </summary>
        [Fact]
        public async Task ReadSingleThrowsWhenMultipleMatches()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            Exception? exception = await Record.ExceptionAsync(() => repository.ReadSingleAsync(p => p.Department == "Engineering"));
            Assert.NotNull(exception);
        }

        /// <summary>
        /// ReadSingle throws when no entity matches the predicate.
        /// </summary>
        [Fact]
        public async Task ReadSingleThrowsWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            Exception? exception = await Record.ExceptionAsync(() => repository.ReadSingleAsync(p => p.Department == "NoSuchDepartment"));
            Assert.NotNull(exception);
        }

        /// <summary>
        /// ReadSingleOrDefault returns null when nothing matches, but throws when more than one matches.
        /// </summary>
        [Fact]
        public async Task ReadSingleOrDefaultNullWhenNoneThrowsWhenMultiple()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            Person? none = await repository.ReadSingleOrDefaultAsync(p => p.Department == "NoSuchDepartment");
            Assert.Null(none);

            Exception? exception = await Record.ExceptionAsync(() => repository.ReadSingleOrDefaultAsync(p => p.Department == "Engineering"));
            Assert.NotNull(exception);
        }

        /// <summary>
        /// Deleting an entity whose identifier no longer exists reports no deletion.
        /// </summary>
        [Fact]
        public async Task DeleteNonExistentEntityReturnsFalse()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            Person ghost = new Person { Id = 999999, FirstName = "Ghost", LastName = "Ghost", Age = 20, Email = "ghost@example.com", Salary = 1m, Department = "None" };
            bool deleted = await repository.DeleteAsync(ghost);
            Assert.False(deleted);
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
                new Person { FirstName = "Carol", LastName = "White", Age = 45, Email = "carol@example.com", Salary = 80000m, Department = "Marketing" }
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
