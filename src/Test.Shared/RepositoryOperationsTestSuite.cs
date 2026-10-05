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
    /// Positive and negative coverage for repository operations that are not exercised by the other
    /// provider-agnostic suites: raw-SQL entity mapping (<c>FromSql</c>), expression-based set updates
    /// (<c>BatchUpdate</c>), single-field updates (<c>UpdateField</c>), multi-row upserts
    /// (<c>UpsertMany</c>), and predicate-based batch deletes (<c>BatchDelete</c>). Executed identically
    /// across all database providers.
    /// </summary>
    public class RepositoryOperationsTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="RepositoryOperationsTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public RepositoryOperationsTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// FromSql maps raw SQL result rows back into entities, honoring bound parameters.
        /// </summary>
        [Fact]
        public async Task FromSqlMapsRawSqlRowsToEntities()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<Person> engineers = repository
                .FromSql("SELECT * FROM people WHERE department = @p0", null, "Engineering")
                .ToList();

            Assert.Equal(3, engineers.Count);
            Assert.All(engineers, p => Assert.Equal("Engineering", p.Department));
            Assert.All(engineers, p => Assert.True(p.Id > 0));
            Assert.All(engineers, p => Assert.False(string.IsNullOrEmpty(p.FirstName)));
        }

        /// <summary>
        /// FromSqlAsync streams raw SQL result rows as an async sequence of entities.
        /// </summary>
        [Fact]
        public async Task FromSqlAsyncStreamsRawSqlRows()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            int streamed = 0;
            await foreach (Person person in repository.FromSqlAsync(
                "SELECT * FROM people WHERE age >= @p0", null, default, 40))
            {
                Assert.True(person.Age >= 40);
                streamed++;
            }

            Assert.True(streamed >= 1);
        }

        /// <summary>
        /// FromSql returns an empty sequence (never null) when the raw query matches no rows.
        /// </summary>
        [Fact]
        public async Task FromSqlReturnsEmptyWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<Person> results = repository
                .FromSql("SELECT * FROM people WHERE department = @p0", null, "NoSuchDepartment")
                .ToList();

            Assert.Empty(results);
        }

        /// <summary>
        /// BatchUpdate applies a member-initialization update expression to every matching row.
        /// </summary>
        [Fact]
        public async Task BatchUpdateUpdatesMatchingRows()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            int updated = await repository.BatchUpdateAsync(
                p => p.Department == "Engineering",
                p => new Person { Department = "Engineering-Updated" });

            Assert.Equal(3, updated);

            long remainingEngineering = await repository.CountAsync(p => p.Department == "Engineering");
            long relabeled = await repository.CountAsync(p => p.Department == "Engineering-Updated");

            Assert.Equal(0, remainingEngineering);
            Assert.Equal(3, relabeled);
        }

        /// <summary>
        /// BatchUpdate reports zero affected rows when the predicate matches nothing.
        /// </summary>
        [Fact]
        public async Task BatchUpdateReturnsZeroWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            int updated = await repository.BatchUpdateAsync(
                p => p.Department == "NoSuchDepartment",
                p => new Person { Department = "Unreached" });

            Assert.Equal(0, updated);
        }

        /// <summary>
        /// UpdateField updates a single column across every matching row.
        /// </summary>
        [Fact]
        public async Task UpdateFieldUpdatesSingleColumn()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            int updated = await repository.UpdateFieldAsync(
                p => p.Department == "Marketing",
                p => p.Salary,
                123456m);

            Assert.Equal(1, updated);

            Person? marketing = await repository.ReadFirstOrDefaultAsync(p => p.Department == "Marketing");
            Assert.NotNull(marketing);
            Assert.Equal(123456m, marketing.Salary);
        }

        /// <summary>
        /// UpdateField reports zero affected rows when the predicate matches nothing.
        /// </summary>
        [Fact]
        public async Task UpdateFieldReturnsZeroWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            int updated = await repository.UpdateFieldAsync(
                p => p.Department == "NoSuchDepartment",
                p => p.Salary,
                1m);

            Assert.Equal(0, updated);
        }

        /// <summary>
        /// UpsertMany updates existing rows and inserts new rows in a single call. Durable's upsert writes
        /// the supplied primary key verbatim (INSERT ... ON CONFLICT(pk) DO UPDATE), so a newly inserted
        /// row must carry its own key rather than relying on auto-increment.
        /// </summary>
        [Fact]
        public async Task UpsertManyInsertsAndUpdatesInOneCall()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<Person> existing = (await repository.Query()
                .Where(p => p.Department == "Engineering")
                .ExecuteAsync()).ToList();
            Assert.Equal(3, existing.Count);

            // Update an existing row (carries a real primary key) ...
            existing[0].Salary = 111111m;

            // ... and insert a new row carrying an explicit, unused primary key in the same batch.
            const int newcomerId = 900001;
            Person newcomer = new Person
            {
                Id = newcomerId,
                FirstName = "Newton",
                LastName = "Fresh",
                Age = 28,
                Email = "newton.fresh@example.com",
                Salary = 64000m,
                Department = "Research"
            };

            List<Person> batch = new List<Person>(existing) { newcomer };

            List<Person> upserted = (await repository.UpsertManyAsync(batch)).ToList();
            Assert.Equal(4, upserted.Count);

            long total = await repository.CountAsync();
            Assert.Equal(5, total); // 4 seeded + 1 inserted

            Person? updatedExisting = await repository.ReadByIdAsync(existing[0].Id);
            Assert.NotNull(updatedExisting);
            Assert.Equal(111111m, updatedExisting.Salary);

            Person? insertedNewcomer = await repository.ReadByIdAsync(newcomerId);
            Assert.NotNull(insertedNewcomer);
            Assert.Equal("newton.fresh@example.com", insertedNewcomer.Email);
            Assert.Equal("Research", insertedNewcomer.Department);
        }

        /// <summary>
        /// BatchDelete removes every row matching the predicate and reports the affected count.
        /// </summary>
        [Fact]
        public async Task BatchDeleteRemovesMatchingRows()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            int deleted = await repository.BatchDeleteAsync(p => p.Department == "Engineering");
            Assert.Equal(3, deleted);

            long remaining = await repository.CountAsync();
            Assert.Equal(1, remaining); // only the Marketing row survives

            bool anyEngineering = await repository.ExistsAsync(p => p.Department == "Engineering");
            Assert.False(anyEngineering);
        }

        /// <summary>
        /// BatchDelete reports zero affected rows when the predicate matches nothing.
        /// </summary>
        [Fact]
        public async Task BatchDeleteReturnsZeroWhenNoMatch()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            int deleted = await repository.BatchDeleteAsync(p => p.Department == "NoSuchDepartment");
            Assert.Equal(0, deleted);

            long remaining = await repository.CountAsync();
            Assert.Equal(4, remaining);
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
                new Person { FirstName = "Carol", LastName = "White", Age = 45, Email = "carol@example.com", Salary = 80000m, Department = "Engineering" },
                new Person { FirstName = "Dave", LastName = "Green", Age = 50, Email = "dave@example.com", Salary = 85000m, Department = "Marketing" }
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
