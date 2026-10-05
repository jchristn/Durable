namespace Test.Shared
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Coverage for explicit transactions: commit persists changes, rollback discards them, and both the
    /// synchronous and asynchronous APIs are exercised. Executed identically across all database providers.
    /// </summary>
    public class TransactionTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public TransactionTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Committing a transaction makes the inserted row visible afterwards.
        /// </summary>
        [Fact]
        public async Task CommitPersistsChanges()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            using (ITransaction transaction = repository.BeginTransaction())
            {
                repository.Create(NewPerson("tx-commit@example.com"), transaction);
                transaction.Commit();
            }

            long count = await repository.CountAsync(p => p.Email == "tx-commit@example.com");
            Assert.Equal(1, count);
        }

        /// <summary>
        /// Rolling back a transaction discards the inserted row.
        /// </summary>
        [Fact]
        public async Task RollbackDiscardsChanges()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            using (ITransaction transaction = repository.BeginTransaction())
            {
                repository.Create(NewPerson("tx-rollback@example.com"), transaction);
                transaction.Rollback();
            }

            long count = await repository.CountAsync(p => p.Email == "tx-rollback@example.com");
            Assert.Equal(0, count);
        }

        /// <summary>
        /// The asynchronous commit path persists changes.
        /// </summary>
        [Fact]
        public async Task CommitAsyncPersistsChanges()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            ITransaction transaction = await repository.BeginTransactionAsync();
            try
            {
                await repository.CreateAsync(NewPerson("tx-commit-async@example.com"), transaction);
                await transaction.CommitAsync();
            }
            finally
            {
                transaction.Dispose();
            }

            long count = await repository.CountAsync(p => p.Email == "tx-commit-async@example.com");
            Assert.Equal(1, count);
        }

        /// <summary>
        /// The asynchronous rollback path discards changes.
        /// </summary>
        [Fact]
        public async Task RollbackAsyncDiscardsChanges()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            ITransaction transaction = await repository.BeginTransactionAsync();
            try
            {
                await repository.CreateAsync(NewPerson("tx-rollback-async@example.com"), transaction);
                await transaction.RollbackAsync();
            }
            finally
            {
                transaction.Dispose();
            }

            long count = await repository.CountAsync(p => p.Email == "tx-rollback-async@example.com");
            Assert.Equal(0, count);
        }

        /// <summary>
        /// Multiple operations within a single committed transaction are all persisted.
        /// </summary>
        [Fact]
        public async Task MultipleOperationsInTransactionCommitTogether()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            using (ITransaction transaction = repository.BeginTransaction())
            {
                repository.Create(NewPerson("tx-multi-1@example.com"), transaction);
                repository.Create(NewPerson("tx-multi-2@example.com"), transaction);
                repository.Create(NewPerson("tx-multi-3@example.com"), transaction);
                transaction.Commit();
            }

            long count = await repository.CountAsync();
            Assert.Equal(3, count);
        }

        #endregion

        #region Private-Methods

        private static Person NewPerson(string email)
        {
            return new Person
            {
                FirstName = "Tx",
                LastName = "Test",
                Age = 30,
                Email = email,
                Salary = 50000m,
                Department = "Transactions"
            };
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
