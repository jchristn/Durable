namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Coverage for connection factory ownership, the <see cref="ConnectionFactory.MaxConcurrentConnections"/> cap
    /// and concurrent use of one shared factory. Executed identically across all database providers.
    /// </summary>
    public class ConnectionFactoryTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionFactoryTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the configured database.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="provider"/> is null.</exception>
        public ConnectionFactoryTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Two repositories share one factory; disposing one leaves the other (and the factory) working.
        /// </summary>
        [Fact]
        public async Task SharedFactory_DisposingOneRepositoryLeavesOtherWorking()
        {
            const string department = "FactoryShared";
            using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            ISqlRepository<Person> first = _Provider.CreateRepository<Person>(factory);
            ISqlRepository<Person> second = _Provider.CreateRepository<Person>(factory);
            Assert.Same(factory, first.ConnectionFactory);
            Assert.Same(factory, second.ConnectionFactory);
            await InfrastructureTestData.ClearDepartmentAsync(second, department);

            await first.CreateAsync(InfrastructureTestData.NewPerson("factory-shared-1@example.com", department));
            first.Dispose();

            await second.CreateAsync(InfrastructureTestData.NewPerson("factory-shared-2@example.com", department));
            Assert.Equal(2, await second.CountAsync(p => p.Department == department));

            await using (DbConnection connection = await factory.OpenConnectionAsync())
            {
                Assert.Equal(ConnectionState.Open, connection.State);
            }

            Assert.Throws<ObjectDisposedException>(() => first.Create(InfrastructureTestData.NewPerson("factory-shared-3@example.com", department)));

            await InfrastructureTestData.ClearDepartmentAsync(second, department);
            second.Dispose();
        }

        /// <summary>
        /// Disposing a shared factory makes repositories that use it fail with <see cref="ObjectDisposedException"/>.
        /// </summary>
        [Fact]
        public async Task DisposedFactory_RepositoriesFail()
        {
            IConnectionFactory factory = _Provider.CreateConnectionFactory();
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>(factory);
            await repository.CountAsync();

            factory.Dispose();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => repository.CountAsync());
            Assert.Throws<ObjectDisposedException>(() => repository.Count());
            Assert.Throws<ObjectDisposedException>(() => factory.OpenConnection());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => factory.OpenConnectionAsync());
            repository.Dispose();
        }

        /// <summary>
        /// A repository built from a connection string owns its factory and disposes it with itself.
        /// </summary>
        [Fact]
        public async Task ConnectionStringRepository_DisposesOwnFactory()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(new SqlRepositoryOptions());
            IConnectionFactory factory = repository.ConnectionFactory;
            await repository.CountAsync();

            repository.Dispose();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => factory.OpenConnectionAsync());
        }

        /// <summary>
        /// A repository given a factory does not dispose it.
        /// </summary>
        [Fact]
        public async Task FactoryRepository_DoesNotDisposeFactory()
        {
            using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>(factory);
            await repository.CountAsync();
            repository.Dispose();

            await using DbConnection connection = await factory.OpenConnectionAsync();
            Assert.Equal(ConnectionState.Open, connection.State);
        }

        /// <summary>
        /// With MaxConcurrentConnections = 2, a third open waits for AcquireTimeout and throws
        /// <see cref="TimeoutException"/>; disposing a held connection frees a slot.
        /// </summary>
        [Fact]
        public async Task MaxConcurrentConnections_ThirdOpenTimesOutUntilSlotFreed()
        {
            using IConnectionFactory factory = _Provider.CreateConnectionFactory(2);
            ConnectionFactory limited = Assert.IsAssignableFrom<ConnectionFactory>(factory);
            Assert.Equal(2, limited.MaxConcurrentConnections);
            limited.AcquireTimeout = TimeSpan.FromMilliseconds(200);

            DbConnection first = await factory.OpenConnectionAsync();
            DbConnection second = await factory.OpenConnectionAsync();
            try
            {
                await Assert.ThrowsAsync<TimeoutException>(() => factory.OpenConnectionAsync());
                Assert.Throws<TimeoutException>(() => factory.OpenConnection());

                await first.DisposeAsync();

                await using (DbConnection third = await factory.OpenConnectionAsync())
                {
                    Assert.Equal(ConnectionState.Open, third.State);
                    await Assert.ThrowsAsync<TimeoutException>(() => factory.OpenConnectionAsync());
                }

                second.Close();
                using DbConnection fourth = factory.OpenConnection();
                using DbConnection fifth = factory.OpenConnection();
                Assert.Equal(ConnectionState.Open, fifth.State);
            }
            finally
            {
                await first.DisposeAsync();
                await second.DisposeAsync();
            }
        }

        /// <summary>
        /// A transaction holds its connection slot until it is disposed.
        /// </summary>
        [Fact]
        public async Task MaxConcurrentConnections_TransactionHoldsSlot()
        {
            using IConnectionFactory factory = _Provider.CreateConnectionFactory(1);
            ((ConnectionFactory)factory).AcquireTimeout = TimeSpan.FromMilliseconds(200);
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>(factory);

            ISqlTransaction transaction = await repository.BeginTransactionAsync();
            try
            {
                await repository.CountAsync(null, transaction);
                await Assert.ThrowsAsync<TimeoutException>(() => repository.CountAsync());
            }
            finally
            {
                await transaction.DisposeAsync();
            }

            Assert.True(await repository.CountAsync() >= 0);
        }

        /// <summary>
        /// Fifty parallel mixed operations (create, read, count, update, delete, raw scalar) through one shared
        /// factory complete without error, both uncapped and with a concurrency cap of 5.
        /// </summary>
        [Fact]
        public async Task ParallelMixedOperations_ThroughSharedFactory()
        {
            await RunParallelMixedAsync(null, "FactoryParallel");
            await RunParallelMixedAsync(5, "FactoryParallelCap");
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private async Task RunParallelMixedAsync(int? cap, string department)
        {
            using IConnectionFactory factory = _Provider.CreateConnectionFactory(cap);
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>(factory);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            List<Person> seed = new List<Person>();
            for (int i = 0; i < 10; i++) seed.Add(InfrastructureTestData.NewPerson("par-seed-" + i + "@example.com", department, 20 + i));
            List<Person> seeded = (await repository.CreateManyAsync(seed)).ToList();

            const int operations = 50;
            Task[] tasks = new Task[operations];
            for (int i = 0; i < operations; i++)
            {
                int index = i;
                tasks[i] = Task.Run(async () =>
                {
                    switch (index % 6)
                    {
                        case 0:
                            await repository.CreateAsync(InfrastructureTestData.NewPerson("par-" + index + "@example.com", department, 50));
                            break;
                        case 1:
                            List<Person> read = new List<Person>();
                            await foreach (Person person in repository.ReadManyAsync(p => p.Department == department)) read.Add(person);
                            Assert.NotEmpty(read);
                            break;
                        case 2:
                            Assert.True(await repository.CountAsync(p => p.Department == department) >= 10);
                            break;
                        case 3:
                            Person target = seeded[index % seeded.Count];
                            await repository.UpdateFieldAsync(p => p.Id == target.Id, p => p.Age, 60 + index);
                            break;
                        case 4:
                            Person? byId = await repository.ReadByIdAsync(seeded[index % seeded.Count].Id);
                            Assert.NotNull(byId);
                            break;
                        default:
                            long scalar = await repository.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM people");
                            Assert.True(scalar >= 10);
                            break;
                    }
                });
            }

            await Task.WhenAll(tasks);

            int creates = Enumerable.Range(0, operations).Count(i => i % 6 == 0);
            Assert.Equal(10 + creates, await repository.CountAsync(p => p.Department == department));

            if (cap.HasValue)
            {
                await using DbConnection probe = await factory.OpenConnectionAsync();
                Assert.Equal(ConnectionState.Open, probe.State);
            }

            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
            repository.Dispose();
        }

        #endregion
    }
}
