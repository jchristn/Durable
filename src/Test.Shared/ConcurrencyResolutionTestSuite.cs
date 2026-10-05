namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.ConcurrencyConflictResolvers;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Optimistic concurrency coverage on every provider: initial version, version bump on Update, default conflict
    /// behavior (throw), ClientWinsResolver retry, deleted-row conflicts, and version bumps from UpdateField and
    /// BatchUpdate.
    /// </summary>
    public class ConcurrencyResolutionTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyResolutionTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public ConcurrencyResolutionTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Create initializes an unset integer version to 1 and Update increments it.
        /// </summary>
        [Fact]
        public async Task VersionInitializedAndIncremented()
        {
            ISqlRepository<RelVersionedItem> repository = await PrepareAsync();
            RelVersionedItem item = await repository.CreateAsync(new RelVersionedItem { Name = "v" });
            Assert.Equal(1, item.Version);
            Assert.Equal(1, (await repository.ReadByIdAsync(item.Id))?.Version);

            item.Name = "v2";
            RelVersionedItem updated = await repository.UpdateAsync(item);
            Assert.Equal(2, updated.Version);
            Assert.Equal(2, (await repository.ReadByIdAsync(item.Id))?.Version);

            updated.Name = "v3";
            repository.Update(updated);
            Assert.Equal(3, (await repository.ReadByIdAsync(item.Id))?.Version);
        }

        /// <summary>
        /// A stale update throws OptimisticConcurrencyException by default (sync and async) and leaves the row intact.
        /// </summary>
        [Fact]
        public async Task StaleUpdateThrowsByDefault()
        {
            ISqlRepository<RelVersionedItem> repository = await PrepareAsync();
            RelVersionedItem item = await repository.CreateAsync(new RelVersionedItem { Name = "orig", Salary = 1m });

            RelVersionedItem? first = await repository.ReadByIdAsync(item.Id);
            RelVersionedItem? second = await repository.ReadByIdAsync(item.Id);
            Assert.NotNull(first);
            Assert.NotNull(second);

            first.Name = "first";
            await repository.UpdateAsync(first);

            second.Name = "second";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(async () => await repository.UpdateAsync(second));
            Assert.Throws<OptimisticConcurrencyException>(() => repository.Update(second));

            RelVersionedItem? stored = await repository.ReadByIdAsync(item.Id);
            Assert.Equal("first", stored?.Name);
            Assert.Equal(2, stored?.Version);
        }

        /// <summary>
        /// Updating a row that another client deleted throws OptimisticConcurrencyException.
        /// </summary>
        [Fact]
        public async Task UpdateOfDeletedRowThrows()
        {
            ISqlRepository<RelVersionedItem> repository = await PrepareAsync();
            RelVersionedItem item = await repository.CreateAsync(new RelVersionedItem { Name = "doomed" });
            await repository.DeleteByIdAsync(item.Id);
            item.Name = "changed";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(async () => await repository.UpdateAsync(item));
        }

        /// <summary>
        /// With ClientWinsResolver a stale update is retried against the current version and succeeds with the client's
        /// values (sync and async).
        /// </summary>
        [Fact]
        public async Task ClientWinsResolverRetriesAndSucceeds()
        {
            ISqlRepository<RelVersionedItem> repository = await PrepareAsync();
            repository.ConflictResolver = new ClientWinsResolver<RelVersionedItem>();
            RelVersionedItem item = await repository.CreateAsync(new RelVersionedItem { Name = "orig", Salary = 1m });

            RelVersionedItem? first = await repository.ReadByIdAsync(item.Id);
            RelVersionedItem? second = await repository.ReadByIdAsync(item.Id);
            Assert.NotNull(first);
            Assert.NotNull(second);

            first.Name = "first";
            first.Salary = 2m;
            await repository.UpdateAsync(first);

            second.Name = "client";
            second.Salary = 3m;
            RelVersionedItem result = await repository.UpdateAsync(second);
            Assert.Equal("client", result.Name);

            RelVersionedItem? stored = await repository.ReadByIdAsync(item.Id);
            Assert.NotNull(stored);
            Assert.Equal("client", stored.Name);
            Assert.Equal(3m, stored.Salary);
            Assert.Equal(3, stored.Version);

            RelVersionedItem stale = new RelVersionedItem { Id = item.Id, Name = "sync-client", Salary = 4m, Version = 1 };
            RelVersionedItem syncResult = repository.Update(stale);
            Assert.Equal("sync-client", (await repository.ReadByIdAsync(item.Id))?.Name);
            Assert.Equal(4, (await repository.ReadByIdAsync(item.Id))?.Version);
            Assert.Equal(4, syncResult.Version);
        }

        /// <summary>
        /// UpdateField and BatchUpdate bump integer versions so that holders of the old version get a conflict.
        /// </summary>
        [Fact]
        public async Task UpdateFieldAndBatchUpdateBumpVersion()
        {
            ISqlRepository<RelVersionedItem> repository = await PrepareAsync();
            List<RelVersionedItem> created = (await repository.CreateManyAsync(new List<RelVersionedItem>
            {
                new RelVersionedItem { Name = "a", Salary = 1m },
                new RelVersionedItem { Name = "b", Salary = 2m }
            })).ToList();
            Assert.All(created, x => Assert.Equal(1, x.Version));

            RelVersionedItem? held = await repository.ReadByIdAsync(created[0].Id);
            Assert.NotNull(held);

            Assert.Equal(1, await repository.UpdateFieldAsync(x => x.Id == created[0].Id, x => x.Name, "a2"));
            Assert.Equal(2, (await repository.ReadByIdAsync(created[0].Id))?.Version);
            Assert.Equal(1, (await repository.ReadByIdAsync(created[1].Id))?.Version);

            Assert.Equal(2, await repository.BatchUpdateAsync(x => x.Salary > 0m, x => new RelVersionedItem { Salary = x.Salary + 10m }));
            Assert.Equal(3, (await repository.ReadByIdAsync(created[0].Id))?.Version);
            Assert.Equal(2, (await repository.ReadByIdAsync(created[1].Id))?.Version);

            held.Name = "stale";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(async () => await repository.UpdateAsync(held));
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<RelVersionedItem>> PrepareAsync()
        {
            ISqlRepository<RelVersionedItem> repository = _Provider.CreateRepository<RelVersionedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            return repository;
        }

        #endregion
    }
}
