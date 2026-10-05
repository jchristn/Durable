namespace Durable.Conformance
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.ConcurrencyConflictResolvers;
    using Xunit;

    /// <summary>
    /// Optimistic concurrency (<see cref="RepositoryCapabilities.OptimisticConcurrency"/>) with an integer version column:
    /// versions start at 1 and increase on every write path, stale updates throw
    /// <see cref="OptimisticConcurrencyException"/> by default, and each built-in resolver resolves the conflict. Merge
    /// resolvers receive an approximation of the original values on backends without change tracking, so only outcomes
    /// that hold for every approximation are asserted.
    /// </summary>
    internal sealed class ConcurrencySuite : KitSuite
    {
        public ConcurrencySuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "Version is 1 after create and increments on each update")]
        public async Task VersionStartsAtOneAndIncrements()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            CfVersionedItem item = await repository.CreateAsync(new CfVersionedItem { Name = "v" }, null, Token);
            Assert.Equal(1, item.Version);
            Assert.Equal(1, repository.ReadById(item.Id)?.Version);
            item.Name = "v2";
            CfVersionedItem updated = await repository.UpdateAsync(item, null, Token);
            Assert.Equal(2, updated.Version);
            Assert.Equal(2, repository.ReadById(item.Id)?.Version);
            updated.Name = "v3";
            repository.Update(updated);
            Assert.Equal(3, updated.Version);
            Assert.Equal(3, repository.ReadById(item.Id)?.Version);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "CreateMany initializes every version to 1")]
        public async Task CreateManyInitializesVersions()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            List<CfVersionedItem> created = repository.CreateMany(new List<CfVersionedItem> { new CfVersionedItem { Name = "a" }, new CfVersionedItem { Name = "b" } }).ToList();
            Assert.All(created, x => Assert.Equal(1, x.Version));
            Assert.All(repository.ReadAll(), x => Assert.Equal(1, x.Version));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "A stale update throws OptimisticConcurrencyException and changes nothing")]
        public async Task StaleUpdateThrows()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            CfVersionedItem item = await repository.CreateAsync(new CfVersionedItem { Name = "orig", Salary = 1m }, null, Token);
            CfVersionedItem first = Read(repository, item.Id);
            CfVersionedItem second = Read(repository, item.Id);
            first.Name = "first";
            await repository.UpdateAsync(first, null, Token);
            second.Name = "second";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(async () => await repository.UpdateAsync(second, null, Token));
            Assert.Throws<OptimisticConcurrencyException>(() => repository.Update(second));
            CfVersionedItem stored = Read(repository, item.Id);
            Assert.Equal("first", stored.Name);
            Assert.Equal(2, stored.Version);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "Updating a row deleted by someone else throws OptimisticConcurrencyException")]
        public async Task UpdateOfDeletedRowThrows()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            CfVersionedItem item = await repository.CreateAsync(new CfVersionedItem { Name = "doomed" }, null, Token);
            await repository.DeleteByIdAsync(item.Id, null, Token);
            item.Name = "changed";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(async () => await repository.UpdateAsync(item, null, Token));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "ClientWinsResolver writes the incoming values")]
        public async Task ClientWinsResolver()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            repository.ConflictResolver = new ClientWinsResolver<CfVersionedItem>();
            CfVersionedItem item = await repository.CreateAsync(new CfVersionedItem { Name = "orig", Salary = 1m }, null, Token);
            CfVersionedItem first = Read(repository, item.Id);
            CfVersionedItem second = Read(repository, item.Id);
            first.Name = "first";
            first.Salary = 2m;
            await repository.UpdateAsync(first, null, Token);
            second.Name = "client";
            second.Salary = 3m;
            CfVersionedItem result = await repository.UpdateAsync(second, null, Token);
            Assert.Equal("client", result.Name);
            CfVersionedItem stored = Read(repository, item.Id);
            Assert.Equal("client", stored.Name);
            Assert.Equal(3m, stored.Salary);
            Assert.Equal(3, stored.Version);
            Assert.Equal(stored.Version, result.Version);

            CfVersionedItem stale = new CfVersionedItem { Id = item.Id, Name = "sync-client", Salary = 4m, Version = 1 };
            CfVersionedItem syncResult = repository.Update(stale);
            Assert.Equal("sync-client", Read(repository, item.Id).Name);
            Assert.Equal(4, Read(repository, item.Id).Version);
            Assert.Equal(4, syncResult.Version);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "DatabaseWinsResolver keeps the stored values and returns them")]
        public async Task DatabaseWinsResolver()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            repository.ConflictResolver = new DatabaseWinsResolver<CfVersionedItem>();
            CfVersionedItem item = await repository.CreateAsync(new CfVersionedItem { Name = "orig", Salary = 1m }, null, Token);
            CfVersionedItem first = Read(repository, item.Id);
            CfVersionedItem second = Read(repository, item.Id);
            first.Name = "first";
            first.Salary = 2m;
            repository.Update(first);
            second.Name = "loser";
            second.Salary = 9m;
            CfVersionedItem result = await repository.UpdateAsync(second, null, Token);
            Assert.Equal("first", result.Name);
            Assert.Equal(2m, result.Salary);
            CfVersionedItem stored = Read(repository, item.Id);
            Assert.Equal("first", stored.Name);
            Assert.Equal(2m, stored.Salary);
            Assert.Equal(stored.Version, result.Version);
            Assert.True(stored.Version >= 2);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "MergeChangesResolver resolves the conflict and keeps changes only the other writer made")]
        public async Task MergeChangesResolver()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            repository.ConflictResolver = new MergeChangesResolver<CfVersionedItem>();
            await AssertMergeResolvesAsync(repository);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "ImprovedMergeChangesResolver resolves the conflict and keeps changes only the other writer made")]
        public async Task ImprovedMergeChangesResolver()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            repository.ConflictResolver = new ImprovedMergeChangesResolver<CfVersionedItem>();
            await AssertMergeResolvesAsync(repository);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency, Description = "UpdateMany increments versions")]
        public async Task UpdateManyIncrementsVersions()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            List<CfVersionedItem> created = repository.CreateMany(new List<CfVersionedItem> { new CfVersionedItem { Name = "a" }, new CfVersionedItem { Name = "b" } }).ToList();
            Assert.Equal(1, repository.UpdateMany(x => x.Name == "a", x => x.Salary = 5m));
            Assert.Equal(2, Read(repository, created[0].Id).Version);
            Assert.Equal(1, Read(repository, created[1].Id).Version);
            Assert.Equal(2, await repository.UpdateManyAsync(x => x.Salary >= 0m, x => { x.Nickname = "n"; return Task.CompletedTask; }, null, Token));
            Assert.Equal(3, Read(repository, created[0].Id).Version);
            Assert.Equal(2, Read(repository, created[1].Id).Version);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.OptimisticConcurrency | RepositoryCapabilities.BatchUpdate, Description = "UpdateField and BatchUpdate increment versions so held copies become stale")]
        public async Task SetBasedUpdatesIncrementVersions()
        {
            IRepository<CfVersionedItem> repository = await PrepareAsync();
            List<CfVersionedItem> created = (await repository.CreateManyAsync(new List<CfVersionedItem>
            {
                new CfVersionedItem { Name = "a", Salary = 1m },
                new CfVersionedItem { Name = "b", Salary = 2m }
            }, null, Token)).ToList();
            CfVersionedItem held = Read(repository, created[0].Id);
            Assert.Equal(1, await repository.UpdateFieldAsync(x => x.Id == created[0].Id, x => x.Name, "a2", null, Token));
            Assert.Equal(2, Read(repository, created[0].Id).Version);
            Assert.Equal(1, Read(repository, created[1].Id).Version);
            Assert.Equal(2, await repository.BatchUpdateAsync(x => x.Salary > 0m, x => new CfVersionedItem { Salary = x.Salary + 10m }, null, Token));
            Assert.Equal(3, Read(repository, created[0].Id).Version);
            Assert.Equal(2, Read(repository, created[1].Id).Version);
            Assert.Equal(12m, Read(repository, created[1].Id).Salary);
            held.Name = "stale";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(async () => await repository.UpdateAsync(held, null, Token));
        }

        private async Task AssertMergeResolvesAsync(IRepository<CfVersionedItem> repository)
        {
            CfVersionedItem item = await repository.CreateAsync(new CfVersionedItem { Name = "orig", Salary = 1m, Nickname = "nick" }, null, Token);
            CfVersionedItem first = Read(repository, item.Id);
            CfVersionedItem second = Read(repository, item.Id);
            first.Name = "first";
            repository.Update(first);
            second.Salary = 5m;
            CfVersionedItem result = await repository.UpdateAsync(second, null, Token);
            CfVersionedItem stored = Read(repository, item.Id);
            Assert.Equal("first", stored.Name);
            Assert.Equal("nick", stored.Nickname);
            Assert.Equal(stored.Version, result.Version);
            Assert.True(stored.Version >= 3, "The resolved write must bump the version past the conflicting one (was " + stored.Version + ")");
            stored.Nickname = "after";
            repository.Update(stored);
            Assert.Equal("after", Read(repository, item.Id).Nickname);
        }

        private async Task<IRepository<CfVersionedItem>> PrepareAsync()
        {
            await ResetAsync(typeof(CfVersionedItem));
            return Repository<CfVersionedItem>();
        }

        private static CfVersionedItem Read(IRepository<CfVersionedItem> repository, int id)
        {
            CfVersionedItem? item = repository.ReadById(id);
            Assert.True(item != null, "Versioned item " + id + " should exist");
            return item!;
        }
    }
}
