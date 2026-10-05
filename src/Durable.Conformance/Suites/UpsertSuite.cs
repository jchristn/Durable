namespace Durable.Conformance
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Upsert (<see cref="RepositoryCapabilities.Upsert"/>): insert-or-update by natural key and by identity key, single
    /// and batch, sync and async, plus version handling (<see cref="RepositoryCapabilities.OptimisticConcurrency"/>).
    /// </summary>
    internal sealed class UpsertSuite : KitSuite
    {
        public UpsertSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Upsert, Description = "Upsert inserts a new natural key, then updates it")]
        public async Task UpsertNaturalKey()
        {
            IRepository<CfUpsertItem> repository = await PrepareAsync();
            CfUpsertItem inserted = repository.Upsert(new CfUpsertItem { Code = "A", Name = "Alpha", Quantity = 1 });
            Assert.Equal("Alpha", inserted.Name);
            Assert.Equal(1L, repository.Count());
            CfUpsertItem updated = await repository.UpsertAsync(new CfUpsertItem { Code = "A", Name = "Alpha 2", Quantity = 5 }, null, Token);
            Assert.Equal("Alpha 2", updated.Name);
            Assert.Equal(1L, repository.Count());
            CfUpsertItem? stored = repository.ReadById("A");
            Assert.NotNull(stored);
            Assert.Equal("Alpha 2", stored.Name);
            Assert.Equal(5, stored.Quantity);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Upsert, Description = "UpsertMany mixes inserts and updates and returns entities in input order")]
        public async Task UpsertManyMixed()
        {
            IRepository<CfUpsertItem> repository = await PrepareAsync();
            repository.Create(new CfUpsertItem { Code = "A", Name = "Alpha", Quantity = 1 });
            List<CfUpsertItem> many = (await repository.UpsertManyAsync(new List<CfUpsertItem>
            {
                new CfUpsertItem { Code = "A", Name = "Alpha 3", Quantity = 6 },
                new CfUpsertItem { Code = "B", Name = "Beta", Quantity = 7 },
                new CfUpsertItem { Code = "C", Name = "Gamma", Quantity = 8 }
            }, null, Token)).ToList();
            Assert.Equal(new[] { "A", "B", "C" }, many.Select(x => x.Code).ToArray());
            Assert.Equal(3L, repository.Count());
            Assert.Equal("Alpha 3", repository.ReadById("A")?.Name);
            Assert.Equal(7, repository.ReadById("B")?.Quantity);
            List<CfUpsertItem> again = repository.UpsertMany(new List<CfUpsertItem> { new CfUpsertItem { Code = "C", Name = "Gamma 2", Quantity = 9 } }).ToList();
            Assert.Single(again);
            Assert.Equal("Gamma 2", repository.ReadById("C")?.Name);
            Assert.Empty(repository.UpsertMany(new List<CfUpsertItem>()));
            Assert.Equal(3L, repository.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Upsert, Description = "Upsert with an unset identity key inserts and assigns a key; with an existing key it updates")]
        public async Task UpsertIdentityKey()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            CfItem first = await repository.UpsertAsync(new CfItem { Name = "first", Category = "u", Quantity = 1 }, null, Token);
            Assert.True(first.Id > 0, "Upsert with an unset identity key must insert and assign a key");
            CfItem second = repository.Upsert(new CfItem { Name = "second", Category = "u", Quantity = 2 });
            Assert.True(second.Id > 0);
            Assert.NotEqual(first.Id, second.Id);
            Assert.Equal(2L, repository.Count());

            CfItem change = new CfItem { Id = first.Id, Name = "first-upserted", Category = "u", Quantity = 11 };
            await repository.UpsertAsync(change, null, Token);
            Assert.Equal(2L, repository.Count());
            CfItem? stored = repository.ReadById(first.Id);
            Assert.NotNull(stored);
            Assert.Equal("first-upserted", stored.Name);
            Assert.Equal(11, stored.Quantity);

            CfItem created = repository.Create(new CfItem { Name = "after-upsert", Category = "u" });
            Assert.True(created.Id != first.Id && created.Id != second.Id, "A Create after upserts must not reuse a key");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Upsert | RepositoryCapabilities.OptimisticConcurrency, Description = "Upsert initializes the version on insert and bumps it on update")]
        public async Task UpsertVersionHandling()
        {
            await ResetAsync(typeof(CfVersionedItem));
            IRepository<CfVersionedItem> repository = Repository<CfVersionedItem>();
            CfVersionedItem first = await repository.UpsertAsync(new CfVersionedItem { Name = "first", Salary = 10m }, null, Token);
            Assert.Equal(1, first.Version);
            CfVersionedItem change = new CfVersionedItem { Id = first.Id, Name = "first-upserted", Salary = 11m, Version = first.Version };
            CfVersionedItem result = await repository.UpsertAsync(change, null, Token);
            CfVersionedItem? stored = repository.ReadById(first.Id);
            Assert.NotNull(stored);
            Assert.Equal("first-upserted", stored.Name);
            Assert.Equal(11m, stored.Salary);
            Assert.Equal(stored.Version, result.Version);
            Assert.True(stored.Version > 1, "Upsert update path should bump the version (now " + stored.Version + ")");
        }

        private async Task<IRepository<CfUpsertItem>> PrepareAsync()
        {
            await ResetAsync(typeof(CfUpsertItem));
            return Repository<CfUpsertItem>();
        }
    }
}
