namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Composite primary keys (<see cref="RepositoryCapabilities.CompositeKeys"/>): keys are object arrays in KeyOrder;
    /// create, read, update, delete and exists target exactly one row; duplicates are rejected; wrong arity throws
    /// <see cref="ArgumentException"/>; upsert matches on the full key (<see cref="RepositoryCapabilities.Upsert"/>).
    /// </summary>
    internal sealed class CompositeKeySuite : KitSuite
    {
        public CompositeKeySuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.CompositeKeys, Description = "Key columns follow KeyOrder")]
        public void KeyColumnsFollowKeyOrder()
        {
            IRepository<CfCompositeItem> repository = Repository<CfCompositeItem>();
            Assert.True(repository.Metadata.HasCompositeKey);
            Assert.Equal(new[] { "TenantId", "Sku" }, repository.Metadata.KeyColumns.Select(c => c.Property.Name).ToArray());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.CompositeKeys, Description = "Create and ReadById by composite key")]
        public async Task CreateAndReadById()
        {
            IRepository<CfCompositeItem> repository = await PrepareAsync();
            await repository.CreateAsync(new CfCompositeItem { TenantId = 1, Sku = "A", Name = "One-A", Quantity = 1 }, null, Token);
            await repository.CreateAsync(new CfCompositeItem { TenantId = 1, Sku = "B", Name = "One-B", Quantity = 2 }, null, Token);
            repository.Create(new CfCompositeItem { TenantId = 2, Sku = "A", Name = "Two-A", Quantity = 3 });
            Assert.Equal("One-A", (await repository.ReadByIdAsync(new object[] { 1, "A" }, null, Token))?.Name);
            CfCompositeItem? twoA = repository.ReadById(new object[] { 2, "A" });
            Assert.NotNull(twoA);
            Assert.Equal("Two-A", twoA.Name);
            Assert.Equal(3, twoA.Quantity);
            Assert.Null(await repository.ReadByIdAsync(new object[] { 2, "B" }, null, Token));
            Assert.Equal(3L, repository.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.CompositeKeys, Description = "A duplicate composite key is rejected and the original row is kept")]
        public async Task DuplicateKeyIsRejected()
        {
            IRepository<CfCompositeItem> repository = await PrepareAsync();
            await repository.CreateAsync(new CfCompositeItem { TenantId = 5, Sku = "DUP", Name = "First" }, null, Token);
            await Assert.ThrowsAnyAsync<Exception>(async () => await repository.CreateAsync(new CfCompositeItem { TenantId = 5, Sku = "DUP", Name = "Second" }, null, Token));
            Assert.Equal("First", repository.ReadById(new object[] { 5, "DUP" })?.Name);
            Assert.Equal(1L, repository.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.CompositeKeys, Description = "Update changes exactly one row")]
        public async Task UpdateTargetsOneRow()
        {
            IRepository<CfCompositeItem> repository = await PrepareAsync();
            await repository.CreateManyAsync(new List<CfCompositeItem>
            {
                new CfCompositeItem { TenantId = 1, Sku = "X", Name = "1X", Quantity = 1 },
                new CfCompositeItem { TenantId = 2, Sku = "X", Name = "2X", Quantity = 1 },
                new CfCompositeItem { TenantId = 1, Sku = "Y", Name = "1Y", Quantity = 1 }
            }, null, Token);
            CfCompositeItem? item = await repository.ReadByIdAsync(new object[] { 2, "X" }, null, Token);
            Assert.NotNull(item);
            item.Name = "2X-updated";
            item.Quantity = 99;
            await repository.UpdateAsync(item, null, Token);
            Assert.Equal("2X-updated", repository.ReadById(new object[] { 2, "X" })?.Name);
            Assert.Equal("1X", repository.ReadById(new object[] { 1, "X" })?.Name);
            Assert.Equal("1Y", repository.ReadById(new object[] { 1, "Y" })?.Name);
            Assert.Equal(1L, repository.Count(x => x.Quantity == 99));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.CompositeKeys, Description = "Delete / DeleteById / ExistsById by composite key")]
        public async Task DeleteAndExistsByKey()
        {
            IRepository<CfCompositeItem> repository = await PrepareAsync();
            await repository.CreateManyAsync(new List<CfCompositeItem>
            {
                new CfCompositeItem { TenantId = 1, Sku = "D1", Name = "a" },
                new CfCompositeItem { TenantId = 1, Sku = "D2", Name = "b" },
                new CfCompositeItem { TenantId = 2, Sku = "D1", Name = "c" }
            }, null, Token);
            Assert.True(await repository.ExistsByIdAsync(new object[] { 1, "D1" }, null, Token));
            Assert.True(repository.ExistsById(new object[] { 2, "D1" }));
            Assert.False(repository.ExistsById(new object[] { 2, "D2" }));
            CfCompositeItem? toDelete = repository.ReadById(new object[] { 1, "D1" });
            Assert.NotNull(toDelete);
            Assert.True(await repository.DeleteAsync(toDelete, null, Token));
            Assert.False(repository.ExistsById(new object[] { 1, "D1" }));
            Assert.True(repository.ExistsById(new object[] { 2, "D1" }));
            Assert.True(repository.DeleteById(new object[] { 2, "D1" }));
            Assert.False(await repository.DeleteByIdAsync(new object[] { 2, "D1" }, null, Token));
            Assert.Equal(1L, repository.Count());
            Assert.True(repository.Exists(x => x.TenantId == 1 && x.Sku == "D2"));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.CompositeKeys, Description = "Predicates and ordering over key parts")]
        public async Task QueryOverKeyParts()
        {
            IRepository<CfCompositeItem> repository = await PrepareAsync();
            await repository.CreateManyAsync(new List<CfCompositeItem>
            {
                new CfCompositeItem { TenantId = 2, Sku = "B", Name = "2B" },
                new CfCompositeItem { TenantId = 1, Sku = "B", Name = "1B" },
                new CfCompositeItem { TenantId = 2, Sku = "A", Name = "2A" },
                new CfCompositeItem { TenantId = 1, Sku = "A", Name = "1A" }
            }, null, Token);
            ConformanceAssert.Sequence(repository.Query().OrderBy(x => x.TenantId).ThenBy(x => x.Sku).Execute().Select(x => x.Name), new[] { "1A", "1B", "2A", "2B" }, "OrderBy(TenantId).ThenBy(Sku)");
            ConformanceAssert.NameSet(repository.ReadMany(x => x.Sku == "A").Select(x => x.Name), new[] { "1A", "2A" }, "ReadMany(Sku == A)");
            Assert.Equal("2B", repository.ReadSingle(x => x.TenantId == 2 && x.Sku == "B").Name);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.CompositeKeys, Description = "A key with the wrong number of values throws ArgumentException")]
        public async Task WrongKeyArityThrows()
        {
            IRepository<CfCompositeItem> repository = await PrepareAsync();
            await ConformanceAssert.ThrowsAsync<ArgumentException>(() => repository.ReadByIdAsync(new object[] { 1 }, null, Token), "ReadByIdAsync with one key value");
            await ConformanceAssert.ThrowsAsync<ArgumentException>(() => repository.ReadByIdAsync(new object[] { 1, "A", "extra" }, null, Token), "ReadByIdAsync with three key values");
            await ConformanceAssert.ThrowsAsync<ArgumentException>(() => repository.ReadByIdAsync(1, null, Token), "ReadByIdAsync with a scalar key");
            ConformanceAssert.Throws<ArgumentException>(() => repository.DeleteById(new object[] { 1 }), "DeleteById with one key value");
            ConformanceAssert.Throws<ArgumentException>(() => repository.ExistsById(new object[] { 1, "A", 3 }), "ExistsById with three key values");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.CompositeKeys | RepositoryCapabilities.Upsert, Description = "Upsert and UpsertMany match on the full composite key")]
        public async Task UpsertByCompositeKey()
        {
            IRepository<CfCompositeItem> repository = await PrepareAsync();
            await repository.UpsertAsync(new CfCompositeItem { TenantId = 7, Sku = "U", Name = "Inserted", Quantity = 1 }, null, Token);
            Assert.Equal("Inserted", repository.ReadById(new object[] { 7, "U" })?.Name);
            CfCompositeItem updated = await repository.UpsertAsync(new CfCompositeItem { TenantId = 7, Sku = "U", Name = "Updated", Quantity = 2 }, null, Token);
            Assert.Equal("Updated", updated.Name);
            Assert.Equal(1L, repository.Count());
            repository.UpsertMany(new List<CfCompositeItem>
            {
                new CfCompositeItem { TenantId = 7, Sku = "U", Name = "Again", Quantity = 3 },
                new CfCompositeItem { TenantId = 8, Sku = "U", Name = "New", Quantity = 4 }
            }).ToList();
            Assert.Equal(2L, repository.Count());
            Assert.Equal("Again", repository.ReadById(new object[] { 7, "U" })?.Name);
            Assert.Equal("New", repository.ReadById(new object[] { 8, "U" })?.Name);
        }

        private async Task<IRepository<CfCompositeItem>> PrepareAsync()
        {
            await ResetAsync(typeof(CfCompositeItem));
            return Repository<CfCompositeItem>();
        }
    }
}
