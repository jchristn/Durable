namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Multi-row writes: CreateMany key assignment and order, UpdateMany (always required), UpdateField and BatchUpdate
    /// (<see cref="RepositoryCapabilities.BatchUpdate"/>), BatchDelete / DeleteMany / DeleteAll and their return values.
    /// </summary>
    internal sealed class WriteSuite : KitSuite
    {
        public WriteSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "CreateMany returns the entities in input order with distinct generated keys matching the stored rows")]
        public async Task CreateManyAssignsKeysInOrder()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            List<CfItem> asyncRows = Enumerable.Range(0, 40).Select(i => new CfItem { Name = "async-" + i, Quantity = i, Category = "w" }).ToList();
            List<CfItem> asyncResult = (await repository.CreateManyAsync(asyncRows, null, Token)).ToList();
            List<CfItem> syncRows = Enumerable.Range(0, 25).Select(i => new CfItem { Name = "sync-" + i, Quantity = i, Category = "w" }).ToList();
            List<CfItem> syncResult = repository.CreateMany(syncRows).ToList();

            foreach (List<CfItem> result in new[] { asyncResult, syncResult })
            {
                Assert.All(result, x => Assert.True(x.Id > 0, "Every entity must receive a generated key"));
                Assert.Equal(result.Count, result.Select(x => x.Id).Distinct().Count());
                for (int i = 0; i < result.Count; i++) Assert.Equal(i, result[i].Quantity);
            }

            Dictionary<int, CfItem> stored = repository.ReadAll().ToDictionary(x => x.Id);
            Assert.Equal(65, stored.Count);
            foreach (CfItem entity in asyncResult.Concat(syncResult))
            {
                Assert.True(stored.TryGetValue(entity.Id, out CfItem? row), "Generated key " + entity.Id + " not found in storage");
                Assert.Equal(entity.Name, row!.Name);
                Assert.Equal(entity.Quantity, row.Quantity);
            }
        }

        [ConformanceTest(Description = "CreateMany with an empty list returns an empty result")]
        public async Task CreateManyEmpty()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            Assert.Empty(repository.CreateMany(new List<CfItem>()));
            Assert.Empty(await repository.CreateManyAsync(new List<CfItem>(), null, Token));
            Assert.Equal(0L, repository.Count());
        }

        [ConformanceTest(Description = "UpdateMany applies the action to every match and returns the count")]
        public async Task UpdateManyAppliesAction()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(2, f.Items.UpdateMany(x => x.Category == "Tools", item => item.Quantity += 100));
            Assert.Equal(new[] { 105, 100 }, new[] { f.Items.ReadById(f.Item(ItemFixture.Alpha).Id)!.Quantity, f.Items.ReadById(f.Item(ItemFixture.Beta).Id)!.Quantity });
            Assert.Equal(2, await f.Items.UpdateManyAsync(x => x.Category == "Kitchen", item => { item.Email = "kitchen@x.com"; return Task.CompletedTask; }, null, Token));
            Assert.Equal(2L, f.Items.Count(x => x.Email == "kitchen@x.com"));
            Assert.Equal(0, f.Items.UpdateMany(x => x.Category == "None", item => item.Quantity = 0));
            Assert.Equal(1L, f.Items.Count(x => x.Quantity == 12));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.BatchUpdate, Description = "UpdateField sets a value or null on matching rows and returns the count")]
        public async Task UpdateField()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(2, await f.Items.UpdateFieldAsync(x => x.Category == "Garden", x => x.Price, 1.25m, null, Token));
            Assert.Equal(2L, f.Items.Count(x => x.Price == 1.25m));
            Assert.Equal(5, f.Items.UpdateField(x => x.Email != null, x => x.Email, (string?)null));
            Assert.Equal(6L, f.Items.Count(x => x.Email == null));
            Assert.Equal(0, f.Items.UpdateField(x => x.Category == "None", x => x.Quantity, 1));
            Assert.Equal(1, f.Items.UpdateField(x => x.Name == ItemFixture.Echo, x => x.Status, CfStatus.Closed));
            Assert.Equal(CfStatus.Closed, f.Items.ReadSingle(x => x.Name == ItemFixture.Echo).Status);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.BatchUpdate, Description = "BatchUpdate assigns members from expressions over the current row and captured values")]
        public async Task BatchUpdateReferencesCurrentRow()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(2, await f.Items.BatchUpdateAsync(x => x.Category == "Garden", x => new CfItem { Quantity = x.Quantity * 2, Name = x.Name + "!" }, null, Token));
            ConformanceAssert.NameSet(f.Items.ReadMany(x => x.Category == "Garden").Select(x => x.Name), new[] { "O'Brien!", "Delta!" }, "Garden names after BatchUpdate");
            Assert.Equal(30m, f.Items.ReadMany(x => x.Category == "Garden").Sum(x => (decimal)x.Quantity));
            decimal bonus = 5m;
            Assert.Equal(1, f.Items.BatchUpdate(x => x.Name == ItemFixture.Alpha, x => new CfItem { Price = x.Price + bonus, IsFeatured = null }));
            CfItem alpha = f.Items.ReadSingle(x => x.Name == ItemFixture.Alpha);
            Assert.Equal(15.50m, alpha.Price);
            Assert.Null(alpha.IsFeatured);
            Assert.Equal(0, f.Items.BatchUpdate(x => x.Category == "None", x => new CfItem { Quantity = 0 }));
            Assert.Equal(6L, f.Items.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.BatchUpdate, Description = "BatchUpdate with a non member-init expression throws NotSupportedException")]
        public async Task BatchUpdateRequiresMemberInit()
        {
            ItemFixture f = await SeedItemsAsync();
            ConformanceAssert.Throws<NotSupportedException>(() => f.Items.BatchUpdate(x => x.Quantity > 0, x => x), "BatchUpdate(x => x)");
            ConformanceAssert.Throws<ArgumentNullException>(() => f.Items.BatchUpdate(x => x.Quantity > 0, null!), "BatchUpdate(predicate, null)");
        }

        [ConformanceTest(Description = "BatchDelete / DeleteMany remove matching rows and return the count")]
        public async Task BatchDeleteAndDeleteMany()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(2, f.Items.BatchDelete(x => x.Category == "Tools"));
            Assert.Equal(0, await f.Items.BatchDeleteAsync(x => x.Category == "Tools", null, Token));
            Assert.Equal(1, await f.Items.BatchDeleteAsync(x => x.Quantity > 10, null, Token));
            Assert.Equal(1, f.Items.DeleteMany(x => x.Name == ItemFixture.Delta));
            ConformanceAssert.NameSet(f.Items.ReadAll().Select(x => x.Name), new[] { ItemFixture.Echo, ItemFixture.Foxtrot }, "rows left");
        }

        [ConformanceTest(Description = "Writes followed by reads through a second repository see the new state")]
        public async Task WritesAreDurableAcrossRepositories()
        {
            ItemFixture f = await SeedItemsAsync();
            f.Items.UpdateMany(x => x.Category == "Kitchen", item => item.Category = "Pantry");
            f.Items.DeleteMany(x => x.Category == "Tools");
            IRepository<CfItem> other = Repository<CfItem>();
            Assert.Equal(2L, other.Count(x => x.Category == "Pantry"));
            Assert.Equal(4L, other.Count());
        }
    }
}
