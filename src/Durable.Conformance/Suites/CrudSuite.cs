namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Basic create/read/update/delete, existence and counting, sync and async, buffered and streamed, plus argument
    /// validation and empty-storage edge cases. Needs no optional capability.
    /// </summary>
    internal sealed class CrudSuite : KitSuite
    {
        public CrudSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "Create assigns a generated key and returns the same instance")]
        public async Task CreateAssignsIdentityKey()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            CfItem first = new CfItem { Name = "first", Category = "c" };
            CfItem returned = repository.Create(first);
            Assert.Same(first, returned);
            Assert.True(first.Id > 0, "Create must assign a positive generated key, got " + first.Id);
            CfItem second = await repository.CreateAsync(new CfItem { Name = "second", Category = "c" }, null, Token);
            Assert.True(second.Id > 0);
            Assert.NotEqual(first.Id, second.Id);
        }

        [ConformanceTest(Description = "ReadById / ReadByIdAsync return the stored row or null")]
        public async Task ReadById()
        {
            ItemFixture f = await SeedItemsAsync();
            CfItem alpha = f.Item(ItemFixture.Alpha);
            CfItem? sync = f.Items.ReadById(alpha.Id);
            CfItem? viaAsync = await f.Items.ReadByIdAsync(alpha.Id, null, Token);
            Assert.NotNull(sync);
            Assert.NotNull(viaAsync);
            Assert.Equal(ItemFixture.Alpha, sync.Name);
            Assert.Equal(alpha.Id, viaAsync.Id);
            int missing = f.SeededItems.Max(i => i.Id) + 1000;
            Assert.Null(f.Items.ReadById(missing));
            Assert.Null(await f.Items.ReadByIdAsync(missing, null, Token));
        }

        [ConformanceTest(Description = "ReadFirst / ReadFirstOrDefault with and without predicates")]
        public async Task ReadFirst()
        {
            ItemFixture f = await SeedItemsAsync();
            CfItem? garden = f.Items.ReadFirst(x => x.Category == "Garden");
            Assert.NotNull(garden);
            Assert.Equal("Garden", garden.Category);
            CfItem? gardenAsync = await f.Items.ReadFirstAsync(x => x.Category == "Garden", null, Token);
            Assert.NotNull(gardenAsync);
            Assert.Equal("Garden", gardenAsync.Category);
            Assert.NotNull(f.Items.ReadFirst());
            Assert.NotNull(await f.Items.ReadFirstOrDefaultAsync(null, null, Token));
            Assert.Null(f.Items.ReadFirst(x => x.Category == "None"));
            Assert.Null(f.Items.ReadFirstOrDefault(x => x.Category == "None"));
            Assert.Null(await f.Items.ReadFirstAsync(x => x.Category == "None", null, Token));
            Assert.Null(await f.Items.ReadFirstOrDefaultAsync(x => x.Category == "None", null, Token));
        }

        [ConformanceTest(Description = "ReadSingle returns the only match; zero or several matches throw InvalidOperationException")]
        public async Task ReadSingle()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(ItemFixture.Delta, f.Items.ReadSingle(x => x.Name == ItemFixture.Delta).Name);
            Assert.Equal(ItemFixture.Delta, (await f.Items.ReadSingleAsync(x => x.Name == ItemFixture.Delta, null, Token)).Name);
            ConformanceAssert.Throws<InvalidOperationException>(() => f.Items.ReadSingle(x => x.Category == "None"), "ReadSingle with no match");
            ConformanceAssert.Throws<InvalidOperationException>(() => f.Items.ReadSingle(x => x.Category == "Tools"), "ReadSingle with two matches");
            await ConformanceAssert.ThrowsAsync<InvalidOperationException>(() => f.Items.ReadSingleAsync(x => x.Category == "None", null, Token), "ReadSingleAsync with no match");
            await ConformanceAssert.ThrowsAsync<InvalidOperationException>(() => f.Items.ReadSingleAsync(x => x.Category == "Tools", null, Token), "ReadSingleAsync with two matches");
        }

        [ConformanceTest(Description = "ReadSingleOrDefault returns null for no match and throws InvalidOperationException for several")]
        public async Task ReadSingleOrDefault()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(ItemFixture.Echo, f.Items.ReadSingleOrDefault(x => x.Name == ItemFixture.Echo)?.Name);
            Assert.Null(f.Items.ReadSingleOrDefault(x => x.Category == "None"));
            Assert.Null(await f.Items.ReadSingleOrDefaultAsync(x => x.Category == "None", null, Token));
            ConformanceAssert.Throws<InvalidOperationException>(() => f.Items.ReadSingleOrDefault(x => x.Category == "Kitchen"), "ReadSingleOrDefault with two matches");
            await ConformanceAssert.ThrowsAsync<InvalidOperationException>(() => f.Items.ReadSingleOrDefaultAsync(x => x.Category == "Kitchen", null, Token), "ReadSingleOrDefaultAsync with two matches");
        }

        [ConformanceTest(Description = "ReadMany / ReadAll return every matching row exactly once")]
        public async Task ReadManyAndReadAll()
        {
            ItemFixture f = await SeedItemsAsync();
            ConformanceAssert.NameSet(f.Items.ReadMany().Select(x => x.Name), ItemFixture.AllNames, "ReadMany()");
            ConformanceAssert.NameSet(f.Items.ReadAll().Select(x => x.Name), ItemFixture.AllNames, "ReadAll()");
            ConformanceAssert.NameSet(f.Items.ReadMany(x => x.Quantity > 4).Select(x => x.Name),
                new[] { ItemFixture.Alpha, ItemFixture.OBrien, ItemFixture.Echo }, "ReadMany(Quantity > 4)");
            Assert.Empty(f.Items.ReadMany(x => x.Quantity > 400));
        }

        [ConformanceTest(Description = "ReadManyAsync / ReadAllAsync stream every matching row")]
        public async Task ReadManyAsyncStreams()
        {
            ItemFixture f = await SeedItemsAsync();
            List<string> streamed = new List<string>();
            await foreach (CfItem item in f.Items.ReadManyAsync(x => x.IsActive, null, Token)) streamed.Add(item.Name);
            ConformanceAssert.NameSet(streamed, f.NamesWhere(x => x.IsActive), "ReadManyAsync(IsActive)");

            List<string> all = new List<string>();
            await foreach (CfItem item in f.Items.ReadAllAsync(null, Token)) all.Add(item.Name);
            ConformanceAssert.NameSet(all, ItemFixture.AllNames, "ReadAllAsync()");

            List<string> none = new List<string>();
            await foreach (CfItem item in f.Items.ReadManyAsync(x => x.Name == "missing", null, Token)) none.Add(item.Name);
            Assert.Empty(none);
        }

        [ConformanceTest(Description = "Exists / ExistsById, sync and async")]
        public async Task ExistsAndExistsById()
        {
            ItemFixture f = await SeedItemsAsync();
            int alphaId = f.Item(ItemFixture.Alpha).Id;
            int missing = f.SeededItems.Max(i => i.Id) + 1000;
            Assert.True(f.Items.Exists(x => x.Name == ItemFixture.Beta));
            Assert.False(f.Items.Exists(x => x.Name == "missing"));
            Assert.True(await f.Items.ExistsAsync(x => x.Category == "Kitchen", null, Token));
            Assert.False(await f.Items.ExistsAsync(x => x.Category == "None", null, Token));
            Assert.True(f.Items.ExistsById(alphaId));
            Assert.False(f.Items.ExistsById(missing));
            Assert.True(await f.Items.ExistsByIdAsync(alphaId, null, Token));
            Assert.False(await f.Items.ExistsByIdAsync(missing, null, Token));
        }

        [ConformanceTest(Description = "Count with and without predicates, sync and async")]
        public async Task CountRows()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(6L, f.Items.Count());
            Assert.Equal(6L, await f.Items.CountAsync(null, null, Token));
            Assert.Equal(2L, f.Items.Count(x => x.Category == "Tools"));
            Assert.Equal(4L, await f.Items.CountAsync(x => x.IsActive, null, Token));
            Assert.Equal(0L, f.Items.Count(x => x.Quantity < 0));
        }

        [ConformanceTest(Description = "Reads on empty storage return nothing without errors")]
        public async Task EmptyStorageReads()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            Assert.Empty(repository.ReadAll());
            Assert.Empty(repository.ReadMany(x => x.Quantity > 0));
            Assert.Null(repository.ReadFirst());
            Assert.Null(await repository.ReadFirstAsync(null, null, Token));
            Assert.Null(repository.ReadSingleOrDefault(x => x.Quantity > 0));
            Assert.Equal(0L, repository.Count());
            Assert.False(repository.Exists(x => x.Quantity > 0));
            Assert.False(repository.ExistsById(1));
            Assert.Null(repository.ReadById(1));
            Assert.Empty(await repository.Query().ExecuteAsync(Token));
            Assert.False(await repository.Query().AnyAsync(Token));
            Assert.Equal(0, repository.DeleteAll());
            Assert.Equal(0, repository.DeleteMany(x => x.Quantity > 0));
        }

        [ConformanceTest(Description = "Update writes every column (sync and async) and returns the instance")]
        public async Task UpdatePersistsChanges()
        {
            ItemFixture f = await SeedItemsAsync();
            CfItem? beta = await f.Items.ReadByIdAsync(f.Item(ItemFixture.Beta).Id, null, Token);
            Assert.NotNull(beta);
            beta.Email = "beta@x.com";
            beta.Quantity = 77;
            beta.Discount = 9;
            beta.IsFeatured = true;
            beta.Status = CfStatus.Closed;
            beta.Priority = CfPriority.Critical;
            beta.DueDate = new DateTime(2025, 1, 2, 3, 4, 5);
            CfItem returned = f.Items.Update(beta);
            Assert.Same(beta, returned);

            CfItem? stored = f.Items.ReadById(beta.Id);
            Assert.NotNull(stored);
            Assert.Equal("beta@x.com", stored.Email);
            Assert.Equal(77, stored.Quantity);
            Assert.Equal(9, stored.Discount);
            Assert.True(stored.IsFeatured);
            Assert.Equal(CfStatus.Closed, stored.Status);
            Assert.Equal(CfPriority.Critical, stored.Priority);
            ConformanceAssert.SameInstant(new DateTime(2025, 1, 2, 3, 4, 5), stored.DueDate!.Value, "DueDate after Update");

            stored.Name = "Beta 2";
            stored.Email = null;
            await f.Items.UpdateAsync(stored, null, Token);
            CfItem? again = await f.Items.ReadByIdAsync(beta.Id, null, Token);
            Assert.Equal("Beta 2", again?.Name);
            Assert.Null(again?.Email);
            Assert.Equal(6L, await f.Items.CountAsync(null, null, Token));
        }

        [ConformanceTest(Description = "Update of a key that does not exist throws InvalidOperationException")]
        public async Task UpdateOfMissingRowThrows()
        {
            ItemFixture f = await SeedItemsAsync();
            CfItem ghost = new CfItem { Id = f.SeededItems.Max(i => i.Id) + 1000, Name = "ghost", Category = "c" };
            ConformanceAssert.Throws<InvalidOperationException>(() => f.Items.Update(ghost), "Update of a missing row");
            await ConformanceAssert.ThrowsAsync<InvalidOperationException>(() => f.Items.UpdateAsync(ghost, null, Token), "UpdateAsync of a missing row");
            Assert.Equal(6L, f.Items.Count());
        }

        [ConformanceTest(Description = "Delete / DeleteById / DeleteMany report what they removed")]
        public async Task DeleteVariants()
        {
            ItemFixture f = await SeedItemsAsync();
            CfItem alpha = f.Item(ItemFixture.Alpha);
            Assert.True(f.Items.Delete(alpha));
            Assert.False(f.Items.Delete(alpha));
            Assert.True(await f.Items.DeleteByIdAsync(f.Item(ItemFixture.Beta).Id, null, Token));
            Assert.False(f.Items.DeleteById(f.Item(ItemFixture.Beta).Id));
            Assert.True(await f.Items.DeleteAsync(f.Item(ItemFixture.Delta), null, Token));
            Assert.Equal(2, f.Items.DeleteMany(x => x.Category == "Kitchen"));
            Assert.Equal(0, await f.Items.DeleteManyAsync(x => x.Category == "Kitchen", null, Token));
            ConformanceAssert.NameSet(f.Items.ReadAll().Select(x => x.Name), new[] { ItemFixture.OBrien }, "rows left after deletes");
            Assert.Null(f.Items.ReadById(alpha.Id));
        }

        [ConformanceTest(Description = "DeleteAll / DeleteAllAsync remove every row and return the count")]
        public async Task DeleteAll()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(6, f.Items.DeleteAll());
            Assert.Equal(0L, f.Items.Count());
            await f.Items.CreateAsync(new CfItem { Name = "again", Category = "c" }, null, Token);
            Assert.Equal(1, await f.Items.DeleteAllAsync(null, Token));
            Assert.Equal(0, await f.Items.DeleteAllAsync(null, Token));
        }

        [ConformanceTest(Description = "Query execution variants return the same rows (buffered, async, streamed, with query text)")]
        public async Task QueryExecutionVariants()
        {
            ItemFixture f = await SeedItemsAsync();
            string[] expected = f.NamesWhere(x => x.Price > 10m);
            ConformanceAssert.NameSet(f.Items.Query().Where(x => x.Price > 10m).Execute().Select(x => x.Name), expected, "Execute()");
            ConformanceAssert.NameSet((await f.Items.Query().Where(x => x.Price > 10m).ExecuteAsync(Token)).Select(x => x.Name), expected, "ExecuteAsync()");

            List<string> streamed = new List<string>();
            await foreach (CfItem item in f.Items.Query().Where(x => x.Price > 10m).ExecuteAsyncEnumerable(Token)) streamed.Add(item.Name);
            ConformanceAssert.NameSet(streamed, expected, "ExecuteAsyncEnumerable()");

            IDurableResult<CfItem> withQuery = f.Items.Query().Where(x => x.Price > 10m).ExecuteWithQuery();
            Assert.NotNull(withQuery.Query);
            ConformanceAssert.NameSet(withQuery.Result.Select(x => x.Name), expected, "ExecuteWithQuery().Result");

            IDurableResult<CfItem> withQueryAsync = await f.Items.Query().Where(x => x.Price > 10m).ExecuteWithQueryAsync(Token);
            Assert.NotNull(withQueryAsync.Query);
            ConformanceAssert.NameSet(withQueryAsync.Result.Select(x => x.Name), expected, "ExecuteWithQueryAsync().Result");

            IAsyncDurableResult<CfItem> streamedWithQuery = f.Items.Query().Where(x => x.Price > 10m).ExecuteAsyncEnumerableWithQuery(Token);
            Assert.NotNull(streamedWithQuery.Query);
            List<string> streamedNames = new List<string>();
            await foreach (CfItem item in streamedWithQuery.Result) streamedNames.Add(item.Name);
            ConformanceAssert.NameSet(streamedNames, expected, "ExecuteAsyncEnumerableWithQuery().Result");
            Assert.NotNull(f.Items.Query().Where(x => x.Price > 10m).Query);
        }

        [ConformanceTest(Description = "Query Count / Any / Delete honor Where")]
        public async Task QueryCountAnyDelete()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(2L, f.Items.Query().Where(x => x.Category == "Garden").Count());
            Assert.Equal(2L, await f.Items.Query().Where(x => x.Category == "Garden").CountAsync(Token));
            Assert.True(f.Items.Query().Where(x => x.Category == "Garden").Any());
            Assert.False(await f.Items.Query().Where(x => x.Category == "None").AnyAsync(Token));
            Assert.Equal(2, f.Items.Query().Where(x => x.Category == "Garden").Delete());
            Assert.Equal(1, await f.Items.Query().Where(x => x.Name == ItemFixture.Alpha).DeleteAsync(Token));
            Assert.Equal(0, await f.Items.Query().Where(x => x.Name == ItemFixture.Alpha).DeleteAsync(Token));
            Assert.Equal(3L, f.Items.Count());
        }

        [ConformanceTest(Description = "Several Where calls are combined with AND")]
        public async Task WhereCallsCombineWithAnd()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertNamesAsync(f.Items.Query().Where(x => x.IsActive).Where(x => x.Price < 10m).Where(x => x.Quantity > 0),
                "Where(IsActive).Where(Price < 10).Where(Quantity > 0)", ItemFixture.OBrien, ItemFixture.Foxtrot);
        }

        [ConformanceTest(Description = "Repositories created by the same target share storage")]
        public async Task RepositoriesShareStorage()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> writer = Repository<CfItem>();
            IRepository<CfItem> reader = Repository<CfItem>();
            CfItem created = await writer.CreateAsync(new CfItem { Name = "shared", Category = "c" }, null, Token);
            Assert.Equal("shared", reader.ReadById(created.Id)?.Name);
            Assert.True(reader.Delete(created));
            Assert.Null(writer.ReadById(created.Id));
        }

        [ConformanceTest(Description = "Repository exposes the entity's metadata")]
        public void RepositoryExposesMetadata()
        {
            IRepository<CfItem> repository = Repository<CfItem>();
            Assert.Same(EntityMetadata.For<CfItem>(), repository.Metadata);
            Assert.Equal("cf_items", repository.Metadata.TableName);
            Assert.Equal("Id", Assert.Single(repository.Metadata.KeyColumns).Property.Name);
        }

        [ConformanceTest(Description = "Null arguments throw ArgumentNullException and negative paging throws ArgumentOutOfRangeException")]
        public async Task ArgumentValidation()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.Create(null!), "Create(null)");
            await ConformanceAssert.ThrowsAsync<ArgumentNullException>(() => repository.CreateAsync(null!, null, Token), "CreateAsync(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.CreateMany(null!).ToList(), "CreateMany(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.CreateMany(new List<CfItem> { new CfItem { Name = "x" }, null! }).ToList(), "CreateMany with a null element");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.Update(null!), "Update(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.Delete(null!), "Delete(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.ReadById(null!), "ReadById(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.ExistsById(null!), "ExistsById(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.DeleteById(null!), "DeleteById(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.ReadSingle(null!), "ReadSingle(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.Exists(null!), "Exists(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.DeleteMany(null!), "DeleteMany(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.AddQueryFilter(null!), "AddQueryFilter(null)");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.Query().Where(null!), "Query().Where(null)");
            ConformanceAssert.Throws<ArgumentOutOfRangeException>(() => repository.Query().Skip(-1), "Query().Skip(-1)");
            ConformanceAssert.Throws<ArgumentOutOfRangeException>(() => repository.Query().Take(-1), "Query().Take(-1)");
            Assert.Equal(0L, repository.Count());
        }
    }
}
