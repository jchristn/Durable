namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;
    using Durable.Query;
    using Xunit;

    /// <summary>
    /// Capability masking: an in-memory backend constructed with a reduced <see cref="RepositoryCapabilities"/> set makes
    /// repositories and query builders reject the missing features with <see cref="NotSupportedException"/> when the
    /// operation is requested (Include, GroupBy, Select and Distinct when the builder method is called; predicates when the
    /// query executes), while the always-required features keep working.
    /// </summary>
    public class InMemoryCapabilityTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// Without Include, Include and ThenInclude throw immediately; without ManyToMany only many-to-many navigations do.
        /// </summary>
        [Fact]
        public async Task IncludeAndManyToManyAreChecked()
        {
            InMemoryLibraryData noInclude = await InMemoryLibraryData.CreateAsync(RepositoryCapabilities.All & ~RepositoryCapabilities.Include);
            IQueryBuilder<Author> query = noInclude.Authors.Query();
            NotSupportedException error = Assert.Throws<NotSupportedException>(() => query.Include(a => a.Books));
            Assert.Contains("Include", error.Message);
            Assert.Equal(4, query.Execute().Count());
            Assert.Throws<NotSupportedException>(() => noInclude.Books.Query().Select(b => new PersonNameDto { FirstName = b.Author!.Name }));

            InMemoryLibraryData noManyToMany = await InMemoryLibraryData.CreateAsync(RepositoryCapabilities.All & ~RepositoryCapabilities.ManyToMany);
            Assert.Throws<NotSupportedException>(() => noManyToMany.Authors.Query().Include(a => a.Categories));
            Assert.Throws<NotSupportedException>(() => noManyToMany.Authors.Query().Include(a => a.Books).Include(a => a.Categories));
            Assert.Equal(2, noManyToMany.Authors.Query().Include(a => a.Books).Execute().First(a => a.Name == "Austen").Books.Count);
            await Assert.ThrowsAsync<NotSupportedException>(() => noManyToMany.Authors.Query().Where(a => a.Categories.Any()).ExecuteAsync());
        }

        /// <summary>
        /// Grouping, projection, distinct and aggregates are rejected when masked.
        /// </summary>
        [Fact]
        public async Task QueryShapingCapabilitiesAreChecked()
        {
            InMemoryQtData none = await InMemoryQtData.CreateAsync(RepositoryCapabilities.None);
            Assert.Throws<NotSupportedException>(() => none.Items.Query().GroupBy(x => x.Category));
            Assert.Throws<NotSupportedException>(() => none.Items.Query().Select(x => new QtItemSummary { Name = x.Name }));
            Assert.Throws<NotSupportedException>(() => none.Items.Query().Distinct());
            Assert.Throws<NotSupportedException>(() => none.Items.Sum(x => x.Price));
            await Assert.ThrowsAsync<NotSupportedException>(() => none.Items.Query().MaxAsync(x => x.Price));

            InMemoryQtData noAggregates = await InMemoryQtData.CreateAsync(RepositoryCapabilities.All & ~RepositoryCapabilities.Aggregates);
            Assert.Throws<NotSupportedException>(() => noAggregates.Items.Query().GroupBy(x => x.Category).Sum(x => x.Price));
            Assert.Throws<NotSupportedException>(() => noAggregates.Items.Query().Select(x => new QtItemSummary { Total = x.Price }).Sum(s => s.Total));
            Assert.Equal(3, noAggregates.Items.Query().GroupBy(x => x.Category).Count());
        }

        /// <summary>
        /// Functions, explicit string modes and navigation predicates are validated when the query executes.
        /// </summary>
        [Fact]
        public async Task PredicateCapabilitiesAreValidatedAtExecution()
        {
            InMemoryQtData none = await InMemoryQtData.CreateAsync(RepositoryCapabilities.None);
            await Assert.ThrowsAsync<NotSupportedException>(() => none.Items.Query().Where(x => x.Name.ToUpper() == "ALPHA").ExecuteAsync());
            Assert.Throws<NotSupportedException>(() => none.Items.Count(x => x.Name.Contains("a", StringComparison.OrdinalIgnoreCase)));
            Assert.Throws<NotSupportedException>(() => none.Items.Query().Where(x => x.Owner!.Name == "Acme").Execute());
            Assert.Throws<NotSupportedException>(() => none.Owners.Query().Where(o => o.Items.Any()).Execute());
            Assert.Throws<NotSupportedException>(() => none.Items.Query().OrderBy(x => x.Name.Length).Execute());

            Assert.Equal(1, await none.Items.CountAsync(x => x.Name.Contains("lph")));
            Assert.Equal(3, none.Items.Count(x => x.Quantity * 2 > 15 && x.Email != null || x.Discount == null && x.Price > 16));
        }

        /// <summary>
        /// Composite keys and version columns are checked when the repository is constructed; set-based updates, upsert
        /// and explicit transactions when called. Without transactions, multi-row operations still run (not atomically).
        /// </summary>
        [Fact]
        public async Task WriteCapabilitiesAreChecked()
        {
            InMemoryBackend none = new InMemoryBackend(RepositoryCapabilities.None);
            Assert.Equal(RepositoryCapabilities.None, none.Capabilities);
            NotSupportedException composite = Assert.Throws<NotSupportedException>(() => none.CreateRepository<RelCompositeItem>());
            Assert.Contains("CompositeKeys", composite.Message);
            NotSupportedException versioned = Assert.Throws<NotSupportedException>(() => none.CreateRepository<RelVersionedItem>());
            Assert.Contains("OptimisticConcurrency", versioned.Message);

            InMemoryRepository<RelTenantNote> notes = none.CreateRepository<RelTenantNote>();
            Assert.Equal(RepositoryCapabilities.None, notes.Capabilities);
            Assert.Throws<NotSupportedException>(() => notes.UpdateField(x => true, x => x.Title, "x"));
            await Assert.ThrowsAsync<NotSupportedException>(() => notes.BatchUpdateAsync(x => true, x => new RelTenantNote { Amount = 1 }));
            Assert.Throws<NotSupportedException>(() => notes.Upsert(new RelTenantNote { Id = 5 }));
            Assert.Throws<NotSupportedException>(() => notes.BeginTransaction());
            Assert.Throws<NotSupportedException>(() => none.BeginTransaction());

            List<RelTenantNote> created = notes.CreateMany(new[] { new RelTenantNote { Title = "a" }, new RelTenantNote { Title = "b" } }).ToList();
            Assert.Equal(new[] { 1, 2 }, created.Select(x => x.Id).ToArray());
            Assert.Equal(2, notes.UpdateMany(x => true, x => x.Amount = 7));
            Assert.Equal(14m, notes.ReadAll().Sum(x => x.Amount));
        }

        /// <summary>
        /// With no optional capabilities, basic CRUD, filtering, ordering, paging, counting, query filters and soft delete
        /// still work.
        /// </summary>
        [Fact]
        public async Task RequiredFeaturesWorkWithoutOptionalCapabilities()
        {
            InMemoryBackend backend = new InMemoryBackend(RepositoryCapabilities.None);
            InMemoryRepository<RelSoftNote> notes = backend.CreateRepository<RelSoftNote>();
            await notes.CreateManyAsync(Enumerable.Range(0, 5).Select(i => new RelSoftNote { ParentId = i % 2, Text = "n" + i }).ToList());
            notes.AddQueryFilter(x => x.ParentId == 0);
            Assert.Equal(3, await notes.CountAsync());
            Assert.True(notes.DeleteById(1));
            Assert.Equal(new[] { "n4", "n2" }, (await notes.Query().OrderByDescending(x => x.Text).Take(2).ExecuteAsync()).Select(x => x.Text).ToArray());
            RelSoftNote first = notes.ReadFirst(x => x.Text == "n2")!;
            first.Text = "n2b";
            notes.Update(first);
            Assert.Equal(1, notes.Count(x => x.Text == "n2b"));
            Assert.Equal(5, notes.Query().IgnoreQueryFilters().Count());
        }

        #endregion
    }
}
