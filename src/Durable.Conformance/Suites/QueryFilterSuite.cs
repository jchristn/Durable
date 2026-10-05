namespace Durable.Conformance
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Global query filters (always required): applied to every predicate-based read and write, combined with AND,
    /// re-evaluating captured variables, bypassed by IgnoreQueryFilters, cleared by ClearQueryFilters, and not applied
    /// to key-based Update / Delete of a specific entity.
    /// Data: tenant 1 has t1-a (10), t1-b (20), t1-c (30); tenant 2 has t2-a (100), t2-b (200).
    /// </summary>
    internal sealed class QueryFilterSuite : KitSuite
    {
        public QueryFilterSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "Filter applies to ReadMany / ReadAll / ReadFirst / ReadById / Count / Exists / streaming")]
        public async Task FilterAppliesToReads()
        {
            IRepository<CfTenantNote> repository = await SeedAsync();
            int otherId = (await repository.Query().IgnoreQueryFilters().Where(x => x.TenantId == 2).ExecuteAsync(Token)).First().Id;
            repository.AddQueryFilter(x => x.TenantId == 1);
            Assert.Equal(3, repository.ReadMany().Count());
            Assert.Equal(3, repository.ReadAll().Count());
            Assert.Equal(2, repository.ReadMany(x => x.Amount >= 20).Count());
            Assert.Equal(3L, repository.Count());
            Assert.Equal(3L, await repository.CountAsync(null, null, Token));
            Assert.False(await repository.ExistsAsync(x => x.Title == "t2-a", null, Token));
            Assert.True(repository.Exists(x => x.Title == "t1-a"));
            Assert.Null(await repository.ReadFirstAsync(x => x.TenantId == 2, null, Token));
            Assert.Null(await repository.ReadByIdAsync(otherId, null, Token));
            Assert.False(repository.ExistsById(otherId));
            Assert.Equal("t1-b", repository.ReadSingle(x => x.Amount == 20).Title);

            int streamed = 0;
            await foreach (CfTenantNote note in repository.ReadManyAsync(null, null, Token))
            {
                Assert.Equal(1, note.TenantId);
                streamed++;
            }

            Assert.Equal(3, streamed);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Filter applies to Sum / Max / Min / Average")]
        public async Task FilterAppliesToAggregates()
        {
            IRepository<CfTenantNote> repository = await SeedAsync();
            repository.AddQueryFilter(x => x.TenantId == 1);
            Assert.Equal(60m, await repository.SumAsync(x => x.Amount, null, null, Token));
            Assert.Equal(30, await repository.MaxAsync(x => x.Amount, null, null, Token));
            Assert.Equal(10, repository.Min(x => x.Amount));
            Assert.Equal(20m, repository.Average(x => x.Amount));
            Assert.Equal(360m, repository.Query().IgnoreQueryFilters().Sum(x => x.Amount));
        }

        [ConformanceTest(Description = "Query builder honors the filter; IgnoreQueryFilters bypasses it; ClearQueryFilters removes it")]
        public async Task QueryBuilderAndIgnoreQueryFilters()
        {
            IRepository<CfTenantNote> repository = await SeedAsync();
            repository.AddQueryFilter(x => x.TenantId == 2);
            Assert.Equal(2, (await repository.Query().ExecuteAsync(Token)).Count());
            Assert.Equal(2L, await repository.Query().CountAsync(Token));
            Assert.Equal(1L, await repository.Query().Where(x => x.Amount > 100).CountAsync(Token));
            Assert.Equal(5L, await repository.Query().IgnoreQueryFilters().CountAsync(Token));
            Assert.Equal(5, (await repository.Query().IgnoreQueryFilters().ExecuteAsync(Token)).Count());
            Assert.Equal(3, repository.Query().IgnoreQueryFilters().Where(x => x.Amount < 100).Execute().Count());
            Assert.Single(repository.QueryFilters);
            repository.ClearQueryFilters();
            Assert.Empty(repository.QueryFilters);
            Assert.Equal(5L, await repository.CountAsync(null, null, Token));
        }

        [ConformanceTest(Description = "Several filters are combined with AND")]
        public async Task FiltersCombineWithAnd()
        {
            IRepository<CfTenantNote> repository = await SeedAsync();
            repository.AddQueryFilter(x => x.TenantId == 1);
            repository.AddQueryFilter(x => x.Amount > 10);
            Assert.Equal(2, repository.QueryFilters.Count);
            Assert.Equal(2L, await repository.CountAsync(null, null, Token));
            ConformanceAssert.NameSet(repository.ReadAll().Select(x => x.Title), new[] { "t1-b", "t1-c" }, "rows visible through two filters");
        }

        [ConformanceTest(Description = "Filter applies to UpdateMany / DeleteMany / DeleteAll / Query().Delete()")]
        public async Task FilterAppliesToWrites()
        {
            IRepository<CfTenantNote> repository = await SeedAsync();
            IRepository<CfTenantNote> unfiltered = Repository<CfTenantNote>();
            repository.AddQueryFilter(x => x.TenantId == 1);

            Assert.Equal(3, repository.UpdateMany(x => x.Amount > 0, note => note.Amount = 1));
            Assert.Equal(3L, unfiltered.Count(x => x.Amount == 1));
            Assert.Equal(0L, unfiltered.Count(x => x.TenantId == 2 && x.Amount == 1));

            Assert.Equal(1, repository.Query().Where(x => x.Title == "t1-a").Delete());
            Assert.Equal(2, await repository.DeleteManyAsync(x => x.Amount > 0, null, Token));
            Assert.Equal(2L, unfiltered.Count());
            Assert.Equal(0L, unfiltered.Count(x => x.TenantId == 1));

            await unfiltered.CreateAsync(new CfTenantNote { TenantId = 1, Title = "again", Amount = 5 }, null, Token);
            Assert.Equal(1, repository.DeleteAll());
            Assert.Equal(2L, unfiltered.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.BatchUpdate, Description = "Filter applies to UpdateField and BatchUpdate")]
        public async Task FilterAppliesToSetBasedUpdates()
        {
            IRepository<CfTenantNote> repository = await SeedAsync();
            IRepository<CfTenantNote> unfiltered = Repository<CfTenantNote>();
            repository.AddQueryFilter(x => x.TenantId == 1);
            Assert.Equal(3, await repository.UpdateFieldAsync(x => x.Amount > 0, x => x.Title, "renamed", null, Token));
            Assert.Equal(3L, unfiltered.Count(x => x.Title == "renamed"));
            Assert.Equal(3, await repository.BatchUpdateAsync(x => x.Amount > 0, x => new CfTenantNote { Amount = 7 }, null, Token));
            Assert.Equal(3L, unfiltered.Count(x => x.Amount == 7));
            Assert.Equal(2L, unfiltered.Count(x => x.TenantId == 2 && x.Amount >= 100 && x.Title.StartsWith("t2-")));
        }

        [ConformanceTest(Description = "Captured variables in a filter are re-evaluated for every operation")]
        public async Task CapturedVariableFollowsCurrentValue()
        {
            IRepository<CfTenantNote> repository = await SeedAsync();
            int currentTenant = 1;
            repository.AddQueryFilter(x => x.TenantId == currentTenant);
            Assert.Equal(3L, await repository.CountAsync(null, null, Token));
            Assert.All(await repository.Query().ExecuteAsync(Token), n => Assert.Equal(1, n.TenantId));
            currentTenant = 2;
            Assert.Equal(2L, await repository.CountAsync(null, null, Token));
            Assert.All(repository.ReadMany(), n => Assert.Equal(2, n.TenantId));
            currentTenant = 3;
            Assert.Equal(0L, repository.Count());
            Assert.Equal(0, await repository.DeleteManyAsync(x => x.Amount > 0, null, Token));
            currentTenant = 2;
            Assert.Equal(2, await repository.DeleteManyAsync(x => x.Amount > 0, null, Token));
            Assert.Equal(3L, await repository.Query().IgnoreQueryFilters().CountAsync(Token));
        }

        [ConformanceTest(Description = "Key-based Update and Delete of a specific entity are not filtered")]
        public async Task KeyBasedWritesAreNotFiltered()
        {
            IRepository<CfTenantNote> repository = await SeedAsync();
            IRepository<CfTenantNote> unfiltered = Repository<CfTenantNote>();
            CfTenantNote other = unfiltered.ReadSingle(x => x.Title == "t2-a");
            repository.AddQueryFilter(x => x.TenantId == 1);
            other.Amount = 101;
            repository.Update(other);
            Assert.Equal(101, unfiltered.ReadById(other.Id)?.Amount);
            Assert.True(await repository.DeleteAsync(other, null, Token));
            Assert.Null(unfiltered.ReadById(other.Id));
            Assert.Equal(4L, unfiltered.Count());
        }

        private async Task<IRepository<CfTenantNote>> SeedAsync()
        {
            await ResetAsync(typeof(CfTenantNote));
            IRepository<CfTenantNote> repository = Repository<CfTenantNote>();
            await repository.CreateManyAsync(new List<CfTenantNote>
            {
                new CfTenantNote { TenantId = 1, Title = "t1-a", Amount = 10 },
                new CfTenantNote { TenantId = 1, Title = "t1-b", Amount = 20 },
                new CfTenantNote { TenantId = 1, Title = "t1-c", Amount = 30 },
                new CfTenantNote { TenantId = 2, Title = "t2-a", Amount = 100 },
                new CfTenantNote { TenantId = 2, Title = "t2-b", Amount = 200 }
            }, null, Token);
            return repository;
        }
    }
}
