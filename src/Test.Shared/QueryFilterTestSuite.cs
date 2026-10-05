namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Global query filter coverage (<see cref="IRepository{T}.AddQueryFilter"/>): reads, counts, existence checks,
    /// aggregates, Query(), DeleteMany, UpdateField, BatchUpdate and UpdateMany honor the filter;
    /// IgnoreQueryFilters bypasses it; a filter capturing a mutable variable follows its current value.
    /// </summary>
    public class QueryFilterTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryFilterTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public QueryFilterTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Reads, counts, existence checks and aggregates only see rows passing the filter.
        /// </summary>
        [Fact]
        public async Task FilterAppliesToReadsCountsExistsAndAggregates()
        {
            ISqlRepository<RelTenantNote> repository = await SeedAsync();
            repository.AddQueryFilter(x => x.TenantId == 1);

            Assert.Equal(3, repository.ReadMany().Count());
            Assert.Equal(3, repository.ReadAll().Count());
            Assert.Equal(2, repository.ReadMany(x => x.Amount >= 20).Count());
            Assert.Equal(3, await repository.CountAsync());
            Assert.Equal(3, repository.Count());
            Assert.False(await repository.ExistsAsync(x => x.Title == "t2-a"));
            Assert.True(repository.Exists(x => x.Title == "t1-a"));
            Assert.Equal(60m, await repository.SumAsync(x => x.Amount));
            Assert.Equal(30, await repository.MaxAsync(x => x.Amount));
            Assert.Equal(10, repository.Min(x => x.Amount));
            Assert.Equal(20m, repository.Average(x => x.Amount));

            int streamed = 0;
            await foreach (RelTenantNote note in repository.ReadManyAsync())
            {
                Assert.Equal(1, note.TenantId);
                streamed++;
            }

            Assert.Equal(3, streamed);

            RelTenantNote? other = await repository.ReadFirstAsync(x => x.TenantId == 2);
            Assert.Null(other);
            int otherId = (await repository.Query().IgnoreQueryFilters().Where(x => x.TenantId == 2).ExecuteAsync()).First().Id;
            Assert.Null(await repository.ReadByIdAsync(otherId));
            Assert.False(await repository.ExistsByIdAsync(otherId));
        }

        /// <summary>
        /// Query() honors the filter (including Count/Any/aggregates on the builder); IgnoreQueryFilters bypasses it.
        /// </summary>
        [Fact]
        public async Task QueryBuilderHonorsFilterAndIgnoreQueryFiltersBypasses()
        {
            ISqlRepository<RelTenantNote> repository = await SeedAsync();
            repository.AddQueryFilter(x => x.TenantId == 2);

            Assert.Equal(2, (await repository.Query().ExecuteAsync()).Count());
            Assert.Equal(2, await repository.Query().CountAsync());
            Assert.Equal(1, await repository.Query().Where(x => x.Amount > 100).CountAsync());
            Assert.Equal(300m, await repository.Query().SumAsync(x => x.Amount));
            Assert.Equal(5, await repository.Query().IgnoreQueryFilters().CountAsync());
            Assert.Equal(5, (await repository.Query().IgnoreQueryFilters().ExecuteAsync()).Count());
            Assert.Equal(360m, await repository.Query().IgnoreQueryFilters().SumAsync(x => x.Amount));

            Assert.Single(repository.QueryFilters);
            repository.ClearQueryFilters();
            Assert.Empty(repository.QueryFilters);
            Assert.Equal(5, await repository.CountAsync());
        }

        /// <summary>
        /// Multiple filters are AND-combined.
        /// </summary>
        [Fact]
        public async Task MultipleFiltersAreCombined()
        {
            ISqlRepository<RelTenantNote> repository = await SeedAsync();
            repository.AddQueryFilter(x => x.TenantId == 1);
            repository.AddQueryFilter(x => x.Amount > 10);
            Assert.Equal(2, await repository.CountAsync());
            Assert.Equal(50m, await repository.SumAsync(x => x.Amount));
        }

        /// <summary>
        /// DeleteMany, DeleteAll, UpdateField, BatchUpdate and UpdateMany only touch rows passing the filter.
        /// </summary>
        [Fact]
        public async Task FilterAppliesToBulkWrites()
        {
            ISqlRepository<RelTenantNote> repository = await SeedAsync();
            ISqlRepository<RelTenantNote> unfiltered = _Provider.CreateRepository<RelTenantNote>();
            repository.AddQueryFilter(x => x.TenantId == 1);

            int fieldRows = await repository.UpdateFieldAsync(x => x.Amount > 0, x => x.Title, "renamed");
            Assert.Equal(3, fieldRows);
            Assert.Equal(3, await unfiltered.CountAsync(x => x.Title == "renamed"));
            Assert.Equal(0, await unfiltered.CountAsync(x => x.TenantId == 2 && x.Title == "renamed"));

            int batchRows = await repository.BatchUpdateAsync(x => x.Amount > 0, x => new RelTenantNote { Amount = 1 });
            Assert.Equal(3, batchRows);
            Assert.Equal(300m, await unfiltered.SumAsync(x => x.Amount, x => x.TenantId == 2));

            int updateManyRows = repository.UpdateMany(x => x.Amount == 1, note => note.Amount = 2);
            Assert.Equal(3, updateManyRows);
            Assert.Equal(6m, await unfiltered.SumAsync(x => x.Amount, x => x.TenantId == 1));

            int deleted = await repository.DeleteManyAsync(x => x.Amount > 0);
            Assert.Equal(3, deleted);
            Assert.Equal(2, await unfiltered.CountAsync());
            Assert.Equal(0, await unfiltered.CountAsync(x => x.TenantId == 1));

            await unfiltered.CreateAsync(new RelTenantNote { TenantId = 1, Title = "again", Amount = 5 });
            int all = repository.DeleteAll();
            Assert.Equal(1, all);
            Assert.Equal(2, await unfiltered.CountAsync());
        }

        /// <summary>
        /// A filter that captures a mutable variable is re-evaluated per query (tenant switching).
        /// </summary>
        [Fact]
        public async Task CapturedVariableFollowsCurrentTenant()
        {
            ISqlRepository<RelTenantNote> repository = await SeedAsync();
            int currentTenant = 1;
            repository.AddQueryFilter(x => x.TenantId == currentTenant);

            Assert.Equal(3, await repository.CountAsync());
            Assert.All(await repository.Query().ExecuteAsync(), n => Assert.Equal(1, n.TenantId));

            currentTenant = 2;
            Assert.Equal(2, await repository.CountAsync());
            Assert.All(repository.ReadMany(), n => Assert.Equal(2, n.TenantId));
            Assert.Equal(300m, await repository.SumAsync(x => x.Amount));

            currentTenant = 3;
            Assert.Equal(0, await repository.CountAsync());
            Assert.Equal(0, await repository.DeleteManyAsync(x => x.Amount > 0));

            currentTenant = 2;
            Assert.Equal(2, await repository.DeleteManyAsync(x => x.Amount > 0));
            Assert.Equal(3, await repository.Query().IgnoreQueryFilters().CountAsync());
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<RelTenantNote>> SeedAsync()
        {
            ISqlRepository<RelTenantNote> repository = _Provider.CreateRepository<RelTenantNote>();
            await RelTestHelpers.RecreateTableAsync(repository);
            await repository.CreateManyAsync(new List<RelTenantNote>
            {
                new RelTenantNote { TenantId = 1, Title = "t1-a", Amount = 10 },
                new RelTenantNote { TenantId = 1, Title = "t1-b", Amount = 20 },
                new RelTenantNote { TenantId = 1, Title = "t1-c", Amount = 30 },
                new RelTenantNote { TenantId = 2, Title = "t2-a", Amount = 100 },
                new RelTenantNote { TenantId = 2, Title = "t2-b", Amount = 200 }
            });
            return repository;
        }

        #endregion
    }
}
