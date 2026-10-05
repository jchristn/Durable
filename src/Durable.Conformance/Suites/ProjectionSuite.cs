namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Select projections (<see cref="RepositoryCapabilities.Projection"/>) into a DTO with computed members, with
    /// Where / OrderBy / Skip / Take / Count / Any applied to projected members, and Distinct
    /// (<see cref="RepositoryCapabilities.Distinct"/>) over entities and projections.
    /// </summary>
    internal sealed class ProjectionSuite : KitSuite
    {
        public ProjectionSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Projection, Description = "Computed members: arithmetic, comparisons, ternary, concatenation, enums, nullable values")]
        public async Task SelectComputedMembers()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfItemSummary> rows = await ExecuteAsync(Summaries(f), "Select(summary)");
            Assert.Equal(6, rows.Count);
            Dictionary<string, CfItemSummary> byName = rows.ToDictionary(r => r.Name, StringComparer.Ordinal);
            CfItemSummary delta = byName[ItemFixture.Delta];
            Assert.Equal("Garden", delta.Category);
            Assert.Equal(299.25m, delta.Total);
            Assert.True(delta.IsActive);
            Assert.True(delta.IsExpensive);
            Assert.Equal("premium", delta.Tier);
            Assert.Equal(CfStatus.Closed, delta.Status);
            Assert.Equal(CfPriority.Medium, delta.Priority);
            Assert.Equal("Delta (Garden)", delta.Label);
            Assert.Equal(5, delta.Discount);

            CfItemSummary beta = byName[ItemFixture.Beta];
            Assert.False(beta.IsActive);
            Assert.False(beta.IsExpensive);
            Assert.Equal("standard", beta.Tier);
            Assert.Equal(0m, beta.Total);
            Assert.Null(beta.Discount);
            Assert.Equal(CfStatus.Draft, beta.Status);
            Assert.Equal(CfPriority.Low, beta.Priority);
            Assert.Equal("O'Brien (Garden)", byName[ItemFixture.OBrien].Label);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Projection, Description = "Where / OrderByDescending / Skip / Take on computed projection members")]
        public async Task QueryOverProjectedMembers()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfItemSummary> rows = await ExecuteAsync(Summaries(f).Where(s => s.Total > 50).OrderByDescending(s => s.Total).Skip(1).Take(2),
                "Select.Where(Total > 50).OrderByDescending(Total).Skip(1).Take(2)");
            Assert.Equal(new[] { ItemFixture.Echo, ItemFixture.OBrien }, rows.Select(r => r.Name).ToArray());
            Assert.Equal(120.00m, rows[0].Total);
            Assert.Equal(87.00m, rows[1].Total);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Projection, Description = "Count and Any over projections filtered by projected members")]
        public async Task CountAndAnyOverProjection()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(4L, await Summaries(f).Where(s => s.Total > 50).CountAsync(Token));
            Assert.Equal(1L, Summaries(f).Where(s => s.IsExpensive).Count());
            Assert.Equal(5L, await Summaries(f).Where(s => s.Tier == "standard").CountAsync(Token));
            Assert.True(await Summaries(f).Where(s => s.Label == "Alpha (Tools)").AnyAsync(Token));
            Assert.False(Summaries(f).Where(s => s.Total > 1000).Any());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Projection, Description = "Where on the entity before Select; unselected members keep their defaults")]
        public async Task WhereBeforeSelect()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfItemSummary> rows = await ExecuteAsync(f.Items.Query().Where(x => x.Category == "Garden").Select(x => new CfItemSummary { Name = x.Name }),
                "Where(Category == Garden).Select(Name only)");
            ConformanceAssert.NameSet(rows.Select(r => r.Name), new[] { ItemFixture.OBrien, ItemFixture.Delta }, "projected names");
            Assert.All(rows, r => Assert.Equal(string.Empty, r.Category));
            Assert.All(rows, r => Assert.Equal(0m, r.Total));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Projection, Description = "Ordering and paging on the entity before Select are kept")]
        public async Task OrderBeforeSelect()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfItemSummary> rows = await ExecuteAsync(f.Items.Query().OrderBy(x => x.Price).Take(3).Select(x => new CfItemSummary { Name = x.Name, Total = x.Price }),
                "OrderBy(Price).Take(3).Select");
            Assert.Equal(new[] { ItemFixture.Foxtrot, ItemFixture.OBrien, ItemFixture.Alpha }, rows.Select(r => r.Name).ToArray());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Projection, Description = "Projections execute synchronously and streamed")]
        public async Task ProjectionExecutionVariants()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfItemSummary> sync = Summaries(f).Where(s => s.IsActive).Execute().ToList();
            ConformanceAssert.NameSet(sync.Select(s => s.Name), f.NamesWhere(x => x.IsActive), "Select.Where(IsActive).Execute()");
            List<string> streamed = new List<string>();
            await foreach (CfItemSummary summary in Summaries(f).Where(s => !s.IsActive).ExecuteAsyncEnumerable(Token)) streamed.Add(summary.Name);
            ConformanceAssert.NameSet(streamed, f.NamesWhere(x => !x.IsActive), "Select.Where(!IsActive).ExecuteAsyncEnumerable()");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Projection | RepositoryCapabilities.Functions, Description = "Functions inside projections")]
        public async Task FunctionsInProjection()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfItemSummary> rows = await ExecuteAsync(f.Items.Query().Select(x => new CfItemSummary { Name = x.Name, NameLength = x.Name.Length, Label = x.Name.ToUpper() }),
                "Select(Name.Length, Name.ToUpper())");
            CfItemSummary obrien = rows.Single(r => r.Name == ItemFixture.OBrien);
            Assert.Equal(7, obrien.NameLength);
            Assert.Equal("O'BRIEN", obrien.Label);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Projection | RepositoryCapabilities.Distinct, Description = "Distinct over a single-member projection")]
        public async Task DistinctProjection()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfItemSummary> rows = await ExecuteAsync(f.Items.Query().Select(x => new CfItemSummary { Category = x.Category }).Distinct(), "Select(Category).Distinct()");
            Assert.Equal(new[] { "Garden", "Kitchen", "Tools" }, rows.Select(r => r.Category).OrderBy(c => c, StringComparer.Ordinal).ToArray());
            Assert.Equal(3L, await f.Items.Query().Select(x => new CfItemSummary { Category = x.Category }).Distinct().CountAsync(Token));
            List<CfItemSummary> activeCategories = await ExecuteAsync(f.Items.Query().Where(x => x.IsActive).Select(x => new CfItemSummary { Category = x.Category, IsActive = x.IsActive }).Distinct(),
                "Where(IsActive).Select(Category, IsActive).Distinct()");
            Assert.Equal(3, activeCategories.Count);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Distinct, Description = "Distinct over entities keeps every distinct row")]
        public async Task DistinctEntities()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(6, (await f.Items.Query().Distinct().ExecuteAsync(Token)).Count());
            Assert.Equal(2, f.Items.Query().Where(x => x.Category == "Tools").Distinct().Execute().Count());
            Assert.Equal(6L, f.Items.Query().Distinct().Count());
        }

        private static IQueryBuilder<CfItemSummary> Summaries(ItemFixture f)
        {
            return f.Items.Query().Select(x => new CfItemSummary
            {
                Name = x.Name,
                Category = x.Category,
                Total = x.Price * x.Quantity,
                IsActive = x.IsActive,
                IsExpensive = x.Price > 50,
                Tier = x.Price > 50 ? "premium" : "standard",
                Status = x.Status,
                Priority = x.Priority,
                Label = x.Name + " (" + x.Category + ")",
                Discount = x.Discount
            });
        }
    }
}
