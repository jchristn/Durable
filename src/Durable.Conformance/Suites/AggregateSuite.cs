namespace Durable.Conformance
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Sum / Average / Min / Max (<see cref="RepositoryCapabilities.Aggregates"/>) on the repository and on queries, sync
    /// and async, over int, long, decimal, double, nullable, DateTime, string and computed selectors, plus the documented
    /// empty-set results (zero for Sum and Average, default for Min and Max). Count and Any need no capability.
    /// </summary>
    internal sealed class AggregateSuite : KitSuite
    {
        public AggregateSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "Count and Any (no capability needed) with and without filters")]
        public async Task CountAndAny()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(6L, f.Items.Query().Count());
            Assert.Equal(3L, await f.Items.Query().Where(x => x.Quantity > 4).CountAsync(Token));
            Assert.True(f.Items.Query().Where(x => x.Name == ItemFixture.Delta).Any());
            Assert.False(await f.Items.Query().Where(x => x.Price > 1000).AnyAsync(Token));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Repository Sum / Average / Min / Max, with and without a predicate")]
        public async Task RepositoryAggregates()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(153.00m, f.Items.Sum(x => x.Price));
            Assert.Equal(25.5m, f.Items.Average(x => x.Price));
            Assert.Equal(0.50m, f.Items.Min(x => x.Price));
            Assert.Equal(99.75m, f.Items.Max(x => x.Price));
            Assert.Equal(107.00m, f.Items.Sum(x => x.Price, x => x.Category == "Garden"));
            Assert.Equal(53.5m, f.Items.Average(x => x.Price, x => x.Category == "Garden"));
            Assert.Equal(3, f.Items.Min(x => x.Quantity, x => x.Category == "Garden"));
            Assert.Equal(12, f.Items.Max(x => x.Quantity, x => x.Category == "Garden"));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Repository aggregates, async")]
        public async Task RepositoryAggregatesAsync()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(29m, await f.Items.SumAsync(x => x.Quantity, null, null, Token));
            Assert.Equal(2.5m, await f.Items.AverageAsync(x => x.Quantity, x => x.Category == "Tools", null, Token));
            Assert.Equal(0, await f.Items.MinAsync(x => x.Quantity, null, null, Token));
            Assert.Equal(12, await f.Items.MaxAsync(x => x.Quantity, null, null, Token));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Query Sum / Average / Min / Max honor Where, sync and async")]
        public async Task QueryAggregates()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(30.50m, f.Items.Query().Where(x => x.Category == "Tools").Sum(x => x.Price));
            Assert.Equal(15.25m, await f.Items.Query().Where(x => x.Category == "Tools").AverageAsync(x => x.Price, Token));
            Assert.Equal(7.25m, f.Items.Query().Where(x => x.IsActive && x.Quantity > 4).Min(x => x.Price));
            Assert.Equal(10.50m, await f.Items.Query().Where(x => x.IsActive && x.Quantity > 4).MaxAsync(x => x.Price, Token));
            Assert.Equal(8m, await f.Items.Query().Where(x => x.IsActive).SumAsync(x => x.Discount, Token));
            Assert.Equal(9m, await f.Items.Query().Where(x => x.Category == "Kitchen").SumAsync(x => x.Quantity, Token));
            Assert.Equal(15.50m, await f.Items.Query().Where(x => x.Category == "Kitchen").SumAsync(x => x.Price, Token));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Averages of integers are not truncated")]
        public async Task IntegerAverageIsExact()
        {
            ItemFixture f = await SeedItemsAsync();
            decimal average = f.Items.Average(x => x.Quantity);
            Assert.True(Math.Abs(average - (29m / 6m)) < 0.0001m, "Average(Quantity) should be 4.8333..., got " + average);
            Assert.Equal(2.5m, f.Items.Query().Where(x => x.Category == "Tools").Average(x => x.Quantity));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Aggregates over long, double and computed selectors")]
        public async Task AggregatesOverOtherTypes()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(14000000036m, f.Items.Sum(x => x.BigNumber));
            Assert.Equal(9000000000L, f.Items.Max(x => x.BigNumber));
            Assert.Equal(3.875m, f.Items.Sum(x => x.Ratio));
            Assert.Equal(-1.25, f.Items.Min(x => x.Ratio));
            Assert.Equal(559.25m, f.Items.Sum(x => x.Price * x.Quantity));
            Assert.Equal(299.25m, f.Items.Max(x => x.Price * x.Quantity));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Aggregates over a nullable column ignore nulls")]
        public async Task AggregatesIgnoreNulls()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(8m, f.Items.Sum(x => x.Discount));
            Assert.Equal(2m, f.Items.Average(x => x.Discount));
            Assert.Equal(0, f.Items.Min(x => x.Discount));
            Assert.Equal(5, f.Items.Max(x => x.Discount));
            Assert.Null(f.Items.Max(x => x.Discount, x => x.Discount == null));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Min / Max over DateTime, nullable DateTime and string")]
        public async Task MinMaxOverDatesAndStrings()
        {
            ItemFixture f = await SeedItemsAsync();
            ConformanceAssert.SameInstant(new DateTime(2024, 7, 4, 12, 0, 0), f.Items.Max(x => x.CreatedUtc), "Max(CreatedUtc)");
            ConformanceAssert.SameInstant(new DateTime(2023, 12, 31, 23, 0, 0), f.Items.Min(x => x.CreatedUtc), "Min(CreatedUtc)");
            DateTime? latestDue = f.Items.Max(x => x.DueDate);
            Assert.NotNull(latestDue);
            ConformanceAssert.SameInstant(new DateTime(2024, 4, 1), latestDue.Value, "Max(DueDate)");
            Assert.Equal(ItemFixture.Alpha, f.Items.Min(x => x.Name));
            Assert.Equal(ItemFixture.OBrien, f.Items.Max(x => x.Name));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Empty sets: Sum and Average return 0, Min and Max return default")]
        public async Task AggregatesOverEmptySet()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(0m, f.Items.Sum(x => x.Price, x => x.Category == "None"));
            Assert.Equal(0m, f.Items.Average(x => x.Price, x => x.Category == "None"));
            Assert.Equal(0, f.Items.Max(x => x.Quantity, x => x.Category == "None"));
            Assert.Equal(0m, f.Items.Min(x => x.Price, x => x.Category == "None"));
            Assert.Null(f.Items.Max(x => x.Discount, x => x.Category == "None"));
            Assert.Equal(0m, await f.Items.Query().Where(x => x.Category == "None").SumAsync(x => x.Quantity, Token));
            Assert.Equal(0m, await f.Items.Query().Where(x => x.Category == "None").AverageAsync(x => x.Quantity, Token));
            Assert.Equal(0, await f.Items.Query().Where(x => x.Category == "None").MaxAsync(x => x.Quantity, Token));
            await ResetAsync(typeof(CfItem));
            Assert.Equal(0m, f.Items.Sum(x => x.Price));
            Assert.Equal(0, f.Items.Min(x => x.Quantity));
        }
    }
}
