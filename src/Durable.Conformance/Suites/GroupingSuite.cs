namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// GroupBy (<see cref="RepositoryCapabilities.Grouping"/>): groups with their members for string, enum, nullable and
    /// boolean keys, Where before grouping, Having with Count / Sum / Average / Min / Max, group counts, aggregates over
    /// the rows of the selected groups, and grouped projections (<see cref="RepositoryCapabilities.Projection"/>).
    /// </summary>
    internal sealed class GroupingSuite : KitSuite
    {
        private const string A = ItemFixture.Alpha;
        private const string B = ItemFixture.Beta;
        private const string O = ItemFixture.OBrien;
        private const string D = ItemFixture.Delta;
        private const string E = ItemFixture.Echo;
        private const string F = ItemFixture.Foxtrot;

        public GroupingSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping, Description = "Each group holds exactly the rows with its key")]
        public async Task GroupsHoldTheirMembers()
        {
            ItemFixture f = await SeedItemsAsync();
            List<IGrouping<string, CfItem>> groups = (await f.Items.Query().GroupBy(x => x.Category).ExecuteAsync(Token)).ToList();
            AssertGroups(groups, new Dictionary<string, string[]>
            {
                { "Tools", new[] { A, B } },
                { "Garden", new[] { O, D } },
                { "Kitchen", new[] { E, F } }
            });
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping, Description = "Where filters rows before grouping")]
        public async Task WhereBeforeGroupBy()
        {
            ItemFixture f = await SeedItemsAsync();
            List<IGrouping<string, CfItem>> groups = f.Items.Query().Where(x => x.IsActive).GroupBy(x => x.Category).Execute().ToList();
            AssertGroups(groups, new Dictionary<string, string[]>
            {
                { "Tools", new[] { A } },
                { "Garden", new[] { O, D } },
                { "Kitchen", new[] { F } }
            });
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping, Description = "Having with Count, Sum and Average")]
        public async Task HavingFiltersGroups()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(new[] { "Garden", "Kitchen", "Tools" }, Keys(f.Items.Query().GroupBy(x => x.Category).Having(g => g.Count() > 1).Execute()));
            Assert.Equal(new[] { "Garden" }, Keys(f.Items.Query().Where(x => x.IsActive).GroupBy(x => x.Category).Having(g => g.Count() > 1).Execute()));
            Assert.Equal(new[] { "Garden", "Tools" }, Keys(f.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Price) > 25).Execute()));
            Assert.Equal(new[] { "Garden" }, Keys(f.Items.Query().GroupBy(x => x.Category).Having(g => g.Average(x => x.Price) > 50).Execute()));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping, Description = "Having with Min / Max, combined conditions and several Having calls")]
        public async Task HavingWithMinMaxAndCombinations()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(new[] { "Kitchen" }, Keys(f.Items.Query().GroupBy(x => x.Category).Having(g => g.Max(x => x.Quantity) >= 8 && g.Min(x => x.Price) < 1).Execute()));
            Assert.Equal(new[] { "Garden", "Kitchen" }, Keys(f.Items.Query().GroupBy(x => x.Category).Having(g => g.Max(x => x.Quantity) >= 8).Execute()));
            Assert.Equal(new[] { "Garden" }, Keys(f.Items.Query().GroupBy(x => x.Category).Having(g => g.Max(x => x.Quantity) >= 8).Having(g => g.Min(x => x.Price) > 1).Execute()));
            Assert.Equal(new[] { "Garden", "Tools" }, Keys(f.Items.Query().GroupBy(x => x.Category).Having(g => g.Count(x => x.IsActive) == 2 || g.Sum(x => x.Quantity) < 6).Execute()));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping, Description = "Count counts groups after Having")]
        public async Task CountCountsGroups()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(3L, f.Items.Query().GroupBy(x => x.Category).Count());
            Assert.Equal(1L, await f.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Quantity) > 10).CountAsync(Token));
            Assert.Equal(4L, f.Items.Query().GroupBy(x => x.Priority).Count());
            Assert.Equal(0L, f.Items.Query().Where(x => x.Quantity > 100).GroupBy(x => x.Category).Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping, Description = "Group aggregates cover all rows of the selected groups")]
        public async Task GroupedAggregates()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(153.00m, f.Items.Query().GroupBy(x => x.Category).Sum(x => x.Price));
            Assert.Equal(137.50m, f.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Price) > 25).Sum(x => x.Price));
            Assert.Equal(34.375m, f.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Price) > 25).Average(x => x.Price));
            Assert.Equal(8, f.Items.Query().GroupBy(x => x.Category).Having(g => g.Min(x => x.Price) < 1).Max(x => x.Quantity));
            Assert.Equal(1, f.Items.Query().GroupBy(x => x.Category).Having(g => g.Min(x => x.Price) < 1).Min(x => x.Quantity));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping, Description = "Async group execution, counting and aggregates")]
        public async Task GroupedAsyncVariants()
        {
            ItemFixture f = await SeedItemsAsync();
            IEnumerable<IGrouping<bool, CfItem>> groups = await f.Items.Query().GroupBy(x => x.IsActive).ExecuteAsync(Token);
            Assert.Equal(new[] { false, true }, groups.Select(g => g.Key).OrderBy(k => k).ToArray());
            Assert.Equal(2L, await f.Items.Query().GroupBy(x => x.IsActive).CountAsync(Token));
            Assert.Equal(29m, await f.Items.Query().GroupBy(x => x.IsActive).SumAsync(x => x.Quantity, Token));
            Assert.Equal(25.5m, await f.Items.Query().GroupBy(x => x.IsActive).AverageAsync(x => x.Price, Token));
            Assert.Equal(12, await f.Items.Query().GroupBy(x => x.IsActive).MaxAsync(x => x.Quantity, Token));
            Assert.Equal(0.50m, await f.Items.Query().GroupBy(x => x.IsActive).MinAsync(x => x.Price, Token));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping, Description = "Enum keys (both storage modes), boolean keys and nullable keys (with a null group)")]
        public async Task GroupKeyTypes()
        {
            ItemFixture f = await SeedItemsAsync();
            List<IGrouping<CfPriority, CfItem>> byPriority = f.Items.Query().GroupBy(x => x.Priority).Execute().ToList();
            Assert.Equal(new[] { B, F }, Members(byPriority.Single(g => g.Key == CfPriority.Low)));
            Assert.Equal(new[] { D, E }, Members(byPriority.Single(g => g.Key == CfPriority.Medium)));
            Assert.Equal(4, byPriority.Count);

            List<IGrouping<CfStatus, CfItem>> byStatus = f.Items.Query().GroupBy(x => x.Status).Execute().ToList();
            Assert.Equal(new[] { A, E, F }, Members(byStatus.Single(g => g.Key == CfStatus.Active)));
            Assert.Equal(4, byStatus.Count);

            List<IGrouping<bool, CfItem>> byActive = f.Items.Query().GroupBy(x => x.IsActive).Execute().ToList();
            Assert.Equal(new[] { B, E }, Members(byActive.Single(g => !g.Key)));

            List<IGrouping<int?, CfItem>> byDiscount = f.Items.Query().GroupBy(x => x.Discount).Execute().ToList();
            Assert.Equal(5, byDiscount.Count);
            Assert.Equal(new[] { B, E }, Members(byDiscount.Single(g => g.Key == null)));
            Assert.Equal(new[] { D }, Members(byDiscount.Single(g => g.Key == 5)));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping | RepositoryCapabilities.Projection, Description = "Grouped Select into a DTO with Count / Count(predicate) / Sum / Min / Max / Average, ordered by an aggregate")]
        public async Task GroupedSelect()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfCategoryTotals> rows = await ExecuteAsync(CategoryTotals(f).OrderByDescending(t => t.TotalPrice), "GroupBy(Category).Select(totals).OrderByDescending(TotalPrice)");
            Assert.Equal(new[] { "Garden", "Tools", "Kitchen" }, rows.Select(r => r.Category).ToArray());
            AssertTotals(rows[0], 2, 2, 107.00m, 12, 3, 53.5m);
            AssertTotals(rows[1], 2, 1, 30.50m, 5, 0, 15.25m);
            AssertTotals(rows[2], 2, 1, 15.50m, 8, 1, 7.75m);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Grouping | RepositoryCapabilities.Projection, Description = "Where / OrderBy / Take / Count over a grouped projection, and Having before Select")]
        public async Task GroupedSelectComposition()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfCategoryTotals> active = await ExecuteAsync(CategoryTotals(f).Where(t => t.ActiveCount >= 2), "totals.Where(ActiveCount >= 2)");
            Assert.Equal(new[] { "Garden" }, active.Select(r => r.Category).ToArray());
            List<CfCategoryTotals> firstTwo = await ExecuteAsync(CategoryTotals(f).OrderBy(t => t.Category).Take(2), "totals.OrderBy(Category).Take(2)");
            Assert.Equal(new[] { "Garden", "Kitchen" }, firstTwo.Select(r => r.Category).ToArray());
            Assert.Equal(3L, await CategoryTotals(f).CountAsync(Token));

            List<CfCategoryTotals> having = await ExecuteAsync(f.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Price) > 25)
                .Select(g => new CfCategoryTotals { Category = g.Key, ItemCount = g.Count() }).OrderBy(t => t.Category), "GroupBy.Having.Select");
            Assert.Equal(new[] { "Garden", "Tools" }, having.Select(r => r.Category).ToArray());
            Assert.All(having, r => Assert.Equal(2, r.ItemCount));
        }

        private static IQueryBuilder<CfCategoryTotals> CategoryTotals(ItemFixture f)
        {
            return f.Items.Query().GroupBy(x => x.Category).Select(g => new CfCategoryTotals
            {
                Category = g.Key,
                ItemCount = g.Count(),
                ActiveCount = g.Count(x => x.IsActive),
                TotalPrice = g.Sum(x => x.Price),
                MaxQuantity = g.Max(x => x.Quantity),
                MinQuantity = g.Min(x => x.Quantity),
                AveragePrice = g.Average(x => x.Price)
            });
        }

        private static void AssertTotals(CfCategoryTotals row, int count, int active, decimal total, int max, int min, decimal average)
        {
            string context = "Totals for " + row.Category;
            Assert.True(row.ItemCount == count, context + ": ItemCount " + row.ItemCount + " != " + count);
            Assert.True(row.ActiveCount == active, context + ": ActiveCount " + row.ActiveCount + " != " + active);
            Assert.True(row.TotalPrice == total, context + ": TotalPrice " + row.TotalPrice + " != " + total);
            Assert.True(row.MaxQuantity == max, context + ": MaxQuantity " + row.MaxQuantity + " != " + max);
            Assert.True(row.MinQuantity == min, context + ": MinQuantity " + row.MinQuantity + " != " + min);
            Assert.True(row.AveragePrice == average, context + ": AveragePrice " + row.AveragePrice + " != " + average);
        }

        private static void AssertGroups(List<IGrouping<string, CfItem>> groups, Dictionary<string, string[]> expected)
        {
            Assert.Equal(expected.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(), groups.Select(g => g.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray());
            foreach (IGrouping<string, CfItem> group in groups)
            {
                ConformanceAssert.NameSet(group.Select(i => i.Name), expected[group.Key], "members of group " + group.Key);
                Assert.All(group, i => Assert.Equal(group.Key, i.Category));
            }
        }

        private static string[] Keys(IEnumerable<IGrouping<string, CfItem>> groups)
        {
            return groups.Select(g => g.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray();
        }

        private static string[] Members<TKey>(IGrouping<TKey, CfItem> group)
        {
            return group.Select(i => i.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        }
    }
}
