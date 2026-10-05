namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Differential tests: the same predicates, projections, aggregates and groupings run against the configured SQL
    /// provider and against the in-memory backend over identical data, and must return identical results. Predicates are
    /// limited to behavior every database defines the same way (no collation-dependent string comparisons and no ordering
    /// by nullable keys, where PostgreSQL sorts nulls last).
    /// </summary>
    public class InMemorySqlParityTestSuite
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the suite.
        /// </summary>
        /// <param name="provider">SQL provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public InMemorySqlParityTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Item predicates covering null semantics, IN lists, enums, arithmetic, math and date functions, navigation
        /// members, pattern characters and explicit string modes return the same rows on both backends.
        /// </summary>
        [Fact]
        public async Task PredicatesMatchSql()
        {
            using QueryTranslationFixture sql = await QueryTranslationFixture.CreateAsync(_Provider);
            InMemoryQtData memory = await InMemoryQtData.CreateAsync();

            int?[] withNull = { 2, null };
            int?[] values = { 0, 5 };
            List<QtStatus> statuses = new List<QtStatus> { QtStatus.Draft, QtStatus.Closed };
            DateTime cutoff = new DateTime(2024, 3, 1);
            List<Expression<Func<QtItem, bool>>> predicates = new List<Expression<Func<QtItem, bool>>>
            {
                x => x.Email == null,
                x => x.Email != null,
                x => x.Discount != 2,
                x => !(x.Discount > 1),
                x => x.IsFeatured == true,
                x => x.IsFeatured != true,
                x => x.IsFeatured == null,
                x => x.Discount == x.Quantity,
                x => x.Discount != x.Quantity,
                x => x.Discount == x.OwnerId,
                x => (x.Discount ?? 0) == 0,
                x => x.Discount.HasValue && x.DueDate != null,
                x => withNull.Contains(x.Discount),
                x => !withNull.Contains(x.Discount),
                x => !values.Contains(x.Discount),
                x => statuses.Contains(x.Status),
                x => !statuses.Contains(x.Status),
                x => x.Status == QtStatus.Active || x.Priority == QtPriority.Critical,
                x => x.Priority >= QtPriority.Medium,
                x => (int)x.Priority == 4,
                x => x.Price * x.Quantity > 50,
                x => x.Quantity / 2 == 2,
                x => x.Quantity % 2 == 0,
                x => x.Quantity - x.Discount == 3,
                x => -x.Ratio > 1,
                x => Math.Abs(x.Ratio) > 1,
                x => Math.Round(x.Price) == 100,
                x => Math.Ceiling(x.Price) == 8,
                x => Math.Floor(x.Price) == 99,
                x => (x.Quantity > 5 ? "big" : "small") == "big",
                x => x.CreatedUtc.Year == 2023,
                x => x.CreatedUtc.Month == 3,
                x => x.CreatedUtc.Day == 29,
                x => x.CreatedUtc.DayOfWeek == DayOfWeek.Sunday,
                x => x.CreatedUtc.AddDays(1) > new DateTime(2024, 7, 5),
                x => x.DueDate > x.CreatedUtc.AddDays(10),
                x => x.CreatedUtc >= cutoff,
                x => x.Owner!.Name == "Acme",
                x => x.Owner!.City == null,
                x => x.Code!.Contains("%"),
                x => x.Code!.Contains("_"),
                x => x.Code!.Contains("["),
                x => x.Name.Contains("'"),
                x => string.IsNullOrEmpty(x.Email),
                x => string.IsNullOrWhiteSpace(x.Email),
                x => x.Email!.EndsWith("@x.com", StringComparison.Ordinal),
                x => x.Email!.EndsWith("@X.COM", StringComparison.OrdinalIgnoreCase),
                x => x.Name.Equals("alpha", StringComparison.OrdinalIgnoreCase),
                x => x.Name.Substring(1, 3) == "lph",
                x => x.Name + "-" + x.Category == "Alpha-Tools"
            };

            List<string> mismatches = new List<string>();
            foreach (Expression<Func<QtItem, bool>> predicate in predicates)
            {
                string[] expected = Sorted((await sql.Items.Query().Where(predicate).ExecuteAsync()).Select(x => x.Name));
                string[] actual = Sorted((await memory.Items.Query().Where(predicate).ExecuteAsync()).Select(x => x.Name));
                if (!expected.SequenceEqual(actual, StringComparer.Ordinal))
                    mismatches.Add(predicate + ": SQL [" + string.Join(", ", expected) + "] in-memory [" + string.Join(", ", actual) + "]");
            }

            Assert.True(mismatches.Count == 0, string.Join(Environment.NewLine, mismatches));
        }

        /// <summary>
        /// Collection predicates over owners return the same owners on both backends.
        /// </summary>
        [Fact]
        public async Task CollectionPredicatesMatchSql()
        {
            using QueryTranslationFixture sql = await QueryTranslationFixture.CreateAsync(_Provider);
            InMemoryQtData memory = await InMemoryQtData.CreateAsync();
            List<Expression<Func<QtOwner, bool>>> predicates = new List<Expression<Func<QtOwner, bool>>>
            {
                o => o.Items.Any(),
                o => !o.Items.Any(),
                o => o.Items.Any(i => i.Price > 50),
                o => o.Items.All(i => i.IsActive),
                o => o.Items.Count() >= 2,
                o => o.Items.Count(i => i.IsActive) == 1,
                o => o.City == null || o.Items.Any(i => i.Status == QtStatus.Closed)
            };

            List<string> mismatches = new List<string>();
            foreach (Expression<Func<QtOwner, bool>> predicate in predicates)
            {
                string[] expected = Sorted((await sql.Owners.Query().Where(predicate).ExecuteAsync()).Select(x => x.Name));
                string[] actual = Sorted((await memory.Owners.Query().Where(predicate).ExecuteAsync()).Select(x => x.Name));
                if (!expected.SequenceEqual(actual, StringComparer.Ordinal))
                    mismatches.Add(predicate + ": SQL [" + string.Join(", ", expected) + "] in-memory [" + string.Join(", ", actual) + "]");
            }

            Assert.True(mismatches.Count == 0, string.Join(Environment.NewLine, mismatches));
        }

        /// <summary>
        /// Aggregates, paged counts, projections and grouped projections return the same values on both backends.
        /// </summary>
        [Fact]
        public async Task AggregatesProjectionsAndGroupingMatchSql()
        {
            using QueryTranslationFixture sql = await QueryTranslationFixture.CreateAsync(_Provider);
            InMemoryQtData memory = await InMemoryQtData.CreateAsync();

            Assert.Equal(await sql.Items.SumAsync(x => x.Price), await memory.Items.SumAsync(x => x.Price));
            Assert.Equal(await sql.Items.SumAsync(x => x.Discount), await memory.Items.SumAsync(x => x.Discount));
            Assert.Equal(Math.Round(await sql.Items.AverageAsync(x => x.Discount), 6), Math.Round(await memory.Items.AverageAsync(x => x.Discount), 6));
            Assert.Equal(await sql.Items.MaxAsync(x => x.Quantity), await memory.Items.MaxAsync(x => x.Quantity));
            Assert.Equal(await sql.Items.MinAsync(x => x.Price, x => x.IsActive), await memory.Items.MinAsync(x => x.Price, x => x.IsActive));
            Assert.Equal(await sql.Items.MaxAsync(x => x.Priority), await memory.Items.MaxAsync(x => x.Priority));
            Assert.Equal(await sql.Items.SumAsync(x => x.Price, x => x.Quantity > 100), await memory.Items.SumAsync(x => x.Price, x => x.Quantity > 100));
            Assert.Equal(await sql.Items.MaxAsync(x => x.Discount, x => x.Discount == null), await memory.Items.MaxAsync(x => x.Discount, x => x.Discount == null));
            Assert.Equal(await sql.Items.Query().OrderBy(x => x.Price).Skip(2).Take(3).CountAsync(), await memory.Items.Query().OrderBy(x => x.Price).Skip(2).Take(3).CountAsync());
            Assert.Equal(await sql.Items.Query().OrderBy(x => x.Price).Take(2).SumAsync(x => x.Price), await memory.Items.Query().OrderBy(x => x.Price).Take(2).SumAsync(x => x.Price));

            string[] sqlProjection = (await sql.Items.Query().Where(x => x.Quantity > 0)
                .Select(x => new QtItemSummary { Name = x.Name, Total = x.Price * x.Quantity, IsExpensive = x.Price > 50 })
                .OrderByDescending(s => s.Total).Take(3).ExecuteAsync()).Select(s => s.Name + ":" + s.Total.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture) + ":" + s.IsExpensive).ToArray();
            string[] memoryProjection = (await memory.Items.Query().Where(x => x.Quantity > 0)
                .Select(x => new QtItemSummary { Name = x.Name, Total = x.Price * x.Quantity, IsExpensive = x.Price > 50 })
                .OrderByDescending(s => s.Total).Take(3).ExecuteAsync()).Select(s => s.Name + ":" + s.Total.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture) + ":" + s.IsExpensive).ToArray();
            Assert.Equal(sqlProjection, memoryProjection);

            string[] sqlGroups = (await sql.Items.Query().GroupBy(x => x.Category).Select(g => new QtCategoryTotals
            {
                Category = g.Key,
                ItemCount = g.Count(),
                ActiveCount = g.Count(x => x.IsActive),
                TotalPrice = g.Sum(x => x.Price),
                MaxQuantity = g.Max(x => x.Quantity)
            }).ExecuteAsync()).Select(Describe).OrderBy(s => s, StringComparer.Ordinal).ToArray();
            string[] memoryGroups = (await memory.Items.Query().GroupBy(x => x.Category).Select(g => new QtCategoryTotals
            {
                Category = g.Key,
                ItemCount = g.Count(),
                ActiveCount = g.Count(x => x.IsActive),
                TotalPrice = g.Sum(x => x.Price),
                MaxQuantity = g.Max(x => x.Quantity)
            }).ExecuteAsync()).Select(Describe).OrderBy(s => s, StringComparer.Ordinal).ToArray();
            Assert.Equal(sqlGroups, memoryGroups);

            Assert.Equal(
                await sql.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Price) > 20).CountAsync(),
                await memory.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Price) > 20).CountAsync());
            Assert.Equal(
                await sql.Items.Query().GroupBy(x => x.Category).Having(g => g.Count(x => x.IsActive) == 1).SumAsync(x => x.Price),
                await memory.Items.Query().GroupBy(x => x.Category).Having(g => g.Count(x => x.IsActive) == 1).SumAsync(x => x.Price));
        }

        #endregion

        #region Private-Methods

        private static string[] Sorted(IEnumerable<string> names)
        {
            return names.OrderBy(n => n, StringComparer.Ordinal).ToArray();
        }

        private static string Describe(QtCategoryTotals totals)
        {
            return totals.Category + "|" + totals.ItemCount + "|" + totals.ActiveCount + "|"
                + totals.TotalPrice.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture) + "|" + totals.MaxQuantity;
        }

        #endregion
    }
}
