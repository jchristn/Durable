namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;
    using Xunit;

    /// <summary>
    /// Query semantics of the in-memory backend over the query-translation data set: C# null semantics, IN lists, string
    /// match modes, string/math/date functions, arithmetic, enums, ordering and paging, navigation predicates, client-side
    /// projections and grouping, and lazy evaluation of captured variables. Expected results are the ones the SQL suites
    /// assert for the same data.
    /// </summary>
    public class InMemoryQueryTestSuite
    {
        #region Private-Members

        private const string Alpha = InMemoryQtData.Alpha;
        private const string Beta = InMemoryQtData.Beta;
        private const string OBrien = InMemoryQtData.OBrien;
        private const string Zoe = InMemoryQtData.Zoe;
        private const string Nihon = InMemoryQtData.Nihon;
        private const string Padded = InMemoryQtData.Padded;
        private static readonly string[] _All = { Alpha, Beta, OBrien, Zoe, Nihon, Padded };

        #endregion

        #region Public-Methods

        /// <summary>
        /// Comparisons with null and between nullable columns follow C#: null == null is true, null != value is true,
        /// ordering comparisons with null are false and their negation true.
        /// </summary>
        [Fact]
        public async Task NullSemanticsFollowCSharp()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await CheckAsync(d, x => x.Email == null, Beta);
            await CheckAsync(d, x => x.Email != null, Alpha, OBrien, Zoe, Nihon, Padded);
            await CheckAsync(d, x => x.Discount != 2, Beta, OBrien, Zoe, Nihon, Padded);
            await CheckAsync(d, x => x.Discount > 1, Alpha, Zoe);
            await CheckAsync(d, x => !(x.Discount > 1), Beta, OBrien, Nihon, Padded);
            await CheckAsync(d, x => x.IsFeatured == true, Alpha, Zoe);
            await CheckAsync(d, x => x.IsFeatured != true, Beta, OBrien, Nihon, Padded);
            await CheckAsync(d, x => x.IsFeatured == null, Beta, Nihon);
            await CheckAsync(d, x => x.Discount == x.Quantity, Padded);
            await CheckAsync(d, x => x.Discount != x.Quantity, Alpha, Beta, OBrien, Zoe, Nihon);
            await CheckAsync(d, x => x.Discount == x.OwnerId, Nihon);
            await CheckAsync(d, x => x.Discount.HasValue, Alpha, OBrien, Zoe, Padded);
            await CheckAsync(d, x => (x.Discount ?? 0) == 0, Beta, OBrien, Nihon);
            await CheckAsync(d, x => x.DueDate == null && x.Discount == null, Beta, Nihon);
        }

        /// <summary>
        /// Collection Contains (IN) with nulls, negation, empty lists and enums.
        /// </summary>
        [Fact]
        public async Task InListsHandleNullsAndNegation()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            int?[] withNull = { 2, null };
            int?[] values = { 0, 5 };
            List<int> empty = new List<int>();
            await CheckAsync(d, x => withNull.Contains(x.Discount), Alpha, Beta, Nihon);
            await CheckAsync(d, x => !withNull.Contains(x.Discount), OBrien, Zoe, Padded);
            await CheckAsync(d, x => !values.Contains(x.Discount), Alpha, Beta, Nihon, Padded);
            await CheckAsync(d, x => empty.Contains(x.Quantity));
            await CheckAsync(d, x => !empty.Contains(x.Quantity), _All);
            await CheckAsync(d, x => new[] { "alpha", "Beta" }.Contains(x.Name), Beta);

            List<QtStatus> statuses = new List<QtStatus> { QtStatus.Draft, QtStatus.Closed };
            QtPriority[] priorities = { QtPriority.Low, QtPriority.Critical };
            await CheckAsync(d, x => statuses.Contains(x.Status), Beta, Zoe);
            await CheckAsync(d, x => priorities.Contains(x.Priority), Beta, OBrien, Padded);
            await CheckAsync(d, x => x.Status.NotIn(QtStatus.Active, QtStatus.Draft), OBrien, Zoe);
        }

        /// <summary>
        /// String modes: Database behaves as ordinal (case- and accent-sensitive); explicit StringComparison arguments
        /// and the repository-wide StringMatching option select ordinal or ordinal-ignore-case matching.
        /// </summary>
        [Fact]
        public async Task StringMatchModes()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await CheckAsync(d, x => x.Name == "alpha");
            await CheckAsync(d, x => x.Name.Equals("alpha", StringComparison.OrdinalIgnoreCase), Alpha);
            await CheckAsync(d, x => x.Name.Contains("ALP", StringComparison.OrdinalIgnoreCase), Alpha);
            await CheckAsync(d, x => x.Name.StartsWith("b"));
            await CheckAsync(d, x => x.Name.StartsWith("B"), Beta);
            await CheckAsync(d, x => x.Email!.EndsWith("@x.com"), Alpha, Zoe);
            await CheckAsync(d, x => x.Email!.EndsWith("@X.COM", StringComparison.OrdinalIgnoreCase), Alpha, OBrien, Zoe);
            await CheckAsync(d, x => x.Name == "Zoe");
            await CheckAsync(d, x => x.Name.Equals("ZOË", StringComparison.OrdinalIgnoreCase), Zoe);
            await CheckAsync(d, x => x.Email!.StartsWith(x.Name));
            await CheckAsync(d, x => x.Email!.StartsWith(x.Name, StringComparison.OrdinalIgnoreCase), Alpha);
            await CheckAsync(d, x => string.Compare(x.Name, "B", StringComparison.Ordinal) < 0, Alpha, Padded);

            InMemoryQtData ignoreCase = await InMemoryQtData.CreateAsync(options: new RepositoryOptions { StringMatching = StringMatchMode.IgnoreCase });
            await CheckAsync(ignoreCase, x => x.Name == "ALPHA", Alpha);
            await CheckAsync(ignoreCase, x => x.Email!.Contains("X.COM"), Alpha, OBrien, Zoe);
            await CheckAsync(ignoreCase, x => new[] { "beta", "zoË" }.Contains(x.Name), Beta, Zoe);
            await CheckAsync(ignoreCase, x => x.Name.Equals("alpha", StringComparison.Ordinal));
        }

        /// <summary>
        /// LIKE metacharacters in patterns are literal; IsNullOrEmpty and IsNullOrWhiteSpace treat null, empty and blank.
        /// </summary>
        [Fact]
        public async Task PatternCharactersAreLiteral()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await CheckAsync(d, x => x.Code!.Contains("%"), Beta, Padded);
            await CheckAsync(d, x => x.Code!.Contains("_"), Beta);
            await CheckAsync(d, x => x.Code!.Contains("\\"), OBrien);
            await CheckAsync(d, x => x.Code!.Contains("["), Zoe);
            await CheckAsync(d, x => x.Code!.Contains("\n"), Nihon);
            await CheckAsync(d, x => x.Name.Contains("'"), OBrien);
            await CheckAsync(d, x => string.IsNullOrEmpty(x.Email), Beta, Nihon);
            await CheckAsync(d, x => string.IsNullOrWhiteSpace(x.Email), Beta, Nihon, Padded);
            await CheckAsync(d, x => !string.IsNullOrWhiteSpace(x.Email), Alpha, OBrien, Zoe);
        }

        /// <summary>
        /// String functions: case conversion (Unicode-aware), trimming, length, substring, replace, index-of, concatenation.
        /// </summary>
        [Fact]
        public async Task StringFunctions()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await CheckAsync(d, x => x.Name.ToUpper() == "ALPHA", Alpha);
            await CheckAsync(d, x => x.Name.ToLowerInvariant() == "beta", Beta);
            await CheckAsync(d, x => x.Name.ToUpper() == "ZOË", Zoe);
            await CheckAsync(d, x => x.Name.Trim() == "padded", Padded);
            await CheckAsync(d, x => x.Name.TrimStart() == "padded  ", Padded);
            await CheckAsync(d, x => x.Name.TrimEnd() == "  padded", Padded);
            await CheckAsync(d, x => x.Name.Length == 4, Beta);
            await CheckAsync(d, x => x.Name.Length < 4, Zoe, Nihon);
            await CheckAsync(d, x => x.Name.Substring(1, 3) == "lph", Alpha);
            await CheckAsync(d, x => x.Name.Substring(2) == "ta", Beta);
            await CheckAsync(d, x => x.Name.Replace("a", "4") == "Alph4", Alpha);
            await CheckAsync(d, x => x.Name.IndexOf("ph") == 2, Alpha);
            await CheckAsync(d, x => x.Name.IndexOf("'") == 1, OBrien);
            await CheckAsync(d, x => x.Name.IndexOf("BRIEN", StringComparison.OrdinalIgnoreCase) == 2, OBrien);
            await CheckAsync(d, x => x.Name.IndexOf("zz") == -1, _All);
            await CheckAsync(d, x => x.Name + "-" + x.Category == "Alpha-Tools", Alpha);
            await CheckAsync(d, x => x.Name + x.Quantity == "Beta0", Beta);
            await CheckAsync(d, x => (x.Email ?? "none") == "none", Beta);
        }

        /// <summary>
        /// Math functions, arithmetic (integer division, modulo, negation) and conditionals. Rounding of midpoints is away
        /// from zero, like SQL ROUND.
        /// </summary>
        [Fact]
        public async Task MathAndArithmetic()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await CheckAsync(d, x => Math.Abs(x.Ratio) > 1, Beta, OBrien, Nihon);
            await CheckAsync(d, x => Math.Round(x.Price) == 100, Zoe);
            await CheckAsync(d, x => Math.Round(x.Price, 1) == 100.0m, Zoe);
            await CheckAsync(d, x => Math.Round(x.Price) == 11, Alpha);
            await CheckAsync(d, x => Math.Ceiling(x.Price) == 8, OBrien);
            await CheckAsync(d, x => Math.Floor(x.Price) == 99, Zoe);
            await CheckAsync(d, x => Math.Pow(x.Quantity, 2) == 144, OBrien);
            await CheckAsync(d, x => Math.Sqrt(x.Quantity) == 1, Padded);
            await CheckAsync(d, x => x.Price * x.Quantity > 50, Alpha, OBrien, Zoe, Nihon);
            await CheckAsync(d, x => x.Quantity / 2 == 2, Alpha);
            await CheckAsync(d, x => x.Quantity % 2 == 0, Beta, OBrien, Nihon);
            await CheckAsync(d, x => x.Quantity - x.Discount == 3, Alpha);
            await CheckAsync(d, x => -x.Ratio > 1, Nihon);
            await CheckAsync(d, x => (x.Quantity > 5 ? "big" : "small") == "big", OBrien, Nihon);
            await CheckAsync(d, x => x.Price + 0.5m == 11m, Alpha);
        }

        /// <summary>
        /// Date parts, Date, Add* methods and DayOfWeek with .NET numbering; nullable dates are false when null.
        /// </summary>
        [Fact]
        public async Task DateFunctions()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await CheckAsync(d, x => x.CreatedUtc.Year == 2023, Beta);
            await CheckAsync(d, x => x.CreatedUtc.Month == 3, Alpha, Padded);
            await CheckAsync(d, x => x.CreatedUtc.Day == 29, Zoe);
            await CheckAsync(d, x => x.CreatedUtc.Hour == 23, Beta);
            await CheckAsync(d, x => x.CreatedUtc.Minute == 15, Zoe);
            await CheckAsync(d, x => x.CreatedUtc.DayOfYear == 1, OBrien);
            await CheckAsync(d, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Friday, Alpha);
            await CheckAsync(d, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Sunday, Beta);
            await CheckAsync(d, x => x.CreatedUtc.Date == new DateTime(2024, 3, 16), Padded);
            await CheckAsync(d, x => x.CreatedUtc.AddDays(1) > new DateTime(2024, 7, 5), Nihon);
            await CheckAsync(d, x => x.CreatedUtc.AddMonths(-1).Month == 6, Nihon);
            await CheckAsync(d, x => x.DueDate!.Value.Month == 3, Zoe, Padded);
            await CheckAsync(d, x => x.DueDate > x.CreatedUtc.AddDays(10), Alpha);
            DateTime cutoff = new DateTime(2024, 3, 1);
            await CheckAsync(d, x => x.CreatedUtc >= cutoff, Alpha, Nihon, Padded);
        }

        /// <summary>
        /// Enums stored by name compare by name (as in SQL); enums stored as integers compare numerically.
        /// </summary>
        [Fact]
        public async Task EnumComparisons()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await CheckAsync(d, x => x.Status == QtStatus.Active, Alpha, Nihon, Padded);
            await CheckAsync(d, x => x.Status != QtStatus.Active, Beta, OBrien, Zoe);
            QtStatus wanted = QtStatus.Closed;
            await CheckAsync(d, x => x.Status == wanted, Zoe);
            await CheckAsync(d, x => x.Priority >= QtPriority.Medium, Alpha, OBrien, Zoe, Nihon);
            await CheckAsync(d, x => x.Priority < QtPriority.Medium, Beta, Padded);
            await CheckAsync(d, x => (int)x.Priority == 4, OBrien);
            await CheckAsync(d, x => x.Status > QtStatus.Draft, OBrien);
        }

        /// <summary>
        /// Ordering is ordinal and stable with nulls first; paging; Count, Any and aggregates apply paging like SQL.
        /// </summary>
        [Fact]
        public async Task OrderingAndPaging()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            Assert.Equal(new[] { Padded, Alpha, Beta, OBrien, Zoe, Nihon }, Names(await d.Items.Query().OrderBy(x => x.Name).ExecuteAsync()));
            Assert.Equal(new[] { Zoe, Beta }, Names(await d.Items.Query().OrderByDescending(x => x.Price).Take(2).ExecuteAsync()));
            Assert.Equal(new[] { Zoe, OBrien, Nihon, Padded, Beta, Alpha }, Names(d.Items.Query().OrderBy(x => x.Category).ThenByDescending(x => x.Price).Execute()));
            Assert.Equal(new[] { Beta, Nihon, OBrien, Padded, Alpha, Zoe }, Names(await d.Items.Query().OrderBy(x => x.Discount).ExecuteAsync()));
            Assert.Equal(new[] { Beta, OBrien }, Names(await d.Items.Query().OrderBy(x => x.Name).Skip(2).Take(2).ExecuteAsync()));
            Assert.Equal(new[] { Nihon, Zoe }, Names(await d.Items.Query().OrderBy(x => x.Name).OrderByDescending(x => x.Name).Take(2).ExecuteAsync()));
            Assert.Equal(new[] { Alpha, Beta, OBrien, Zoe, Nihon, Padded }, Names(await d.Items.Query().ExecuteAsync()));

            Assert.Equal(3, await d.Items.Query().Take(3).CountAsync());
            Assert.Equal(1, d.Items.Query().Skip(5).Count());
            Assert.False(await d.Items.Query().Skip(6).AnyAsync());
            Assert.True(d.Items.Query().Where(x => x.Quantity > 10).Any());
            Assert.Equal(6, (await d.Items.Query().Distinct().ExecuteAsync()).Count());
            Assert.Equal(110.49m, await d.Items.Query().OrderByDescending(x => x.Price).Take(2).Skip(0).SumAsync(x => x.Price) - 9.50m);
            Assert.Contains("qt_items", d.Items.Query().Where(x => x.Quantity > 1).OrderBy(x => x.Name).Take(2).Query);
            Assert.Throws<ArgumentOutOfRangeException>(() => d.Items.Query().Skip(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => d.Items.Query().Take(-1));
        }

        /// <summary>
        /// Reference navigation members and collection Any/All/Count in predicates read related tables.
        /// </summary>
        [Fact]
        public async Task NavigationPredicates()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await CheckAsync(d, x => x.Owner!.Name == "Acme", Alpha, Beta);
            await CheckAsync(d, x => x.Owner!.City == null, OBrien, Zoe, Nihon);
            await CheckAsync(d, x => x.Owner!.City!.StartsWith("B"), Alpha, Beta);

            Assert.Equal(new[] { "Acme", "Globex", "Initech" }, OwnerNames(await d.Owners.Query().Where(o => o.Items.Any()).ExecuteAsync()));
            Assert.Equal(new[] { "Globex" }, OwnerNames(await d.Owners.Query().Where(o => o.Items.Any(i => i.Price > 50)).ExecuteAsync()));
            Assert.Equal(new[] { "Acme", "Globex" }, OwnerNames(await d.Owners.Query().Where(o => o.Items.Count() >= 2).ExecuteAsync()));
            Assert.Equal(new[] { "Globex", "Initech", "Umbrella" }, OwnerNames(await d.Owners.Query().Where(o => o.Items.All(i => i.IsActive)).ExecuteAsync()));
            Assert.Equal(new[] { "Umbrella" }, OwnerNames(await d.Owners.Query().Where(o => !o.Items.Any()).ExecuteAsync()));
            Assert.Equal(new[] { "Acme", "Initech" }, OwnerNames(await d.Owners.Query().Where(o => o.Items.Count(i => i.IsActive) == 1).ExecuteAsync()));
            Assert.Equal(new[] { "Globex" }, OwnerNames(await d.Owners.Query().Where(o => o.Items.Any(i => i.Owner!.Name == o.Name && i.Status == QtStatus.Closed)).ExecuteAsync()));
        }

        /// <summary>
        /// Select projections are evaluated over the filtered, ordered rows; the projected query supports Where, ordering,
        /// paging, Distinct, Count and aggregates; navigations read by the projection are loaded automatically (null when
        /// absent); unsupported operations throw the SQL builder's NotSupportedException.
        /// </summary>
        [Fact]
        public async Task ProjectionsAreEvaluatedOverRows()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            IQueryBuilder<QtItemSummary> summaries = d.Items.Query().Select(x => new QtItemSummary
            {
                Name = x.Name,
                Category = x.Category,
                Total = x.Price * x.Quantity,
                IsExpensive = x.Price > 50,
                Tier = x.Price > 50 ? "high" : "low",
                NameLength = x.Name.Length,
                Status = x.Status,
                Label = x.Name + "/" + x.Category
            });

            QtItemSummary expensive = (await summaries.Where(s => s.IsExpensive).ExecuteAsync()).Single();
            Assert.Equal(Zoe, expensive.Name);
            Assert.Equal(299.97m, expensive.Total);
            Assert.Equal("high", expensive.Tier);
            Assert.Equal("Zoë/Garden", expensive.Label);
            Assert.Equal(QtStatus.Closed, expensive.Status);

            IQueryBuilder<QtItemSummary> ordered = d.Items.Query().Select(x => new QtItemSummary { Name = x.Name, Total = x.Price * x.Quantity, NameLength = x.Name.Length });
            Assert.Equal(new[] { Zoe, Nihon }, (await ordered.OrderByDescending(s => s.Total).Take(2).ExecuteAsync()).Select(s => s.Name).ToArray());
            Assert.Equal(559.57m, await d.Items.Query().Select(x => new QtItemSummary { Total = x.Price * x.Quantity }).SumAsync(s => s.Total));
            Assert.Equal(2, d.Items.Query().Select(x => new QtItemSummary { NameLength = x.Name.Length }).Min(s => s.NameLength));
            Assert.Equal(Nihon, await d.Items.Query().Select(x => new QtItemSummary { Name = x.Name }).MaxAsync(s => s.Name));
            Assert.Equal(3, await d.Items.Query().Select(x => new QtItemSummary { Category = x.Category }).Distinct().CountAsync());
            Assert.True(d.Items.Query().Select(x => new QtItemSummary { Name = x.Name }).Any());

            Assert.Equal(new[] { Padded, OBrien, Alpha, Zoe },
                (await d.Items.Query().Where(x => x.IsActive).OrderBy(x => x.Price).Select(x => new QtItemSummary { Name = x.Name }).ExecuteAsync()).Select(s => s.Name).ToArray());

            List<QtItemSummary> owners = (await d.Items.Query().Select(x => new QtItemSummary { Name = x.Name, Label = x.Owner!.Name }).ExecuteAsync()).ToList();
            Assert.Equal("Acme", owners.Single(s => s.Name == Alpha).Label);
            Assert.Null(owners.Single(s => s.Name == Nihon).Label);

            Assert.Throws<NotSupportedException>(() => d.Items.Query().Select(x => new QtItemSummary { Name = x.Name }).Include(s => s.Name));
            Assert.Throws<NotSupportedException>(() => d.Items.Query().Select(x => new QtItemSummary { Name = x.Name }).Delete());
            await Assert.ThrowsAsync<NotSupportedException>(() => d.Items.Query().Select(x => new QtItemSummary()).ExecuteAsync());
            Assert.Throws<NotSupportedException>(() => d.Items.Query().Select(x => new QtItemSummary { Name = x.Name }).Select(s => new QtItemSummary()));

            d.Items.AddQueryFilter(x => x.IsActive);
            Assert.Equal(4, d.Items.Query().Select(x => new QtItemSummary { Name = x.Name }).Count());
            Assert.Equal(6, d.Items.Query().Select(x => new QtItemSummary { Name = x.Name }).IgnoreQueryFilters().Count());
        }

        /// <summary>
        /// GroupBy: groups, Having, group Count, group projections with aggregates, aggregates over selected groups, and
        /// grouping by a navigation member.
        /// </summary>
        [Fact]
        public async Task GroupingIsEvaluatedOverRows()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            List<IGrouping<string, QtItem>> groups = (await d.Items.Query().GroupBy(x => x.Category).ExecuteAsync()).ToList();
            Assert.Equal(3, groups.Count);
            Assert.All(groups, g => Assert.Equal(2, g.Count()));

            Assert.Equal(new[] { "Garden" }, (await d.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Price) > 50).ExecuteAsync()).Select(g => g.Key).ToArray());
            Assert.Equal(1, d.Items.Query().GroupBy(x => x.Category).Having(g => g.Sum(x => x.Price) > 50).Count());
            Assert.Equal(3, await d.Items.Query().GroupBy(x => x.Category).CountAsync());
            Assert.Equal(2, await d.Items.Query().Where(x => x.Category != "Tools").GroupBy(x => x.Category).CountAsync());

            List<QtCategoryTotals> totals = (await d.Items.Query().GroupBy(x => x.Category).Select(g => new QtCategoryTotals
            {
                Category = g.Key,
                ItemCount = g.Count(),
                ActiveCount = g.Count(x => x.IsActive),
                TotalPrice = g.Sum(x => x.Price),
                MaxQuantity = g.Max(x => x.Quantity)
            }).OrderBy(t => t.Category).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Garden", "Kitchen", "Tools" }, totals.Select(t => t.Category).ToArray());
            Assert.Equal(new[] { 2, 1, 1 }, totals.Select(t => t.ActiveCount).ToArray());
            Assert.Equal(new[] { 107.24m, 15.10m, 30.50m }, totals.Select(t => t.TotalPrice).ToArray());
            Assert.Equal(new[] { 12, 8, 5 }, totals.Select(t => t.MaxQuantity).ToArray());

            Assert.Equal(107.24m, d.Items.Query().GroupBy(x => x.Category).Having(g => g.Count(x => x.IsActive) == 2).Sum(x => x.Price));
            Assert.Equal(12, await d.Items.Query().GroupBy(x => x.Category).Having(g => g.Count(x => x.IsActive) == 2).MaxAsync(x => x.Quantity));
            Assert.Equal(0.10m, await d.Items.Query().GroupBy(x => x.Category).MinAsync(x => x.Price));
            Assert.Equal(25.473333m, Math.Round(await d.Items.Query().GroupBy(x => x.Category).AverageAsync(x => x.Price), 6));
            Assert.Equal(0m, d.Items.Query().GroupBy(x => x.Category).Having(g => g.Count() > 5).Sum(x => x.Price));

            List<IGrouping<string, QtItem>> byOwner = (await d.Items.Query().GroupBy(x => x.Owner!.Name).ExecuteAsync()).ToList();
            Assert.Equal(4, byOwner.Count);
            Assert.Equal(2, byOwner.Single(g => g.Key == "Acme").Count());
            Assert.Equal(Nihon, byOwner.Single(g => g.Key == null).Single().Name);
        }

        /// <summary>
        /// Captured variables are read when the query executes, not when Where is called; sync and async execution agree.
        /// </summary>
        [Fact]
        public async Task CapturedVariablesAreReadAtExecution()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            int threshold = 100;
            IQueryBuilder<QtItem> query = d.Items.Query().Where(x => x.Quantity > threshold).OrderBy(x => x.Name);
            threshold = 5;
            Assert.Equal(new[] { OBrien, Nihon }, Names(query.Execute()));
            Assert.Equal(new[] { OBrien, Nihon }, Names(await query.ExecuteAsync()));
            List<string> streamed = new List<string>();
            await foreach (QtItem item in query.ExecuteAsyncEnumerable()) streamed.Add(item.Name);
            Assert.Equal(new[] { OBrien, Nihon }, streamed.ToArray());
            IDurableResult<QtItem> withQuery = await query.ExecuteWithQueryAsync();
            Assert.Contains("quantity", withQuery.Query);
            Assert.Equal(2, withQuery.Result.Count());
        }

        /// <summary>
        /// Expressions that cannot be translated throw NotSupportedException when the query executes.
        /// </summary>
        [Fact]
        public async Task UntranslatableExpressionsThrow()
        {
            InMemoryQtData d = await InMemoryQtData.CreateAsync();
            await Assert.ThrowsAsync<NotSupportedException>(() => d.Items.Query().Where(x => x.Name.GetHashCode() == 1).ExecuteAsync());
            Assert.Throws<NotSupportedException>(() => d.Items.Query().Where(x => x.Owner == null).Execute());
        }

        #endregion

        #region Private-Methods

        private static async Task CheckAsync(InMemoryQtData data, Expression<Func<QtItem, bool>> predicate, params string[] expected)
        {
            List<string> actual = (await data.Items.Query().Where(predicate).ExecuteAsync()).Select(x => x.Name).ToList();
            List<string> actualSorted = actual.OrderBy(n => n, StringComparer.Ordinal).ToList();
            List<string> expectedSorted = expected.OrderBy(n => n, StringComparer.Ordinal).ToList();
            Assert.True(actualSorted.SequenceEqual(expectedSorted, StringComparer.Ordinal),
                "Predicate " + predicate + ": expected [" + string.Join(", ", expectedSorted) + "] but got [" + string.Join(", ", actualSorted) + "]");
            Assert.Equal(expected.Length, data.Items.Count(predicate));
        }

        private static string[] Names(IEnumerable<QtItem> items)
        {
            return items.Select(x => x.Name).ToArray();
        }

        private static string[] OwnerNames(IEnumerable<QtOwner> owners)
        {
            return owners.Select(x => x.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        }

        #endregion
    }
}
