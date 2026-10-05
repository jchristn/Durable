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
    /// Query-shape translation correctness with real data round-trips on every provider: navigation predicates
    /// (reference subqueries, collection Any/All/Count), raw predicates and subqueries, set operations, window functions,
    /// CTEs, CASE expressions, projections (computed members, filtering/ordering/paging the projection), grouped
    /// projections with HAVING, and paging/Count/Any.
    /// </summary>
    public class QueryTranslationAdvancedTestSuite : IDisposable
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private const string Alpha = QueryTranslationFixture.Alpha;
        private const string Beta = QueryTranslationFixture.Beta;
        private const string OBrien = QueryTranslationFixture.OBrien;
        private const string Zoe = QueryTranslationFixture.Zoe;
        private const string Nihon = QueryTranslationFixture.Nihon;
        private const string Padded = QueryTranslationFixture.Padded;

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryTranslationAdvancedTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public QueryTranslationAdvancedTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// A reference navigation member in a predicate becomes a correlated subquery.
        /// </summary>
        [Fact]
        public async Task ReferenceNavigationPredicate()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().Where(i => i.Owner!.Name == "Acme"), Alpha, Beta);
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().Where(i => i.Owner!.City == "Austin"), Padded);
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().Where(i => i.Owner!.Name.StartsWith("Glo") && i.Price > 50), Zoe);
        }

        /// <summary>
        /// Collection navigation Any() with and without a predicate, and its negation.
        /// </summary>
        [Fact]
        public async Task CollectionNavigationAny()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Any()), "Acme", "Globex", "Initech");
            await AssertOwnersAsync(f.Owners.Query().Where(o => !o.Items.Any()), "Umbrella");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Any(i => i.Price > 50)), "Globex");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Any(i => i.Name == "O'Brien" || i.Code!.Contains("%"))), "Acme", "Globex", "Initech");
        }

        /// <summary>
        /// Collection navigation Count() / Count property / Count(predicate) comparisons.
        /// </summary>
        [Fact]
        public async Task CollectionNavigationCount()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Count() > 1), "Acme", "Globex");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Count == 0), "Umbrella");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Count(i => i.IsActive) == 1), "Acme", "Initech");
        }

        /// <summary>
        /// Collection navigation All(predicate), which is vacuously true for owners without items.
        /// </summary>
        [Fact]
        public async Task CollectionNavigationAll()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.All(i => i.IsActive)), "Globex", "Initech", "Umbrella");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.All(i => i.Price < 50) && o.Items.Any()), "Acme", "Initech");
        }

        /// <summary>
        /// WhereRaw with {0} placeholders binds values (including apostrophes and LIKE characters) as parameters.
        /// </summary>
        [Fact]
        public async Task WhereRawWithPlaceholders()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().WhereRaw("name = {0}", "O'Brien"), OBrien);
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().WhereRaw("name = {0} OR code = {1}", "Zoë", "100%"), Zoe, Padded);
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().WhereRaw("name = {0} OR email = {0}", "alpha@x.com"), Alpha);
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().Where(x => x.IsActive).WhereRaw("category = {0}", "Kitchen"), Padded);
        }

        /// <summary>
        /// WhereInRaw / WhereNotInRaw with a parameterized raw subquery.
        /// </summary>
        [Fact]
        public async Task WhereInRawAndNotInRaw()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await QueryTranslationFixture.AssertNamesAsync(
                f.Items.Query().WhereInRaw(x => x.OwnerId, "SELECT id FROM qt_owners WHERE name = {0}", "Acme"), Alpha, Beta);
            await QueryTranslationFixture.AssertNamesAsync(
                f.Items.Query().WhereNotInRaw(x => x.Id, "SELECT id FROM qt_items WHERE quantity > {0}", 5), Alpha, Beta, Zoe, Padded);
        }

        /// <summary>
        /// WhereIn / WhereNotIn with a subquery built from another query builder.
        /// </summary>
        [Fact]
        public async Task WhereInAndNotInWithSubqueryBuilder()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await QueryTranslationFixture.AssertNamesAsync(
                f.Items.Query().WhereIn<int?, QtOwner>(x => x.OwnerId, f.Owners.Query().Where(o => o.City != null), o => o.Id),
                Alpha, Beta, Padded);
            await QueryTranslationFixture.AssertNamesAsync(
                f.Items.Query().WhereIn<int?, QtOwner>(x => x.OwnerId, f.Owners.Query().Where(o => o.Name == "O'Hara" || o.Name == "Globex"), o => o.Id),
                OBrien, Zoe);
            await QueryTranslationFixture.AssertNamesAsync(
                f.Items.Query().WhereNotIn(x => x.Id, f.Items.Query().Where(i => i.Quantity > 5), i => i.Id),
                Alpha, Beta, Zoe, Padded);
        }

        /// <summary>
        /// WhereExists / WhereNotExists with a correlation lambda, and an uncorrelated EXISTS.
        /// </summary>
        [Fact]
        public async Task WhereExistsWithCorrelation()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await AssertOwnersAsync(
                f.Owners.Query().WhereExists(f.Items.Query().Where(i => i.Price > 50), (o, i) => i.OwnerId == o.Id), "Globex");
            await AssertOwnersAsync(
                f.Owners.Query().WhereNotExists(f.Items.Query(), (o, i) => i.OwnerId == o.Id), "Umbrella");
            await QueryTranslationFixture.AssertNamesAsync(
                f.Items.Query().WhereExists(f.Owners.Query().Where(o => o.Name == "Umbrella")), Alpha, Beta, OBrien, Zoe, Nihon, Padded);
            await QueryTranslationFixture.AssertNamesAsync(
                f.Items.Query().WhereExists(f.Owners.Query().Where(o => o.Name == "Nobody")));
        }

        /// <summary>
        /// UNION followed by ORDER BY and TAKE over the combined result.
        /// </summary>
        [Fact]
        public async Task UnionWithOrderByAndTake()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItem> query = f.Items.Query().Where(x => x.Category == "Tools")
                .Union(f.Items.Query().Where(x => x.Category == "Garden"))
                .OrderBy(x => x.Price)
                .Take(3);
            await AssertSequenceAsync(query, OBrien, Alpha, Beta);
        }

        /// <summary>
        /// UNION ALL keeps duplicates and Count over it is correct.
        /// </summary>
        [Fact]
        public async Task UnionAllCount()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItem> query = f.Items.Query().Where(x => x.IsActive).UnionAll(f.Items.Query().Where(x => x.Price > 10));
            Assert.Equal(8L, await query.CountAsync());
        }

        /// <summary>
        /// INTERSECT of an enum-stored-as-string filter with an enum-stored-as-integer filter.
        /// </summary>
        [Fact]
        public async Task IntersectEnumFilters()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItem> query = f.Items.Query().Where(x => x.Status == QtStatus.Active)
                .Intersect(f.Items.Query().Where(x => x.Priority == QtPriority.Medium));
            await QueryTranslationFixture.AssertNamesAsync(query, Nihon);
        }

        /// <summary>
        /// EXCEPT where both operands carry parameters (including LIKE patterns).
        /// </summary>
        [Fact]
        public async Task ExceptWithParametersInBothOperands()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItem> query = f.Items.Query().Where(x => x.Name.Contains("'") || x.IsActive)
                .Except(f.Items.Query().Where(x => x.Code!.Contains("%")));
            await QueryTranslationFixture.AssertNamesAsync(query, Alpha, OBrien, Zoe);
        }

        /// <summary>
        /// ROW_NUMBER / RANK / COUNT / running SUM partitioned by category and ordered by price; values are verified.
        /// </summary>
        [Fact]
        public async Task WindowRankingAndRunningSum()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItemComputedRow> query = f.ComputedRows.Query()
                .WithWindowFunction("ROW_NUMBER")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .RowNumber("rn")
                .Rank("rnk")
                .Count("cnt")
                .Sum(x => x.Price, "running_total")
                .EndWindow()
                .OrderBy(x => x.Category)
                .ThenBy(x => x.Price);
            List<QtItemComputedRow> rows = await ExecuteAsync(query);

            Assert.Equal(6, rows.Count);
            Dictionary<string, QtItemComputedRow> byName = rows.ToDictionary(r => r.Name, StringComparer.Ordinal);
            Assert.Equal(1L, byName[Alpha].RowNumber);
            Assert.Equal(2L, byName[Beta].RowNumber);
            Assert.Equal(1L, byName[OBrien].RowNumber);
            Assert.Equal(2L, byName[Zoe].RowNumber);
            Assert.Equal(1L, byName[Padded].RowNumber);
            Assert.Equal(2L, byName[Nihon].RowNumber);
            Assert.Equal(2L, byName[Beta].RankValue);
            // With an ORDER BY in the window the default frame ends at the current row, so COUNT(*) is a running count.
            Assert.All(rows, r => Assert.Equal(r.RowNumber, r.PartitionCount));
            Assert.Equal(10.50m, byName[Alpha].RunningTotal);
            Assert.Equal(30.50m, byName[Beta].RunningTotal);
            Assert.Equal(107.24m, byName[Zoe].RunningTotal);
            Assert.Equal(15.10m, byName[Nihon].RunningTotal);
        }

        /// <summary>
        /// LEAD with a string default containing an apostrophe and LAG with a numeric default.
        /// </summary>
        [Fact]
        public async Task WindowLeadAndLagWithDefaults()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItemComputedRow> query = f.ComputedRows.Query()
                .WithWindowFunction("LEAD")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .Lead(x => x.Name, 1, "it's none", "next_name")
                .Lag(x => x.Price, 1, 0m, "prev_price")
                .EndWindow();
            List<QtItemComputedRow> rows = await ExecuteAsync(query);

            Dictionary<string, QtItemComputedRow> byName = rows.ToDictionary(r => r.Name, StringComparer.Ordinal);
            Assert.Equal(Beta, byName[Alpha].NextName);
            Assert.Equal("it's none", byName[Beta].NextName);
            Assert.Equal(Zoe, byName[OBrien].NextName);
            Assert.Equal(Nihon, byName[Padded].NextName);
            Assert.Equal(0m, byName[Alpha].PreviousPrice);
            Assert.Equal(10.50m, byName[Beta].PreviousPrice);
            Assert.Equal(0.10m, byName[Nihon].PreviousPrice);
        }

        /// <summary>
        /// A window with only a raw ORDER BY and the default function/alias.
        /// </summary>
        [Fact]
        public async Task WindowDefaultFunctionWithRawOrder()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItemComputedRow> query = f.ComputedRows.Query()
                .WithWindowFunction("ROW_NUMBER", null, "t0.price DESC")
                .EndWindow()
                .Where(x => x.Price > 10);
            List<QtItemComputedRow> rows = await ExecuteAsync(query);

            Assert.Equal(4, rows.Count);
            Dictionary<string, QtItemComputedRow> byName = rows.ToDictionary(r => r.Name, StringComparer.Ordinal);
            Assert.Equal(1L, byName[Zoe].DefaultRowNumber);
            Assert.Equal(2L, byName[Beta].DefaultRowNumber);
            Assert.Equal(4L, byName[Alpha].DefaultRowNumber);
        }

        /// <summary>
        /// WITH (CTE) combined with FromRaw and a lambda predicate.
        /// </summary>
        [Fact]
        public async Task CteWithFromRaw()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItem> query = f.Items.Query()
                .WithCte("expensive", "SELECT * FROM qt_items WHERE price > 10")
                .FromRaw("expensive t0")
                .Where(x => x.IsActive);
            await QueryTranslationFixture.AssertNamesAsync(query, Alpha, Zoe);
        }

        /// <summary>
        /// Recursive CTE generating numbers 1..5 used in a raw predicate.
        /// </summary>
        [Fact]
        public async Task RecursiveCte()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItem> query = f.Items.Query()
                .WithRecursiveCte("nums", "SELECT 1 AS n", "SELECT n + 1 FROM nums WHERE n < 5")
                .WhereRaw("t0.quantity IN (SELECT n FROM nums)");
            await QueryTranslationFixture.AssertNamesAsync(query, Alpha, Zoe, Padded);
        }

        /// <summary>
        /// SelectCase with lambda conditions and string results (one containing an apostrophe) mapped to an alias column.
        /// </summary>
        [Fact]
        public async Task SelectCaseTiers()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItemComputedRow> query = f.ComputedRows.Query()
                .SelectCase()
                .When(x => x.Price > 50, "premium")
                .When(x => x.Price > 10, "mid's")
                .Else("budget")
                .EndCase("tier");
            List<QtItemComputedRow> rows = await ExecuteAsync(query);

            Dictionary<string, QtItemComputedRow> byName = rows.ToDictionary(r => r.Name, StringComparer.Ordinal);
            Assert.Equal(6, rows.Count);
            Assert.Equal("premium", byName[Zoe].Tier);
            Assert.Equal("mid's", byName[Alpha].Tier);
            Assert.Equal("mid's", byName[Beta].Tier);
            Assert.Equal("mid's", byName[Nihon].Tier);
            Assert.Equal("budget", byName[OBrien].Tier);
            Assert.Equal("budget", byName[Padded].Tier);
        }

        /// <summary>
        /// SelectCase combined with Where and OrderBy.
        /// </summary>
        [Fact]
        public async Task SelectCaseWithWhereAndOrderBy()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItemComputedRow> query = f.ComputedRows.Query()
                .SelectCase()
                .When(x => x.Price > 50, "premium")
                .Else("budget")
                .EndCase("tier")
                .Where(x => x.Category == "Garden")
                .OrderBy(x => x.Price);
            List<QtItemComputedRow> rows = await ExecuteAsync(query);

            Assert.Equal(2, rows.Count);
            Assert.Equal(OBrien, rows[0].Name);
            Assert.Equal("budget", rows[0].Tier);
            Assert.Equal(Zoe, rows[1].Name);
            Assert.Equal("premium", rows[1].Tier);
        }

        /// <summary>
        /// Select into a DTO with computed members (arithmetic, comparisons, ternary, length, enums, concatenation).
        /// </summary>
        [Fact]
        public async Task ProjectionComputedMembers()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItemSummary> query = SummaryQuery(f);
            List<QtItemSummary> rows = await ExecuteAsync(query);

            Assert.Equal(6, rows.Count);
            Dictionary<string, QtItemSummary> byName = rows.ToDictionary(r => r.Name, StringComparer.Ordinal);
            QtItemSummary zoe = byName[Zoe];
            Assert.Equal("Garden", zoe.Category);
            Assert.Equal(299.97m, zoe.Total);
            Assert.True(zoe.IsActive);
            Assert.True(zoe.IsExpensive);
            Assert.Equal("premium", zoe.Tier);
            Assert.Equal(3, zoe.NameLength);
            Assert.Equal(QtStatus.Closed, zoe.Status);
            Assert.Equal(QtPriority.Medium, zoe.Priority);
            Assert.Equal("Zoë (Garden)", zoe.Label);

            QtItemSummary beta = byName[Beta];
            Assert.False(beta.IsActive);
            Assert.False(beta.IsExpensive);
            Assert.Equal("standard", beta.Tier);
            Assert.Equal(0m, beta.Total);
            Assert.Equal(QtStatus.Draft, beta.Status);
            Assert.Equal(QtPriority.Low, beta.Priority);
            Assert.Equal("O'Brien (Garden)", byName[OBrien].Label);
        }

        /// <summary>
        /// Where / OrderByDescending / Skip / Take applied to computed projection members.
        /// </summary>
        [Fact]
        public async Task ProjectionWhereOrderSkipTake()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItemSummary> query = SummaryQuery(f)
                .Where(s => s.Total > 50)
                .OrderByDescending(s => s.Total)
                .Skip(1)
                .Take(2);
            List<QtItemSummary> rows = await ExecuteAsync(query);

            Assert.Equal(new[] { Nihon, OBrien }, rows.Select(r => r.Name).ToArray());
            Assert.Equal(120.00m, rows[0].Total);
            Assert.Equal(87.00m, rows[1].Total);
        }

        /// <summary>
        /// Count / Any over a filtered and paged projection, and Where on boolean/ternary projection members.
        /// </summary>
        [Fact]
        public async Task ProjectionCountAndAny()
        {
            using QueryTranslationFixture f = await SeedAsync();
            Assert.Equal(4L, await SummaryQuery(f).Where(s => s.Total > 50).CountAsync());
            Assert.Equal(2L, await SummaryQuery(f).Where(s => s.Total > 50).Take(2).CountAsync());
            Assert.Equal(1L, await SummaryQuery(f).Where(s => s.IsExpensive).CountAsync());
            Assert.Equal(5L, await SummaryQuery(f).Where(s => s.Tier == "standard").CountAsync());
            Assert.True(await SummaryQuery(f).Where(s => s.Label == "Alpha (Tools)").AnyAsync());
            Assert.False(await SummaryQuery(f).Where(s => s.NameLength > 50).AnyAsync());
        }

        /// <summary>
        /// Distinct over a single-member projection.
        /// </summary>
        [Fact]
        public async Task ProjectionDistinct()
        {
            using QueryTranslationFixture f = await SeedAsync();
            ISqlQueryBuilder<QtItemSummary> query = f.Items.Query().Select(x => new QtItemSummary { Category = x.Category }).Distinct();
            List<QtItemSummary> rows = await ExecuteAsync(query);
            Assert.Equal(new[] { "Garden", "Kitchen", "Tools" }, rows.Select(r => r.Category).OrderBy(c => c, StringComparer.Ordinal).ToArray());
            Assert.Equal(3L, await f.Items.Query().Select(x => new QtItemSummary { Category = x.Category }).Distinct().CountAsync());
        }

        /// <summary>
        /// GroupBy + Having + Select into a DTO with Count/Count(predicate)/Sum/Max, ordered by an aggregate.
        /// </summary>
        [Fact]
        public async Task GroupBySelectHavingOrderedByAggregate()
        {
            using QueryTranslationFixture f = await SeedAsync();
            IQueryBuilder<QtCategoryTotals> query = f.Items.Query()
                .GroupBy(x => x.Category)
                .Having(g => g.Sum(x => x.Price) > 25)
                .Select(g => new QtCategoryTotals
                {
                    Category = g.Key,
                    ItemCount = g.Count(),
                    ActiveCount = g.Count(x => x.IsActive),
                    TotalPrice = g.Sum(x => x.Price),
                    MaxQuantity = g.Max(x => x.Quantity)
                })
                .OrderByDescending(t => t.TotalPrice);
            List<QtCategoryTotals> rows = (await query.ExecuteAsync()).ToList();

            Assert.Equal(new[] { "Garden", "Tools" }, rows.Select(r => r.Category).ToArray());
            Assert.Equal(2, rows[0].ItemCount);
            Assert.Equal(2, rows[0].ActiveCount);
            Assert.Equal(107.24m, rows[0].TotalPrice);
            Assert.Equal(12, rows[0].MaxQuantity);
            Assert.Equal(2, rows[1].ItemCount);
            Assert.Equal(1, rows[1].ActiveCount);
            Assert.Equal(30.50m, rows[1].TotalPrice);
            Assert.Equal(5, rows[1].MaxQuantity);
        }

        /// <summary>
        /// A Where on a grouped projection's aggregate member filters groups (HAVING), with Take and Count.
        /// </summary>
        [Fact]
        public async Task GroupedProjectionWhereTakeAndCount()
        {
            using QueryTranslationFixture f = await SeedAsync();
            List<QtCategoryTotals> active = (await CategoryTotals(f).Where(t => t.ActiveCount >= 2).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Garden" }, active.Select(r => r.Category).ToArray());

            List<QtCategoryTotals> firstTwo = (await CategoryTotals(f).OrderBy(t => t.Category).Take(2).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Garden", "Kitchen" }, firstTwo.Select(r => r.Category).ToArray());

            Assert.Equal(3L, await CategoryTotals(f).CountAsync());
            Assert.Equal(1L, await f.Items.Query().GroupBy(x => x.Category).Having(g => g.Max(x => x.Price) > 50).CountAsync());
        }

        /// <summary>
        /// Skip/Take with ORDER BY returns the right window in the right order; ThenByDescending is honored.
        /// </summary>
        [Fact]
        public async Task PagingWithOrderBy()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Price).Skip(1).Take(2), OBrien, Alpha);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Price).Skip(4), Beta, Zoe);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Category).ThenByDescending(x => x.Price).Take(3), Zoe, OBrien, Nihon);
        }

        /// <summary>
        /// Skip/Take without ORDER BY still return the right number of rows.
        /// </summary>
        [Fact]
        public async Task PagingWithoutOrderBy()
        {
            using QueryTranslationFixture f = await SeedAsync();
            Assert.Equal(3, (await f.Items.Query().Take(3).ExecuteAsync()).Count());
            Assert.Equal(4, (await f.Items.Query().Skip(2).ExecuteAsync()).Count());
            Assert.Equal(2, (await f.Items.Query().Skip(2).Take(2).ExecuteAsync()).Count());
        }

        /// <summary>
        /// Count and Sum honor Skip/Take.
        /// </summary>
        [Fact]
        public async Task CountAndSumAfterPaging()
        {
            using QueryTranslationFixture f = await SeedAsync();
            Assert.Equal(2L, await f.Items.Query().Take(2).CountAsync());
            Assert.Equal(1L, await f.Items.Query().Skip(5).CountAsync());
            Assert.Equal(2L, await f.Items.Query().Where(x => x.IsActive).OrderBy(x => x.Price).Skip(2).CountAsync());
            Assert.Equal(13m, await f.Items.Query().OrderBy(x => x.Price).Take(2).SumAsync(x => x.Quantity));
        }

        /// <summary>
        /// Any() with filters and paging.
        /// </summary>
        [Fact]
        public async Task AnyTranslation()
        {
            using QueryTranslationFixture f = await SeedAsync();
            Assert.False(await f.Items.Query().Where(x => x.Price > 1000).AnyAsync());
            Assert.True(await f.Items.Query().Where(x => x.Name == "Zoë").AnyAsync());
            Assert.False(await f.Items.Query().Skip(6).AnyAsync());
            Assert.True(await f.Items.Query().OrderBy(x => x.Price).Skip(5).AnyAsync());
        }

        /// <summary>
        /// Take(0) returns no rows and Count over it is zero.
        /// </summary>
        [Fact]
        public async Task TakeZeroReturnsNoRows()
        {
            using QueryTranslationFixture f = await SeedAsync();
            Assert.Empty(await f.Items.Query().OrderBy(x => x.Price).Take(0).ExecuteAsync());
            Assert.Equal(0L, await f.Items.Query().Take(0).CountAsync());
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private Task<QueryTranslationFixture> SeedAsync()
        {
            return QueryTranslationFixture.CreateAsync(_Provider);
        }

        private static ISqlQueryBuilder<QtItemSummary> SummaryQuery(QueryTranslationFixture f)
        {
            return f.Items.Query().Select(x => new QtItemSummary
            {
                Name = x.Name,
                Category = x.Category,
                Total = x.Price * x.Quantity,
                IsActive = x.IsActive,
                IsExpensive = x.Price > 50,
                Tier = x.Price > 50 ? "premium" : "standard",
                NameLength = x.Name.Length,
                Status = x.Status,
                Priority = x.Priority,
                Label = x.Name + " (" + x.Category + ")"
            });
        }

        private static IQueryBuilder<QtCategoryTotals> CategoryTotals(QueryTranslationFixture f)
        {
            return f.Items.Query()
                .GroupBy(x => x.Category)
                .Select(g => new QtCategoryTotals
                {
                    Category = g.Key,
                    ItemCount = g.Count(),
                    ActiveCount = g.Count(x => x.IsActive),
                    TotalPrice = g.Sum(x => x.Price),
                    MaxQuantity = g.Max(x => x.Quantity)
                });
        }

        private static async Task<List<T>> ExecuteAsync<T>(ISqlQueryBuilder<T> query) where T : class, new()
        {
            string sql = QueryTranslationFixture.SafeSql(query);
            try
            {
                return (await query.ExecuteAsync()).ToList();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.GetType().Name + ": " + ex.Message + " | SQL: " + sql, ex);
            }
        }

        private static async Task AssertSequenceAsync(ISqlQueryBuilder<QtItem> query, params string[] expected)
        {
            List<QtItem> rows = await ExecuteAsync(query);
            string[] actual = rows.Select(r => r.Name).ToArray();
            Assert.True(actual.SequenceEqual(expected, StringComparer.Ordinal),
                "Expected sequence [" + string.Join(", ", expected) + "] but got [" + string.Join(", ", actual) + "] | SQL: " + QueryTranslationFixture.SafeSql(query));
        }

        private static async Task AssertOwnersAsync(ISqlQueryBuilder<QtOwner> query, params string[] expected)
        {
            List<QtOwner> rows = await ExecuteAsync(query);
            QueryTranslationFixture.AssertNameSet(rows.Select(r => r.Name), expected, QueryTranslationFixture.SafeSql(query));
        }

        #endregion
    }
}
