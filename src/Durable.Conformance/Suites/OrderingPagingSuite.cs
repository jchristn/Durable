namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// OrderBy / OrderByDescending / ThenBy / ThenByDescending over every scalar type, Skip / Take windows (including
    /// Take(0) and windows past the end), paging with filters and streaming. Ordering by strings uses values whose order
    /// is the same under every collation.
    /// </summary>
    internal sealed class OrderingPagingSuite : KitSuite
    {
        private const string A = ItemFixture.Alpha;
        private const string B = ItemFixture.Beta;
        private const string O = ItemFixture.OBrien;
        private const string D = ItemFixture.Delta;
        private const string E = ItemFixture.Echo;
        private const string F = ItemFixture.Foxtrot;

        public OrderingPagingSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "OrderBy / OrderByDescending on a decimal column")]
        public async Task OrderByDecimal()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Price), "OrderBy(Price)", F, O, A, E, B, D);
            await AssertSequenceAsync(f.Items.Query().OrderByDescending(x => x.Price), "OrderByDescending(Price)", D, B, E, A, O, F);
        }

        [ConformanceTest(Description = "OrderBy on string, int, long, double, DateTime and integer-stored enum columns")]
        public async Task OrderByScalarTypes()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Name), "OrderBy(Name)", A, B, D, E, F, O);
            await AssertSequenceAsync(f.Items.Query().OrderByDescending(x => x.Name), "OrderByDescending(Name)", O, F, E, D, B, A);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Quantity), "OrderBy(Quantity)", B, F, D, A, E, O);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.BigNumber), "OrderBy(BigNumber)", B, O, F, D, A, E);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Ratio), "OrderBy(Ratio)", E, F, A, D, B, O);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.CreatedUtc), "OrderBy(CreatedUtc)", B, O, D, A, F, E);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Priority).ThenBy(x => x.Name), "OrderBy(Priority).ThenBy(Name)", B, F, D, E, A, O);
        }

        [ConformanceTest(Description = "ThenBy / ThenByDescending, including booleans")]
        public async Task ThenByOrdering()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Category).ThenByDescending(x => x.Price), "OrderBy(Category).ThenByDescending(Price)", D, O, E, F, B, A);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Category).ThenBy(x => x.Name), "OrderBy(Category).ThenBy(Name)", D, O, E, F, A, B);
            await AssertSequenceAsync(f.Items.Query().OrderByDescending(x => x.IsActive).ThenBy(x => x.Price), "OrderByDescending(IsActive).ThenBy(Price)", F, O, A, D, E, B);
            await AssertSequenceAsync(f.Items.Query().OrderByDescending(x => x.Category).ThenByDescending(x => x.Name), "OrderByDescending(Category).ThenByDescending(Name)", B, A, F, E, O, D);
        }

        [ConformanceTest(Description = "A later OrderBy replaces earlier sorts")]
        public async Task OrderByReplacesEarlierSorts()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Price).OrderBy(x => x.Name), "OrderBy(Price).OrderBy(Name)", A, B, D, E, F, O);
        }

        [ConformanceTest(Description = "Ordering by a computed expression")]
        public async Task OrderByComputedExpression()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().OrderByDescending(x => x.Price * x.Quantity), "OrderByDescending(Price * Quantity)", D, E, O, A, F, B);
        }

        [ConformanceTest(Description = "Nulls sort first ascending and last descending, like LINQ-to-objects")]
        public async Task NullsOrderLikeLinq()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.DueDate).ThenBy(x => x.Name), "OrderBy(DueDate).ThenBy(Name)", B, E, O, D, F, A);
            await AssertSequenceAsync(f.Items.Query().OrderByDescending(x => x.Discount).ThenBy(x => x.Name), "OrderByDescending(Discount).ThenBy(Name)", D, A, F, O, B, E);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "Ordering by a function result")]
        public async Task OrderByFunction()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Name.Length).ThenBy(x => x.Name), "OrderBy(Name.Length).ThenBy(Name)", B, E, A, D, F, O);
        }

        [ConformanceTest(Description = "Skip / Take return the right window in the right order")]
        public async Task SkipTakeWindows()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Price).Skip(1).Take(2), "OrderBy(Price).Skip(1).Take(2)", O, A);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Price).Skip(4), "OrderBy(Price).Skip(4)", B, D);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Price).Take(3), "OrderBy(Price).Take(3)", F, O, A);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Price).Skip(2).Take(2), "OrderBy(Price).Skip(2).Take(2)", A, E);
        }

        [ConformanceTest(Description = "Consecutive pages cover the ordered set exactly once")]
        public async Task PagesPartitionTheResult()
        {
            ItemFixture f = await SeedItemsAsync();
            List<string> collected = new List<string>();
            for (int page = 0; page < 4; page++)
            {
                List<CfItem> rows = await ExecuteAsync(f.Items.Query().OrderBy(x => x.Name).Skip(page * 2).Take(2), "page " + page);
                collected.AddRange(rows.Select(r => r.Name));
            }

            ConformanceAssert.Sequence(collected, new[] { A, B, D, E, F, O }, "pages of 2 ordered by Name");
        }

        [ConformanceTest(Description = "Take(0) and windows past the end return no rows; Take larger than the set returns all")]
        public async Task EmptyAndOversizedWindows()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Empty(await f.Items.Query().OrderBy(x => x.Price).Take(0).ExecuteAsync(Token));
            Assert.Empty(await f.Items.Query().Take(0).ExecuteAsync(Token));
            Assert.Empty(await f.Items.Query().OrderBy(x => x.Price).Skip(6).ExecuteAsync(Token));
            Assert.Empty(await f.Items.Query().Skip(100).Take(5).ExecuteAsync(Token));
            Assert.Equal(6, (await f.Items.Query().OrderBy(x => x.Price).Take(100).ExecuteAsync(Token)).Count());
        }

        [ConformanceTest(Description = "Skip / Take without OrderBy return the right number of distinct rows")]
        public async Task PagingWithoutOrderBy()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfItem> take = await ExecuteAsync(f.Items.Query().Take(3), "Take(3)");
            List<CfItem> skip = await ExecuteAsync(f.Items.Query().Skip(2), "Skip(2)");
            List<CfItem> both = await ExecuteAsync(f.Items.Query().Skip(2).Take(2), "Skip(2).Take(2)");
            Assert.Equal(3, take.Count);
            Assert.Equal(4, skip.Count);
            Assert.Equal(2, both.Count);
            Assert.Equal(3, take.Select(x => x.Id).Distinct().Count());
        }

        [ConformanceTest(Description = "Paging applies after Where")]
        public async Task PagingAfterWhere()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertSequenceAsync(f.Items.Query().Where(x => x.IsActive).OrderByDescending(x => x.Price).Skip(1).Take(2),
                "Where(IsActive).OrderByDescending(Price).Skip(1).Take(2)", A, O);
            await AssertSequenceAsync(f.Items.Query().OrderBy(x => x.Name).Where(x => x.Category != "Garden").Take(3),
                "OrderBy(Name).Where(Category != Garden).Take(3)", A, B, E);
        }

        [ConformanceTest(Description = "Streaming execution honors ordering and paging")]
        public async Task StreamingHonorsOrderAndPaging()
        {
            ItemFixture f = await SeedItemsAsync();
            List<string> streamed = new List<string>();
            await foreach (CfItem item in f.Items.Query().OrderBy(x => x.Name).Skip(1).Take(3).ExecuteAsyncEnumerable(Token)) streamed.Add(item.Name);
            ConformanceAssert.Sequence(streamed, new[] { B, D, E }, "ExecuteAsyncEnumerable with OrderBy(Name).Skip(1).Take(3)");
            ConformanceAssert.Sequence(f.Items.Query().OrderByDescending(x => x.Quantity).Take(2).Execute().Select(x => x.Name), new[] { O, E }, "Execute with OrderByDescending(Quantity).Take(2)");
        }

        [ConformanceTest(Description = "Any honors paging")]
        public async Task AnyHonorsPaging()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.False(await f.Items.Query().OrderBy(x => x.Price).Skip(6).AnyAsync(Token));
            Assert.True(await f.Items.Query().OrderBy(x => x.Price).Skip(5).AnyAsync(Token));
            Assert.False(f.Items.Query().Take(0).Any());
        }
    }
}
