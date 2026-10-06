namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// The window-frame API of <see cref="IWindowedQueryBuilder{T}"/> (ROWS / RANGE frames, FIRST_VALUE, LAST_VALUE,
    /// NTH_VALUE, DENSE_RANK, AVG) with values verified against the seeded query-translation data on every provider.
    /// Data per category ordered by price: Tools = Alpha 10.50, Beta 20.00; Garden = O'Brien 7.25, Zoë 99.99;
    /// Kitchen = padded 0.10, 日本 15.00.
    /// </summary>
    public class WindowFrameTestSuite
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the suite.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public WindowFrameTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// RowsUnboundedFollowing: SUM, COUNT and LAST_VALUE see the current row through the end of the partition.
        /// </summary>
        [Fact]
        public async Task RowsUnboundedFollowingFrame()
        {
            using QueryTranslationFixture f = await QueryTranslationFixture.CreateAsync(_Provider);
            Dictionary<string, QtItemComputedRow> rows = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("SUM")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .Sum(x => x.Price, "running_total")
                .Count("cnt")
                .LastValue(x => x.Name, "tier")
                .RowsUnboundedFollowing()
                .EndWindow());

            Assert.Equal(30.50m, rows[QueryTranslationFixture.Alpha].RunningTotal);
            Assert.Equal(20.00m, rows[QueryTranslationFixture.Beta].RunningTotal);
            Assert.Equal(107.24m, rows[QueryTranslationFixture.OBrien].RunningTotal);
            Assert.Equal(15.00m, rows[QueryTranslationFixture.Nihon].RunningTotal);
            Assert.Equal(2L, rows[QueryTranslationFixture.Alpha].PartitionCount);
            Assert.Equal(1L, rows[QueryTranslationFixture.Beta].PartitionCount);
            Assert.Equal(QueryTranslationFixture.Beta, rows[QueryTranslationFixture.Alpha].Tier);
            Assert.Equal(QueryTranslationFixture.Zoe, rows[QueryTranslationFixture.OBrien].Tier);
        }

        /// <summary>
        /// RowsUnboundedPreceding gives a running total; Rows(1, 1) covers both rows of each two-row partition.
        /// </summary>
        [Fact]
        public async Task RowsUnboundedPrecedingAndOffsetFrames()
        {
            using QueryTranslationFixture f = await QueryTranslationFixture.CreateAsync(_Provider);
            Dictionary<string, QtItemComputedRow> running = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("SUM")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .Sum(x => x.Price, "running_total")
                .RowsUnboundedPreceding()
                .EndWindow());
            Assert.Equal(10.50m, running[QueryTranslationFixture.Alpha].RunningTotal);
            Assert.Equal(30.50m, running[QueryTranslationFixture.Beta].RunningTotal);
            Assert.Equal(0.10m, running[QueryTranslationFixture.Padded].RunningTotal);

            Dictionary<string, QtItemComputedRow> neighbors = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("SUM")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .Sum(x => x.Price, "running_total")
                .Count("cnt")
                .Rows(1, 1)
                .EndWindow());
            Assert.Equal(30.50m, neighbors[QueryTranslationFixture.Alpha].RunningTotal);
            Assert.Equal(30.50m, neighbors[QueryTranslationFixture.Beta].RunningTotal);
            Assert.All(neighbors.Values, r => Assert.Equal(2L, r.PartitionCount));
        }

        /// <summary>
        /// RowsBetween with explicit UNBOUNDED bounds: FIRST_VALUE and LAST_VALUE see the whole partition.
        /// </summary>
        [Fact]
        public async Task RowsBetweenWholePartition()
        {
            using QueryTranslationFixture f = await QueryTranslationFixture.CreateAsync(_Provider);
            Dictionary<string, QtItemComputedRow> rows = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("FIRST_VALUE")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .FirstValue(x => x.Name, "next_name")
                .LastValue(x => x.Name, "tier")
                .RowsBetween("UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING")
                .EndWindow());

            Assert.Equal(QueryTranslationFixture.Alpha, rows[QueryTranslationFixture.Beta].NextName);
            Assert.Equal(QueryTranslationFixture.Beta, rows[QueryTranslationFixture.Alpha].Tier);
            Assert.Equal(QueryTranslationFixture.Padded, rows[QueryTranslationFixture.Nihon].NextName);
            Assert.Equal(QueryTranslationFixture.Nihon, rows[QueryTranslationFixture.Padded].Tier);
            Assert.Throws<ArgumentException>(() => f.ComputedRows.Query().WithWindowFunction("SUM").RowsBetween("1; DROP TABLE x", "CURRENT ROW"));
        }

        /// <summary>
        /// RANGE frames with UNBOUNDED / CURRENT ROW bounds; numeric RANGE offsets where the database supports them
        /// (SQL Server rejects them at the call site).
        /// </summary>
        [Fact]
        public async Task RangeFrames()
        {
            using QueryTranslationFixture f = await QueryTranslationFixture.CreateAsync(_Provider);
            Dictionary<string, QtItemComputedRow> preceding = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("SUM")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .Sum(x => x.Price, "running_total")
                .RangeUnboundedPreceding()
                .EndWindow());
            Assert.Equal(10.50m, preceding[QueryTranslationFixture.Alpha].RunningTotal);
            Assert.Equal(30.50m, preceding[QueryTranslationFixture.Beta].RunningTotal);

            Dictionary<string, QtItemComputedRow> following = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("SUM")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .Sum(x => x.Price, "running_total")
                .RangeUnboundedFollowing()
                .EndWindow());
            Assert.Equal(30.50m, following[QueryTranslationFixture.Alpha].RunningTotal);
            Assert.Equal(20.00m, following[QueryTranslationFixture.Beta].RunningTotal);

            Dictionary<string, QtItemComputedRow> between = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("COUNT")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .Count("cnt")
                .RangeBetween("UNBOUNDED PRECEDING", "CURRENT ROW")
                .EndWindow());
            Assert.Equal(1L, between[QueryTranslationFixture.Alpha].PartitionCount);
            Assert.Equal(2L, between[QueryTranslationFixture.Beta].PartitionCount);

            if (!f.ComputedRows.Dialect.SupportsRangeFrameOffsets)
            {
                Assert.Throws<NotSupportedException>(() => f.ComputedRows.Query().WithWindowFunction("COUNT").OrderBy(x => x.Price).Range(1, 1));
                return;
            }

            Dictionary<string, QtItemComputedRow> ranged = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("COUNT")
                .OrderBy(x => x.Price)
                .Count("cnt")
                .Range(10, 0)
                .EndWindow());
            // Prices: 0.10, 7.25, 10.50, 15.00, 20.00, 99.99. Rows with price in [p - 10, p]:
            Assert.Equal(1L, ranged[QueryTranslationFixture.Padded].PartitionCount);
            Assert.Equal(2L, ranged[QueryTranslationFixture.OBrien].PartitionCount);
            Assert.Equal(2L, ranged[QueryTranslationFixture.Alpha].PartitionCount);
            Assert.Equal(3L, ranged[QueryTranslationFixture.Nihon].PartitionCount);
            Assert.Equal(3L, ranged[QueryTranslationFixture.Beta].PartitionCount);
            Assert.Equal(1L, ranged[QueryTranslationFixture.Zoe].PartitionCount);
        }

        /// <summary>
        /// DENSE_RANK over categories and AVG over a partition without ordering.
        /// </summary>
        [Fact]
        public async Task DenseRankAndAverage()
        {
            using QueryTranslationFixture f = await QueryTranslationFixture.CreateAsync(_Provider);
            Dictionary<string, QtItemComputedRow> ranked = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("DENSE_RANK")
                .OrderBy(x => x.Category)
                .DenseRank("rnk")
                .EndWindow());
            Assert.Equal(1L, ranked[QueryTranslationFixture.OBrien].RankValue);
            Assert.Equal(1L, ranked[QueryTranslationFixture.Zoe].RankValue);
            Assert.Equal(2L, ranked[QueryTranslationFixture.Nihon].RankValue);
            Assert.Equal(3L, ranked[QueryTranslationFixture.Alpha].RankValue);
            Assert.Equal(3L, ranked[QueryTranslationFixture.Beta].RankValue);

            Dictionary<string, QtItemComputedRow> averaged = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("AVG")
                .PartitionBy(x => x.Category)
                .Avg(x => x.Price, "running_total")
                .EndWindow());
            Assert.Equal(15.25m, Math.Round(averaged[QueryTranslationFixture.Alpha].RunningTotal!.Value, 2));
            Assert.Equal(53.62m, Math.Round(averaged[QueryTranslationFixture.Zoe].RunningTotal!.Value, 2));
            Assert.Equal(7.55m, Math.Round(averaged[QueryTranslationFixture.Nihon].RunningTotal!.Value, 2));
        }

        /// <summary>
        /// NTH_VALUE returns the second price of each partition where supported; SQL Server rejects it at the call site.
        /// Invalid arguments are rejected.
        /// </summary>
        [Fact]
        public async Task NthValue()
        {
            using QueryTranslationFixture f = await QueryTranslationFixture.CreateAsync(_Provider);
            Assert.Throws<ArgumentOutOfRangeException>(() => f.ComputedRows.Query().WithWindowFunction("SUM").Rows(-1, 0));

            if (!f.ComputedRows.Dialect.SupportsNthValue)
            {
                Assert.Throws<NotSupportedException>(() => f.ComputedRows.Query().WithWindowFunction("NTH_VALUE").NthValue(x => x.Price, 2, "prev_price"));
                return;
            }

            Assert.Throws<ArgumentOutOfRangeException>(() => f.ComputedRows.Query().WithWindowFunction("NTH_VALUE").NthValue(x => x.Price, 0));
            Dictionary<string, QtItemComputedRow> rows = await RunAsync(f.ComputedRows.Query()
                .WithWindowFunction("NTH_VALUE")
                .PartitionBy(x => x.Category)
                .OrderBy(x => x.Price)
                .NthValue(x => x.Price, 2, "prev_price")
                .RowsBetween("UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING")
                .EndWindow());
            Assert.Equal(20.00m, rows[QueryTranslationFixture.Alpha].PreviousPrice);
            Assert.Equal(99.99m, rows[QueryTranslationFixture.OBrien].PreviousPrice);
            Assert.Equal(15.00m, rows[QueryTranslationFixture.Padded].PreviousPrice);
        }

        #endregion

        #region Private-Methods

        private static async Task<Dictionary<string, QtItemComputedRow>> RunAsync(ISqlQueryBuilder<QtItemComputedRow> query)
        {
            string sql = QueryTranslationFixture.SafeSql(query);
            try
            {
                List<QtItemComputedRow> rows = (await query.ExecuteAsync()).ToList();
                Assert.Equal(6, rows.Count);
                return rows.ToDictionary(r => r.Name, StringComparer.Ordinal);
            }
            catch (Exception ex) when (!(ex is Xunit.Sdk.XunitException))
            {
                throw new InvalidOperationException(ex.GetType().Name + ": " + ex.Message + " | SQL: " + sql, ex);
            }
        }

        #endregion
    }
}
