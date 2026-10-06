namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// The <c>*WithQuery</c> extensions and builder methods return results together with the SQL that produced them, the
    /// SQL text names the statement and table, the repository's own capture setting is left unchanged, and the
    /// <see cref="RepositoryResultExtensions"/> helpers unwrap the results.
    /// </summary>
    public class WithQueryTestSuite
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
        public WithQueryTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Synchronous Create/ReadMany/Update/Delete/DeleteMany WithQuery return their SQL and results.
        /// </summary>
        [Fact]
        public async Task SyncWithQueryExtensionsReturnSql()
        {
            ISqlRepository<RelUpsertItem> repository = _Provider.CreateRepository<RelUpsertItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            Assert.False(repository.CaptureSql);

            IDurableResult<RelUpsertItem> created = repository.CreateWithQuery(new RelUpsertItem { Code = "a", Name = "A", Quantity = 1 });
            AssertSql(created.Query, "INSERT", "rel_upsert_items");
            Assert.Equal("a", created.AsEntity().Code);
            repository.Create(new RelUpsertItem { Code = "b", Name = "B", Quantity = 2 });

            IDurableResult<RelUpsertItem> read = repository.ReadManyWithQuery(x => x.Quantity > 0);
            AssertSql(read.Query, "SELECT", "rel_upsert_items");
            Assert.Equal(2, read.AsEnumerable().Count());

            RelUpsertItem toUpdate = read.AsEnumerable().First(x => x.Code == "a");
            toUpdate.Name = "A2";
            IDurableResult<RelUpsertItem> updated = repository.UpdateWithQuery(toUpdate);
            AssertSql(updated.Query, "UPDATE", "rel_upsert_items");
            Assert.Equal("A2", updated.AsEntity().Name);

            IDurableResult<bool> deleted = repository.DeleteWithQuery(toUpdate);
            AssertSql(deleted.Query, "DELETE", "rel_upsert_items");
            Assert.True(deleted.AsValue());

            IDurableResult<int> deletedMany = repository.DeleteManyWithQuery(x => x.Quantity > 0);
            AssertSql(deletedMany.Query, "DELETE", "rel_upsert_items");
            Assert.Equal(1, deletedMany.AsCount());
            Assert.False(repository.CaptureSql);
            Assert.Throws<ArgumentNullException>(() => repository.DeleteManyWithQuery(null!));
        }

        /// <summary>
        /// Asynchronous WithQuery extensions return their SQL and results; the *Async unwrap helpers work on pending tasks.
        /// </summary>
        [Fact]
        public async Task AsyncWithQueryExtensionsReturnSql()
        {
            ISqlRepository<RelUpsertItem> repository = _Provider.CreateRepository<RelUpsertItem>();
            await RelTestHelpers.RecreateTableAsync(repository);

            RelUpsertItem created = await repository.CreateWithQueryAsync(new RelUpsertItem { Code = "a", Name = "A", Quantity = 1 }).AsEntityAsync();
            Assert.Equal("a", created.Code);
            IDurableResult<RelUpsertItem> second = await repository.CreateWithQueryAsync(new RelUpsertItem { Code = "b", Name = "B", Quantity = 2 });
            AssertSql(second.Query, "INSERT", "rel_upsert_items");

            IDurableResult<RelUpsertItem> read = await repository.ReadManyWithQueryAsync(x => x.Code == "b");
            AssertSql(read.Query, "SELECT", "rel_upsert_items");
            RelUpsertItem item = Assert.Single(read.AsEnumerable());

            item.Quantity = 20;
            IDurableResult<RelUpsertItem> updated = await repository.UpdateWithQueryAsync(item);
            AssertSql(updated.Query, "UPDATE", "rel_upsert_items");

            Assert.True(await repository.DeleteWithQueryAsync(item).AsValueAsync());
            Assert.Equal(1, await repository.DeleteManyWithQueryAsync(x => x.Code == "a").AsCountAsync());
            Assert.Equal(0L, await repository.CountAsync());

            using CancellationTokenSource canceled = new CancellationTokenSource();
            canceled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new TaskCompletionSource<IDurableResult<int>>().Task.AsCountAsync(canceled.Token));
        }

        /// <summary>
        /// The builder's ExecuteWithQuery, ExecuteWithQueryAsync and ExecuteAsyncEnumerableWithQuery return the same SQL
        /// as <see cref="IQueryBuilder{T}.Query"/>, and AsAsyncEnumerable unwraps the stream.
        /// </summary>
        [Fact]
        public async Task BuilderWithQueryMethodsReturnSql()
        {
            ISqlRepository<RelUpsertItem> repository = _Provider.CreateRepository<RelUpsertItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            await repository.CreateManyAsync(new List<RelUpsertItem>
            {
                new RelUpsertItem { Code = "a", Name = "A", Quantity = 1 },
                new RelUpsertItem { Code = "b", Name = "B", Quantity = 2 },
                new RelUpsertItem { Code = "c", Name = "C", Quantity = 3 }
            });

            IQueryBuilder<RelUpsertItem> query = repository.Query().Where(x => x.Quantity >= 2).OrderBy(x => x.Code);
            string expected = query.Query;
            AssertSql(expected, "SELECT", "rel_upsert_items");

            IDurableResult<RelUpsertItem> sync = query.ExecuteWithQuery();
            Assert.Equal(expected, sync.Query);
            Assert.Equal(new[] { "b", "c" }, sync.AsEnumerable().Select(x => x.Code).ToArray());

            IDurableResult<RelUpsertItem> async = await repository.Query().Where(x => x.Quantity >= 2).OrderBy(x => x.Code).ExecuteWithQueryAsync();
            Assert.Equal(expected, async.Query);

            IAsyncDurableResult<RelUpsertItem> streamed = repository.Query().Where(x => x.Quantity >= 2).OrderBy(x => x.Code).ExecuteAsyncEnumerableWithQuery();
            Assert.Equal(expected, streamed.Query);
            List<string> codes = new List<string>();
            await foreach (RelUpsertItem row in streamed.AsAsyncEnumerable()) codes.Add(row.Code);
            Assert.Equal(new[] { "b", "c" }, codes.ToArray());

            List<string> fromTask = new List<string>();
            await foreach (RelUpsertItem row in Task.FromResult(streamed).AsAsyncEnumerable()) fromTask.Add(row.Code);
            Assert.Empty(fromTask.Except(new[] { "b", "c" }));

            Assert.Empty(((IDurableResult<RelUpsertItem>?)null).AsEnumerable());
            Assert.Equal(0, ((IDurableResult<int>?)null).AsCount());
        }

        #endregion

        #region Private-Methods

        private static void AssertSql(string sql, string verb, string table)
        {
            Assert.False(string.IsNullOrWhiteSpace(sql), "Expected SQL text.");
            Assert.Contains(verb, sql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(table, sql, StringComparison.OrdinalIgnoreCase);
        }

        #endregion
    }
}
