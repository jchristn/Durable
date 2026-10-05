namespace Durable.Conformance
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Explicit transactions (<see cref="RepositoryCapabilities.Transactions"/>): commit and rollback (sync and async),
    /// rollback on dispose, read-your-writes inside the transaction for every read path, updates and deletes rolled back,
    /// and one transaction spanning repositories of different entity types. Isolation from other connections is not
    /// asserted (it legitimately differs between backends).
    /// </summary>
    internal sealed class TransactionSuite : KitSuite
    {
        public TransactionSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions, Description = "Commit persists; IsCompleted becomes true")]
        public async Task CommitPersists()
        {
            IRepository<CfTenantNote> repository = await PrepareAsync();
            using (ITransaction transaction = repository.BeginTransaction())
            {
                Assert.False(transaction.IsCompleted);
                repository.Create(Note("commit"), transaction);
                repository.Create(Note("commit-2"), transaction);
                transaction.Commit();
                Assert.True(transaction.IsCompleted);
            }

            Assert.Equal(2L, repository.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions, Description = "Rollback discards; IsCompleted becomes true")]
        public async Task RollbackDiscards()
        {
            IRepository<CfTenantNote> repository = await PrepareAsync();
            using (ITransaction transaction = repository.BeginTransaction())
            {
                repository.Create(Note("rollback"), transaction);
                transaction.Rollback();
                Assert.True(transaction.IsCompleted);
            }

            Assert.Equal(0L, repository.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions, Description = "CommitAsync and RollbackAsync")]
        public async Task AsyncCommitAndRollback()
        {
            IRepository<CfTenantNote> repository = await PrepareAsync();
            await using (ITransaction transaction = await repository.BeginTransactionAsync(Token))
            {
                await repository.CreateAsync(Note("async-commit"), transaction, Token);
                await transaction.CommitAsync(Token);
            }

            await using (ITransaction transaction = await repository.BeginTransactionAsync(Token))
            {
                await repository.CreateAsync(Note("async-rollback"), transaction, Token);
                await transaction.RollbackAsync(Token);
            }

            Assert.Equal(new[] { "async-commit" }, repository.ReadAll().Select(x => x.Title).ToArray());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions, Description = "Disposing an uncommitted transaction rolls it back (sync and async dispose)")]
        public async Task DisposeRollsBack()
        {
            IRepository<CfTenantNote> repository = await PrepareAsync();
            using (ITransaction transaction = repository.BeginTransaction())
            {
                repository.Create(Note("disposed"), transaction);
            }

            await using (ITransaction transaction = await repository.BeginTransactionAsync(Token))
            {
                await repository.CreateAsync(Note("disposed-async"), transaction, Token);
            }

            Assert.Equal(0L, repository.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions, Description = "Reads inside the transaction see its uncommitted writes")]
        public async Task ReadYourWrites()
        {
            IRepository<CfTenantNote> repository = await PrepareAsync();
            using (ITransaction transaction = await repository.BeginTransactionAsync(Token))
            {
                CfTenantNote created = await repository.CreateAsync(Note("mine", 5), transaction, Token);
                repository.CreateMany(new List<CfTenantNote> { Note("mine-2", 6), Note("mine-3", 7) }, transaction).ToList();
                Assert.Equal(3L, repository.Count(null, transaction));
                Assert.Equal(3L, await repository.CountAsync(x => x.Amount >= 5, transaction, Token));
                Assert.Equal("mine", repository.ReadById(created.Id, transaction)?.Title);
                Assert.Equal("mine", (await repository.ReadByIdAsync(created.Id, transaction, Token))?.Title);
                Assert.True(repository.Exists(x => x.Title == "mine-2", transaction));
                Assert.True(repository.ExistsById(created.Id, transaction));
                Assert.Equal("mine-3", repository.ReadSingle(x => x.Amount == 7, transaction).Title);
                Assert.Equal(3, repository.ReadMany(null, transaction).Count());
                Assert.Equal(3, (await repository.Query(transaction).OrderBy(x => x.Amount).ExecuteAsync(Token)).Count());
                Assert.Equal(1L, repository.Query(transaction).Where(x => x.Title == "mine").Count());
                int streamed = 0;
                await foreach (CfTenantNote note in repository.ReadManyAsync(x => x.Amount > 5, transaction, Token)) streamed++;
                Assert.Equal(2, streamed);
                transaction.Rollback();
            }

            Assert.Equal(0L, repository.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions, Description = "Updates and deletes inside a rolled-back transaction are undone")]
        public async Task UpdatesAndDeletesRollBack()
        {
            IRepository<CfTenantNote> repository = await PrepareAsync();
            CfTenantNote keep = repository.Create(Note("keep", 1));
            CfTenantNote change = repository.Create(Note("change", 2));
            using (ITransaction transaction = repository.BeginTransaction())
            {
                change.Amount = 99;
                repository.Update(change, transaction);
                Assert.True(repository.Delete(keep, transaction));
                Assert.Equal(1, repository.UpdateMany(x => x.Title == "change", n => n.Title = "changed", transaction));
                Assert.Equal(1, repository.DeleteMany(x => x.Title == "changed", transaction));
                Assert.Equal(0L, repository.Count(null, transaction));
                transaction.Rollback();
            }

            ConformanceAssert.NameSet(repository.ReadAll().Select(x => x.Title), new[] { "keep", "change" }, "rows after rollback");
            Assert.Equal(2, repository.ReadById(change.Id)?.Amount);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions, Description = "A transaction spans repositories of different entity types")]
        public async Task TransactionSpansRepositories()
        {
            await ResetAsync(typeof(CfPublisher), typeof(CfAuthor));
            IRepository<CfPublisher> publishers = Repository<CfPublisher>();
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            using (ITransaction transaction = publishers.BeginTransaction())
            {
                CfPublisher publisher = publishers.Create(new CfPublisher { Name = "Rolled" }, transaction);
                authors.Create(new CfAuthor { Name = "Rolled Author", PublisherId = publisher.Id }, transaction);
                Assert.Equal(1L, authors.Count(x => x.PublisherId == publisher.Id, transaction));
                transaction.Rollback();
            }

            Assert.Equal(0L, publishers.Count());
            Assert.Equal(0L, authors.Count());

            using (ITransaction transaction = await authors.BeginTransactionAsync(Token))
            {
                CfPublisher publisher = await publishers.CreateAsync(new CfPublisher { Name = "Committed" }, transaction, Token);
                await authors.CreateAsync(new CfAuthor { Name = "Committed Author", PublisherId = publisher.Id }, transaction, Token);
                await transaction.CommitAsync(Token);
            }

            Assert.Equal("Committed", publishers.ReadSingle(x => x.Name == "Committed").Name);
            Assert.Equal(1L, authors.Count(x => x.Name == "Committed Author"));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions | RepositoryCapabilities.BatchUpdate, Description = "UpdateField and BatchUpdate participate in the transaction")]
        public async Task SetBasedUpdatesRollBack()
        {
            IRepository<CfTenantNote> repository = await PrepareAsync();
            repository.CreateMany(new List<CfTenantNote> { Note("a", 1), Note("b", 2) }).ToList();
            using (ITransaction transaction = repository.BeginTransaction())
            {
                Assert.Equal(2, repository.UpdateField(x => x.Amount > 0, x => x.Amount, 50, transaction));
                Assert.Equal(2, await repository.BatchUpdateAsync(x => x.Amount == 50, x => new CfTenantNote { Title = x.Title + "!" }, transaction, Token));
                Assert.Equal(2L, repository.Count(x => x.Title.EndsWith("!"), transaction));
                transaction.Rollback();
            }

            Assert.Equal(3m, repository.ReadAll().Sum(x => (decimal)x.Amount));
            Assert.Equal(0L, repository.Count(x => x.Title.EndsWith("!")));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Transactions | RepositoryCapabilities.Upsert, Description = "Upsert participates in the transaction")]
        public async Task UpsertRollsBack()
        {
            await ResetAsync(typeof(CfUpsertItem));
            IRepository<CfUpsertItem> repository = Repository<CfUpsertItem>();
            repository.Create(new CfUpsertItem { Code = "A", Name = "Original", Quantity = 1 });
            using (ITransaction transaction = repository.BeginTransaction())
            {
                repository.Upsert(new CfUpsertItem { Code = "A", Name = "Changed", Quantity = 2 }, transaction);
                await repository.UpsertAsync(new CfUpsertItem { Code = "B", Name = "New", Quantity = 3 }, transaction, Token);
                Assert.Equal(2L, repository.Count(null, transaction));
                transaction.Rollback();
            }

            Assert.Equal("Original", repository.ReadById("A")?.Name);
            Assert.Null(repository.ReadById("B"));
        }

        private async Task<IRepository<CfTenantNote>> PrepareAsync()
        {
            await ResetAsync(typeof(CfTenantNote));
            return Repository<CfTenantNote>();
        }

        private static CfTenantNote Note(string title, int amount = 1)
        {
            return new CfTenantNote { TenantId = 1, Title = title, Amount = amount };
        }
    }
}
