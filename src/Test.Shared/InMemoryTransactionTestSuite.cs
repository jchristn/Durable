namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;
    using Durable.Query;
    using Xunit;

    /// <summary>
    /// Transactions and concurrency of the in-memory backend: commit, rollback and dispose; snapshot isolation (own writes
    /// visible inside, committed data only outside, no blocking on the same thread); first-committer-wins conflict
    /// detection; ambient scopes; atomic multi-row operations; concurrent writers; and synchronous members that do not
    /// deadlock under a non-pumping synchronization context.
    /// </summary>
    public class InMemoryTransactionTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// Commit publishes writes; rollback and dispose without commit discard them.
        /// </summary>
        [Fact]
        public async Task CommitPublishesAndRollbackDiscards()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();

            using (ITransaction committed = repository.BeginTransaction())
            {
                repository.Create(new RelTenantNote { Title = "committed" }, committed);
                committed.Commit();
                Assert.True(committed.IsCompleted);
            }

            ITransaction rolledBack = await repository.BeginTransactionAsync();
            await repository.CreateAsync(new RelTenantNote { Title = "rolled back" }, rolledBack);
            await rolledBack.RollbackAsync();
            await rolledBack.DisposeAsync();

            using (ITransaction abandoned = repository.BeginTransaction())
            {
                repository.Create(new RelTenantNote { Title = "abandoned" }, abandoned);
            }

            Assert.Equal(new[] { "committed" }, repository.ReadAll().Select(x => x.Title).ToArray());
        }

        /// <summary>
        /// Inside a transaction reads see its own writes (reads, counts, aggregates, updates, deletes); outside, on the same
        /// thread and while the transaction is open, reads see committed data only and do not block.
        /// </summary>
        [Fact]
        public async Task ReadsInsideSeeOwnWritesAndOutsideSeeCommittedOnly()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            RelTenantNote existing = await repository.CreateAsync(new RelTenantNote { Title = "existing", Amount = 1 });

            await using ITransaction transaction = await repository.BeginTransactionAsync();
            RelTenantNote added = await repository.CreateAsync(new RelTenantNote { Title = "added", Amount = 2 }, transaction);
            existing.Title = "changed";
            await repository.UpdateAsync(existing, transaction);

            Assert.Equal(2, await repository.CountAsync(null, transaction));
            Assert.Equal("changed", (await repository.ReadByIdAsync(existing.Id, transaction))!.Title);
            Assert.Equal(3m, await repository.SumAsync(x => x.Amount, null, transaction));
            Assert.Equal(1, await repository.CountAsync());
            Assert.Equal("existing", repository.ReadById(existing.Id)!.Title);
            Assert.Null(repository.ReadById(added.Id));

            Assert.Equal(1, await repository.DeleteManyAsync(x => x.Title == "added", transaction));
            Assert.Equal(1, await repository.CountAsync(null, transaction));
            await transaction.CommitAsync();

            Assert.Equal(new[] { "changed" }, repository.ReadAll().Select(x => x.Title).ToArray());
        }

        /// <summary>
        /// A transaction reads the snapshot taken when it began: rows committed by others afterwards are not visible to it.
        /// </summary>
        [Fact]
        public async Task TransactionsReadTheirSnapshot()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            await repository.CreateAsync(new RelTenantNote { Title = "before" });
            using ITransaction transaction = repository.BeginTransaction();
            await repository.CreateAsync(new RelTenantNote { Title = "after" });
            Assert.Equal(1, await repository.CountAsync(null, transaction));
            Assert.Equal(2, await repository.CountAsync());
        }

        /// <summary>
        /// First committer wins: a transaction that wrote a row changed by another writer after it began fails to commit
        /// with InvalidOperationException and is rolled back; transactions writing different rows both commit.
        /// </summary>
        [Fact]
        public async Task ConflictingCommitsFail()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            RelTenantNote a = await repository.CreateAsync(new RelTenantNote { Title = "a" });
            RelTenantNote b = await repository.CreateAsync(new RelTenantNote { Title = "b" });

            ITransaction first = repository.BeginTransaction();
            ITransaction second = repository.BeginTransaction();
            a.Title = "first";
            repository.Update(a, first);
            a.Title = "second";
            repository.Update(a, second);
            b.Title = "second-b";
            repository.Update(b, second);
            first.Commit();
            InvalidOperationException conflict = Assert.Throws<InvalidOperationException>(() => second.Commit());
            Assert.Contains("changed by another writer", conflict.Message);
            Assert.True(second.IsCompleted);
            Assert.Equal("first", repository.ReadById(a.Id)!.Title);
            Assert.Equal("b", repository.ReadById(b.Id)!.Title);

            ITransaction third = repository.BeginTransaction();
            repository.UpdateField(x => x.Id == b.Id, x => x.Title, "outside");
            repository.UpdateField(x => x.Id == b.Id, x => x.Title, "inside", third);
            Assert.Throws<InvalidOperationException>(() => third.Commit());
            Assert.Equal("outside", repository.ReadById(b.Id)!.Title);

            ITransaction left = repository.BeginTransaction();
            ITransaction right = repository.BeginTransaction();
            repository.UpdateField(x => x.Id == a.Id, x => x.Amount, 1, left);
            repository.UpdateField(x => x.Id == b.Id, x => x.Amount, 2, right);
            left.Commit();
            right.Commit();
            Assert.Equal(new[] { 1, 2 }, repository.Query().OrderBy(x => x.Id).Execute().Select(x => x.Amount).ToArray());

            InMemoryRepository<RelUpsertItem> keyed = new InMemoryBackend().CreateRepository<RelUpsertItem>();
            ITransaction one = keyed.BeginTransaction();
            ITransaction two = keyed.BeginTransaction();
            keyed.Create(new RelUpsertItem { Code = "K", Name = "one" }, one);
            keyed.Create(new RelUpsertItem { Code = "K", Name = "two" }, two);
            one.Commit();
            Assert.Throws<InvalidOperationException>(() => two.Commit());
            Assert.Equal("one", keyed.ReadById("K")!.Name);
        }

        /// <summary>
        /// Completed transactions cannot be reused; transactions from another backend are rejected.
        /// </summary>
        [Fact]
        public async Task InvalidTransactionUseIsRejected()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelTenantNote> repository = backend.CreateRepository<RelTenantNote>();
            ITransaction transaction = repository.BeginTransaction();
            transaction.Commit();
            Assert.Throws<InvalidOperationException>(() => transaction.Commit());
            Assert.Throws<InvalidOperationException>(() => transaction.Rollback());
            await Assert.ThrowsAsync<InvalidOperationException>(() => repository.CreateAsync(new RelTenantNote(), transaction));
            Assert.Throws<InvalidOperationException>(() => repository.Count(null, transaction));

            ITransaction foreign = new InMemoryBackend().BeginTransaction();
            Assert.Throws<ArgumentException>(() => repository.Create(new RelTenantNote(), foreign));
            Assert.False(backend.Owns(foreign));
            Assert.True(backend.Owns(backend.BeginTransaction()));
        }

        /// <summary>
        /// Ambient TransactionScope: operations without an explicit transaction join it; Complete commits, disposing
        /// without Complete rolls back; a scope from another backend is ignored.
        /// </summary>
        [Fact]
        public async Task AmbientScopesAreHonored()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelTenantNote> repository = backend.CreateRepository<RelTenantNote>();

            using (TransactionScope scope = TransactionScope.Create(repository))
            {
                repository.Create(new RelTenantNote { Title = "scoped" });
                Assert.Equal(1, repository.Count());
                scope.Complete();
            }

            using (TransactionScope scope = await TransactionScope.CreateAsync(repository))
            {
                await repository.CreateAsync(new RelTenantNote { Title = "discarded" });
                Assert.Equal(2, await repository.CountAsync());
            }

            Assert.Equal(new[] { "scoped" }, repository.ReadAll().Select(x => x.Title).ToArray());

            InMemoryRepository<RelTenantNote> other = new InMemoryBackend().CreateRepository<RelTenantNote>();
            using (TransactionScope foreign = TransactionScope.Create(other))
            {
                repository.Create(new RelTenantNote { Title = "outside foreign scope" });
            }

            Assert.Equal(2, repository.Count());
        }

        /// <summary>
        /// CreateMany, UpdateMany and UpsertMany are atomic when no transaction is supplied: a failure leaves no partial
        /// writes.
        /// </summary>
        [Fact]
        public async Task MultiRowOperationsAreAtomic()
        {
            InMemoryRepository<RelUpsertItem> repository = new InMemoryBackend().CreateRepository<RelUpsertItem>();
            await repository.CreateAsync(new RelUpsertItem { Code = "B", Name = "existing", Quantity = 1 });

            await Assert.ThrowsAsync<InvalidOperationException>(() => repository.CreateManyAsync(new[]
            {
                new RelUpsertItem { Code = "A", Name = "new" },
                new RelUpsertItem { Code = "B", Name = "duplicate" }
            }));
            Assert.Equal(1, await repository.CountAsync());

            Assert.Throws<InvalidOperationException>(() => repository.UpdateMany(x => true, x =>
            {
                x.Quantity = 99;
                if (x.Code == "B") throw new InvalidOperationException("boom");
            }));
            Assert.Equal(1, (await repository.ReadByIdAsync("B"))!.Quantity);
        }

        /// <summary>
        /// Concurrent writers from many threads get unique generated keys and lose no rows.
        /// </summary>
        [Fact]
        public async Task ConcurrentWritersAreSafe()
        {
            InMemoryBackend backend = new InMemoryBackend();
            List<Task> writers = new List<Task>();
            for (int w = 0; w < 8; w++)
            {
                int writer = w;
                writers.Add(Task.Run(async () =>
                {
                    InMemoryRepository<RelTenantNote> repository = backend.CreateRepository<RelTenantNote>();
                    for (int i = 0; i < 50; i++)
                    {
                        RelTenantNote note = await repository.CreateAsync(new RelTenantNote { TenantId = writer, Amount = i });
                        await repository.UpdateFieldAsync(x => x.Id == note.Id, x => x.Title, "w" + writer);
                    }
                }));
            }

            await Task.WhenAll(writers);
            InMemoryRepository<RelTenantNote> reader = backend.CreateRepository<RelTenantNote>();
            List<RelTenantNote> all = reader.ReadAll().ToList();
            Assert.Equal(400, all.Count);
            Assert.Equal(400, all.Select(x => x.Id).Distinct().Count());
            Assert.All(all, x => Assert.Equal("w" + x.TenantId, x.Title));
        }

        /// <summary>
        /// Synchronous members called under a synchronization context that never pumps do not deadlock, even when the
        /// backend never completes synchronously; with the in-memory backend nothing is posted to the context at all.
        /// </summary>
        [Fact]
        public void SynchronousMembersDoNotDeadlockUnderSynchronizationContext()
        {
            SynchronizationContext? previous = SynchronizationContext.Current;
            NonPumpingSynchronizationContext context = new NonPumpingSynchronizationContext();
            SynchronizationContext.SetSynchronizationContext(context);
            try
            {
                RepositoryBase<RelTenantNote> yielding = new RepositoryBase<RelTenantNote>(new YieldingBackend(new InMemoryBackend()));
                yielding.Create(new RelTenantNote { Title = "y", Amount = 3 });
                Assert.Equal(1, yielding.Count());
                Assert.Equal("y", yielding.ReadAll().Single().Title);
                Assert.Equal(3m, yielding.Sum(x => x.Amount));
                using (ITransaction transaction = yielding.BeginTransaction())
                {
                    yielding.UpdateField(x => x.Amount == 3, x => x.Title, "z", transaction);
                    transaction.Commit();
                }

                Assert.Equal("z", yielding.Query().Execute().Single().Title);
                Assert.Same(context, SynchronizationContext.Current);

                InMemoryRepository<RelTenantNote> direct = new InMemoryBackend().CreateRepository<RelTenantNote>();
                int thread = Environment.CurrentManagedThreadId;
                direct.Create(new RelTenantNote { Title = "d" });
                Assert.Single(direct.ReadMany(x => x.Title == "d"));
                Assert.Equal(thread, Environment.CurrentManagedThreadId);
                Assert.Equal(0, context.PostedCount);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previous);
            }
        }

        #endregion
    }
}
