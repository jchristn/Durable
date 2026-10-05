namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Write-path coverage: CreateMany generated keys across chunks, BulkInsert (with and without transactions),
    /// Upsert/UpsertMany insert and update paths, BatchUpdate expressions over the current row, UpdateField with
    /// null, DeleteAll, and default value providers.
    /// </summary>
    public class WriteFeaturesTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="WriteFeaturesTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public WriteFeaturesTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// CreateMany with MaxRowsPerBatch = 7 (several chunks) assigns generated keys to the right entities in input
        /// order, for both sync and async variants.
        /// </summary>
        [Fact]
        public async Task CreateManyAssignsKeysInOrderAcrossSmallChunks()
        {
            SqlRepositoryOptions options = new SqlRepositoryOptions { BatchConfiguration = new BatchInsertConfiguration { MaxRowsPerBatch = 7 } };
            ISqlRepository<RelSequenceItem> repository = RelTestHelpers.CreateRepository<RelSequenceItem>(_Provider, options);
            await RelTestHelpers.RecreateTableAsync(repository);

            List<RelSequenceItem> asyncRows = Enumerable.Range(0, 30).Select(i => new RelSequenceItem { Ordinal = i, Label = "async-" + i }).ToList();
            List<RelSequenceItem> asyncResult = (await repository.CreateManyAsync(asyncRows)).ToList();
            await AssertKeysMatchStoredRowsAsync(repository, asyncResult, "async-");

            List<RelSequenceItem> syncRows = Enumerable.Range(0, 23).Select(i => new RelSequenceItem { Ordinal = i, Label = "sync-" + i }).ToList();
            List<RelSequenceItem> syncResult = repository.CreateMany(syncRows).ToList();
            await AssertKeysMatchStoredRowsAsync(repository, syncResult, "sync-");

            Assert.Equal(53, await repository.CountAsync());
        }

        /// <summary>
        /// CreateMany with the default batch configuration and 1200 rows (more than one chunk) assigns keys in order.
        /// </summary>
        [Fact]
        public async Task CreateManyAssignsKeysInOrderWithDefaultChunking()
        {
            ISqlRepository<RelSequenceItem> repository = _Provider.CreateRepository<RelSequenceItem>();
            await RelTestHelpers.RecreateTableAsync(repository);

            List<RelSequenceItem> rows = Enumerable.Range(0, 1200).Select(i => new RelSequenceItem { Ordinal = i, Label = "d-" + i.ToString("0000", CultureInfo.InvariantCulture) }).ToList();
            List<RelSequenceItem> result = (await repository.CreateManyAsync(rows)).ToList();
            await AssertKeysMatchStoredRowsAsync(repository, result, "d-");
        }

        /// <summary>
        /// BulkInsert and BulkInsertAsync insert every row, return the row count, and apply default values.
        /// </summary>
        [Fact]
        public async Task BulkInsertInsertsAllRows()
        {
            ISqlRepository<RelBulkItem> repository = _Provider.CreateRepository<RelBulkItem>();
            await RelTestHelpers.RecreateTableAsync(repository);

            List<RelBulkItem> first = Enumerable.Range(0, 750).Select(i => new RelBulkItem { Name = "a" + i, Amount = i * 1.5m, Note = i % 2 == 0 ? null : "odd" }).ToList();
            long inserted = await repository.BulkInsertAsync(first);
            Assert.Equal(750L, inserted);

            List<RelBulkItem> second = Enumerable.Range(0, 120).Select(i => new RelBulkItem { Name = "b" + i, Amount = 1m }).ToList();
            long insertedSync = repository.BulkInsert(second);
            Assert.Equal(120L, insertedSync);

            Assert.Equal(870, await repository.CountAsync());
            Assert.Equal(375 + 120, await repository.CountAsync(x => x.Note == null));
            Assert.Equal(375, await repository.CountAsync(x => x.Note == "odd"));
            Assert.Equal(first.Sum(x => x.Amount) + 120m, await repository.SumAsync(x => x.Amount));
            Assert.Equal(0L, await repository.BulkInsertAsync(new List<RelBulkItem>()));

            List<RelBulkItem> stored = (await repository.Query().ExecuteAsync()).ToList();
            Assert.All(stored, x => Assert.NotEqual(Guid.Empty, x.Token));
            Assert.Equal(870, stored.Select(x => x.Token).Distinct().Count());
        }

        /// <summary>
        /// BulkInsert inside an explicit transaction is visible inside it, rolls back with it, and persists on commit.
        /// </summary>
        [Fact]
        public async Task BulkInsertParticipatesInTransaction()
        {
            ISqlRepository<RelBulkItem> repository = _Provider.CreateRepository<RelBulkItem>();
            await RelTestHelpers.RecreateTableAsync(repository);

            using (ISqlTransaction transaction = await repository.BeginTransactionAsync())
            {
                long rows = await repository.BulkInsertAsync(Enumerable.Range(0, 40).Select(i => new RelBulkItem { Name = "rb" + i, Amount = 1m }).ToList(), transaction);
                Assert.Equal(40L, rows);
                Assert.Equal(40, await repository.CountAsync(null, transaction));
                await transaction.RollbackAsync();
            }

            Assert.Equal(0, await repository.CountAsync());

            using (ISqlTransaction transaction = repository.BeginTransaction())
            {
                long rows = repository.BulkInsert(Enumerable.Range(0, 25).Select(i => new RelBulkItem { Name = "cm" + i, Amount = 2m }).ToList(), transaction);
                Assert.Equal(25L, rows);
                transaction.Commit();
            }

            Assert.Equal(25, await repository.CountAsync());
            Assert.Equal(50m, await repository.SumAsync(x => x.Amount));
        }

        /// <summary>
        /// Upsert on a natural key inserts a new row and updates an existing one; UpsertMany(Async) mixes both.
        /// </summary>
        [Fact]
        public async Task UpsertNaturalKeyInsertAndUpdatePaths()
        {
            ISqlRepository<RelUpsertItem> repository = _Provider.CreateRepository<RelUpsertItem>();
            await RelTestHelpers.RecreateTableAsync(repository);

            RelUpsertItem inserted = repository.Upsert(new RelUpsertItem { Code = "A", Name = "Alpha", Quantity = 1 });
            Assert.Equal("Alpha", inserted.Name);
            Assert.Equal(1, await repository.CountAsync());

            RelUpsertItem updated = await repository.UpsertAsync(new RelUpsertItem { Code = "A", Name = "Alpha 2", Quantity = 5 });
            Assert.Equal("Alpha 2", updated.Name);
            Assert.Equal(1, await repository.CountAsync());
            RelUpsertItem? stored = await repository.ReadByIdAsync("A");
            Assert.NotNull(stored);
            Assert.Equal(5, stored.Quantity);

            List<RelUpsertItem> many = (await repository.UpsertManyAsync(new List<RelUpsertItem>
            {
                new RelUpsertItem { Code = "A", Name = "Alpha 3", Quantity = 6 },
                new RelUpsertItem { Code = "B", Name = "Beta", Quantity = 7 },
                new RelUpsertItem { Code = "C", Name = "Gamma", Quantity = 8 }
            })).ToList();
            Assert.Equal(3, many.Count);
            Assert.Equal(3, await repository.CountAsync());
            Assert.Equal("Alpha 3", (await repository.ReadByIdAsync("A"))?.Name);
            Assert.Equal(7, (await repository.ReadByIdAsync("B"))?.Quantity);

            repository.UpsertMany(new List<RelUpsertItem> { new RelUpsertItem { Code = "C", Name = "Gamma 2", Quantity = 9 } });
            Assert.Equal("Gamma 2", (await repository.ReadByIdAsync("C"))?.Name);
        }

        /// <summary>
        /// Upsert on an auto-increment entity with an unset key inserts and assigns the generated key; upserting
        /// with an existing key updates that row and bumps (never resets) its version.
        /// </summary>
        [Fact]
        public async Task UpsertAutoIncrementAndVersionHandling()
        {
            ISqlRepository<RelVersionedItem> repository = _Provider.CreateRepository<RelVersionedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);

            RelVersionedItem first = await repository.UpsertAsync(new RelVersionedItem { Name = "first", Salary = 10m });
            Assert.True(first.Id > 0, "Upsert with an unset auto-increment key must insert and assign a key");
            Assert.Equal(1, first.Version);

            RelVersionedItem second = repository.Upsert(new RelVersionedItem { Name = "second", Salary = 20m });
            Assert.True(second.Id > first.Id);
            Assert.Equal(2, await repository.CountAsync());

            RelVersionedItem change = new RelVersionedItem { Id = first.Id, Name = "first-upserted", Salary = 11m, Version = first.Version };
            RelVersionedItem result = await repository.UpsertAsync(change);
            Assert.Equal(2, await repository.CountAsync());
            RelVersionedItem? stored = await repository.ReadByIdAsync(first.Id);
            Assert.NotNull(stored);
            Assert.Equal("first-upserted", stored.Name);
            Assert.Equal(11m, stored.Salary);
            Assert.Equal(stored.Version, result.Version);
            Assert.True(stored.Version > first.Version, "Upsert update path should bump the version (was " + first.Version + ", now " + stored.Version + ")");

            RelVersionedItem created = await repository.CreateAsync(new RelVersionedItem { Name = "after-upsert", Salary = 1m });
            Assert.True(created.Id > second.Id, "A Create after an upsert with an explicit key must not collide");
        }

        /// <summary>
        /// BatchUpdate can reference the current row (Salary = Salary * 2, Name = Name + suffix) and only touches
        /// matching rows.
        /// </summary>
        [Fact]
        public async Task BatchUpdateReferencesCurrentRow()
        {
            ISqlRepository<RelVersionedItem> repository = _Provider.CreateRepository<RelVersionedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            await repository.CreateManyAsync(new List<RelVersionedItem>
            {
                new RelVersionedItem { Name = "a", Salary = 100m },
                new RelVersionedItem { Name = "b", Salary = 200m },
                new RelVersionedItem { Name = "c", Salary = 300m }
            });

            int rows = await repository.BatchUpdateAsync(x => x.Salary >= 200m, x => new RelVersionedItem { Salary = x.Salary * 2, Name = x.Name + "!" });
            Assert.Equal(2, rows);

            List<RelVersionedItem> all = (await repository.Query().OrderBy(x => x.Salary).ExecuteAsync()).ToList();
            Assert.Equal(new[] { 100m, 400m, 600m }, all.Select(x => x.Salary).ToArray());
            Assert.Equal(new[] { "a", "b!", "c!" }, all.Select(x => x.Name).ToArray());

            decimal bonus = 5m;
            int syncRows = repository.BatchUpdate(x => x.Name == "a", x => new RelVersionedItem { Salary = x.Salary + bonus });
            Assert.Equal(1, syncRows);
            Assert.Equal(105m, (await repository.ReadFirstAsync(x => x.Name == "a"))?.Salary);
        }

        /// <summary>
        /// UpdateField can set a nullable column to null; DeleteAll removes every row and returns the count.
        /// </summary>
        [Fact]
        public async Task UpdateFieldNullAndDeleteAll()
        {
            ISqlRepository<RelVersionedItem> repository = _Provider.CreateRepository<RelVersionedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            await repository.CreateManyAsync(new List<RelVersionedItem>
            {
                new RelVersionedItem { Name = "x", Nickname = "nick-x" },
                new RelVersionedItem { Name = "y", Nickname = "nick-y" },
                new RelVersionedItem { Name = "z", Nickname = "nick-z" }
            });

            int cleared = await repository.UpdateFieldAsync(x => x.Name != "z", x => x.Nickname, null);
            Assert.Equal(2, cleared);
            Assert.Equal(2, await repository.CountAsync(x => x.Nickname == null));
            Assert.Equal("nick-z", (await repository.ReadFirstAsync(x => x.Name == "z"))?.Nickname);

            int syncCleared = repository.UpdateField(x => x.Name == "z", x => x.Nickname, (string?)null);
            Assert.Equal(1, syncCleared);
            Assert.Equal(3, await repository.CountAsync(x => x.Nickname == null));

            Assert.Equal(3, repository.DeleteAll());
            Assert.Equal(0, await repository.CountAsync());
            await repository.CreateAsync(new RelVersionedItem { Name = "again" });
            Assert.Equal(1, await repository.DeleteAllAsync());
            Assert.Equal(0, await repository.DeleteAllAsync());
        }

        /// <summary>
        /// Default value providers (NewGuid, CurrentDateTimeUtc) fill unset values on Create and CreateMany, and do not
        /// overwrite explicitly supplied values.
        /// </summary>
        [Fact]
        public async Task DefaultValueProvidersApplyOnCreateAndCreateMany()
        {
            ISqlRepository<RelDefaultedItem> repository = _Provider.CreateRepository<RelDefaultedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            DateTime before = DateTime.UtcNow.AddMinutes(-1);

            RelDefaultedItem single = await repository.CreateAsync(new RelDefaultedItem { Name = "single" });
            Assert.NotEqual(Guid.Empty, single.Token);
            Assert.True(single.CreatedUtc >= before);

            Guid explicitToken = Guid.NewGuid();
            DateTime explicitTime = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
            List<RelDefaultedItem> many = (await repository.CreateManyAsync(new List<RelDefaultedItem>
            {
                new RelDefaultedItem { Name = "m1" },
                new RelDefaultedItem { Name = "m2" },
                new RelDefaultedItem { Name = "explicit", Token = explicitToken, CreatedUtc = explicitTime }
            })).ToList();

            Assert.NotEqual(Guid.Empty, many[0].Token);
            Assert.NotEqual(many[0].Token, many[1].Token);
            Assert.True(many[1].CreatedUtc >= before);
            Assert.Equal(explicitToken, many[2].Token);

            RelDefaultedItem? storedSingle = await repository.ReadByIdAsync(single.Id);
            Assert.NotNull(storedSingle);
            Assert.Equal(single.Token, storedSingle.Token);
            Assert.True(storedSingle.CreatedUtc >= before);

            RelDefaultedItem? storedExplicit = await repository.ReadByIdAsync(many[2].Id);
            Assert.NotNull(storedExplicit);
            Assert.Equal(explicitToken, storedExplicit.Token);
            Assert.Equal(explicitTime, DateTime.SpecifyKind(storedExplicit.CreatedUtc, DateTimeKind.Utc));
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private static async Task AssertKeysMatchStoredRowsAsync(ISqlRepository<RelSequenceItem> repository, List<RelSequenceItem> result, string prefix)
        {
            Assert.All(result, x => Assert.True(x.Id > 0, "Every entity must receive a generated key"));
            Assert.Equal(result.Count, result.Select(x => x.Id).Distinct().Count());
            for (int i = 0; i < result.Count; i++) Assert.Equal(i, result[i].Ordinal);

            List<RelSequenceItem> stored = (await repository.Query().Where(x => x.Label.StartsWith(prefix)).ExecuteAsync()).ToList();
            Assert.Equal(result.Count, stored.Count);
            Dictionary<int, RelSequenceItem> byId = stored.ToDictionary(x => x.Id);
            foreach (RelSequenceItem entity in result)
            {
                Assert.True(byId.TryGetValue(entity.Id, out RelSequenceItem? row), "Generated key " + entity.Id + " not found in table");
                Assert.Equal(entity.Ordinal, row!.Ordinal);
                Assert.Equal(entity.Label, row.Label);
            }
        }

        #endregion
    }
}
