namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.ConcurrencyConflictResolvers;
    using Durable.InMemory;
    using Xunit;

    /// <summary>
    /// The in-memory backend through <see cref="Durable.Query.RepositoryBase{T}"/>: CRUD with the same return values and
    /// exceptions as the SQL repositories, copy semantics, generated keys, composite keys, value converters, JSON and enum
    /// storage, upsert, set-based updates and deletes, soft delete, query filters, optimistic concurrency with resolvers,
    /// aggregates and disposal. Runs in every provider configuration (no database needed).
    /// </summary>
    public class InMemoryBackendTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// Create writes back generated keys (sync and async) and reads return equal but distinct instances.
        /// </summary>
        [Fact]
        public async Task CreateAssignsKeysAndReadsReturnCopies()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelTenantNote> repository = backend.CreateRepository<RelTenantNote>();

            RelTenantNote first = repository.Create(new RelTenantNote { TenantId = 1, Title = "one", Amount = 10 });
            RelTenantNote second = await repository.CreateAsync(new RelTenantNote { TenantId = 1, Title = "two", Amount = 20 });
            Assert.Equal(1, first.Id);
            Assert.Equal(2, second.Id);

            RelTenantNote? read = await repository.ReadByIdAsync(second.Id);
            Assert.NotNull(read);
            Assert.NotSame(second, read);
            Assert.Equal("two", read!.Title);
            Assert.Equal(20, read.Amount);
            Assert.Null(repository.ReadById(42));
        }

        /// <summary>
        /// A value already present in an auto-increment key is replaced by the generated one, as SQL repositories do.
        /// </summary>
        [Fact]
        public async Task CreateReplacesExplicitAutoIncrementValue()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            RelTenantNote created = await repository.CreateAsync(new RelTenantNote { Id = 999, Title = "x" });
            Assert.Equal(1, created.Id);
            Assert.Null(await repository.ReadByIdAsync(999));
        }

        /// <summary>
        /// Stored rows are copies: mutating an entity after Create or after a read never changes stored data, and
        /// navigation properties are not stored.
        /// </summary>
        [Fact]
        public async Task StoredDataIsCopiedNotReferenced()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<Author> authors = backend.CreateRepository<Author>();
            InMemoryRepository<Book> books = backend.CreateRepository<Book>();

            Author author = new Author { Name = "Original" };
            author.Books.Add(new Book { Title = "Not stored" });
            await authors.CreateAsync(author);
            author.Name = "Changed after create";

            Author? read = await authors.ReadByIdAsync(author.Id);
            Assert.Equal("Original", read!.Name);
            Assert.Empty(read.Books);
            read.Name = "Changed after read";

            Assert.Equal("Original", (await authors.ReadByIdAsync(author.Id))!.Name);
            Assert.Equal(0, await books.CountAsync());

            InMemoryRepository<RelJsonDocument> documents = backend.CreateRepository<RelJsonDocument>();
            RelJsonDocument document = await documents.CreateAsync(new RelJsonDocument { Name = "d", Tags = new List<string> { "a" } });
            document.Tags!.Add("mutated");
            RelJsonDocument? stored = await documents.ReadByIdAsync(document.Id);
            Assert.Equal(new[] { "a" }, stored!.Tags);
            stored.Tags!.Add("mutated again");
            Assert.Equal(new[] { "a" }, (await documents.ReadByIdAsync(document.Id))!.Tags);
        }

        /// <summary>
        /// CreateMany writes keys back in input order; nulls are rejected; an empty input returns an empty result.
        /// </summary>
        [Fact]
        public async Task CreateManyWritesBackKeysInOrder()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            List<RelTenantNote> created = (await repository.CreateManyAsync(Enumerable.Range(0, 5).Select(i => new RelTenantNote { Title = "n" + i }).ToList())).ToList();
            Assert.Equal(new[] { 1, 2, 3, 4, 5 }, created.Select(c => c.Id).ToArray());
            Assert.Equal(new[] { "n0", "n1", "n2", "n3", "n4" }, repository.Query().OrderBy(x => x.Id).Execute().Select(x => x.Title).ToArray());

            Assert.Empty(repository.CreateMany(new List<RelTenantNote>()));
            ArgumentNullException error = Assert.Throws<ArgumentNullException>(() => repository.CreateMany(new List<RelTenantNote> { new RelTenantNote(), null! }));
            Assert.Contains("index 1", error.Message);
            Assert.Equal(5, repository.Count());
        }

        /// <summary>
        /// Inserting a duplicate primary key throws InvalidOperationException and leaves the stored row unchanged.
        /// </summary>
        [Fact]
        public async Task DuplicatePrimaryKeyThrows()
        {
            InMemoryRepository<RelUpsertItem> repository = new InMemoryBackend().CreateRepository<RelUpsertItem>();
            await repository.CreateAsync(new RelUpsertItem { Code = "A", Name = "first" });
            await Assert.ThrowsAsync<InvalidOperationException>(() => repository.CreateAsync(new RelUpsertItem { Code = "A", Name = "second" }));
            Assert.Equal("first", (await repository.ReadByIdAsync("A"))!.Name);

            await repository.CreateAsync(new RelUpsertItem { Code = "a", Name = "lower" });
            Assert.Equal(2, await repository.CountAsync());
        }

        /// <summary>
        /// ReadSingle, ReadSingleOrDefault, ReadFirst, Exists and Count follow the SQL repository contract.
        /// </summary>
        [Fact]
        public async Task ReadSemanticsMatchSqlRepositories()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            await repository.CreateManyAsync(new[]
            {
                new RelTenantNote { TenantId = 1, Title = "a", Amount = 1 },
                new RelTenantNote { TenantId = 1, Title = "b", Amount = 2 },
                new RelTenantNote { TenantId = 2, Title = "c", Amount = 3 }
            });

            Assert.Equal("c", repository.ReadSingle(x => x.TenantId == 2).Title);
            InvalidOperationException none = await Assert.ThrowsAsync<InvalidOperationException>(() => repository.ReadSingleAsync(x => x.TenantId == 9));
            Assert.Equal("Sequence contains no matching element.", none.Message);
            InvalidOperationException many = Assert.Throws<InvalidOperationException>(() => repository.ReadSingle(x => x.TenantId == 1));
            Assert.Equal("Sequence contains more than one matching element.", many.Message);
            Assert.Null(await repository.ReadSingleOrDefaultAsync(x => x.TenantId == 9));
            Assert.Throws<InvalidOperationException>(() => repository.ReadSingleOrDefault(x => x.TenantId == 1));
            Assert.Throws<ArgumentNullException>(() => repository.ReadSingle(null!));

            Assert.Equal("a", repository.ReadFirst()!.Title);
            Assert.Equal("b", (await repository.ReadFirstOrDefaultAsync(x => x.Amount > 1))!.Title);
            Assert.Null(repository.ReadFirst(x => x.Amount > 100));
            Assert.True(repository.Exists(x => x.Title == "b"));
            Assert.False(await repository.ExistsAsync(x => x.Title == "z"));
            Assert.True(await repository.ExistsByIdAsync(3));
            Assert.False(repository.ExistsById(4));
            Assert.Equal(2, repository.Count(x => x.TenantId == 1));
            Assert.Equal(3, await repository.CountAsync());
            Assert.Equal(3, repository.ReadAll().Count());
            List<string> streamed = new List<string>();
            await foreach (RelTenantNote note in repository.ReadManyAsync(x => x.Amount >= 2)) streamed.Add(note.Title);
            Assert.Equal(new[] { "b", "c" }, streamed.ToArray());
        }

        /// <summary>
        /// Update persists changes; updating a missing row throws the SQL repository's message; UpdateMany applies an
        /// action to each match (sync and async).
        /// </summary>
        [Fact]
        public async Task UpdateAndUpdateMany()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            RelTenantNote note = await repository.CreateAsync(new RelTenantNote { TenantId = 1, Title = "before", Amount = 1 });
            note.Title = "after";
            Assert.Same(note, repository.Update(note));
            Assert.Equal("after", (await repository.ReadByIdAsync(note.Id))!.Title);

            InvalidOperationException missing = await Assert.ThrowsAsync<InvalidOperationException>(() => repository.UpdateAsync(new RelTenantNote { Id = 42, Title = "x" }));
            Assert.Equal("No rows were affected during update for entity with key 42.", missing.Message);

            await repository.CreateManyAsync(new[] { new RelTenantNote { TenantId = 2, Amount = 5 }, new RelTenantNote { TenantId = 2, Amount = 6 } });
            Assert.Equal(2, repository.UpdateMany(x => x.TenantId == 2, x => x.Amount *= 10));
            Assert.Equal(new[] { 50, 60 }, repository.ReadMany(x => x.TenantId == 2).Select(x => x.Amount).ToArray());
            Assert.Equal(2, await repository.UpdateManyAsync(x => x.TenantId == 2, x => { x.Title = "async"; return Task.CompletedTask; }));
            Assert.All(repository.ReadMany(x => x.TenantId == 2), x => Assert.Equal("async", x.Title));
        }

        /// <summary>
        /// UpdateField and BatchUpdate update matching rows in one call, may reference the current row, and reject
        /// invalid expressions with the SQL repository's exceptions.
        /// </summary>
        [Fact]
        public async Task UpdateFieldAndBatchUpdate()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            await repository.CreateManyAsync(new[]
            {
                new RelTenantNote { TenantId = 1, Title = "a", Amount = 1 },
                new RelTenantNote { TenantId = 1, Title = "b", Amount = 2 },
                new RelTenantNote { TenantId = 2, Title = "c", Amount = 3 }
            });

            Assert.Equal(2, repository.UpdateField(x => x.TenantId == 1, x => x.Title, "renamed"));
            Assert.Equal(2, await repository.CountAsync(x => x.Title == "renamed"));

            int bonus = 100;
            Assert.Equal(2, await repository.BatchUpdateAsync(x => x.TenantId == 1, x => new RelTenantNote { Amount = x.Amount + bonus, Title = x.Title + "!" }));
            Assert.Equal(new[] { 101, 102, 3 }, repository.Query().OrderBy(x => x.Id).Execute().Select(x => x.Amount).ToArray());
            Assert.Equal(2, await repository.CountAsync(x => x.Title == "renamed!"));
            Assert.Equal(0, repository.BatchUpdate(x => x.TenantId == 9, x => new RelTenantNote { Amount = 0 }));

            await Assert.ThrowsAsync<NotSupportedException>(() => repository.BatchUpdateAsync(x => true, x => x));
            Assert.Throws<NotSupportedException>(() => repository.BatchUpdate(x => true, x => new RelTenantNote { Id = 5 }));
            Assert.Throws<ArgumentException>(() => repository.UpdateField(x => true, x => x.Title.Length, 3));
            Assert.Throws<ArgumentNullException>(() => repository.UpdateField(null!, x => x.Title, "x"));
        }

        /// <summary>
        /// Delete, DeleteById, DeleteMany, BatchDelete and DeleteAll return the SQL repository's values.
        /// </summary>
        [Fact]
        public async Task DeleteVariants()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            List<RelTenantNote> notes = (await repository.CreateManyAsync(Enumerable.Range(1, 6).Select(i => new RelTenantNote { TenantId = i % 2, Amount = i }).ToList())).ToList();

            Assert.True(repository.Delete(notes[0]));
            Assert.False(await repository.DeleteAsync(notes[0]));
            Assert.True(await repository.DeleteByIdAsync(notes[1].Id));
            Assert.False(repository.DeleteById(notes[1].Id));
            Assert.Equal(1, repository.DeleteMany(x => x.Amount == 3));
            Assert.Equal(1, await repository.BatchDeleteAsync(x => x.Amount == 4));
            Assert.Throws<NotSupportedException>(() => repository.Query().Take(1).Delete());
            Assert.Equal(2, await repository.DeleteAllAsync());
            Assert.Equal(0, repository.Count());
        }

        /// <summary>
        /// Soft delete: deletes set the marker, reads, counts and existence checks hide marked rows, IgnoreQueryFilters
        /// shows them, and set-based updates skip them. Both bool and timestamp markers.
        /// </summary>
        [Fact]
        public async Task SoftDeleteMarksRowsAndHidesThem()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelSoftNote> repository = backend.CreateRepository<RelSoftNote>();
            List<RelSoftNote> created = (await repository.CreateManyAsync(Enumerable.Range(0, 6).Select(i => new RelSoftNote { ParentId = 1, Text = "n" + i }).ToList())).ToList();

            Assert.True(await repository.DeleteAsync(created[0]));
            Assert.True(repository.DeleteById(created[1].Id));
            Assert.Equal(2, await repository.DeleteManyAsync(x => x.Text == "n2" || x.Text == "n3"));
            Assert.Equal(2, await repository.CountAsync());
            Assert.Null(await repository.ReadByIdAsync(created[0].Id));
            Assert.False(await repository.ExistsByIdAsync(created[1].Id));
            Assert.Equal(6, backend.GetStoredRows(typeof(RelSoftNote)).Count);
            Assert.Equal(4, (await repository.Query().IgnoreQueryFilters().ExecuteAsync()).Count(x => x.IsDeleted));

            Assert.Equal(2, await repository.UpdateFieldAsync(x => x.ParentId == 1, x => x.Text, "touched"));
            Assert.Equal("n2", (await repository.Query().IgnoreQueryFilters().Where(x => x.Id == created[2].Id).ExecuteAsync()).Single().Text);
            Assert.Equal(2, repository.DeleteAll());
            Assert.Equal(0, repository.Count());
            Assert.Equal(6, repository.Query().IgnoreQueryFilters().Count());

            InMemoryRepository<RelSoftStampNote> stamps = backend.CreateRepository<RelSoftStampNote>();
            RelSoftStampNote stamp = await stamps.CreateAsync(new RelSoftStampNote { Text = "s" });
            await stamps.CreateAsync(new RelSoftStampNote { Text = "keep" });
            DateTime before = DateTime.UtcNow.AddMinutes(-1);
            Assert.True(stamps.Delete(stamp));
            RelSoftStampNote marked = (await stamps.Query().IgnoreQueryFilters().Where(x => x.Id == stamp.Id).ExecuteAsync()).Single();
            Assert.True(marked.DeletedUtc > before);
            Assert.Equal("keep", stamps.ReadAll().Single().Text);
        }

        /// <summary>
        /// Query filters combine with every predicate-based read and write, read captured variables each time, do not
        /// apply to key-based Update, and can be bypassed or cleared.
        /// </summary>
        [Fact]
        public async Task QueryFiltersApplyToPredicateOperations()
        {
            InMemoryRepository<RelTenantNote> repository = new InMemoryBackend().CreateRepository<RelTenantNote>();
            await repository.CreateManyAsync(new[]
            {
                new RelTenantNote { TenantId = 1, Title = "t1a", Amount = 1 },
                new RelTenantNote { TenantId = 1, Title = "t1b", Amount = 2 },
                new RelTenantNote { TenantId = 2, Title = "t2a", Amount = 3 }
            });

            int tenant = 1;
            repository.AddQueryFilter(x => x.TenantId == tenant);
            Assert.Single(repository.QueryFilters);
            Assert.Equal(2, await repository.CountAsync());
            tenant = 2;
            Assert.Equal("t2a", repository.ReadAll().Single().Title);
            Assert.Null(await repository.ReadByIdAsync(1));
            Assert.Equal(1, repository.UpdateField(x => x.Amount > 0, x => x.Amount, 50));
            Assert.Equal(3, await repository.Query().IgnoreQueryFilters().CountAsync());

            RelTenantNote other = (await repository.Query().IgnoreQueryFilters().Where(x => x.Id == 1).ExecuteAsync()).Single();
            other.Title = "updated by key";
            await repository.UpdateAsync(other);
            Assert.Equal("updated by key", (await repository.Query().IgnoreQueryFilters().Where(x => x.Id == 1).ExecuteAsync()).Single().Title);

            Assert.Equal(1, await repository.DeleteAllAsync());
            repository.ClearQueryFilters();
            Assert.Empty(repository.QueryFilters);
            Assert.Equal(2, await repository.CountAsync());
        }

        /// <summary>
        /// Composite keys: ids are object arrays in key order, a wrong number of values throws, and every key-based
        /// operation works.
        /// </summary>
        [Fact]
        public async Task CompositeKeys()
        {
            InMemoryRepository<RelCompositeItem> repository = new InMemoryBackend().CreateRepository<RelCompositeItem>();
            await repository.CreateAsync(new RelCompositeItem { TenantId = 1, Sku = "A", Name = "one", Quantity = 1 });
            await repository.CreateAsync(new RelCompositeItem { TenantId = 2, Sku = "A", Name = "two", Quantity = 2 });
            await Assert.ThrowsAsync<InvalidOperationException>(() => repository.CreateAsync(new RelCompositeItem { TenantId = 1, Sku = "A" }));

            Assert.Equal("two", (await repository.ReadByIdAsync(new object[] { 2, "A" }))!.Name);
            Assert.Null(repository.ReadById(new object[] { 3, "A" }));
            Assert.Throws<ArgumentException>(() => repository.ReadById(new object[] { 1 }));
            Assert.Throws<ArgumentException>(() => repository.ReadById(1));

            RelCompositeItem item = (await repository.ReadByIdAsync(new object[] { 1L, "A" }))!;
            item.Quantity = 10;
            await repository.UpdateAsync(item);
            Assert.Equal(10, (await repository.ReadByIdAsync(new object[] { 1, "A" }))!.Quantity);
            Assert.True(repository.ExistsById(new object[] { 1, "A" }));
            Assert.True(await repository.DeleteByIdAsync(new object[] { 1, "A" }));
            Assert.Equal(1, repository.Count());
        }

        /// <summary>
        /// Value converters store the provider value, round-trip on read, and are applied to predicate values and to
        /// UpdateField and BatchUpdate values.
        /// </summary>
        [Fact]
        public async Task ValueConvertersRoundTripAndApplyToPredicates()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelConvertedItem> repository = backend.CreateRepository<RelConvertedItem>();
            await repository.CreateAsync(new RelConvertedItem { Name = "cheap", Price = new Money(150), Tags = new List<string> { "a", "b" }, Priority = RelPriority.Low });
            await repository.CreateAsync(new RelConvertedItem { Name = "dear", Price = new Money(99999), Tags = new List<string> { "c" }, Priority = RelPriority.High });

            IReadOnlyDictionary<string, object?> stored = backend.GetStoredRows(typeof(RelConvertedItem))[0];
            Assert.Equal(150L, stored["price"]);
            Assert.Equal("a,b", stored["tags"]);
            Assert.Equal("L", stored["priority"]);

            RelConvertedItem cheap = (await repository.ReadFirstAsync(x => x.Name == "cheap"))!;
            Assert.Equal(new Money(150), cheap.Price);
            Assert.Equal(new[] { "a", "b" }, cheap.Tags);
            Assert.Equal(RelPriority.Low, cheap.Priority);

            Assert.Equal("dear", repository.ReadSingle(x => x.Priority == RelPriority.High).Name);
            Assert.Equal("dear", repository.ReadSingle(x => x.Price == new Money(99999)).Name);
            Assert.Equal("cheap", repository.ReadSingle(x => x.Tags == new List<string> { "a", "b" }).Name);

            Assert.Equal(1, await repository.UpdateFieldAsync(x => x.Name == "cheap", x => x.Priority, RelPriority.Medium));
            Assert.Equal("M", backend.GetStoredRows(typeof(RelConvertedItem))[0]["priority"]);
            Assert.Equal(1, await repository.BatchUpdateAsync(x => x.Name == "dear", x => new RelConvertedItem { Price = new Money(1) }));
            Assert.Equal(new Money(1), repository.ReadSingle(x => x.Name == "dear").Price);
        }

        /// <summary>
        /// JSON columns store serialized text and round-trip nested objects, collections, dictionaries and nulls.
        /// </summary>
        [Fact]
        public async Task JsonColumnsRoundTrip()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelJsonDocument> repository = backend.CreateRepository<RelJsonDocument>();
            RelJsonDocument document = await repository.CreateAsync(new RelJsonDocument
            {
                Name = "doc",
                Payload = new RelJsonPayload { Label = "p", Score = 7, Values = new List<string> { "x" }, Child = new RelJsonPayload { Label = "child" } },
                Tags = new List<string> { "t1", "t2" },
                Counters = new Dictionary<string, int> { ["a"] = 1 }
            });
            await repository.CreateAsync(new RelJsonDocument { Name = "empty" });

            Assert.IsType<string>(backend.GetStoredRows(typeof(RelJsonDocument))[0]["payload"]);
            Assert.Contains("\"label\":\"p\"", (string)backend.GetStoredRows(typeof(RelJsonDocument))[0]["payload"]!);

            RelJsonDocument read = (await repository.ReadByIdAsync(document.Id))!;
            Assert.Equal(7, read.Payload!.Score);
            Assert.Equal("child", read.Payload.Child!.Label);
            Assert.Equal(new[] { "t1", "t2" }, read.Tags);
            Assert.Equal(1, read.Counters!["a"]);

            RelJsonDocument empty = repository.ReadSingle(x => x.Name == "empty");
            Assert.Null(empty.Payload);
            Assert.Null(empty.Tags);
            Assert.Equal("empty", repository.ReadSingle(x => x.Payload == null).Name);
        }

        /// <summary>
        /// Enums are stored by name by default and by number with Flags.Integer, like the SQL providers.
        /// </summary>
        [Fact]
        public async Task EnumStorageMatchesSqlProviders()
        {
            InMemoryQtData data = await InMemoryQtData.CreateAsync();
            IReadOnlyDictionary<string, object?> alpha = data.Backend.GetStoredRows(typeof(QtItem))[0];
            Assert.Equal("Active", alpha["status"]);
            Assert.Equal(3L, alpha["priority"]);
            Assert.Equal(QtStatus.Active, data.Items.ReadSingle(x => x.Name == InMemoryQtData.Alpha).Status);
        }

        /// <summary>
        /// Upsert inserts a missing row and replaces an existing one; an unset generated key inserts; UpsertMany runs
        /// each entity; the returned entity reflects what is stored.
        /// </summary>
        [Fact]
        public async Task UpsertInsertsOrUpdates()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelUpsertItem> repository = backend.CreateRepository<RelUpsertItem>();
            await repository.UpsertAsync(new RelUpsertItem { Code = "A", Name = "first", Quantity = 1 });
            RelUpsertItem replaced = repository.Upsert(new RelUpsertItem { Code = "A", Name = "second", Quantity = 2 });
            Assert.Equal("second", replaced.Name);
            Assert.Equal(1, await repository.CountAsync());
            Assert.Equal(2, (await repository.ReadByIdAsync("A"))!.Quantity);

            List<RelUpsertItem> many = (await repository.UpsertManyAsync(new[]
            {
                new RelUpsertItem { Code = "A", Name = "third", Quantity = 3 },
                new RelUpsertItem { Code = "B", Name = "bee", Quantity = 4 }
            })).ToList();
            Assert.Equal(2, many.Count);
            Assert.Equal(new[] { "third", "bee" }, repository.Query().OrderBy(x => x.Code).Execute().Select(x => x.Name).ToArray());

            InMemoryRepository<RelVersionedItem> versioned = backend.CreateRepository<RelVersionedItem>();
            RelVersionedItem fresh = await versioned.UpsertAsync(new RelVersionedItem { Name = "v" });
            Assert.Equal(1, fresh.Id);
            Assert.Equal(1, fresh.Version);
            RelVersionedItem again = await versioned.UpsertAsync(new RelVersionedItem { Id = fresh.Id, Name = "v2", Version = fresh.Version });
            Assert.Equal(2, again.Version);
            Assert.Equal("v2", again.Name);
            RelVersionedItem explicitKey = await versioned.UpsertAsync(new RelVersionedItem { Id = 50, Name = "explicit" });
            Assert.Equal(50, explicitKey.Id);
            Assert.Equal(51, (await versioned.CreateAsync(new RelVersionedItem { Name = "next" })).Id);
        }

        /// <summary>
        /// Integer version columns: Create sets version 1; Update increments it; a stale update throws
        /// OptimisticConcurrencyException; set-based updates bump the version unless it is assigned explicitly.
        /// </summary>
        [Fact]
        public async Task OptimisticConcurrencyDetectsStaleUpdates()
        {
            InMemoryRepository<RelVersionedItem> repository = new InMemoryBackend().CreateRepository<RelVersionedItem>();
            RelVersionedItem item = await repository.CreateAsync(new RelVersionedItem { Name = "a", Salary = 10m });
            Assert.Equal(1, item.Version);

            RelVersionedItem copyA = (await repository.ReadByIdAsync(item.Id))!;
            RelVersionedItem copyB = (await repository.ReadByIdAsync(item.Id))!;
            copyA.Salary = 20m;
            await repository.UpdateAsync(copyA);
            Assert.Equal(2, copyA.Version);

            copyB.Salary = 30m;
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(() => repository.UpdateAsync(copyB));
            Assert.Throws<OptimisticConcurrencyException>(() => repository.Update(copyB));
            Assert.Equal(20m, (await repository.ReadByIdAsync(item.Id))!.Salary);

            Assert.Equal(1, repository.UpdateField(x => x.Id == item.Id, x => x.Nickname, "nick"));
            Assert.Equal(3, (await repository.ReadByIdAsync(item.Id))!.Version);
            Assert.Equal(1, await repository.BatchUpdateAsync(x => x.Id == item.Id, x => new RelVersionedItem { Salary = x.Salary * 2 }));
            RelVersionedItem afterBatch = (await repository.ReadByIdAsync(item.Id))!;
            Assert.Equal(4, afterBatch.Version);
            Assert.Equal(40m, afterBatch.Salary);
            Assert.Equal(1, await repository.BatchUpdateAsync(x => x.Id == item.Id, x => new RelVersionedItem { Version = 100 }));
            Assert.Equal(100, (await repository.ReadByIdAsync(item.Id))!.Version);

            RelVersionedItem deleted = (await repository.ReadByIdAsync(item.Id))!;
            await repository.DeleteByIdAsync(item.Id);
            OptimisticConcurrencyException gone = await Assert.ThrowsAsync<OptimisticConcurrencyException>(() => repository.UpdateAsync(deleted));
            Assert.Contains("was deleted by another process", gone.Message);
        }

        /// <summary>
        /// Conflict resolvers: client-wins retries with the client's values; database-wins keeps the stored values; both
        /// through the sync and async paths.
        /// </summary>
        [Fact]
        public async Task ConflictResolversDecideTheOutcome()
        {
            InMemoryRepository<RelVersionedItem> repository = new InMemoryBackend().CreateRepository<RelVersionedItem>();
            RelVersionedItem item = await repository.CreateAsync(new RelVersionedItem { Name = "a", Salary = 10m });
            RelVersionedItem stale = (await repository.ReadByIdAsync(item.Id))!;
            RelVersionedItem winner = (await repository.ReadByIdAsync(item.Id))!;
            winner.Salary = 11m;
            await repository.UpdateAsync(winner);

            repository.ConflictResolver = new DefaultConflictResolver<RelVersionedItem>(ConflictResolutionStrategy.ClientWins);
            stale.Salary = 99m;
            RelVersionedItem clientWon = await repository.UpdateAsync(stale);
            Assert.Equal(99m, clientWon.Salary);
            Assert.Equal(3, clientWon.Version);
            Assert.Equal(99m, (await repository.ReadByIdAsync(item.Id))!.Salary);

            RelVersionedItem stale2 = (await repository.ReadByIdAsync(item.Id))!;
            RelVersionedItem winner2 = (await repository.ReadByIdAsync(item.Id))!;
            winner2.Name = "db";
            repository.Update(winner2);
            repository.ConflictResolver = new DefaultConflictResolver<RelVersionedItem>(ConflictResolutionStrategy.DatabaseWins);
            stale2.Name = "client";
            RelVersionedItem databaseWon = repository.Update(stale2);
            Assert.Equal("db", databaseWon.Name);
            Assert.Equal("db", (await repository.ReadByIdAsync(item.Id))!.Name);
            Assert.Throws<ArgumentNullException>(() => repository.ConflictResolver = null!);
        }

        /// <summary>
        /// Sum, Average, Min and Max over matching rows, and their values over no rows (zero or default), like SQL.
        /// </summary>
        [Fact]
        public async Task AggregatesMatchSqlSemantics()
        {
            InMemoryQtData data = await InMemoryQtData.CreateAsync();
            InMemoryRepository<QtItem> items = data.Items;
            Assert.Equal(152.84m, items.Sum(x => x.Price));
            Assert.Equal(21m, await items.SumAsync(x => x.Quantity, x => x.IsActive));
            Assert.Equal(8, await items.SumAsync(x => x.Discount));
            Assert.Equal(2m, items.Average(x => x.Discount));
            Assert.Equal(12, items.Max(x => x.Quantity));
            Assert.Equal(0.10m, await items.MinAsync(x => x.Price));
            Assert.Equal(InMemoryQtData.Padded, items.Min(x => x.Name));
            Assert.Equal(new DateTime(2024, 7, 4, 12, 0, 0), await items.MaxAsync(x => x.CreatedUtc));
            Assert.Equal(QtPriority.Critical, items.Max(x => x.Priority));
            Assert.Equal(QtStatus.Suspended, items.Max(x => x.Status));

            Assert.Equal(0m, items.Sum(x => x.Price, x => x.Quantity > 100));
            Assert.Equal(0m, await items.AverageAsync(x => x.Price, x => x.Quantity > 100));
            Assert.Equal(0, items.Max(x => x.Quantity, x => x.Quantity > 100));
            Assert.Null(items.Min(x => x.Name, x => x.Quantity > 100));
            Assert.Null(await items.MaxAsync(x => x.Discount, x => x.Discount == null));
            Assert.Equal(2.75, items.Max(x => x.Ratio));
        }

        /// <summary>
        /// A disposed repository rejects new queries and writes; disposing the repository does not affect the backend.
        /// </summary>
        [Fact]
        public async Task DisposedRepositoryRejectsOperations()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelTenantNote> repository = backend.CreateRepository<RelTenantNote>();
            await repository.CreateAsync(new RelTenantNote { Title = "kept" });
            repository.Dispose();
            Assert.Throws<ObjectDisposedException>(() => repository.Query());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => repository.CreateAsync(new RelTenantNote()));
            Assert.Equal(1, backend.CreateRepository<RelTenantNote>().Count());
            Assert.Throws<ArgumentNullException>(() => new InMemoryRepository<RelTenantNote>(null!));
        }

        #endregion
    }
}
