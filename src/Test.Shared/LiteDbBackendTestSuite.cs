namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;
    using Durable.LiteDb;
    using Durable.Query;
    using LiteDB;
    using Xunit;

    /// <summary>
    /// LiteDB backend tests beyond the conformance kit: file persistence across reopen, one database shared by several
    /// repositories and backends, push-down of exact filters (and parity with the in-memory backend where push-down does
    /// not apply), lossless value round-trips, unique auto-increment keys under concurrent writers, optimistic concurrency
    /// without lost updates, settings validation, non-ordinal collations, disposal semantics, transactions that span
    /// awaits and threads, failed operations inside transactions, unsupported mappings and synchronous use under a
    /// non-pumping synchronization context.
    /// </summary>
    public class LiteDbBackendTestSuite
    {
        #region Private-Members

        private static readonly DateTime _Base = new DateTime(2024, 3, 4, 5, 6, 7, DateTimeKind.Utc).AddTicks(1234567);

        #endregion

        #region Public-Methods

        /// <summary>
        /// Data written to a file survives closing and reopening it: values, exact DateTime ticks and kind, indexes, and the
        /// auto-increment sequence.
        /// </summary>
        [Fact]
        public async Task FilePersistsAcrossReopen()
        {
            string path = TempFile();
            try
            {
                int firstId;
                using (LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForFile(path)))
                {
                    Assert.True(backend.OwnsDatabase);
                    LiteDbRepository<LiteDbNote> notes = backend.CreateRepository<LiteDbNote>();
                    LiteDbNote first = await notes.CreateAsync(new LiteDbNote { Title = "persisted", Amount = 7, Created = _Base });
                    notes.Create(new LiteDbNote { Title = "second", Amount = 8, Created = _Base });
                    firstId = first.Id;
                }

                using (LiteDbBackend reopened = LiteDbBackend.Create(LiteDbRepositorySettings.ForFile(path)))
                {
                    Assert.True(reopened.HasOrdinalCollation);
                    LiteDbRepository<LiteDbNote> notes = reopened.CreateRepository<LiteDbNote>();
                    Assert.Equal(2L, notes.Count());
                    LiteDbNote? stored = notes.ReadById(firstId);
                    Assert.NotNull(stored);
                    Assert.Equal("persisted", stored.Title);
                    Assert.Equal(7, stored.Amount);
                    Assert.Equal(_Base.Ticks, stored.Created.Ticks);
                    Assert.Equal(DateTimeKind.Utc, stored.Created.Kind);

                    LiteDbNote third = notes.Create(new LiteDbNote { Title = "third" });
                    Assert.True(third.Id > firstId + 1, "the sequence continues after reopening");

                    List<string> indexes = reopened.Database.GetCollection("$indexes").FindAll()
                        .Where(d => d["collection"].AsString == "ldb_notes")
                        .Select(d => d["name"].AsString)
                        .ToList();
                    Assert.Contains("ix_owner_id", indexes);
                    Assert.Contains("ix_title", indexes);
                }
            }
            finally
            {
                DeleteFile(path);
            }
        }

        /// <summary>
        /// Two backends opening one file in Shared mode (as two processes would) see each other's writes, generate unique
        /// keys under concurrency, and support transactions, including a transaction that LiteDB rolls back after a failure.
        /// </summary>
        [Fact]
        public async Task SharedConnectionModeAcrossBackends()
        {
            string path = TempFile();
            try
            {
                LiteDbRepositorySettings settings = new LiteDbRepositorySettings { Filename = path, ConnectionType = LiteDbConnectionType.Shared };
                using LiteDbBackend first = LiteDbBackend.Create(settings);
                using LiteDbBackend second = LiteDbBackend.Create(settings);
                LiteDbRepository<LiteDbNote> a = first.CreateRepository<LiteDbNote>();
                LiteDbRepository<LiteDbNote> b = second.CreateRepository<LiteDbNote>();

                a.Create(new LiteDbNote { Title = "from first" });
                Assert.Equal("from first", b.ReadAll().Single().Title);

                using (ITransaction transaction = await b.BeginTransactionAsync())
                {
                    await b.CreateAsync(new LiteDbNote { Title = "from second" }, transaction);
                    await Task.Yield();
                    Assert.Equal(2L, await b.CountAsync(null, transaction));
                    await transaction.CommitAsync();
                }

                Assert.Equal(2L, a.Count());

                ITransaction failing = await a.BeginTransactionAsync();
                await a.CreateAsync(new LiteDbNote { Title = "lost" }, failing);
                await Assert.ThrowsAnyAsync<LiteException>(() => a.CreateAsync(new LiteDbNote { Title = new string('x', 2000) }, failing));
                await Assert.ThrowsAsync<InvalidOperationException>(() => failing.CommitAsync());
                await failing.DisposeAsync();
                Assert.Equal(2L, b.Count());

                await Task.WhenAll(Enumerable.Range(0, 20).Select(i => Task.Run(() => (i % 2 == 0 ? a : b).Create(new LiteDbNote { Title = "c" + i }))));
                List<int> ids = a.ReadAll().Select(x => x.Id).ToList();
                Assert.Equal(22, ids.Count);
                Assert.Equal(22, ids.Distinct().Count());
            }
            finally
            {
                DeleteFile(path);
            }
        }

        /// <summary>
        /// Repositories created over one LiteDatabase (directly or through separate backends) share data, includes,
        /// navigation predicates and transactions.
        /// </summary>
        [Fact]
        public async Task SharedDatabaseAcrossRepositories()
        {
            using LiteDatabase database = new LiteDatabase(LiteDbRepositorySettings.ForInMemory().ToConnectionString());
            LiteDbRepository<LiteDbOwner> owners = LiteDbBackend.Create(LiteDbRepositorySettings.ForDatabase(database)).CreateRepository<LiteDbOwner>();
            LiteDbRepository<LiteDbNote> notes = new LiteDbRepository<LiteDbNote>(LiteDbBackend.Create(LiteDbRepositorySettings.ForDatabase(database)));
            Assert.NotSame(owners.Backend, notes.Backend);
            Assert.Same(owners.Backend.Database, notes.Backend.Database);
            Assert.False(owners.Backend.OwnsDatabase);

            LiteDbOwner ada = owners.Create(new LiteDbOwner { Name = "Ada" });
            owners.Create(new LiteDbOwner { Name = "Grace" });
            await notes.CreateAsync(new LiteDbNote { Title = "by ada", OwnerId = ada.Id });
            notes.Create(new LiteDbNote { Title = "orphan" });

            LiteDbNote withOwner = (await notes.Query().Where(x => x.Title == "by ada").Include(x => x.Owner).ExecuteAsync()).Single();
            Assert.Equal("Ada", withOwner.Owner?.Name);
            LiteDbOwner withNotes = owners.Query().Where(x => x.Id == ada.Id).Include(x => x.Notes).Execute().Single();
            Assert.Equal(new[] { "by ada" }, withNotes.Notes.Select(n => n.Title).ToArray());
            Assert.Equal(new[] { "by ada" }, notes.ReadMany(x => x.Owner!.Name == "Ada").Select(n => n.Title).ToArray());
            Assert.Equal(new[] { "Ada" }, owners.ReadMany(x => x.Notes.Any()).Select(o => o.Name).ToArray());

            LiteDbBackend other = LiteDbBackend.Create(LiteDbRepositorySettings.ForDatabase(database));
            Assert.Equal(2L, other.CreateRepository<LiteDbNote>().Count());

            using (ITransaction transaction = owners.BeginTransaction())
            {
                Assert.True(notes.Backend.Owns(transaction));
                LiteDbOwner rolled = owners.Create(new LiteDbOwner { Name = "Rolled" }, transaction);
                notes.Create(new LiteDbNote { Title = "rolled", OwnerId = rolled.Id }, transaction);
                Assert.Equal(1L, other.CreateRepository<LiteDbNote>().Count(x => x.Title == "rolled", transaction));
                transaction.Rollback();
            }

            Assert.Equal(2L, owners.Count());
            Assert.Equal(2L, notes.Count());
        }

        /// <summary>
        /// Simple equality, range, null-guarded, IN and key filters are pushed down to LiteDB (using its indexes) and are
        /// exact; filters LiteDB cannot evaluate with C# semantics stay client-side; paging and counting run inside LiteDB
        /// when the filter is exact and unordered.
        /// </summary>
        [Fact]
        public async Task SimpleFiltersArePushedDown()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            backend.ExplainQueries = true;
            List<LiteDbQueryPlan> observed = new List<LiteDbQueryPlan>();
            backend.QueryPlanned += (sender, plan) =>
            {
                Assert.Same(backend, sender);
                lock (observed) observed.Add(plan);
            };
            LiteDbRepository<LiteDbOwner> owners = backend.CreateRepository<LiteDbOwner>();
            LiteDbRepository<LiteDbNote> notes = backend.CreateRepository<LiteDbNote>();
            LiteDbOwner owner = owners.Create(new LiteDbOwner { Name = "Owner" });
            List<LiteDbNote> seed = new List<LiteDbNote>();
            for (int i = 0; i < 20; i++)
            {
                seed.Add(await notes.CreateAsync(new LiteDbNote
                {
                    Title = "note-" + i,
                    Amount = i,
                    Rating = i % 3 == 0 ? null : i,
                    OwnerId = i % 2 == 0 ? owner.Id : null,
                    Created = _Base.AddDays(i)
                }));
            }

            AssertPushed(notes.ReadMany(x => x.Amount == 5), seed.Where(x => x.Amount == 5), backend, 1, 1);
            AssertPushed(notes.ReadMany(x => x.Amount >= 3 && x.Amount < 8), seed.Where(x => x.Amount >= 3 && x.Amount < 8), backend, 2, 5);
            AssertPushed(notes.ReadMany(x => x.Rating < 10), seed.Where(x => x.Rating < 10), backend, 1, 6);
            AssertPushed(notes.ReadMany(x => x.Rating != 4), seed.Where(x => x.Rating != 4), backend, 1, 19);
            AssertPushed(notes.ReadMany(x => x.Rating == null), seed.Where(x => x.Rating == null), backend, 1, 7);
            AssertPushed(notes.ReadMany(x => x.Created > _Base.AddDays(10)), seed.Where(x => x.Created > _Base.AddDays(10)), backend, 1, 9);
            AssertPushed(notes.ReadMany(x => x.Created == _Base.AddDays(3)), seed.Where(x => x.Created == _Base.AddDays(3)), backend, 1, 1);
            AssertPushed(notes.ReadMany(x => x.Amount == 1 || x.Amount == 18), seed.Where(x => x.Amount == 1 || x.Amount == 18), backend, 1, 2);

            int[] wanted = new[] { 2, 4, 6 };
            AssertPushed(notes.ReadMany(x => wanted.Contains(x.Amount)), seed.Where(x => wanted.Contains(x.Amount)), backend, 1, 3);
            Assert.Contains(" IN ", backend.LastQueryPlan!.PushedPredicates[0]);

            AssertPushed(notes.ReadMany(x => x.Title == "note-3"), seed.Where(x => x.Title == "note-3"), backend, 1, 1);
            Assert.Contains("ix_title", backend.LastQueryPlan!.LiteDbExplain);
            AssertPushed(notes.ReadMany(x => x.OwnerId == owner.Id), seed.Where(x => x.OwnerId == owner.Id), backend, 1, 10);
            Assert.Contains("ix_owner_id", backend.LastQueryPlan!.LiteDbExplain);

            Assert.Equal(seed[4].Title, notes.ReadById(seed[4].Id)?.Title);
            Assert.Contains("$._id", backend.LastQueryPlan!.PushedPredicates[0]);
            Assert.True(backend.LastQueryPlan.Exact);
            Assert.Contains("\"_id\"", backend.LastQueryPlan.LiteDbExplain);

            List<LiteDbNote> residual = notes.ReadMany(x => x.Amount > 5 && x.Title.EndsWith("7")).ToList();
            Assert.Equal(new[] { 7, 17 }, residual.Select(x => x.Amount).ToArray());
            LiteDbQueryPlan plan = backend.LastQueryPlan!;
            Assert.False(plan.Exact);
            Assert.Single(plan.PushedPredicates);
            Assert.NotNull(plan.ClientSide);
            Assert.Equal(14, plan.DocumentsRead);

            Assert.Single(notes.ReadMany(x => x.Title.ToUpper() == "NOTE-1"));
            Assert.Empty(backend.LastQueryPlan!.PushedPredicates);
            Assert.NotNull(backend.LastQueryPlan.ClientSide);

            List<LiteDbNote> page = notes.Query().Where(x => x.Amount > 2).Skip(2).Take(3).Execute().ToList();
            Assert.Equal(new[] { 5, 6, 7 }, page.Select(x => x.Amount).ToArray());
            Assert.True(backend.LastQueryPlan!.PagingPushedDown);
            Assert.Equal(3, backend.LastQueryPlan.DocumentsRead);

            List<LiteDbNote> ordered = notes.Query().Where(x => x.Amount > 2).OrderByDescending(x => x.Amount).Take(3).Execute().ToList();
            Assert.Equal(new[] { 19, 18, 17 }, ordered.Select(x => x.Amount).ToArray());
            Assert.False(backend.LastQueryPlan!.PagingPushedDown);
            Assert.Contains("ORDER BY", backend.LastQueryPlan.ClientSide);

            Assert.Equal(9L, notes.Count(x => x.Amount > 10));
            Assert.Equal("Count", backend.LastQueryPlan!.Operation);
            Assert.Equal(-1L, backend.LastQueryPlan.DocumentsRead);
            Assert.True(backend.LastQueryPlan.Exact);

            Assert.Equal(2, notes.DeleteMany(x => x.Amount >= 18));
            Assert.Equal("Delete", backend.LastQueryPlan!.Operation);
            Assert.True(backend.LastQueryPlan.Exact);
            lock (observed) Assert.NotEmpty(observed);
        }

        /// <summary>
        /// Push-down never changes results: a battery of predicates (nulls, negations, OR, DateTime kinds, strings) returns
        /// exactly what the in-memory reference backend returns for the same data.
        /// </summary>
        [Fact]
        public async Task ResultsMatchTheInMemoryBackend()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            LiteDbRepository<LiteDbNote> lite = backend.CreateRepository<LiteDbNote>();
            InMemoryRepository<LiteDbNote> reference = InMemoryBackend.Create().CreateRepository<LiteDbNote>();
            DateTime local = new DateTime(_Base.Ticks, DateTimeKind.Local);
            DateTime unspecified = new DateTime(_Base.Ticks, DateTimeKind.Unspecified);
            for (int i = 0; i < 12; i++)
            {
                DateTime created = i % 3 == 0 ? _Base : i % 3 == 1 ? local.AddHours(i) : unspecified.AddMinutes(-i);
                string title = i % 4 == 0 ? "Alpha" : i % 4 == 1 ? "alpha" : i % 4 == 2 ? "ALPHA-" + i : "beta";
                foreach (IRepository<LiteDbNote> repository in new IRepository<LiteDbNote>[] { lite, reference })
                    await repository.CreateAsync(new LiteDbNote { Title = title, Amount = i - 4, Rating = i % 2 == 0 ? null : i, Created = created });
            }

            List<Expression<Func<LiteDbNote, bool>>> predicates = new List<Expression<Func<LiteDbNote, bool>>>
            {
                x => x.Rating > 3,
                x => x.Rating <= 5,
                x => x.Rating != 5,
                x => !(x.Rating > 3),
                x => x.Rating == null || x.Amount < 0,
                x => x.Amount != 0 && x.Rating != null,
                x => x.Created == _Base,
                x => x.Created == local,
                x => x.Created != unspecified,
                x => x.Created < _Base.AddMinutes(1),
                x => x.Created >= local.AddHours(-1),
                x => x.Title == "alpha",
                x => x.Title != "Alpha",
                x => x.Title.CompareTo("alpha") > 0,
                x => string.Compare(x.Title, "B") < 0,
                x => x.Title == "ALPHA-2" || x.Title == "beta",
                x => new[] { "Alpha", "beta", "missing" }.Contains(x.Title),
                x => new int?[] { 1, null }.Contains(x.Rating),
                x => x.Title.StartsWith("AL"),
                x => x.Amount > -2 && x.Amount <= 5 && x.Title != "beta"
            };

            foreach (Expression<Func<LiteDbNote, bool>> predicate in predicates)
            {
                int[] expected = reference.ReadMany(predicate).Select(x => x.Id).OrderBy(x => x).ToArray();
                int[] actual = lite.ReadMany(predicate).Select(x => x.Id).OrderBy(x => x).ToArray();
                Assert.True(expected.SequenceEqual(actual), predicate + ": expected [" + string.Join(",", expected) + "] but LiteDB returned [" + string.Join(",", actual) + "] (" + backend.LastQueryPlan + ")");
                Assert.Equal(reference.Count(predicate), lite.Count(predicate));
            }
        }

        /// <summary>
        /// Every encoded type round-trips exactly (DateTime ticks and kind, DateTimeOffset offset, decimal scale and range,
        /// unsigned and small integers, char, byte arrays, enums, null), string keys are case-sensitive, and predicates over
        /// those values follow C# equality.
        /// </summary>
        [Fact]
        public async Task ValuesRoundTripExactly()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            LiteDbRepository<LiteDbPrecisionItem> items = backend.CreateRepository<LiteDbPrecisionItem>();
            DateTime when = new DateTime(2023, 12, 31, 23, 59, 59, DateTimeKind.Local).AddTicks(9999999);
            DateTimeOffset moment = new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.FromMinutes(330)).AddTicks(7777);
            Guid token = Guid.NewGuid();
            byte[] payload = new byte[] { 0, 1, 2, 254, 255 };
            LiteDbPrecisionItem original = new LiteDbPrecisionItem
            {
                Code = "a",
                When = when,
                WhenNullable = new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(1),
                Moment = moment,
                Amount = 1.10m,
                Ratio = double.NaN,
                Single = float.MaxValue,
                Token = token,
                Duration = TimeSpan.FromTicks(123456789012345),
                Day = DateOnly.MaxValue,
                Time = new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(9999999)),
                Payload = payload,
                Letter = 'é',
                Big = ulong.MaxValue,
                Unsigned = uint.MaxValue,
                Small = short.MinValue,
                Tiny = byte.MaxValue,
                SignedTiny = sbyte.MinValue,
                UnsignedSmall = ushort.MaxValue,
                LongValue = long.MinValue,
                Flag = null,
                Status = Status.Pending,
                StatusNumber = Status.Inactive
            };
            await items.CreateAsync(original);
            items.Create(new LiteDbPrecisionItem { Code = "A", When = DateTime.MaxValue, Moment = DateTimeOffset.MinValue, Amount = decimal.MaxValue, Ratio = -0.0, Flag = true, Day = DateOnly.MinValue });

            LiteDbPrecisionItem? stored = await items.ReadByIdAsync("a");
            Assert.NotNull(stored);
            Assert.Equal(when.Ticks, stored.When.Ticks);
            Assert.Equal(DateTimeKind.Local, stored.When.Kind);
            Assert.Equal(original.WhenNullable!.Value.Ticks, stored.WhenNullable!.Value.Ticks);
            Assert.Equal(DateTimeKind.Utc, stored.WhenNullable.Value.Kind);
            Assert.Equal(moment.Ticks, stored.Moment.Ticks);
            Assert.Equal(moment.Offset, stored.Moment.Offset);
            Assert.Equal("1.10", stored.Amount.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Assert.True(double.IsNaN(stored.Ratio));
            Assert.Equal(float.MaxValue, stored.Single);
            Assert.Equal(token, stored.Token);
            Assert.Equal(original.Duration, stored.Duration);
            Assert.Equal(DateOnly.MaxValue, stored.Day);
            Assert.Equal(original.Time, stored.Time);
            Assert.Equal(payload, stored.Payload);
            Assert.Equal('é', stored.Letter);
            Assert.Equal(ulong.MaxValue, stored.Big);
            Assert.Equal(uint.MaxValue, stored.Unsigned);
            Assert.Equal(short.MinValue, stored.Small);
            Assert.Equal(byte.MaxValue, stored.Tiny);
            Assert.Equal(sbyte.MinValue, stored.SignedTiny);
            Assert.Equal(ushort.MaxValue, stored.UnsignedSmall);
            Assert.Equal(long.MinValue, stored.LongValue);
            Assert.Null(stored.Flag);
            Assert.Equal(Status.Pending, stored.Status);
            Assert.Equal(Status.Inactive, stored.StatusNumber);

            LiteDbPrecisionItem? upper = items.ReadById("A");
            Assert.NotNull(upper);
            Assert.Equal(DateTime.MaxValue, upper.When);
            Assert.Equal(DateTimeOffset.MinValue, upper.Moment);
            Assert.Equal(decimal.MaxValue, upper.Amount);
            Assert.True(upper.Flag);
            Assert.Equal(2L, items.Count());

            DateTime sameTicksUtc = new DateTime(when.Ticks, DateTimeKind.Utc);
            DateTimeOffset sameInstant = moment.ToOffset(TimeSpan.FromHours(-7));
            AssertSingleCode(items, x => x.When == sameTicksUtc, "a", backend);
            AssertSingleCode(items, x => x.Moment == sameInstant, "a", backend);
            AssertSingleCode(items, x => x.Amount == 1.1m, "a", backend);
            AssertSingleCode(items, x => x.Big == ulong.MaxValue, "a", backend);
            AssertSingleCode(items, x => x.Duration > TimeSpan.Zero, "a", backend);
            AssertSingleCode(items, x => x.Day == DateOnly.MaxValue, "a", backend);
            AssertSingleCode(items, x => x.Letter == 'é', "a", backend);
            AssertSingleCode(items, x => x.Status == Status.Pending && x.StatusNumber == Status.Inactive, "a", backend);
            AssertSingleCode(items, x => x.Flag == null, "a", backend);
            AssertSingleCode(items, x => x.Code == "A", "A", backend);
            AssertSingleCode(items, x => x.Small < 0, "a", backend);

            BsonDocument document = backend.GetStoredRows(typeof(LiteDbPrecisionItem)).Single(d => d["_id"].AsString == "a");
            Assert.True(document["amount"].IsDecimal);
            Assert.Equal("Pending", document["status"].AsString);
            Assert.Equal(1L, document["status_number"].AsInt64);
            Assert.False(document.ContainsKey("code"));
        }

        /// <summary>
        /// Concurrent creates from many threads get unique, gap-free auto-increment keys.
        /// </summary>
        [Fact]
        public async Task ConcurrentCreatesGetUniqueKeys()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            LiteDbRepository<LiteDbNote> notes = backend.CreateRepository<LiteDbNote>();
            const int Writers = 8;
            const int PerWriter = 50;
            Task<List<int>>[] writers = Enumerable.Range(0, Writers).Select(w => Task.Run(async () =>
            {
                List<int> ids = new List<int>();
                for (int i = 0; i < PerWriter; i++)
                {
                    LiteDbNote note = i % 2 == 0
                        ? await notes.CreateAsync(new LiteDbNote { Title = "w" + w + "-" + i, Amount = w })
                        : notes.Create(new LiteDbNote { Title = "w" + w + "-" + i, Amount = w });
                    ids.Add(note.Id);
                }

                return ids;
            })).ToArray();

            List<int> all = (await Task.WhenAll(writers)).SelectMany(x => x).ToList();
            Assert.Equal(Writers * PerWriter, all.Distinct().Count());
            Assert.Equal(Enumerable.Range(1, Writers * PerWriter), all.OrderBy(x => x));
            Assert.Equal((long)(Writers * PerWriter), notes.Count());
        }

        /// <summary>
        /// Concurrent optimistic-concurrency updates never lose an update: every successful update is reflected.
        /// </summary>
        [Fact]
        public async Task OptimisticConcurrencyLosesNoUpdates()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            LiteDbRepository<LiteDbVersionedCounter> counters = backend.CreateRepository<LiteDbVersionedCounter>();
            LiteDbVersionedCounter counter = counters.Create(new LiteDbVersionedCounter { Counter = 0 });
            const int Workers = 6;
            const int Increments = 10;
            int conflicts = 0;
            await Task.WhenAll(Enumerable.Range(0, Workers).Select(_ => Task.Run(async () =>
            {
                for (int i = 0; i < Increments; i++)
                {
                    while (true)
                    {
                        LiteDbVersionedCounter current = (await counters.ReadByIdAsync(counter.Id))!;
                        current.Counter++;
                        try
                        {
                            await counters.UpdateAsync(current);
                            break;
                        }
                        catch (OptimisticConcurrencyException)
                        {
                            Interlocked.Increment(ref conflicts);
                        }
                    }
                }
            })));

            LiteDbVersionedCounter final = counters.ReadById(counter.Id)!;
            Assert.Equal(Workers * Increments, final.Counter);
            Assert.Equal(1 + Workers * Increments, final.Version);
        }

        /// <summary>
        /// Settings validate their values and build a connection string with the ordinal collation.
        /// </summary>
        [Fact]
        public void SettingsAreValidated()
        {
            LiteDbRepositorySettings settings = new LiteDbRepositorySettings();
            Assert.True(settings.IsInMemory);
            Assert.Equal(LiteDbRepositorySettings.InMemoryFilename, settings.Filename);
            Assert.Equal(LiteDbConnectionType.Direct, settings.ConnectionType);
            Assert.Equal(TimeSpan.FromMinutes(1), settings.Timeout);
            Assert.Throws<ArgumentNullException>(() => settings.Filename = null!);
            Assert.Throws<ArgumentException>(() => settings.Filename = " ");
            Assert.Throws<ArgumentException>(() => LiteDbRepositorySettings.ForFile(string.Empty));
            Assert.Throws<ArgumentException>(() => settings.Password = string.Empty);
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.Timeout = TimeSpan.FromMilliseconds(10));
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.Timeout = TimeSpan.FromHours(2));
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.InitialSizeBytes = -1);
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.ConnectionType = (LiteDbConnectionType)7);
            Assert.Throws<ArgumentNullException>(() => LiteDbRepositorySettings.ForDatabase(null!));
            Assert.True(LiteDbRepositorySettings.ForInMemory().IsInMemory);
            using (LiteDbBackend defaults = LiteDbBackend.Create())
            {
                Assert.True(defaults.OwnsDatabase);
                Assert.True(defaults.HasOrdinalCollation);
            }

            using (LiteDatabase existing = new LiteDatabase(new MemoryStream()))
            {
                LiteDbRepositorySettings wrapping = LiteDbRepositorySettings.ForDatabase(existing);
                Assert.False(wrapping.IsInMemory);
                wrapping.Validate();
                wrapping.Password = "secret";
                Assert.Throws<ArgumentException>(() => wrapping.Validate());
                Assert.Throws<ArgumentException>(() => LiteDbBackend.Create(wrapping));
                wrapping.Password = null;
                wrapping.Filename = "other.db";
                Assert.Throws<ArgumentException>(() => wrapping.Validate());
            }

            LiteDbRepositorySettings file = LiteDbRepositorySettings.ForFile("data.db");
            file.Password = "secret";
            file.ConnectionType = LiteDbConnectionType.Shared;
            file.ReadOnly = true;
            file.InitialSizeBytes = 8192;
            ConnectionString connection = file.ToConnectionString();
            Assert.Equal("data.db", connection.Filename);
            Assert.Equal("secret", connection.Password);
            Assert.Equal(LiteDB.ConnectionType.Shared, connection.Connection);
            Assert.True(connection.ReadOnly);
            Assert.Equal(8192L, connection.InitialSize);
            Assert.Equal(System.Globalization.CompareOptions.Ordinal, connection.Collation.SortOptions);
            Assert.False(file.IsInMemory);

            using LiteDbBackend backend = LiteDbBackend.Create(new LiteDbRepositorySettings { Timeout = TimeSpan.FromSeconds(5) });
            Assert.Equal(TimeSpan.FromSeconds(5), backend.Database.Timeout);
            Assert.True(backend.HasOrdinalCollation);
        }

        /// <summary>
        /// A database with LiteDB's default (culture, case-insensitive) collation still returns C#-ordinal results: only
        /// string equality is pushed down, as a superset re-checked client-side.
        /// </summary>
        [Fact]
        public void NonOrdinalCollationStaysCorrect()
        {
            using LiteDatabase database = new LiteDatabase(new MemoryStream());
            LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForDatabase(database));
            Assert.False(backend.HasOrdinalCollation);
            LiteDbRepository<LiteDbNote> notes = backend.CreateRepository<LiteDbNote>();
            notes.Create(new LiteDbNote { Title = "Alpha", Amount = 1 });
            notes.Create(new LiteDbNote { Title = "alpha", Amount = 2 });
            notes.Create(new LiteDbNote { Title = "ALPHA", Amount = 3 });

            Assert.Equal(new[] { 2 }, notes.ReadMany(x => x.Title == "alpha").Select(x => x.Amount).ToArray());
            Assert.False(backend.LastQueryPlan!.Exact);
            Assert.Single(backend.LastQueryPlan.PushedPredicates);
            Assert.Equal(1L, notes.Count(x => x.Title == "alpha"));
            Assert.Equal(new[] { 1, 3 }, notes.ReadMany(x => x.Title != "alpha").Select(x => x.Amount).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 1, 3 }, notes.ReadMany(x => x.Title.CompareTo("Z") < 0).Select(x => x.Amount).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 2 }, notes.ReadMany(x => x.Amount > 1 && x.Title == "alpha").Select(x => x.Amount).ToArray());
        }

        /// <summary>
        /// A backend opened from settings owns and disposes its database; a backend over a database passed in, and a
        /// repository, never dispose what they were given; operations on a disposed backend throw.
        /// </summary>
        [Fact]
        public void DisposalSemantics()
        {
            LiteDbBackend owned = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            LiteDatabase ownedDatabase = owned.Database;
            LiteDbRepository<LiteDbNote> first = owned.CreateRepository<LiteDbNote>();
            first.Create(new LiteDbNote { Title = "kept" });
            first.Dispose();
            LiteDbRepository<LiteDbNote> second = owned.CreateRepository<LiteDbNote>();
            Assert.Equal(1L, second.Count());
            owned.Dispose();
            owned.Dispose();
            Assert.Throws<ObjectDisposedException>(() => second.Count());
            Assert.Throws<ObjectDisposedException>(() => owned.CreateRepository<LiteDbNote>());
            Assert.ThrowsAny<Exception>(() => ownedDatabase.GetCollection("ldb_notes").Count());

            using LiteDatabase shared = new LiteDatabase(LiteDbRepositorySettings.ForInMemory().ToConnectionString());
            LiteDbBackend borrowing = LiteDbBackend.Create(LiteDbRepositorySettings.ForDatabase(shared));
            Assert.False(borrowing.OwnsDatabase);
            borrowing.CreateRepository<LiteDbNote>().Create(new LiteDbNote { Title = "survives" });
            borrowing.Dispose();
            Assert.Equal(1, shared.GetCollection("ldb_notes").Count());

            LiteDbRepository<LiteDbNote> direct = new LiteDbRepository<LiteDbNote>(LiteDbBackend.Create(LiteDbRepositorySettings.ForDatabase(shared)));
            direct.Dispose();
            Assert.Equal(1L, LiteDbBackend.Create(LiteDbRepositorySettings.ForDatabase(shared)).CreateRepository<LiteDbNote>().Count());
        }

        /// <summary>
        /// The members every non-SQL backend shares: async factory with default settings, typed async transactions,
        /// Clear/ClearAsync of everything or one type (resetting sequences), stored-row diagnostics, async index creation
        /// and async disposal.
        /// </summary>
        [Fact]
        public async Task BackendConventionMembers()
        {
            LiteDbBackend backend = await LiteDbBackend.CreateAsync();
            Assert.True(backend.OwnsDatabase);
            LiteDbRepository<LiteDbOwner> owners = backend.CreateRepository<LiteDbOwner>();
            LiteDbRepository<LiteDbNote> notes = new LiteDbRepository<LiteDbNote>(backend);
            Assert.Same(backend, owners.Backend);
            Assert.Same(backend, ((RepositoryBase<LiteDbNote>)notes).Backend);
            await backend.EnsureIndexesAsync(typeof(LiteDbNote));

            await using (LiteDbTransaction transaction = await backend.BeginTransactionAsync())
            {
                Assert.True(backend.Owns(transaction));
                Assert.Same(backend, transaction.Backend);
                await owners.CreateAsync(new LiteDbOwner { Name = "rolled back" }, transaction);
            }

            Assert.Equal(0L, owners.Count());
            LiteDbOwner first = owners.Create(new LiteDbOwner { Name = "first" });
            owners.Create(new LiteDbOwner { Name = "second" });
            notes.Create(new LiteDbNote { Title = "note" });
            Assert.Equal(2, (await backend.GetStoredRowsAsync(typeof(LiteDbOwner))).Count);
            Assert.Equal(2, backend.Clear(typeof(LiteDbOwner)));
            Assert.Equal(0, await backend.ClearAsync(typeof(LiteDbOwner)));
            Assert.Equal(1L, notes.Count());
            Assert.Equal(1, owners.Create(new LiteDbOwner { Name = "after clear" }).Id);

            await backend.ClearAsync();
            Assert.Equal(0L, notes.Count());
            Assert.Equal(0L, owners.Count());
            notes.Create(new LiteDbNote { Title = "again" });
            backend.Clear();
            Assert.Empty(backend.GetStoredRows(typeof(LiteDbNote)));
            backend.QueryPlanned += (sender, plan) => throw new InvalidOperationException("handler failure");
            Assert.Equal(0L, notes.Count());
            Assert.Equal("Count", backend.LastQueryPlan!.Operation);
            Assert.Equal(typeof(LiteDbNote), backend.LastQueryPlan.EntityType);

            using CancellationTokenSource canceled = new CancellationTokenSource();
            canceled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => backend.ClearAsync(canceled.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => LiteDbBackend.CreateAsync(null, canceled.Token));

            await backend.DisposeAsync();
            await backend.DisposeAsync();
            Assert.Throws<ObjectDisposedException>(() => owners.Count());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => backend.BeginTransactionAsync());
            Assert.Throws<ObjectDisposedException>(() => backend.Clear());
        }

        /// <summary>
        /// A transaction stays usable across awaits and threads: its writes are visible inside it only, outside readers are
        /// not blocked, and rollback, dispose and commit behave; set-based writes participate.
        /// </summary>
        [Fact]
        public async Task TransactionsSpanAwaitsAndThreads()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            LiteDbRepository<LiteDbNote> notes = backend.CreateRepository<LiteDbNote>();
            LiteDbRepository<LiteDbOwner> owners = backend.CreateRepository<LiteDbOwner>();

            await using (ITransaction transaction = await notes.BeginTransactionAsync())
            {
                Assert.IsType<LiteDbTransaction>(transaction);
                await notes.CreateAsync(new LiteDbNote { Title = "t1", Amount = 1 }, transaction);
                await Task.Yield();
                await Task.Delay(5).ConfigureAwait(false);
                await notes.CreateAsync(new LiteDbNote { Title = "t2", Amount = 2 }, transaction);
                await Task.Run(async () => await notes.CreateAsync(new LiteDbNote { Title = "t3", Amount = 3 }, transaction));
                Assert.Equal(3L, await notes.CountAsync(null, transaction));
                Assert.Equal(3L, notes.Count(null, transaction));
                Assert.Equal(0L, await notes.CountAsync());
                owners.Create(new LiteDbOwner { Name = "outside, other collection" });
                await transaction.RollbackAsync();
                Assert.True(transaction.IsCompleted);
            }

            Assert.Equal(0L, notes.Count());
            Assert.Equal(1L, owners.Count());

            using (ITransaction abandoned = notes.BeginTransaction())
            {
                notes.Create(new LiteDbNote { Title = "abandoned" }, abandoned);
            }

            Assert.Equal(0L, notes.Count());
            notes.Create(new LiteDbNote { Title = "base", Amount = 10 });

            ITransaction committed = await backend.BeginTransactionAsync(CancellationToken.None);
            await Task.Run(async () =>
            {
                await notes.CreateAsync(new LiteDbNote { Title = "c1", Amount = 1 }, committed);
                await notes.CreateManyAsync(new[] { new LiteDbNote { Title = "c2", Amount = 2 }, new LiteDbNote { Title = "c3", Amount = 3 } }, committed);
            });
            await Task.Delay(1).ConfigureAwait(false);
            Assert.Equal(3, await notes.UpdateFieldAsync(x => x.Amount < 5, x => x.Rating, 9, committed));
            Assert.Equal(1, await notes.BatchUpdateAsync(x => x.Title == "c3", x => new LiteDbNote { Title = x.Title + "!" }, committed));
            Assert.Equal(1, await notes.DeleteManyAsync(x => x.Title == "c2", committed));
            LiteDbNote baseNote = (await notes.ReadFirstAsync(x => x.Title == "base", committed))!;
            baseNote.Amount = 11;
            await notes.UpdateAsync(baseNote, committed);
            await committed.CommitAsync();
            await committed.DisposeAsync();

            Assert.Equal(new[] { "base", "c1", "c3!" }, notes.ReadAll().Select(x => x.Title).ToArray());
            Assert.Equal(11, notes.ReadSingle(x => x.Title == "base").Amount);
            Assert.Equal(2L, notes.Count(x => x.Rating == 9));
            await Assert.ThrowsAsync<InvalidOperationException>(() => committed.CommitAsync());
            Assert.Throws<InvalidOperationException>(() => notes.Create(new LiteDbNote { Title = "late" }, committed));

            using (AmbientTransactionScope scope = AmbientTransactionScope.Create(notes))
            {
                await notes.CreateAsync(new LiteDbNote { Title = "ambient" });
                Assert.Equal(1L, await notes.CountAsync(x => x.Title == "ambient"));
            }

            Assert.Equal(0L, notes.Count(x => x.Title == "ambient"));
        }

        /// <summary>
        /// When an operation inside a transaction fails inside LiteDB (which rolls the whole transaction back), the
        /// transaction rejects further work and commit, and nothing it wrote is kept; outside a transaction a failed write
        /// leaves no lock or partial data behind.
        /// </summary>
        [Fact]
        public async Task FailedOperationsRollBack()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            LiteDbRepository<LiteDbNote> notes = backend.CreateRepository<LiteDbNote>();
            string tooLongForIndex = new string('x', 2000);

            ITransaction transaction = await notes.BeginTransactionAsync();
            await notes.CreateAsync(new LiteDbNote { Title = "ok" }, transaction);
            await Assert.ThrowsAnyAsync<LiteException>(() => notes.CreateAsync(new LiteDbNote { Title = tooLongForIndex }, transaction));
            await Assert.ThrowsAsync<InvalidOperationException>(() => notes.CreateAsync(new LiteDbNote { Title = "after" }, transaction));
            await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync());
            await transaction.DisposeAsync();
            Assert.Equal(0L, notes.Count());

            Assert.ThrowsAny<LiteException>(() => notes.Create(new LiteDbNote { Title = tooLongForIndex }));
            notes.Create(new LiteDbNote { Title = "fine" });
            Assert.Equal(new[] { "fine" }, notes.ReadAll().Select(x => x.Title).ToArray());

            LiteDbRepository<LiteDbPrecisionItem> items = backend.CreateRepository<LiteDbPrecisionItem>();
            items.Create(new LiteDbPrecisionItem { Code = "k" });
            Assert.Throws<InvalidOperationException>(() => items.Create(new LiteDbPrecisionItem { Code = "k" }));
            using (ITransaction duplicate = items.BeginTransaction())
            {
                items.Create(new LiteDbPrecisionItem { Code = "m" }, duplicate);
                Assert.Throws<InvalidOperationException>(() => items.Create(new LiteDbPrecisionItem { Code = "k" }, duplicate));
                items.Create(new LiteDbPrecisionItem { Code = "n" }, duplicate);
                duplicate.Commit();
            }

            Assert.Equal(new[] { "k", "m", "n" }, items.ReadAll().Select(x => x.Code).ToArray());
        }

        /// <summary>
        /// Entities LiteDB cannot store are rejected when the repository is created.
        /// </summary>
        [Fact]
        public void UnsupportedMappingsAreRejected()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            Assert.Throws<NotSupportedException>(() => backend.CreateRepository<LiteDbInvalidName>());
            Assert.Throws<NotSupportedException>(() => backend.CreateRepository<LiteDbGuidIdentity>());
        }

        /// <summary>
        /// Synchronous members, including transactions, do not deadlock under a synchronization context that never runs
        /// posted callbacks.
        /// </summary>
        [Fact]
        public void SynchronousMembersDoNotDeadlock()
        {
            using LiteDbBackend backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
            LiteDbRepository<LiteDbNote> notes = backend.CreateRepository<LiteDbNote>();
            SynchronizationContext? previous = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(new NonPumpingSynchronizationContext());
            try
            {
                notes.Create(new LiteDbNote { Title = "sync", Amount = 3 });
                using (ITransaction transaction = notes.BeginTransaction())
                {
                    notes.Create(new LiteDbNote { Title = "in transaction", Amount = 4 }, transaction);
                    Assert.Equal(2L, notes.Count(null, transaction));
                    Assert.Equal(2, notes.ReadMany(null, transaction).Count());
                    Assert.Equal(1, notes.UpdateField(x => x.Amount == 3, x => x.Title, "updated", transaction));
                    transaction.Commit();
                }

                Assert.Equal(new[] { "updated", "in transaction" }, notes.ReadAll().Select(x => x.Title).ToArray());
                Assert.Equal(7m, notes.Sum(x => x.Amount));
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previous);
            }
        }

        #endregion

        #region Private-Methods

        private static void AssertPushed(IEnumerable<LiteDbNote> actual, IEnumerable<LiteDbNote> expected, LiteDbBackend backend, int predicates, int count)
        {
            int[] actualIds = actual.Select(x => x.Id).ToArray();
            int[] expectedIds = expected.Select(x => x.Id).OrderBy(x => x).ToArray();
            LiteDbQueryPlan plan = backend.LastQueryPlan!;
            Assert.True(expectedIds.SequenceEqual(actualIds), "expected [" + string.Join(",", expectedIds) + "] but got [" + string.Join(",", actualIds) + "] (" + plan + ")");
            Assert.Equal(count, actualIds.Length);
            Assert.Equal("Query", plan.Operation);
            Assert.True(plan.Exact, "the filter should be pushed down exactly: " + plan);
            Assert.Null(plan.ClientSide);
            Assert.Equal(predicates, plan.PushedPredicates.Count);
            Assert.Equal(count, plan.DocumentsRead);
        }

        private static void AssertSingleCode(LiteDbRepository<LiteDbPrecisionItem> items, Expression<Func<LiteDbPrecisionItem, bool>> predicate, string code, LiteDbBackend backend)
        {
            string[] codes = items.ReadMany(predicate).Select(x => x.Code).ToArray();
            Assert.True(codes.Length == 1 && codes[0] == code, predicate + " should match only '" + code + "' but matched [" + string.Join(",", codes) + "] (" + backend.LastQueryPlan + ")");
        }

        private static string TempFile()
        {
            return Path.Combine(Path.GetTempPath(), "durable-litedb-" + Guid.NewGuid().ToString("N") + ".db");
        }

        private static void DeleteFile(string path)
        {
            foreach (string file in new[] { path, Path.ChangeExtension(path, null) + "-log.db" })
            {
                try
                {
                    if (File.Exists(file)) File.Delete(file);
                }
                catch (IOException)
                {
                }
            }
        }

        #endregion
    }
}
