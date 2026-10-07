namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;
    using Durable.MongoDb;
    using Durable.Query;
    using MongoDB.Bson;
    using MongoDB.Driver;
    using Xunit;

    /// <summary>
    /// MongoDB backend tests beyond the conformance kit, against the server of a <see cref="MongoDbTestTarget"/>: data
    /// shared across backends and clients, push-down verified through query plans (filters, case-insensitive regexes,
    /// ordering, paging, counts and aggregates on the server, explain output using indexes), parity with the in-memory
    /// backend, lossless value round-trips (decimal scale, DateTime ticks and kind, DateTimeOffset offsets, unsigned
    /// integers), indexes (unique, null-tolerant, composite), unique auto-increment keys under concurrent writers, optimistic
    /// concurrency and computed set-based updates without lost updates, transactions (commit, rollback, abort after a failed
    /// operation, ambient scopes), ownership and disposal, settings validation, unsupported mappings and synchronous use
    /// under a non-pumping synchronization context.
    /// </summary>
    public class MongoDbBackendTestSuite
    {
        #region Private-Members

        private static readonly DateTime _Base = new DateTime(2024, 3, 4, 5, 6, 7, DateTimeKind.Utc).AddTicks(1234567);
        private readonly MongoDbTestTarget _Target;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the suite over a target.
        /// </summary>
        /// <param name="target">Initialized target. Must not be null.</param>
        public MongoDbBackendTestSuite(MongoDbTestTarget target)
        {
            _Target = target ?? throw new ArgumentNullException(nameof(target));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// A second backend (its own client) over the same database sees the first one's writes, exact values, indexes and
        /// the continuing auto-increment sequence.
        /// </summary>
        [Fact]
        public async Task DataIsSharedAcrossBackends()
        {
            MongoDbBackend backend = await ResetAsync(typeof(MongoDbNote), typeof(MongoDbOwner));
            MongoDbRepository<MongoDbNote> notes = backend.CreateRepository<MongoDbNote>();
            MongoDbNote first = await notes.CreateAsync(new MongoDbNote { Title = "persisted", Amount = 7, Created = _Base });
            notes.Create(new MongoDbNote { Title = "second", Amount = 8, Created = _Base });

            await using (MongoDbBackend other = await MongoDbBackend.CreateAsync(_Target.CreateSettings()))
            {
                Assert.True(other.OwnsClient);
                MongoDbRepository<MongoDbNote> otherNotes = other.CreateRepository<MongoDbNote>();
                Assert.Equal(2L, otherNotes.Count());
                MongoDbNote? stored = otherNotes.ReadById(first.Id);
                Assert.NotNull(stored);
                Assert.Equal("persisted", stored.Title);
                Assert.Equal(7, stored.Amount);
                Assert.Equal(_Base.Ticks, stored.Created.Ticks);
                Assert.Equal(DateTimeKind.Utc, stored.Created.Kind);

                MongoDbNote third = otherNotes.Create(new MongoDbNote { Title = "third" });
                Assert.Equal(first.Id + 2, third.Id);
            }

            List<string> indexes = (await (await backend.Database.GetCollection<BsonDocument>("mdb_notes").Indexes.ListAsync()).ToListAsync())
                .Select(d => d["name"].AsString)
                .ToList();
            Assert.Contains("ix_title", indexes);
            Assert.Contains("ix_owner_id", indexes);
            Assert.Equal(3, notes.Count());
        }

        /// <summary>
        /// Comparisons, null checks, IN lists, OR, ordinal string matches and case-insensitive equality are pushed down
        /// exactly; ordering, paging, counting and aggregates then run on the server; functions stay client-side; the
        /// explain output shows index use.
        /// </summary>
        [Fact]
        public async Task QueriesArePushedDown()
        {
            MongoDbBackend backend = await ResetAsync(typeof(MongoDbNote), typeof(MongoDbOwner));
            List<MongoDbQueryPlan> observed = new List<MongoDbQueryPlan>();
            EventHandler<MongoDbQueryPlan> handler = (sender, plan) =>
            {
                Assert.Same(backend, sender);
                lock (observed) observed.Add(plan);
            };
            backend.QueryPlanned += handler;
            try
            {
                MongoDbRepository<MongoDbOwner> owners = backend.CreateRepository<MongoDbOwner>();
                MongoDbRepository<MongoDbNote> notes = backend.CreateRepository<MongoDbNote>();
                MongoDbOwner owner = owners.Create(new MongoDbOwner { Name = "Owner" });
                List<MongoDbNote> seed = new List<MongoDbNote>();
                for (int i = 0; i < 20; i++)
                {
                    seed.Add(await notes.CreateAsync(new MongoDbNote
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
                AssertPushed(notes.ReadMany(x => !(x.Amount > 2)), seed.Where(x => !(x.Amount > 2)), backend, 1, 3);

                int[] wanted = new[] { 2, 4, 6 };
                AssertPushed(notes.ReadMany(x => wanted.Contains(x.Amount)), seed.Where(x => wanted.Contains(x.Amount)), backend, 1, 3);
                Assert.Contains("$in", backend.LastQueryPlan!.PushedFilters[0]);

                AssertPushed(notes.ReadMany(x => x.Title.StartsWith("note-1")), seed.Where(x => x.Title.StartsWith("note-1", StringComparison.Ordinal)), backend, 1, 11);
                Assert.Contains("$regularExpression", backend.LastQueryPlan!.PushedFilters[0]);
                AssertPushed(notes.ReadMany(x => x.Title.EndsWith("-7")), seed.Where(x => x.Title.EndsWith("-7", StringComparison.Ordinal)), backend, 1, 1);
                AssertPushed(notes.ReadMany(x => x.Title.Contains("e-1")), seed.Where(x => x.Title.Contains("e-1", StringComparison.Ordinal)), backend, 1, 11);
                AssertPushed(notes.ReadMany(x => x.Title.Equals("NOTE-3", StringComparison.OrdinalIgnoreCase)), seed.Where(x => x.Amount == 3), backend, 1, 1);
                AssertPushed(notes.ReadMany(x => x.Title.Contains("OTE-1", StringComparison.OrdinalIgnoreCase)), seed.Where(x => x.Title.Contains("ote-1", StringComparison.Ordinal)), backend, 1, 11);

                AssertPushed(notes.ReadMany(x => x.OwnerId == owner.Id), seed.Where(x => x.OwnerId == owner.Id), backend, 1, 10);
                backend.ExplainQueries = true;
                try
                {
                    Assert.Single(notes.ReadMany(x => x.Title == "note-3"));
                    Assert.Contains("ix_title", backend.LastQueryPlan!.MongoDbExplain);
                    Assert.Single(notes.ReadMany(x => x.Id == seed[4].Id));
                    Assert.Contains("_id", backend.LastQueryPlan!.PushedFilters[0]);
                    Assert.NotNull(backend.LastQueryPlan.MongoDbExplain);
                    Assert.DoesNotContain("COLLSCAN", backend.LastQueryPlan.MongoDbExplain);
                }
                finally
                {
                    backend.ExplainQueries = false;
                }

                List<MongoDbNote> residual = notes.ReadMany(x => x.Amount > 5 && x.Title.Length == 6).ToList();
                Assert.Equal(new[] { 6, 7, 8, 9 }, residual.Select(x => x.Amount).ToArray());
                MongoDbQueryPlan plan = backend.LastQueryPlan!;
                Assert.False(plan.Exact);
                Assert.Single(plan.PushedFilters);
                Assert.NotNull(plan.ClientSide);
                Assert.Equal(14, plan.DocumentsRead);

                Assert.Single(notes.ReadMany(x => x.Title.ToUpper() == "NOTE-1"));
                Assert.Empty(backend.LastQueryPlan!.PushedFilters);
                Assert.NotNull(backend.LastQueryPlan.ClientSide);

                List<MongoDbNote> page = notes.Query().Where(x => x.Amount > 2).Skip(2).Take(3).Execute().ToList();
                Assert.Equal(new[] { 5, 6, 7 }, page.Select(x => x.Amount).ToArray());
                Assert.True(backend.LastQueryPlan!.PagingPushedDown);
                Assert.Equal(3, backend.LastQueryPlan.DocumentsRead);

                List<MongoDbNote> ordered = notes.Query().Where(x => x.Amount > 2).OrderByDescending(x => x.Amount).Take(3).Execute().ToList();
                Assert.Equal(new[] { 19, 18, 17 }, ordered.Select(x => x.Amount).ToArray());
                Assert.True(backend.LastQueryPlan!.PagingPushedDown);
                Assert.Contains("amount", backend.LastQueryPlan.PushedSort);
                Assert.Null(backend.LastQueryPlan.ClientSide);
                Assert.Equal(3, backend.LastQueryPlan.DocumentsRead);

                List<MongoDbNote> byNullable = notes.Query().OrderBy(x => x.Rating).ThenByDescending(x => x.Amount).Take(4).Execute().ToList();
                Assert.Equal(seed.OrderBy(x => x.Rating).ThenByDescending(x => x.Amount).Take(4).Select(x => x.Id), byNullable.Select(x => x.Id));
                Assert.True(backend.LastQueryPlan!.PagingPushedDown);

                List<MongoDbNote> computedOrder = notes.Query().OrderBy(x => x.Amount % 5).ThenBy(x => x.Amount).Take(3).Execute().ToList();
                Assert.Equal(new[] { 0, 5, 10 }, computedOrder.Select(x => x.Amount).ToArray());
                Assert.False(backend.LastQueryPlan!.PagingPushedDown);
                Assert.Contains("ORDER BY", backend.LastQueryPlan.ClientSide);

                Assert.Equal(9L, notes.Count(x => x.Amount > 10));
                Assert.Equal("Count", backend.LastQueryPlan!.Operation);
                Assert.Equal(-1L, backend.LastQueryPlan.DocumentsRead);
                Assert.True(backend.LastQueryPlan.Exact);

                Assert.Equal((decimal)seed.Where(x => x.Amount > 10).Sum(x => x.Amount), notes.Query().Where(x => x.Amount > 10).Sum(x => x.Amount));
                Assert.Equal("Aggregate", backend.LastQueryPlan!.Operation);
                Assert.NotNull(backend.LastQueryPlan.Pipeline);
                Assert.Contains("$group", backend.LastQueryPlan.Pipeline);
                Assert.Equal(seed.Where(x => x.Rating != null).Select(x => (decimal)x.Rating!.Value).Average(), notes.Query().Average(x => x.Rating));
                Assert.NotNull(backend.LastQueryPlan!.Pipeline);
                Assert.Equal(_Base.AddDays(19), notes.Query().Max(x => x.Created));
                Assert.NotNull(backend.LastQueryPlan!.Pipeline);
                Assert.Equal("note-0", notes.Query().Min(x => x.Title));
                Assert.NotNull(backend.LastQueryPlan!.Pipeline);
                Assert.Equal((decimal)seed.Sum(x => x.Amount * 2), notes.Query().Sum(x => x.Amount * 2));
                Assert.Null(backend.LastQueryPlan!.Pipeline);

                Assert.Equal(2, notes.DeleteMany(x => x.Amount >= 18));
                Assert.Equal("Delete", backend.LastQueryPlan!.Operation);
                Assert.True(backend.LastQueryPlan.Exact);
                Assert.Equal(3, notes.UpdateField(x => x.Amount < 3, x => x.Rating, 42));
                Assert.Equal("Update", backend.LastQueryPlan!.Operation);
                Assert.True(backend.LastQueryPlan.Exact);
                Assert.Equal(3L, notes.Count(x => x.Rating == 42));
                lock (observed) Assert.NotEmpty(observed);
            }
            finally
            {
                backend.QueryPlanned -= handler;
            }
        }

        /// <summary>
        /// Push-down never changes results: a battery of predicates (nulls, negations, OR, DateTime kinds, ordinal and
        /// case-insensitive strings including non-ASCII text, string functions) returns exactly what the in-memory reference
        /// backend returns for the same data, in both string matching modes.
        /// </summary>
        [Fact]
        public async Task ResultsMatchTheInMemoryBackend()
        {
            MongoDbBackend backend = await ResetAsync(typeof(MongoDbNote));
            foreach (StringMatchMode mode in new[] { StringMatchMode.Database, StringMatchMode.IgnoreCase })
            {
                await backend.ClearAsync(typeof(MongoDbNote));
                RepositoryOptions options = new RepositoryOptions { StringMatching = mode };
                MongoDbRepository<MongoDbNote> mongo = backend.CreateRepository<MongoDbNote>(options);
                InMemoryRepository<MongoDbNote> reference = InMemoryBackend.Create().CreateRepository<MongoDbNote>(options);
                DateTime local = new DateTime(_Base.Ticks, DateTimeKind.Local);
                DateTime unspecified = new DateTime(_Base.Ticks, DateTimeKind.Unspecified);
                string[] titles = new[] { "Alpha", "alpha", "ALPHA-2", "beta", "Straße", "STRASSE", "Ünïcödé", "ünïCÖDÉ", "émoji \U0001F600", "Kelvin K", "kelvin k", "dotless ı", "50%_[x]", "a.b*c" };
                for (int i = 0; i < titles.Length; i++)
                {
                    DateTime created = i % 3 == 0 ? _Base : i % 3 == 1 ? local.AddHours(i) : unspecified.AddMinutes(-i);
                    foreach (IRepository<MongoDbNote> repository in new IRepository<MongoDbNote>[] { mongo, reference })
                        await repository.CreateAsync(new MongoDbNote { Title = titles[i], Amount = i - 4, Rating = i % 2 == 0 ? null : i, Created = created });
                }

                List<Expression<Func<MongoDbNote, bool>>> predicates = new List<Expression<Func<MongoDbNote, bool>>>
                {
                    x => x.Rating > 3,
                    x => x.Rating <= 5,
                    x => x.Rating != 5,
                    x => !(x.Rating > 3),
                    x => !(x.Rating == null),
                    x => x.Rating == null || x.Amount < 0,
                    x => x.Amount != 0 && x.Rating != null,
                    x => x.Created == _Base,
                    x => x.Created == local,
                    x => x.Created != unspecified,
                    x => x.Created < _Base.AddMinutes(1),
                    x => x.Created >= local.AddHours(-1),
                    x => new[] { _Base, local.AddHours(1) }.Contains(x.Created),
                    x => x.Title == "alpha",
                    x => x.Title != "Alpha",
                    x => x.Title.CompareTo("alpha") > 0,
                    x => string.Compare(x.Title, "B") < 0,
                    x => x.Title == "ALPHA-2" || x.Title == "beta",
                    x => new[] { "Alpha", "beta", "missing", "STRASSE" }.Contains(x.Title),
                    x => !new[] { "Alpha", "beta" }.Contains(x.Title),
                    x => new int?[] { 1, null }.Contains(x.Rating),
                    x => x.Title.StartsWith("AL"),
                    x => x.Title.StartsWith("al"),
                    x => x.Title.Contains("strasse"),
                    x => x.Title.Contains("ß"),
                    x => x.Title.EndsWith("ödé"),
                    x => x.Title.Contains("k"),
                    x => x.Title.Contains("I"),
                    x => x.Title.Contains("\U0001F600"),
                    x => x.Title.Contains("%_["),
                    x => x.Title.Contains(".b*"),
                    x => x.Title.Contains(string.Empty),
                    x => x.Title == "ÜNÏCÖDÉ",
                    x => x.Title.Equals("kelvin K", StringComparison.OrdinalIgnoreCase),
                    x => x.Title.Equals("ALPHA", StringComparison.Ordinal),
                    x => string.IsNullOrEmpty(x.Title),
                    x => x.Title.ToLower() == "alpha",
                    x => x.Amount > -2 && x.Amount <= 5 && x.Title != "beta"
                };

                foreach (Expression<Func<MongoDbNote, bool>> predicate in predicates)
                {
                    int[] expected = reference.ReadMany(predicate).Select(x => x.Amount).OrderBy(x => x).ToArray();
                    int[] actual = mongo.ReadMany(predicate).Select(x => x.Amount).OrderBy(x => x).ToArray();
                    Assert.True(expected.SequenceEqual(actual), mode + " " + predicate + ": expected [" + string.Join(",", expected) + "] but MongoDB returned [" + string.Join(",", actual) + "] (" + backend.LastQueryPlan + ")");
                    Assert.Equal(reference.Count(predicate), mongo.Count(predicate));
                }

                int[] expectedOrder = reference.Query().OrderBy(x => x.Created).ThenBy(x => x.Amount).Execute().Select(x => x.Amount).ToArray();
                int[] actualOrder = mongo.Query().OrderBy(x => x.Created).ThenBy(x => x.Amount).Execute().Select(x => x.Amount).ToArray();
                Assert.Equal(expectedOrder, actualOrder);
            }
        }

        /// <summary>
        /// Every encoded type round-trips exactly (DateTime ticks and kind, DateTimeOffset offset, decimal scale and range,
        /// unsigned and small integers, char, byte arrays, enums, null), string keys are case-sensitive, predicates over
        /// those values follow C# equality, and the stored documents use the documented BSON types.
        /// </summary>
        [Fact]
        public async Task ValuesRoundTripExactly()
        {
            MongoDbBackend backend = await ResetAsync(typeof(MongoDbPrecisionItem));
            MongoDbRepository<MongoDbPrecisionItem> items = backend.CreateRepository<MongoDbPrecisionItem>();
            DateTime when = new DateTime(2023, 12, 31, 23, 59, 59, DateTimeKind.Local).AddTicks(9999999);
            DateTimeOffset moment = new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.FromMinutes(330)).AddTicks(7777);
            Guid token = Guid.NewGuid();
            byte[] payload = new byte[] { 0, 1, 2, 254, 255 };
            MongoDbPrecisionItem original = new MongoDbPrecisionItem
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
            items.Create(new MongoDbPrecisionItem { Code = "A", When = DateTime.MaxValue, Moment = DateTimeOffset.MinValue, Amount = decimal.MaxValue, Ratio = -0.0, Flag = true, Day = DateOnly.MinValue });
            items.Create(new MongoDbPrecisionItem { Code = "tiny", Amount = 0.0000000000000000000000000001m, Moment = DateTimeOffset.MaxValue, When = DateTime.MinValue });

            MongoDbPrecisionItem? stored = await items.ReadByIdAsync("a");
            Assert.NotNull(stored);
            Assert.Equal(when.Ticks, stored.When.Ticks);
            Assert.Equal(DateTimeKind.Local, stored.When.Kind);
            Assert.Equal(original.WhenNullable!.Value.Ticks, stored.WhenNullable!.Value.Ticks);
            Assert.Equal(DateTimeKind.Utc, stored.WhenNullable.Value.Kind);
            Assert.Equal(moment.Ticks, stored.Moment.Ticks);
            Assert.Equal(moment.Offset, stored.Moment.Offset);
            Assert.Equal("1.10", stored.Amount.ToString(CultureInfo.InvariantCulture));
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

            MongoDbPrecisionItem? upper = items.ReadById("A");
            Assert.NotNull(upper);
            Assert.Equal(DateTime.MaxValue, upper.When);
            Assert.Equal(DateTimeOffset.MinValue, upper.Moment);
            Assert.Equal(decimal.MaxValue, upper.Amount);
            Assert.True(upper.Flag);
            MongoDbPrecisionItem? tiny = items.ReadById("tiny");
            Assert.NotNull(tiny);
            Assert.Equal(0.0000000000000000000000000001m, tiny.Amount);
            Assert.Equal(DateTimeOffset.MaxValue, tiny.Moment);
            Assert.Equal(DateTime.MinValue, tiny.When);
            Assert.Equal(3L, items.Count());

            DateTime sameTicksUtc = new DateTime(when.Ticks, DateTimeKind.Utc);
            DateTimeOffset sameInstant = moment.ToOffset(TimeSpan.FromHours(-7));
            AssertSingleCode(items, x => x.When == sameTicksUtc, "a", backend);
            AssertSingleCode(items, x => x.Moment == sameInstant, "a", backend);
            AssertSingleCode(items, x => x.Amount == 1.1m, "a", backend);
            AssertSingleCode(items, x => x.Big == ulong.MaxValue, "a", backend);
            AssertSingleCode(items, x => x.Duration > TimeSpan.Zero, "a", backend);
            AssertSingleCode(items, x => x.Day == DateOnly.MaxValue, "a", backend);
            AssertSingleCode(items, x => x.Letter == 'é', "a", backend);
            AssertSingleCode(items, x => x.Token == token, "a", backend);
            AssertSingleCode(items, x => x.Payload == payload, "a", backend);
            AssertSingleCode(items, x => x.Status == Status.Pending && x.StatusNumber == Status.Inactive, "a", backend);
            AssertSingleCode(items, x => x.Flag == null && x.Code != "tiny", "a", backend);
            AssertSingleCode(items, x => x.Code == "A", "A", backend);
            AssertSingleCode(items, x => x.Small < 0, "a", backend);
            AssertSingleCode(items, x => x.Amount > 0 && x.Amount < 0.000001m, "tiny", backend);
            Assert.Equal(decimal.MaxValue, items.Query().Max(x => x.Amount));
            Assert.NotNull(backend.LastQueryPlan!.Pipeline);
            Assert.Equal(1.1000000000000000000000000001m, items.Query().Where(x => x.Code != "A").Sum(x => x.Amount));
            Assert.NotNull(backend.LastQueryPlan!.Pipeline);

            BsonDocument document = backend.GetStoredRows(typeof(MongoDbPrecisionItem)).Single(d => d["_id"].AsString == "a");
            Assert.Equal(BsonType.Decimal128, document["amount"].BsonType);
            Assert.Equal(BsonType.Decimal128, document["when"].BsonType);
            Assert.Equal(BsonType.Decimal128, document["big"].BsonType);
            Assert.Equal(BsonType.Int64, document["unsigned"].BsonType);
            Assert.Equal(BsonType.Int32, document["small"].BsonType);
            Assert.Equal(BsonBinarySubType.UuidStandard, document["token"].AsBsonBinaryData.SubType);
            Assert.Equal("Pending", document["status"].AsString);
            Assert.Equal(1L, document["status_number"].AsInt64);
            Assert.False(document.Contains("code"));
        }

        /// <summary>
        /// Unique indexes are enforced (also for composite indexes) and, as in SQL, allow any number of rows whose indexed
        /// columns are null; violations throw <see cref="InvalidOperationException"/>; Guid keys set by the caller work.
        /// </summary>
        [Fact]
        public async Task UniqueIndexesAreEnforced()
        {
            MongoDbBackend backend = await ResetAsync(typeof(MongoDbAccount));
            MongoDbRepository<MongoDbAccount> accounts = backend.CreateRepository<MongoDbAccount>();
            accounts.Create(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 1, Handle = "joel", Email = "a@example.com" });
            accounts.Create(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 2, Handle = "joel", Email = null });
            accounts.Create(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 1, Handle = null, Email = null });
            accounts.Create(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 1, Handle = null, Email = "b@example.com" });
            Assert.Throws<InvalidOperationException>(() => accounts.Create(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 3, Email = "a@example.com" }));
            Assert.Throws<InvalidOperationException>(() => accounts.Create(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 1, Handle = "joel" }));
            Guid duplicate = accounts.ReadFirst(x => x.Tenant == 2)!.Id;
            Assert.Throws<InvalidOperationException>(() => accounts.Create(new MongoDbAccount { Id = duplicate, Tenant = 9 }));
            Assert.Equal(4L, accounts.Count());

            List<BsonDocument> indexes = await (await backend.Database.GetCollection<BsonDocument>("mdb_accounts").Indexes.ListAsync()).ToListAsync();
            BsonDocument email = indexes.Single(d => d["name"].AsString == "ux_accounts_email");
            Assert.True(email["unique"].ToBoolean());
            Assert.True(email.Contains("partialFilterExpression"));
            Assert.Contains(indexes, d => d["name"].AsString == "ux_accounts_tenant_handle");
        }

        /// <summary>
        /// Concurrent creates from many threads get unique, gap-free auto-increment keys.
        /// </summary>
        [Fact]
        public async Task ConcurrentCreatesGetUniqueKeys()
        {
            MongoDbBackend backend = await ResetAsync(typeof(MongoDbNote));
            MongoDbRepository<MongoDbNote> notes = backend.CreateRepository<MongoDbNote>();
            const int Writers = 8;
            const int PerWriter = 25;
            Task<List<int>>[] writers = Enumerable.Range(0, Writers).Select(w => Task.Run(async () =>
            {
                List<int> ids = new List<int>();
                for (int i = 0; i < PerWriter; i++)
                {
                    MongoDbNote note = i % 2 == 0
                        ? await notes.CreateAsync(new MongoDbNote { Title = "w" + w + "-" + i, Amount = w })
                        : notes.Create(new MongoDbNote { Title = "w" + w + "-" + i, Amount = w });
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
        /// Concurrent optimistic-concurrency updates never lose an update, and concurrent computed set-based updates
        /// (<c>Counter = Counter + 1</c>, applied document by document) never lose one either.
        /// </summary>
        [Fact]
        public async Task ConcurrentUpdatesLoseNothing()
        {
            MongoDbBackend backend = await ResetAsync(typeof(MongoDbVersionedCounter));
            MongoDbRepository<MongoDbVersionedCounter> counters = backend.CreateRepository<MongoDbVersionedCounter>();
            MongoDbVersionedCounter counter = counters.Create(new MongoDbVersionedCounter { Counter = 0 });
            const int Workers = 6;
            const int Increments = 8;
            await Task.WhenAll(Enumerable.Range(0, Workers).Select(_ => Task.Run(async () =>
            {
                for (int i = 0; i < Increments; i++)
                {
                    while (true)
                    {
                        MongoDbVersionedCounter current = (await counters.ReadByIdAsync(counter.Id))!;
                        current.Counter++;
                        try
                        {
                            await counters.UpdateAsync(current);
                            break;
                        }
                        catch (OptimisticConcurrencyException)
                        {
                        }
                    }
                }
            })));

            MongoDbVersionedCounter final = counters.ReadById(counter.Id)!;
            Assert.Equal(Workers * Increments, final.Counter);
            Assert.Equal(1 + Workers * Increments, final.Version);

            MongoDbVersionedCounter second = counters.Create(new MongoDbVersionedCounter { Counter = 100 });
            await Task.WhenAll(Enumerable.Range(0, Workers).Select(_ => Task.Run(async () =>
            {
                for (int i = 0; i < Increments; i++)
                    Assert.Equal(2, await counters.BatchUpdateAsync(x => x.Counter >= 0, x => new MongoDbVersionedCounter { Counter = x.Counter + 1 }));
            })));

            Assert.Equal(Workers * Increments * 2, counters.ReadById(counter.Id)!.Counter);
            MongoDbVersionedCounter secondFinal = counters.ReadById(second.Id)!;
            Assert.Equal(100 + Workers * Increments, secondFinal.Counter);
            Assert.Equal(1 + Workers * Increments, secondFinal.Version);
        }

        /// <summary>
        /// Transactions (on a replica set): writes are visible inside the transaction only, commit and rollback behave,
        /// set-based writes participate, abandoned transactions roll back, the ambient scope works, a failed server operation
        /// aborts the transaction, and a duplicate key detected before writing keeps it usable. On a standalone server
        /// transactions are reported unsupported and BeginTransaction throws <see cref="NotSupportedException"/>.
        /// </summary>
        [Fact]
        public async Task TransactionsCommitAndRollBack()
        {
            MongoDbBackend backend = await ResetAsync(typeof(MongoDbNote), typeof(MongoDbOwner), typeof(MongoDbAccount));
            MongoDbRepository<MongoDbNote> notes = backend.CreateRepository<MongoDbNote>();
            MongoDbRepository<MongoDbOwner> owners = backend.CreateRepository<MongoDbOwner>();
            if (!backend.SupportsTransactions)
            {
                Assert.Equal(RepositoryCapabilities.None, backend.Capabilities & RepositoryCapabilities.Transactions);
                Assert.Throws<NotSupportedException>(() => backend.BeginTransaction());
                Assert.Throws<NotSupportedException>(() => notes.BeginTransaction());
                notes.CreateMany(new[] { new MongoDbNote { Title = "a" }, new MongoDbNote { Title = "b" } });
                Assert.Equal(2L, notes.Count());
                return;
            }

            await using (ITransaction transaction = await notes.BeginTransactionAsync())
            {
                Assert.IsType<MongoDbTransaction>(transaction);
                Assert.True(backend.Owns(transaction));
                await notes.CreateAsync(new MongoDbNote { Title = "t1", Amount = 1 }, transaction);
                await Task.Yield();
                await notes.CreateAsync(new MongoDbNote { Title = "t2", Amount = 2 }, transaction);
                await Task.Run(async () => await notes.CreateAsync(new MongoDbNote { Title = "t3", Amount = 3 }, transaction));
                Assert.Equal(3L, await notes.CountAsync(null, transaction));
                Assert.Equal(3L, notes.Count(null, transaction));
                Assert.Equal(0L, await notes.CountAsync());
                owners.Create(new MongoDbOwner { Name = "outside" });
                await transaction.RollbackAsync();
                Assert.True(transaction.IsCompleted);
            }

            Assert.Equal(0L, notes.Count());
            Assert.Equal(1L, owners.Count());

            using (ITransaction abandoned = notes.BeginTransaction())
            {
                notes.Create(new MongoDbNote { Title = "abandoned" }, abandoned);
            }

            Assert.Equal(0L, notes.Count());
            notes.Create(new MongoDbNote { Title = "base", Amount = 10 });

            MongoDbTransaction committed = await backend.BeginTransactionAsync(CancellationToken.None);
            await notes.CreateAsync(new MongoDbNote { Title = "c1", Amount = 1 }, committed);
            await notes.CreateManyAsync(new[] { new MongoDbNote { Title = "c2", Amount = 2 }, new MongoDbNote { Title = "c3", Amount = 3 } }, committed);
            Assert.Equal(3, await notes.UpdateFieldAsync(x => x.Amount < 5, x => x.Rating, 9, committed));
            Assert.Equal(1, await notes.BatchUpdateAsync(x => x.Title == "c3", x => new MongoDbNote { Title = x.Title + "!" }, committed));
            Assert.Equal(1, await notes.DeleteManyAsync(x => x.Title == "c2", committed));
            MongoDbNote baseNote = (await notes.ReadFirstAsync(x => x.Title == "base", committed))!;
            baseNote.Amount = 11;
            await notes.UpdateAsync(baseNote, committed);
            Assert.Equal(1L, await committed.Session.Client.GetDatabase(backend.DatabaseName).GetCollection<BsonDocument>("mdb_notes").CountDocumentsAsync(committed.Session, new BsonDocument("title", "c1")));
            await committed.CommitAsync();
            await committed.DisposeAsync();

            Assert.Equal(new[] { "base", "c1", "c3!" }, notes.ReadAll().Select(x => x.Title).ToArray());
            Assert.Equal(11, notes.ReadSingle(x => x.Title == "base").Amount);
            Assert.Equal(2L, notes.Count(x => x.Rating == 9));
            await Assert.ThrowsAsync<InvalidOperationException>(() => committed.CommitAsync());
            Assert.Throws<InvalidOperationException>(() => notes.Create(new MongoDbNote { Title = "late" }, committed));

            await using (AmbientTransactionScope scope = AmbientTransactionScope.Create(notes))
            {
                await notes.CreateAsync(new MongoDbNote { Title = "ambient" });
                Assert.Equal(1L, await notes.CountAsync(x => x.Title == "ambient"));
            }

            Assert.Equal(0L, notes.Count(x => x.Title == "ambient"));

            MongoDbRepository<MongoDbAccount> accounts = backend.CreateRepository<MongoDbAccount>();
            Guid existing = Guid.NewGuid();
            accounts.Create(new MongoDbAccount { Id = existing, Tenant = 1, Email = "taken@example.com" });
            using (ITransaction keyCheck = accounts.BeginTransaction())
            {
                accounts.Create(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 2 }, keyCheck);
                Assert.Throws<InvalidOperationException>(() => accounts.Create(new MongoDbAccount { Id = existing, Tenant = 3 }, keyCheck));
                accounts.Create(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 4 }, keyCheck);
                keyCheck.Commit();
            }

            Assert.Equal(3L, accounts.Count());

            ITransaction failing = await accounts.BeginTransactionAsync();
            await accounts.CreateAsync(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 5 }, failing);
            await Assert.ThrowsAsync<InvalidOperationException>(() => accounts.CreateAsync(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 6, Email = "taken@example.com" }, failing));
            await Assert.ThrowsAsync<InvalidOperationException>(() => accounts.CreateAsync(new MongoDbAccount { Id = Guid.NewGuid(), Tenant = 7 }, failing));
            await Assert.ThrowsAsync<InvalidOperationException>(() => failing.CommitAsync());
            await failing.DisposeAsync();
            Assert.Equal(3L, accounts.Count());
        }

        /// <summary>
        /// A backend created from settings owns and disposes its client; a backend over a client passed in, and a
        /// repository, never dispose what they were given; operations on a disposed backend throw
        /// <see cref="ObjectDisposedException"/>.
        /// </summary>
        [Fact]
        public async Task OwnershipAndDisposal()
        {
            await ResetAsync(typeof(MongoDbNote));
            MongoDbBackend owned = MongoDbBackend.Create(_Target.CreateSettings());
            Assert.True(owned.OwnsClient);
            MongoDbRepository<MongoDbNote> first = owned.CreateRepository<MongoDbNote>();
            first.Create(new MongoDbNote { Title = "kept" });
            first.Dispose();
            MongoDbRepository<MongoDbNote> second = owned.CreateRepository<MongoDbNote>();
            Assert.Equal(1L, second.Count());
            owned.Dispose();
            owned.Dispose();
            Assert.Throws<ObjectDisposedException>(() => second.Count());
            Assert.Throws<ObjectDisposedException>(() => owned.CreateRepository<MongoDbNote>());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => owned.BeginTransactionAsync());
            Assert.Throws<ObjectDisposedException>(() => owned.Clear());

            MongoDbRepositorySettings clientSettings = _Target.CreateSettings();
            using (MongoClient client = new MongoClient(clientSettings.ToClientSettings()))
            {
                MongoDbBackend borrowing = await MongoDbBackend.CreateAsync(MongoDbRepositorySettings.ForClient(client, clientSettings.DatabaseName));
                Assert.False(borrowing.OwnsClient);
                Assert.Same(client, borrowing.Client);
                borrowing.CreateRepository<MongoDbNote>().Create(new MongoDbNote { Title = "survives" });
                await borrowing.DisposeAsync();
                Assert.Equal(2L, await client.GetDatabase(clientSettings.DatabaseName).GetCollection<BsonDocument>("mdb_notes").CountDocumentsAsync(new BsonDocument()));

                MongoDbRepository<MongoDbNote> direct = new MongoDbRepository<MongoDbNote>(MongoDbBackend.Create(MongoDbRepositorySettings.ForClient(client, clientSettings.DatabaseName)));
                direct.Dispose();
                Assert.Equal(2L, MongoDbBackend.Create(MongoDbRepositorySettings.ForClient(client, clientSettings.DatabaseName)).CreateRepository<MongoDbNote>().Count());
            }
        }

        /// <summary>
        /// The members every non-SQL backend shares: async factory, typed async transactions, Clear/ClearAsync of everything
        /// or one type (resetting sequences), stored-row diagnostics, async index creation, query plans whose handler
        /// exceptions are swallowed, and cancellation.
        /// </summary>
        [Fact]
        public async Task BackendConventionMembers()
        {
            await ResetAsync(typeof(MongoDbNote), typeof(MongoDbOwner));
            MongoDbBackend backend = await MongoDbBackend.CreateAsync(_Target.CreateSettings("durable_touchstone_conv"));
            try
            {
                Assert.True(backend.OwnsClient);
                Assert.Equal("durable_touchstone_conv", backend.DatabaseName);
                Assert.Equal("durable_sequences", backend.SequenceCollectionName);
                Assert.Equal(_Target.Backend.Capabilities, backend.Capabilities);
                MongoDbRepository<MongoDbOwner> owners = backend.CreateRepository<MongoDbOwner>();
                MongoDbRepository<MongoDbNote> notes = new MongoDbRepository<MongoDbNote>(backend);
                Assert.Same(backend, owners.Backend);
                Assert.Same(backend, ((RepositoryBase<MongoDbNote>)notes).Backend);
                await backend.ClearAsync();
                await backend.EnsureIndexesAsync(typeof(MongoDbNote));

                if (backend.SupportsTransactions)
                {
                    await using (MongoDbTransaction transaction = await backend.BeginTransactionAsync())
                    {
                        Assert.True(backend.Owns(transaction));
                        Assert.False(_Target.Backend.Owns(transaction));
                        Assert.Same(backend, transaction.Backend);
                        await owners.CreateAsync(new MongoDbOwner { Name = "rolled back" }, transaction);
                    }
                }

                Assert.Equal(0L, owners.Count());
                owners.Create(new MongoDbOwner { Name = "first" });
                owners.Create(new MongoDbOwner { Name = "second" });
                notes.Create(new MongoDbNote { Title = "note" });
                Assert.Equal(2, (await backend.GetStoredRowsAsync(typeof(MongoDbOwner))).Count);
                Assert.Equal(2, backend.Clear(typeof(MongoDbOwner)));
                Assert.Equal(0, await backend.ClearAsync(typeof(MongoDbOwner)));
                Assert.Equal(1L, notes.Count());
                Assert.Equal(1, owners.Create(new MongoDbOwner { Name = "after clear" }).Id);

                await backend.ClearAsync();
                Assert.Equal(0L, notes.Count());
                Assert.Equal(0L, owners.Count());
                notes.Create(new MongoDbNote { Title = "again" });
                backend.Clear();
                Assert.Empty(backend.GetStoredRows(typeof(MongoDbNote)));
                backend.QueryPlanned += (sender, plan) => throw new InvalidOperationException("handler failure");
                Assert.Equal(0L, notes.Count());
                Assert.Equal("Count", backend.LastQueryPlan!.Operation);
                Assert.Equal(typeof(MongoDbNote), backend.LastQueryPlan.EntityType);

                using CancellationTokenSource canceled = new CancellationTokenSource();
                canceled.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => backend.ClearAsync(canceled.Token));
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => MongoDbBackend.CreateAsync(_Target.CreateSettings(), canceled.Token));
                await backend.Client.DropDatabaseAsync("durable_touchstone_conv");
            }
            finally
            {
                await backend.DisposeAsync();
                await backend.DisposeAsync();
            }
        }

        /// <summary>
        /// Settings validate their values and build driver settings; a backend over an unreachable server fails fast.
        /// </summary>
        [Fact]
        public async Task SettingsAreValidated()
        {
            MongoDbRepositorySettings settings = new MongoDbRepositorySettings();
            Assert.False(settings.IsInMemory);
            Assert.Equal("durable", settings.DatabaseName);
            Assert.Equal("localhost", settings.Hostname);
            Assert.Equal(27017, settings.Port);
            Assert.Equal(100, settings.MaxPoolSize);
            Assert.Null(settings.TransactionsEnabled);
            Assert.Throws<ArgumentNullException>(() => settings.DatabaseName = null!);
            Assert.Throws<ArgumentException>(() => settings.DatabaseName = "bad.name");
            Assert.Throws<ArgumentException>(() => settings.DatabaseName = string.Empty);
            Assert.Throws<ArgumentException>(() => settings.DatabaseName = new string('d', 64));
            Assert.Throws<ArgumentException>(() => settings.Hostname = " ");
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.Port = 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.Port = 70000);
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.MaxPoolSize = 0);
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.MinPoolSize = -1);
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.ConnectionTimeout = TimeSpan.Zero);
            Assert.Throws<ArgumentOutOfRangeException>(() => settings.ServerSelectionTimeout = TimeSpan.FromHours(1));
            Assert.Throws<ArgumentException>(() => settings.ConnectionString = " ");
            Assert.Throws<ArgumentException>(() => settings.SequenceCollectionName = "system.seq");

            settings.MinPoolSize = 200;
            Assert.Throws<ArgumentException>(() => settings.Validate());
            settings.MinPoolSize = 0;
            settings.Password = "secret";
            Assert.Throws<ArgumentException>(() => settings.Validate());
            settings.Username = "user";
            settings.Validate();

            MongoDbRepositorySettings host = MongoDbRepositorySettings.ForHost("db.example", 27018, "app");
            host.ReplicaSetName = "rs1";
            host.UseTls = true;
            host.Username = "user";
            host.Password = "pw";
            host.AuthenticationDatabase = "auth";
            MongoClientSettings built = host.ToClientSettings();
            Assert.Equal("db.example", built.Server.Host);
            Assert.Equal(27018, built.Server.Port);
            Assert.Equal("rs1", built.ReplicaSetName);
            Assert.True(built.UseTls);
            Assert.Equal("auth", built.Credential.Source);

            MongoDbRepositorySettings fromUrl = MongoDbRepositorySettings.ForConnectionString("mongodb://h1:27017/inventory?replicaSet=rs0");
            Assert.Equal("inventory", fromUrl.DatabaseName);
            Assert.Equal("rs0", fromUrl.ToClientSettings().ReplicaSetName);
            Assert.Equal("durable", MongoDbRepositorySettings.ForConnectionString("mongodb://h1:27017").DatabaseName);
            Assert.Equal("other", MongoDbRepositorySettings.ForConnectionString("mongodb://h1:27017/inventory", "other").DatabaseName);

            using (MongoClient client = new MongoClient("mongodb://localhost:1"))
            {
                MongoDbRepositorySettings both = MongoDbRepositorySettings.ForClient(client, "x");
                both.ConnectionString = "mongodb://localhost";
                Assert.Throws<ArgumentException>(() => MongoDbBackend.Create(both));
            }

            MongoDbRepositorySettings unreachable = MongoDbRepositorySettings.ForHost("127.0.0.1", 1, "x");
            unreachable.ServerSelectionTimeout = TimeSpan.FromMilliseconds(300);
            await Assert.ThrowsAnyAsync<Exception>(() => MongoDbBackend.CreateAsync(unreachable));

            MongoDbRepositorySettings forced = MongoDbRepositorySettings.ForHost("127.0.0.1", 1, "x");
            forced.TransactionsEnabled = false;
            using MongoDbBackend lazy = MongoDbBackend.Create(forced);
            Assert.False(lazy.SupportsTransactions);
            Assert.Equal(RepositoryCapabilities.All & ~RepositoryCapabilities.Transactions, lazy.Capabilities);
        }

        /// <summary>
        /// Entities MongoDB cannot store are rejected when the repository is created.
        /// </summary>
        [Fact]
        public void UnsupportedMappingsAreRejected()
        {
            MongoDbBackend backend = _Target.Backend;
            Assert.Throws<NotSupportedException>(() => backend.CreateRepository<MongoDbInvalidName>());
            Assert.Throws<NotSupportedException>(() => backend.CreateRepository<MongoDbInvalidField>());
            Assert.Throws<NotSupportedException>(() => backend.CreateRepository<LiteDbGuidIdentity>());
        }

        /// <summary>
        /// Synchronous members, including transactions, do not deadlock under a synchronization context that never runs
        /// posted callbacks.
        /// </summary>
        [Fact]
        public void SynchronousMembersDoNotDeadlock()
        {
            MongoDbBackend backend = _Target.Backend;
            backend.Clear(typeof(MongoDbNote));
            MongoDbRepository<MongoDbNote> notes = backend.CreateRepository<MongoDbNote>();
            SynchronizationContext? previous = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(new NonPumpingSynchronizationContext());
            try
            {
                notes.Create(new MongoDbNote { Title = "sync", Amount = 3 });
                if (backend.SupportsTransactions)
                {
                    using (ITransaction transaction = notes.BeginTransaction())
                    {
                        notes.Create(new MongoDbNote { Title = "in transaction", Amount = 4 }, transaction);
                        Assert.Equal(2L, notes.Count(null, transaction));
                        Assert.Equal(2, notes.ReadMany(null, transaction).Count());
                        Assert.Equal(1, notes.UpdateField(x => x.Amount == 3, x => x.Title, "updated", transaction));
                        transaction.Commit();
                    }
                }
                else
                {
                    notes.Create(new MongoDbNote { Title = "in transaction", Amount = 4 });
                    notes.UpdateField(x => x.Amount == 3, x => x.Title, "updated");
                }

                Assert.Equal(new[] { "updated", "in transaction" }, notes.ReadAll().Select(x => x.Title).ToArray());
                Assert.Equal(7m, notes.Sum(x => x.Amount));
                Assert.Single(backend.GetStoredRows(typeof(MongoDbNote)), d => d["title"].AsString == "updated");
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previous);
            }
        }

        #endregion

        #region Private-Methods

        private async Task<MongoDbBackend> ResetAsync(params Type[] types)
        {
            MongoDbBackend backend = _Target.Backend;
            foreach (Type type in types) await backend.ClearAsync(type);
            return backend;
        }

        private static void AssertPushed(IEnumerable<MongoDbNote> actual, IEnumerable<MongoDbNote> expected, MongoDbBackend backend, int filters, int count)
        {
            int[] actualIds = actual.Select(x => x.Id).ToArray();
            int[] expectedIds = expected.Select(x => x.Id).OrderBy(x => x).ToArray();
            MongoDbQueryPlan plan = backend.LastQueryPlan!;
            Assert.True(expectedIds.SequenceEqual(actualIds), "expected [" + string.Join(",", expectedIds) + "] but got [" + string.Join(",", actualIds) + "] (" + plan + ")");
            Assert.Equal(count, actualIds.Length);
            Assert.Equal("Query", plan.Operation);
            Assert.True(plan.Exact, "the filter should be pushed down exactly: " + plan);
            Assert.Null(plan.ClientSide);
            Assert.Equal(filters, plan.PushedFilters.Count);
            Assert.Equal(count, plan.DocumentsRead);
        }

        private static void AssertSingleCode(MongoDbRepository<MongoDbPrecisionItem> items, Expression<Func<MongoDbPrecisionItem, bool>> predicate, string code, MongoDbBackend backend)
        {
            string[] codes = items.ReadMany(predicate).Select(x => x.Code).ToArray();
            Assert.True(codes.Length == 1 && codes[0] == code, predicate + " should match only '" + code + "' but matched [" + string.Join(",", codes) + "] (" + backend.LastQueryPlan + ")");
            Assert.True(backend.LastQueryPlan!.Exact, predicate + " should be pushed down exactly: " + backend.LastQueryPlan);
        }

        #endregion
    }
}
