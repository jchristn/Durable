namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json.Nodes;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.CosmosDb;
    using Durable.InMemory;
    using Microsoft.Azure.Cosmos;
    using Xunit;

    /// <summary>
    /// Cosmos DB backend tests beyond the conformance kit: persistence across backends and clients, the stored document
    /// shape, push-down of exact filters, ordering, paging and aggregates (checked through query plans) and parity with the
    /// in-memory backend where push-down does not apply, lossless value round-trips with precision-aware push-down, unique
    /// auto-increment keys and no lost updates under concurrent writers (ETag-conditioned writes), partition keys, key
    /// changes, settings validation, unsupported mappings and transactions, and client ownership and disposal.
    /// </summary>
    public class CosmosDbBackendTestSuite
    {
        #region Private-Members

        private static readonly DateTime _Base = new DateTime(2024, 3, 4, 5, 6, 7, DateTimeKind.Utc).AddTicks(1234567);
        private readonly CosmosDbTestTarget _Target;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the suite over an initialized test target.
        /// </summary>
        /// <param name="target">Test target. Must not be null.</param>
        public CosmosDbBackendTestSuite(CosmosDbTestTarget target)
        {
            _Target = target ?? throw new ArgumentNullException(nameof(target));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Data written through one backend is read back exactly through another backend with its own client, and the stored
        /// documents have the documented shape: a string id, columns as properties, a numeric key column named id stored as
        /// durable_id, no shadow for exact values.
        /// </summary>
        [Fact]
        public async Task PersistsAcrossBackendsAndDocumentShape()
        {
            CosmosDbBackend shared = _Target.Backend;
            await shared.ClearAsync(typeof(CosmosDbNote));
            CosmosDbRepository<CosmosDbNote> notes = shared.CreateRepository<CosmosDbNote>();
            CosmosDbNote first = await notes.CreateAsync(new CosmosDbNote { Title = "persisted", Amount = 7, Price = 12.5m, Big = 42, Created = _Base });
            Assert.Equal(1, first.Id);

            await using (CosmosDbBackend other = await CosmosDbBackend.CreateAsync(_Target.CreateSettings()))
            {
                Assert.True(other.OwnsClient);
                CosmosDbRepository<CosmosDbNote> again = other.CreateRepository<CosmosDbNote>();
                CosmosDbNote? stored = await again.ReadByIdAsync(first.Id);
                Assert.NotNull(stored);
                Assert.Equal("persisted", stored!.Title);
                Assert.Equal(7, stored.Amount);
                Assert.Equal(12.5m, stored.Price);
                Assert.Equal(_Base.Ticks, stored.Created.Ticks);
                Assert.Equal(DateTimeKind.Utc, stored.Created.Kind);
                CosmosDbNote second = await again.CreateAsync(new CosmosDbNote { Title = "second" });
                Assert.Equal(2, second.Id);
            }

            IReadOnlyList<JsonObject> documents = await shared.GetStoredRowsAsync(typeof(CosmosDbNote));
            Assert.Equal(2, documents.Count);
            JsonObject document = documents[0];
            Assert.Equal("1", document["id"]!.GetValue<string>());
            Assert.Equal(1, document["durable_id"]!.GetValue<int>());
            Assert.Equal("persisted", document["title"]!.GetValue<string>());
            Assert.Equal("2024-03-04T05:06:07.1234567Z", document["created"]!.GetValue<string>());
            Assert.Null(document["rating"]);
            Assert.True(document.ContainsKey("rating"), "null columns are written explicitly");
            Assert.False(document.ContainsKey("_durable"), "exact values need no shadow");
            Assert.Equal("/id", shared.GetPartitionKeyPath(typeof(CosmosDbNote)));
        }

        /// <summary>
        /// Exact filters, single-key ordering, paging, counts and aggregates run inside Cosmos DB (checked through the query
        /// plan), key lookups are point reads, and the results equal the client-side evaluation.
        /// </summary>
        [Fact]
        public async Task FiltersOrderingPagingAndAggregatesArePushedDown()
        {
            CosmosDbBackend backend = _Target.Backend;
            await backend.ClearAsync(typeof(CosmosDbNote));
            CosmosDbRepository<CosmosDbNote> notes = backend.CreateRepository<CosmosDbNote>();
            for (int i = 1; i <= 10; i++)
                await notes.CreateAsync(new CosmosDbNote { Title = (i % 2 == 0 ? "even-" : "odd-") + i, Amount = i, Rating = i % 3 == 0 ? null : i, Price = i * 1.25m, Big = i * 1000L, Created = _Base.AddDays(i) });

            List<CosmosDbNote> filtered = (await notes.Query().Where(x => x.Amount > 3 && x.Title.StartsWith("even") && x.Rating != null).ExecuteAsync()).ToList();
            CosmosDbQueryPlan plan = backend.LastQueryPlan!;
            Assert.Equal(new[] { 4, 8, 10 }, filtered.Select(x => x.Id).ToArray());
            Assert.True(plan.Exact, plan.ToString());
            Assert.Contains("STARTSWITH", plan.QueryText);
            Assert.Null(plan.ClientSide);

            List<CosmosDbNote> page = (await notes.Query().Where(x => x.Amount >= 2).OrderByDescending(x => x.Created).Skip(1).Take(3).ExecuteAsync()).ToList();
            plan = backend.LastQueryPlan!;
            Assert.Equal(new[] { 9, 8, 7 }, page.Select(x => x.Id).ToArray());
            Assert.True(plan.OrderingPushedDown && plan.PagingPushedDown, plan.ToString());
            Assert.Contains("ORDER BY", plan.QueryText);
            Assert.Contains("OFFSET 1 LIMIT 3", plan.QueryText);
            Assert.Equal(3L, plan.DocumentsRead);

            List<CosmosDbNote> unordered = (await notes.Query().Skip(8).ExecuteAsync()).ToList();
            Assert.Equal(new[] { 9, 10 }, unordered.Select(x => x.Id).ToArray());
            Assert.True(backend.LastQueryPlan!.PagingPushedDown, backend.LastQueryPlan.ToString());

            Assert.Equal(5L, await notes.CountAsync(x => x.Amount > 5));
            plan = backend.LastQueryPlan!;
            Assert.StartsWith("SELECT VALUE COUNT(1)", plan.QueryText);

            Assert.Equal(55m, await notes.SumAsync(x => x.Amount));
            Assert.Contains("SUM(", backend.LastQueryPlan!.QueryText);
            Assert.Equal(10, await notes.MaxAsync(x => x.Amount));
            Assert.Contains("MAX(", backend.LastQueryPlan!.QueryText);
            Assert.Equal(1, await notes.MinAsync(x => x.Amount));
            Assert.Equal(5.5m, await notes.AverageAsync(x => x.Amount));
            Assert.Equal(12.5m, await notes.SumAsync(x => x.Price, x => x.Amount <= 4));

            CosmosDbNote? byId = await notes.ReadByIdAsync(4);
            Assert.Equal("even-4", byId!.Title);
            Assert.True(backend.LastQueryPlan!.PointRead, backend.LastQueryPlan.ToString());

            List<CosmosDbNote> functions = (await notes.Query().Where(x => x.Title.ToUpper() == "ODD-3").ExecuteAsync()).ToList();
            Assert.Single(functions);
            Assert.False(backend.LastQueryPlan!.Exact);
            Assert.NotNull(backend.LastQueryPlan.ClientSide);

            List<CosmosDbNote> caseInsensitive = (await backend.CreateRepository<CosmosDbNote>(new RepositoryOptions { StringMatching = StringMatchMode.IgnoreCase })
                .Query().Where(x => x.Title.StartsWith("EVEN")).ExecuteAsync()).ToList();
            Assert.Equal(5, caseInsensitive.Count);
            Assert.Contains("STARTSWITH", backend.LastQueryPlan!.QueryText);
            Assert.False(backend.LastQueryPlan.Exact, "case-insensitive matches are re-checked client-side");

            List<CosmosDbNote> nullRatings = (await notes.Query().Where(x => x.Rating == null).OrderBy(x => x.Id).ExecuteAsync()).ToList();
            Assert.Equal(new[] { 3, 6, 9 }, nullRatings.Select(x => x.Id).ToArray());
            Assert.True(backend.LastQueryPlan!.Exact);

            List<CosmosDbNote> notIn = (await notes.Query().Where(x => !new[] { 1, 2, 3 }.Contains(x.Amount) && !(x.Rating > 5)).ExecuteAsync()).ToList();
            Assert.Equal(new[] { 4, 5, 6, 9 }, notIn.Select(x => x.Id).ToArray());
            Assert.True(backend.LastQueryPlan!.Exact, backend.LastQueryPlan.ToString());
        }

        /// <summary>
        /// Pushed-down and client-side evaluation agree with the in-memory backend for comparisons, null semantics, negation,
        /// IN lists, string matches and DateTime values of every kind.
        /// </summary>
        [Fact]
        public async Task ResultsMatchTheInMemoryBackend()
        {
            CosmosDbBackend backend = _Target.Backend;
            await backend.ClearAsync(typeof(CosmosDbNote));
            CosmosDbRepository<CosmosDbNote> cosmos = backend.CreateRepository<CosmosDbNote>();
            InMemoryRepository<CosmosDbNote> reference = InMemoryBackend.Create().CreateRepository<CosmosDbNote>();
            DateTime local = new DateTime(_Base.Ticks, DateTimeKind.Local);
            DateTime unspecified = new DateTime(_Base.Ticks, DateTimeKind.Unspecified);
            for (int i = 0; i < 12; i++)
            {
                DateTime created = i % 3 == 0 ? _Base : i % 3 == 1 ? local.AddHours(i) : unspecified.AddMinutes(-i);
                string title = i % 4 == 0 ? "Alpha" : i % 4 == 1 ? "alpha" : i % 4 == 2 ? "ALPHA-" + i : "beta";
                foreach (IRepository<CosmosDbNote> repository in new IRepository<CosmosDbNote>[] { cosmos, reference })
                    await repository.CreateAsync(new CosmosDbNote { Title = title, Amount = i - 4, Rating = i % 2 == 0 ? null : i, Price = i * 0.1m, Big = (i - 6) * 3_000_000_000_000_000L, Created = created });
            }

            List<Expression<Func<CosmosDbNote, bool>>> predicates = new List<Expression<Func<CosmosDbNote, bool>>>
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
                x => !new int?[] { 1, null }.Contains(x.Rating),
                x => x.Title.StartsWith("AL"),
                x => x.Title.EndsWith("a"),
                x => x.Title.Contains("lph"),
                x => string.IsNullOrEmpty(x.Title),
                x => x.Amount > -2 && x.Amount <= 5 && x.Title != "beta",
                x => x.Price > 0.3m,
                x => x.Price == 0.7m,
                x => x.Price <= 0.5m || x.Price >= 1.0m,
                x => x.Big > 0,
                x => x.Big >= 9_000_000_000_000_001L,
                x => x.Big < -9_000_000_000_000_000L,
                x => x.Amount * 2 > 3,
                x => x.Title.ToLower() == "alpha"
            };

            foreach (Expression<Func<CosmosDbNote, bool>> predicate in predicates)
            {
                int[] expected = reference.ReadMany(predicate).Select(x => x.Id).OrderBy(x => x).ToArray();
                int[] actual = (await Collect(cosmos.ReadManyAsync(predicate))).Select(x => x.Id).OrderBy(x => x).ToArray();
                Assert.True(expected.SequenceEqual(actual), predicate + ": expected [" + string.Join(",", expected) + "] but Cosmos DB returned [" + string.Join(",", actual) + "] (" + backend.LastQueryPlan + ")");
                Assert.Equal(reference.Count(predicate), await cosmos.CountAsync(predicate));
            }

            Assert.Equal(reference.Query().OrderBy(x => x.Title).ThenBy(x => x.Id).Execute().Select(x => x.Id), (await cosmos.Query().OrderBy(x => x.Title).ThenBy(x => x.Id).ExecuteAsync()).Select(x => x.Id));
            Assert.Equal(reference.Query().OrderByDescending(x => x.Rating).Skip(2).Take(5).Execute().Select(x => x.Rating), (await cosmos.Query().OrderByDescending(x => x.Rating).Skip(2).Take(5).ExecuteAsync()).Select(x => x.Rating));
            Assert.Equal(reference.Query().OrderBy(x => x.Big).Execute().Select(x => x.Id), (await cosmos.Query().OrderBy(x => x.Big).ExecuteAsync()).Select(x => x.Id));
            Assert.Equal(reference.Min(x => x.Created), await cosmos.MinAsync(x => x.Created));
            Assert.Equal(reference.Max(x => x.Title), await cosmos.MaxAsync(x => x.Title));
        }

        /// <summary>
        /// Every encoded type round-trips exactly (integers beyond 2^53, decimals beyond double precision, NaN and
        /// infinities, DateTime ticks and kind, DateTimeOffset offset, TimeSpan extremes, unsigned and small integers, char,
        /// byte arrays, null), approximate values carry a shadow, and filters over them follow C#.
        /// </summary>
        [Fact]
        public async Task ValuesRoundTripExactly()
        {
            CosmosDbBackend backend = _Target.Backend;
            await backend.ClearAsync(typeof(CosmosDbPrecisionItem));
            CosmosDbRepository<CosmosDbPrecisionItem> items = backend.CreateRepository<CosmosDbPrecisionItem>();
            CosmosDbPrecisionItem extreme = new CosmosDbPrecisionItem
            {
                Code = "extreme",
                When = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Local).AddTicks(7),
                WhenNullable = DateTime.MaxValue,
                Moment = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromMinutes(330)).AddTicks(3),
                Amount = 79228162514264337593543950335m,
                Ratio = double.NaN,
                Single = float.PositiveInfinity,
                Token = Guid.NewGuid(),
                Duration = TimeSpan.MaxValue,
                Day = DateOnly.MaxValue,
                Time = TimeOnly.MaxValue,
                Payload = new byte[] { 0, 1, 2, 250, 255 },
                Letter = 'é',
                Big = ulong.MaxValue,
                LongValue = long.MinValue,
                Unsigned = uint.MaxValue,
                Small = short.MinValue,
                Tiny = byte.MaxValue,
                Text = "😀 emoji",
                Flag = true
            };
            CosmosDbPrecisionItem plain = new CosmosDbPrecisionItem
            {
                Code = "plain",
                When = new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Unspecified),
                Moment = DateTimeOffset.MinValue,
                Amount = 0.1m,
                Ratio = double.Epsilon,
                Single = 0.1f,
                Duration = TimeSpan.FromTicks(-1),
                Day = DateOnly.MinValue,
                Payload = Array.Empty<byte>(),
                LongValue = 9007199254740993L,
                Text = null
            };
            await items.CreateAsync(extreme);
            await items.CreateAsync(plain);

            CosmosDbPrecisionItem stored = (await items.ReadByIdAsync("extreme"))!;
            Assert.Equal(extreme.When.Ticks, stored.When.Ticks);
            Assert.Equal(DateTimeKind.Local, stored.When.Kind);
            Assert.Equal(DateTime.MaxValue, stored.WhenNullable);
            Assert.Equal(extreme.Moment, stored.Moment);
            Assert.Equal(extreme.Moment.Offset, stored.Moment.Offset);
            Assert.Equal(extreme.Amount, stored.Amount);
            Assert.True(double.IsNaN(stored.Ratio));
            Assert.Equal(float.PositiveInfinity, stored.Single);
            Assert.Equal(extreme.Token, stored.Token);
            Assert.Equal(TimeSpan.MaxValue, stored.Duration);
            Assert.Equal(DateOnly.MaxValue, stored.Day);
            Assert.Equal(TimeOnly.MaxValue, stored.Time);
            Assert.Equal(extreme.Payload, stored.Payload);
            Assert.Equal('é', stored.Letter);
            Assert.Equal(ulong.MaxValue, stored.Big);
            Assert.Equal(long.MinValue, stored.LongValue);
            Assert.Equal(uint.MaxValue, stored.Unsigned);
            Assert.Equal(short.MinValue, stored.Small);
            Assert.Equal(byte.MaxValue, stored.Tiny);
            Assert.Equal(extreme.Text, stored.Text);
            Assert.True(stored.Flag);

            CosmosDbPrecisionItem storedPlain = (await items.ReadByIdAsync("plain"))!;
            Assert.Equal(DateTimeKind.Unspecified, storedPlain.When.Kind);
            Assert.Equal(DateTimeOffset.MinValue, storedPlain.Moment);
            Assert.Equal(0.1m, storedPlain.Amount);
            Assert.Equal(double.Epsilon, storedPlain.Ratio);
            Assert.Equal(0.1f, storedPlain.Single);
            Assert.Equal(TimeSpan.FromTicks(-1), storedPlain.Duration);
            Assert.Empty(storedPlain.Payload!);
            Assert.Equal(9007199254740993L, storedPlain.LongValue);
            Assert.Null(storedPlain.Text);

            IReadOnlyList<JsonObject> documents = await backend.GetStoredRowsAsync(typeof(CosmosDbPrecisionItem));
            JsonObject shadows = (JsonObject)documents.Single(d => d["id"]!.GetValue<string>() == "extreme")["_durable"]!;
            Assert.Equal("79228162514264337593543950335", shadows["amount"]!.GetValue<string>());
            Assert.Equal("+05:30", shadows["moment"]!.GetValue<string>());
            Assert.Equal(ulong.MaxValue.ToString(), shadows["big"]!.GetValue<string>());
            Assert.Equal(long.MinValue.ToString(), shadows["long_value"]!.GetValue<string>());
            JsonObject plainShadows = (JsonObject)documents.Single(d => d["id"]!.GetValue<string>() == "plain")["_durable"]!;
            Assert.False(plainShadows.ContainsKey("amount"), "0.1 round-trips through a double");
            Assert.True(plainShadows.ContainsKey("long_value"), "2^53 + 1 does not fit a double");

            Assert.Equal(new[] { "extreme" }, (await Collect(items.ReadManyAsync(x => x.Amount > 79228162514264337593543950334m))).Select(x => x.Code));
            Assert.Equal(new[] { "plain" }, (await Collect(items.ReadManyAsync(x => x.LongValue == 9007199254740993L))).Select(x => x.Code));
            Assert.Empty(await Collect(items.ReadManyAsync(x => x.LongValue == 9007199254740992L)));
            Assert.Equal(new[] { "extreme" }, (await Collect(items.ReadManyAsync(x => x.Single > 1e30f))).Select(x => x.Code));
            Assert.Equal(new[] { "extreme" }, (await Collect(items.ReadManyAsync(x => x.Ratio < 0))).Select(x => x.Code));
            Assert.Equal(new[] { "extreme" }, (await Collect(items.ReadManyAsync(x => x.Moment == extreme.Moment.ToOffset(TimeSpan.Zero)))).Select(x => x.Code));
            Assert.Equal(new[] { "extreme" }, (await Collect(items.ReadManyAsync(x => x.Payload == extreme.Payload))).Select(x => x.Code));
            Assert.Equal(new[] { "plain" }, (await Collect(items.ReadManyAsync(x => x.Duration < TimeSpan.Zero))).Select(x => x.Code));
            Assert.Equal(new[] { "extreme" }, (await Collect(items.ReadManyAsync(x => x.Big > 18446744073709551614UL))).Select(x => x.Code));
        }

        /// <summary>
        /// Comparisons on decimal columns are exact (with paging in Cosmos DB) while the column holds only decimals a double
        /// round-trips, and become re-checked supersets once it holds one that does not; orderings that Cosmos DB could get
        /// wrong (code point order of characters above U+D800, shadowed values) are corrected client-side, also when paged.
        /// </summary>
        [Fact]
        public async Task PrecisionAwarePushDown()
        {
            CosmosDbBackend backend = _Target.Backend;
            await backend.ClearAsync(typeof(CosmosDbNote));
            CosmosDbRepository<CosmosDbNote> notes = backend.CreateRepository<CosmosDbNote>();
            await notes.CreateAsync(new CosmosDbNote { Title = "Ａ", Price = 0.1m });
            await notes.CreateAsync(new CosmosDbNote { Title = "😀", Price = 0.2m });
            await notes.CreateAsync(new CosmosDbNote { Title = "b", Price = 0.3m });

            List<CosmosDbNote> page = (await notes.Query().Where(x => x.Price > 0.1m).OrderBy(x => x.Id).Take(5).ExecuteAsync()).ToList();
            Assert.Equal(new[] { 2, 3 }, page.Select(x => x.Id).ToArray());
            Assert.True(backend.LastQueryPlan!.Exact && backend.LastQueryPlan.PagingPushedDown, backend.LastQueryPlan.ToString());

            await notes.CreateAsync(new CosmosDbNote { Title = "c", Price = 0.1000000000000000000000000001m });
            List<CosmosDbNote> after = (await notes.Query().Where(x => x.Price > 0.1m).OrderBy(x => x.Id).Take(5).ExecuteAsync()).ToList();
            Assert.Equal(new[] { 2, 3, 4 }, after.Select(x => x.Id).ToArray());
            Assert.False(backend.LastQueryPlan!.Exact, backend.LastQueryPlan.ToString());
            Assert.Equal(new[] { 1 }, (await Collect(notes.ReadManyAsync(x => x.Price == 0.1m))).Select(x => x.Id).ToArray());
            Assert.Equal(0.1000000000000000000000000001m, (await notes.ReadByIdAsync(4))!.Price);

            // C# (UTF-16 ordinal) orders the emoji's surrogates before U+FF21; Cosmos DB (code points) orders them after.
            List<string> titles = (await notes.Query().OrderBy(x => x.Title).ExecuteAsync()).Select(x => x.Title).ToList();
            Assert.Equal(new[] { "b", "c", "😀", "Ａ" }, titles);
            List<string> paged = (await notes.Query().OrderBy(x => x.Title).Skip(2).Take(1).ExecuteAsync()).Select(x => x.Title).ToList();
            Assert.Equal(new[] { "😀" }, paged);
            Assert.False(backend.LastQueryPlan!.PagingPushedDown, "the page holds a value Cosmos DB may order differently");
            Assert.Equal(new[] { "c" }, (await notes.Query().OrderBy(x => x.Price).Skip(1).Take(1).ExecuteAsync()).Select(x => x.Title));
            Assert.Equal(0.3m, await notes.MaxAsync(x => x.Price));
        }

        /// <summary>
        /// Concurrent creates through several backends (separate clients, as separate processes would use) get unique,
        /// gap-free auto-increment keys from the ETag-conditioned counter document, and Clear restarts the counter.
        /// </summary>
        [Fact]
        public async Task ConcurrentCreatesGetUniqueKeys()
        {
            await _Target.Backend.ClearAsync(typeof(CosmosDbOwner));
            await using CosmosDbBackend second = await CosmosDbBackend.CreateAsync(_Target.CreateSettings());
            CosmosDbRepository<CosmosDbOwner> a = _Target.Backend.CreateRepository<CosmosDbOwner>();
            CosmosDbRepository<CosmosDbOwner> b = second.CreateRepository<CosmosDbOwner>();
            const int Writers = 4;
            const int PerWriter = 5;
            int[][] ids = await Task.WhenAll(Enumerable.Range(0, Writers).Select(w => Task.Run(async () =>
            {
                List<int> created = new List<int>();
                for (int i = 0; i < PerWriter; i++) created.Add((await (w % 2 == 0 ? a : b).CreateAsync(new CosmosDbOwner { Name = "w" + w + "-" + i })).Id);
                return created.ToArray();
            })));

            Assert.Equal(Enumerable.Range(1, Writers * PerWriter), ids.SelectMany(x => x).OrderBy(x => x));
            Assert.Equal((long)(Writers * PerWriter), await a.CountAsync());
            Assert.Equal(Writers * PerWriter, await _Target.Backend.ClearAsync(typeof(CosmosDbOwner)));
            Assert.Equal(1, (await a.CreateAsync(new CosmosDbOwner { Name = "restart" })).Id);
            CosmosDbOwner explicitKey = await a.UpsertAsync(new CosmosDbOwner { Id = 50, Name = "explicit" });
            Assert.Equal(50, explicitKey.Id);
            Assert.Equal(51, (await a.CreateAsync(new CosmosDbOwner { Name = "after explicit" })).Id);
        }

        /// <summary>
        /// Concurrent writers never lose updates: version-checked updates conflict and retry, and set-based updates that
        /// race on one document are serialized by ETag-conditioned replaces.
        /// </summary>
        [Fact]
        public async Task ConcurrentWritersLoseNoUpdates()
        {
            CosmosDbBackend backend = _Target.Backend;
            await backend.ClearAsync(typeof(CosmosDbCounter));
            await backend.ClearAsync(typeof(CosmosDbNote));
            CosmosDbRepository<CosmosDbCounter> counters = backend.CreateRepository<CosmosDbCounter>();
            CosmosDbCounter counter = await counters.CreateAsync(new CosmosDbCounter { Counter = 0 });
            const int Workers = 4;
            const int Increments = 5;
            await Task.WhenAll(Enumerable.Range(0, Workers).Select(_ => Task.Run(async () =>
            {
                for (int i = 0; i < Increments; i++)
                {
                    while (true)
                    {
                        CosmosDbCounter current = (await counters.ReadByIdAsync(counter.Id))!;
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

            CosmosDbCounter final = (await counters.ReadByIdAsync(counter.Id))!;
            Assert.Equal(Workers * Increments, final.Counter);
            Assert.Equal(1 + Workers * Increments, final.Version);

            CosmosDbCounter stale = (await counters.ReadByIdAsync(counter.Id))!;
            await counters.UpdateAsync((await counters.ReadByIdAsync(counter.Id))!);
            stale.Counter = -1;
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(() => counters.UpdateAsync(stale));

            CosmosDbRepository<CosmosDbNote> notes = backend.CreateRepository<CosmosDbNote>();
            CosmosDbNote note = await notes.CreateAsync(new CosmosDbNote { Title = "shared", Amount = 0 });
            await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => Task.Run(() => notes.BatchUpdateAsync(x => x.Id == note.Id, x => new CosmosDbNote { Amount = x.Amount + 1 }))));
            Assert.Equal(12, (await notes.ReadByIdAsync(note.Id))!.Amount);
        }

        /// <summary>
        /// Entities partitioned by a column: the container uses that path, equality on it scopes queries to one partition,
        /// key lookups still work (cross-partition by id, or point reads when the partition column is part of the key), key
        /// uniqueness holds across partitions, changing the partition value moves the document, and a container created with
        /// another partition key path is rejected.
        /// </summary>
        [Fact]
        public async Task PartitionKeys()
        {
            CosmosDbRepositorySettings settings = _Target.CreateSettings()
                .WithPartitionKey<CosmosDbOrder>(nameof(CosmosDbOrder.TenantId))
                .WithPartitionKey<CosmosDbTenantLine>(nameof(CosmosDbTenantLine.TenantId));
            await using CosmosDbBackend backend = await CosmosDbBackend.CreateAsync(settings);
            Assert.Equal("/tenant_id", backend.GetPartitionKeyPath(typeof(CosmosDbOrder)));
            await backend.ClearAsync(typeof(CosmosDbOrder));
            await backend.ClearAsync(typeof(CosmosDbTenantLine));

            CosmosDbRepository<CosmosDbOrder> orders = backend.CreateRepository<CosmosDbOrder>();
            for (int i = 1; i <= 6; i++) await orders.CreateAsync(new CosmosDbOrder { TenantId = i % 2 == 0 ? "t-even" : "t-odd", Total = i * 10 });

            List<CosmosDbOrder> even = (await orders.Query().Where(x => x.TenantId == "t-even" && x.Total > 20).OrderBy(x => x.Total).ExecuteAsync()).ToList();
            Assert.Equal(new[] { 40, 60 }, even.Select(x => x.Total).ToArray());
            Assert.NotNull(backend.LastQueryPlan!.PartitionKey);
            Assert.Contains("t-even", backend.LastQueryPlan.PartitionKey);
            Assert.Equal(40m, await orders.SumAsync(x => x.Total, x => x.TenantId == "t-odd" && x.Total < 50));

            CosmosDbOrder? third = await orders.ReadByIdAsync(3);
            Assert.Equal("t-odd", third!.TenantId);
            Assert.False(backend.LastQueryPlan!.PointRead, "the partition key is not part of the primary key");
            await orders.UpsertAsync(new CosmosDbOrder { Id = 100, TenantId = "t-x", Total = 1 });
            await orders.UpsertAsync(new CosmosDbOrder { Id = 100, TenantId = "t-y", Total = 2 });
            Assert.Equal(1L, await orders.CountAsync(x => x.Id == 100));
            Assert.Equal("t-y", (await orders.ReadByIdAsync(100))!.TenantId);
            await orders.DeleteByIdAsync(100);

            third.TenantId = "t-moved";
            await orders.UpdateAsync(third);
            CosmosDbOrder? moved = await orders.ReadByIdAsync(3);
            Assert.Equal("t-moved", moved!.TenantId);
            Assert.Equal(6L, await orders.CountAsync());
            Assert.Equal(1L, await orders.CountAsync(x => x.TenantId == "t-moved"));

            CosmosDbRepository<CosmosDbTenantLine> lines = backend.CreateRepository<CosmosDbTenantLine>();
            await lines.CreateAsync(new CosmosDbTenantLine { TenantId = "a/b", LineNo = 1, Text = "first" });
            await lines.CreateAsync(new CosmosDbTenantLine { TenantId = "a/b", LineNo = 2, Text = "second" });
            await lines.CreateAsync(new CosmosDbTenantLine { TenantId = "c", LineNo = 1, Text = "other" });
            CosmosDbTenantLine? line = await lines.ReadByIdAsync(new object[] { "a/b", 2 });
            Assert.Equal("second", line!.Text);
            Assert.True(backend.LastQueryPlan!.PointRead, backend.LastQueryPlan.ToString());
            Assert.Equal("a~2Fb|2", (await backend.GetStoredRowsAsync(typeof(CosmosDbTenantLine))).Select(d => d["id"]!.GetValue<string>()).Single(id => id.EndsWith("|2")));
            await Assert.ThrowsAsync<InvalidOperationException>(() => lines.CreateAsync(new CosmosDbTenantLine { TenantId = "c", LineNo = 1, Text = "duplicate" }));

            await using CosmosDbBackend mismatched = await CosmosDbBackend.CreateAsync(_Target.CreateSettings());
            await Assert.ThrowsAsync<InvalidOperationException>(() => mismatched.CreateRepository<CosmosDbOrder>().CountAsync());
        }

        /// <summary>
        /// Changing a primary key moves the document to its new id, string keys named id are the document id itself, and ids
        /// Cosmos DB cannot hold are rejected.
        /// </summary>
        [Fact]
        public async Task KeysAndIds()
        {
            CosmosDbBackend backend = _Target.Backend;
            await backend.ClearAsync(typeof(CosmosDbTextItem));
            CosmosDbRepository<CosmosDbTextItem> items = backend.CreateRepository<CosmosDbTextItem>();
            await items.CreateAsync(new CosmosDbTextItem { Id = "Key", Text = "upper" });
            await items.CreateAsync(new CosmosDbTextItem { Id = "key", Text = "lower" });
            Assert.Equal("lower", (await items.ReadByIdAsync("key"))!.Text);
            Assert.Equal(new[] { "Key", "key" }, (await backend.GetStoredRowsAsync(typeof(CosmosDbTextItem))).Select(d => d["id"]!.GetValue<string>()).ToArray());
            await Assert.ThrowsAsync<InvalidOperationException>(() => items.CreateAsync(new CosmosDbTextItem { Id = "a/b" }));
            await Assert.ThrowsAsync<InvalidOperationException>(() => items.CreateAsync(new CosmosDbTextItem { Id = "key" }));

            Assert.Equal(1, await items.UpdateFieldAsync(x => x.Id == "Key", x => x.Id, "renamed"));
            Assert.Null(await items.ReadByIdAsync("Key"));
            Assert.Equal("upper", (await items.ReadByIdAsync("renamed"))!.Text);
            await Assert.ThrowsAsync<InvalidOperationException>(() => items.UpdateFieldAsync(x => x.Id == "renamed", x => x.Id, "key"));
            Assert.Equal(2L, await items.CountAsync());
        }

        /// <summary>
        /// Settings validate their values, never print secrets, and mappings Cosmos DB cannot hold are rejected when a
        /// repository is created; transactions are reported as unsupported.
        /// </summary>
        [Fact]
        public async Task SettingsMappingsAndTransactions()
        {
            Assert.Throws<ArgumentException>(() => new CosmosDbRepositorySettings().Validate());
            Assert.Throws<ArgumentException>(() => new CosmosDbRepositorySettings { Endpoint = "http://localhost:8081/" }.Validate());
            Assert.Throws<ArgumentException>(() => new CosmosDbRepositorySettings { Endpoint = "not a uri" });
            Assert.Throws<ArgumentOutOfRangeException>(() => new CosmosDbRepositorySettings { ContainerThroughput = 450 });
            Assert.Throws<ArgumentOutOfRangeException>(() => new CosmosDbRepositorySettings { DatabaseAutoscaleMaxThroughput = 1500 });
            Assert.Throws<ArgumentException>(() => new CosmosDbRepositorySettings { DatabaseName = "bad/name" });
            Assert.Throws<ArgumentException>(() => new CosmosDbRepositorySettings { ConnectionString = "AccountEndpoint=x;AccountKey=y;", ContainerThroughput = 400, ContainerAutoscaleMaxThroughput = 1000 }.Validate());
            CosmosDbRepositorySettings emulator = CosmosDbRepositorySettings.ForEmulator();
            emulator.Validate();
            Assert.DoesNotContain(CosmosDbRepositorySettings.EmulatorAccountKey, emulator.ToString());
            Assert.False(emulator.IsInMemory);

            CosmosDbBackend backend = _Target.Backend;
            Assert.Throws<NotSupportedException>(() => backend.CreateRepository<CosmosDbInvalidName>());
            CosmosDbRepositorySettings badPartition = _Target.CreateSettings().WithPartitionKey<CosmosDbPrecisionItem>(nameof(CosmosDbPrecisionItem.Amount));
            await using (CosmosDbBackend partitioned = await CosmosDbBackend.CreateAsync(badPartition))
            {
                Assert.Throws<NotSupportedException>(() => partitioned.CreateRepository<CosmosDbPrecisionItem>());
            }

            Assert.Equal(RepositoryCapabilities.All & ~RepositoryCapabilities.Transactions, backend.Capabilities);
            Assert.Throws<NotSupportedException>(() => backend.BeginTransaction());
            Assert.Throws<NotSupportedException>(() => backend.CreateRepository<CosmosDbNote>().BeginTransaction());
            Assert.False(backend.Owns(null));
        }

        /// <summary>
        /// A backend over a client passed in does not own or dispose it; an owned client is disposed with the backend;
        /// operations after disposal throw <see cref="ObjectDisposedException"/>; disposal is idempotent; repositories never
        /// dispose the backend.
        /// </summary>
        [Fact]
        public async Task OwnershipAndDisposal()
        {
            CosmosDbRepositorySettings owned = _Target.CreateSettings();
            using CosmosClient client = new CosmosClient(_Target.Endpoint, owned.AccountKey, new CosmosClientOptions { ConnectionMode = ConnectionMode.Gateway, LimitToEndpoint = true });
            CosmosDbBackend borrowed = await CosmosDbBackend.CreateAsync(CosmosDbRepositorySettings.ForClient(client, _Target.DatabaseName));
            Assert.False(borrowed.OwnsClient);
            Assert.Same(client, borrowed.Client);
            await borrowed.ClearAsync(typeof(CosmosDbOwner));
            using (CosmosDbRepository<CosmosDbOwner> repository = borrowed.CreateRepository<CosmosDbOwner>())
            {
                await repository.CreateAsync(new CosmosDbOwner { Name = "borrowed" });
            }

            Assert.Equal(1L, await borrowed.CreateRepository<CosmosDbOwner>().CountAsync());
            borrowed.Dispose();
            borrowed.Dispose();
            Assert.Throws<ObjectDisposedException>(() => borrowed.CreateRepository<CosmosDbOwner>());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => borrowed.ClearAsync(typeof(CosmosDbOwner)));
            ResponseMessage stillOpen = await client.GetDatabase(_Target.DatabaseName).ReadStreamAsync();
            Assert.True(stillOpen.IsSuccessStatusCode, "a client passed in stays usable after the backend is disposed");

            CosmosDbBackend own = await CosmosDbBackend.CreateAsync(owned);
            Assert.True(own.OwnsClient);
            CosmosDbRepository<CosmosDbOwner> owners = own.CreateRepository<CosmosDbOwner>();
            await own.DisposeAsync();
            await Assert.ThrowsAsync<ObjectDisposedException>(() => owners.CountAsync());
            Assert.Throws<ObjectDisposedException>(() => own.Client.GetDatabase(_Target.DatabaseName));
        }

        /// <summary>
        /// Includes and navigation predicates resolve related documents across containers.
        /// </summary>
        [Fact]
        public async Task IncludesAndNavigations()
        {
            CosmosDbBackend backend = _Target.Backend;
            await backend.ClearAsync(typeof(CosmosDbOwner));
            await backend.ClearAsync(typeof(CosmosDbNote));
            CosmosDbRepository<CosmosDbOwner> owners = backend.CreateRepository<CosmosDbOwner>();
            CosmosDbRepository<CosmosDbNote> notes = backend.CreateRepository<CosmosDbNote>();
            CosmosDbOwner ann = await owners.CreateAsync(new CosmosDbOwner { Name = "Ann" });
            CosmosDbOwner bob = await owners.CreateAsync(new CosmosDbOwner { Name = "Bob" });
            await notes.CreateAsync(new CosmosDbNote { Title = "a1", OwnerId = ann.Id });
            await notes.CreateAsync(new CosmosDbNote { Title = "a2", OwnerId = ann.Id });
            await notes.CreateAsync(new CosmosDbNote { Title = "b1", OwnerId = bob.Id });
            await notes.CreateAsync(new CosmosDbNote { Title = "none" });

            List<CosmosDbNote> withOwner = (await notes.Query().Include(x => x.Owner).OrderBy(x => x.Title).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Ann", "Ann", "Bob", null }, withOwner.Select(x => x.Owner?.Name).ToArray());
            List<CosmosDbNote> annNotes = (await notes.Query().Where(x => x.Owner!.Name == "Ann").ExecuteAsync()).ToList();
            Assert.Equal(new[] { "a1", "a2" }, annNotes.Select(x => x.Title).ToArray());
            List<CosmosDbOwner> withNotes = (await owners.Query().Include(x => x.Notes).OrderBy(x => x.Name).ExecuteAsync()).ToList();
            Assert.Equal(new[] { 2, 1 }, withNotes.Select(x => x.Notes.Count).ToArray());
            Assert.Equal(new[] { "Ann" }, (await owners.Query().Where(x => x.Notes.Count() > 1).ExecuteAsync()).Select(x => x.Name).ToArray());
        }

        #endregion

        #region Private-Methods

        private static async Task<List<T>> Collect<T>(IAsyncEnumerable<T> source)
        {
            List<T> list = new List<T>();
            await foreach (T item in source) list.Add(item);
            return list;
        }

        #endregion
    }
}
