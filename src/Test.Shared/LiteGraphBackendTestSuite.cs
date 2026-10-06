namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Durable;
    using Durable.Conformance;
    using Durable.LiteGraph;
    using LiteGraph;
    using Xunit;

    /// <summary>
    /// The LiteGraph backend beyond the conformance kit: rows as labelled nodes with column data, deterministic node GUIDs,
    /// foreign keys maintained as edges (created, moved and removed with key changes and deletes, back-filled when the
    /// principal arrives later, junction rows with an edge to each side), traversal of Durable relationships with
    /// LiteGraph's own client, persistence across reopen, exact value round-trips, push-down plans and their parity with
    /// client-side evaluation, interactive transactions applied atomically, concurrent creates, preservation of
    /// LiteGraph-side annotations, edge rebuilds, settings validation and disposal semantics. Runs on temporary SQLite
    /// files (and once on LiteGraph's in-memory mode).
    /// </summary>
    public class LiteGraphBackendTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// Each row is a node labelled with the table name, named table:key, whose data holds the column values by name.
        /// </summary>
        [Fact]
        public async Task RowsAreLabelledNodesWithColumnData()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            LiteGraphBackend backend = store.Backend;
            CfPublisher publisher = await backend.CreateRepository<CfPublisher>().CreateAsync(new CfPublisher { Name = "Acme" });
            CfAuthor author = await backend.CreateRepository<CfAuthor>().CreateAsync(new CfAuthor { Name = "Ada", PublisherId = publisher.Id });

            Node node = await ReadNodeAsync(backend, backend.GetNodeGuid<CfAuthor>(author.Id));
            Assert.Contains("cf_authors", node.Labels);
            Assert.Equal("cf_authors:" + author.Id, node.Name);
            JsonElement data = Assert.IsType<JsonElement>(node.Data);
            Assert.Equal(author.Id, data.GetProperty("id").GetInt32());
            Assert.Equal("Ada", data.GetProperty("name").GetString());
            Assert.Equal(publisher.Id, data.GetProperty("publisher_id").GetInt32());

            int labelled = 0;
            await foreach (Node found in backend.Client.Node.ReadMany(backend.TenantGuid, backend.GraphGuid, null, new List<string> { "cf_authors" }, null, null, EnumerationOrderEnum.CreatedAscending, 0, true, false))
            {
                labelled++;
            }

            Assert.Equal(1, labelled);
        }

        /// <summary>
        /// Node GUIDs are derived from graph, table and key: stable across key types and backends on the same graph,
        /// distinct across tables, keys (ordinal) and graphs, and composite keys are supported.
        /// </summary>
        [Fact]
        public async Task NodeGuidsAreDeterministic()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            LiteGraphBackend backend = store.Backend;
            Assert.Equal(backend.GetNodeGuid<CfAuthor>(5), backend.GetNodeGuid<CfAuthor>(5L));
            Assert.Equal(backend.GetNodeGuid<CfAuthor>(5), backend.GetNodeGuid(typeof(CfAuthor), 5));
            Assert.NotEqual(backend.GetNodeGuid<CfAuthor>(5), backend.GetNodeGuid<CfBook>(5));
            Assert.NotEqual(backend.GetNodeGuid<CfAuthor>(5), backend.GetNodeGuid<CfAuthor>(6));
            Assert.NotEqual(backend.GetNodeGuid<CfUpsertItem>("a"), backend.GetNodeGuid<CfUpsertItem>("A"));
            Assert.Throws<ArgumentException>(() => backend.GetNodeGuid<CfCompositeItem>(5));

            IRepository<CfCompositeItem> composites = backend.CreateRepository<CfCompositeItem>();
            await composites.CreateAsync(new CfCompositeItem { TenantId = 7, Sku = "SKU-1", Name = "Widget", Quantity = 2 });
            Guid compositeGuid = backend.GetNodeGuid<CfCompositeItem>(new object[] { 7, "SKU-1" });
            Node compositeNode = await ReadNodeAsync(backend, compositeGuid);
            Assert.Equal("cf_composite_items:7,SKU-1", compositeNode.Name);
            Assert.Equal("Widget", ((JsonElement)compositeNode.Data).GetProperty("name").GetString());
            Assert.NotEqual(compositeGuid, backend.GetNodeGuid<CfCompositeItem>(new object[] { 7, "sku-1" }));

            await using LiteGraphBackend sameGraph = await LiteGraphBackend.CreateAsync(LiteGraphBackendSettings.ForClient(backend.Client, backend.TenantGuid, backend.GraphGuid));
            Assert.Equal(backend.GetNodeGuid<CfAuthor>(5), sameGraph.GetNodeGuid<CfAuthor>(5));
            Assert.Equal("Widget", (await sameGraph.CreateRepository<CfCompositeItem>().ReadByIdAsync(new object[] { 7, "SKU-1" }))!.Name);

            await using LiteGraphBackend otherGraph = await LiteGraphBackend.CreateAsync(LiteGraphBackendSettings.ForClient(backend.Client, backend.TenantGuid, Guid.NewGuid()));
            Assert.NotEqual(backend.GetNodeGuid<CfAuthor>(5), otherGraph.GetNodeGuid<CfAuthor>(5));
            Assert.Null(await otherGraph.CreateRepository<CfCompositeItem>().ReadByIdAsync(new object[] { 7, "SKU-1" }));
        }

        /// <summary>
        /// The foreign key edge is created with the row, moved when the key changes (Update and set-based updates),
        /// removed when the key becomes null and when either node is deleted, and back-filled when a principal is created
        /// after its dependents.
        /// </summary>
        [Fact]
        public async Task EdgesFollowForeignKeyChangesAndDeletes()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            LiteGraphBackend backend = store.Backend;
            IRepository<CfPublisher> publishers = backend.CreateRepository<CfPublisher>();
            IRepository<CfAuthor> authors = backend.CreateRepository<CfAuthor>();
            CfPublisher first = await publishers.CreateAsync(new CfPublisher { Name = "First" });
            CfPublisher second = await publishers.CreateAsync(new CfPublisher { Name = "Second" });
            CfAuthor author = await authors.CreateAsync(new CfAuthor { Name = "Ada", PublisherId = first.Id });

            LiteGraphRelationship relationship = backend.GetRelationships(typeof(CfAuthor)).Single(r => r.Id == "cf_authors.publisher_id->cf_publishers");
            Assert.Equal("Publisher", relationship.Label);
            Guid authorNode = backend.GetNodeGuid<CfAuthor>(author.Id);
            Guid edgeGuid = backend.GetEdgeGuid(relationship, authorNode);

            Edge edge = await ReadEdgeAsync(backend, edgeGuid);
            Assert.Equal(authorNode, edge.From);
            Assert.Equal(backend.GetNodeGuid<CfPublisher>(first.Id), edge.To);
            Assert.Contains("Publisher", edge.Labels);
            Assert.Equal("Publisher", edge.Name);

            author.PublisherId = second.Id;
            await authors.UpdateAsync(author);
            Assert.Equal(backend.GetNodeGuid<CfPublisher>(second.Id), (await ReadEdgeAsync(backend, edgeGuid)).To);

            author.PublisherId = null;
            await authors.UpdateAsync(author);
            Assert.False(await backend.Client.Edge.ExistsByGuid(backend.TenantGuid, backend.GraphGuid, edgeGuid));

            Assert.Equal(1, await authors.UpdateFieldAsync(x => x.Id == author.Id, x => x.PublisherId, (int?)first.Id));
            Assert.Equal(backend.GetNodeGuid<CfPublisher>(first.Id), (await ReadEdgeAsync(backend, edgeGuid)).To);

            Assert.True(await publishers.DeleteAsync(first));
            Assert.False(await backend.Client.Edge.ExistsByGuid(backend.TenantGuid, backend.GraphGuid, edgeGuid));
            Assert.NotNull(await authors.ReadByIdAsync(author.Id));

            await publishers.UpsertAsync(new CfPublisher { Id = first.Id, Name = "First again" });
            Assert.Equal(backend.GetNodeGuid<CfPublisher>(first.Id), (await ReadEdgeAsync(backend, edgeGuid)).To);

            IRepository<CfBook> books = backend.CreateRepository<CfBook>();
            CfBook orphan = await books.CreateAsync(new CfBook { Title = "Orphan", AuthorId = 9999 });
            LiteGraphRelationship bookAuthor = backend.GetRelationships(typeof(CfBook)).Single(r => r.Id == "cf_books.author_id->cf_authors");
            Guid orphanEdge = backend.GetEdgeGuid(bookAuthor, backend.GetNodeGuid<CfBook>(orphan.Id));
            Assert.False(await backend.Client.Edge.ExistsByGuid(backend.TenantGuid, backend.GraphGuid, orphanEdge));
            await authors.UpsertAsync(new CfAuthor { Id = 9999, Name = "Late author" });
            Assert.Equal(backend.GetNodeGuid<CfAuthor>(9999), (await ReadEdgeAsync(backend, orphanEdge)).To);

            Assert.True(await authors.DeleteByIdAsync(author.Id));
            Assert.False(await backend.Client.Edge.ExistsByGuid(backend.TenantGuid, backend.GraphGuid, edgeGuid));
            Assert.Equal(0, await CountEdgesAsync(backend, backend.GetNodeGuid<CfPublisher>(first.Id), false));
        }

        /// <summary>
        /// A many-to-many junction row is a node with an edge to each side, and both Durable navigations and LiteGraph
        /// traversals see the relationship.
        /// </summary>
        [Fact]
        public async Task JunctionRowsAreNodesWithEdgesToBothSides()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            LiteGraphBackend backend = store.Backend;
            IRepository<CfAuthor> authors = backend.CreateRepository<CfAuthor>();
            CfAuthor ada = await authors.CreateAsync(new CfAuthor { Name = "Ada" });
            CfAuthor grace = await authors.CreateAsync(new CfAuthor { Name = "Grace" });
            CfTag math = await backend.CreateRepository<CfTag>().CreateAsync(new CfTag { Label = "math" });
            CfAuthorTag link = await backend.CreateRepository<CfAuthorTag>().CreateAsync(new CfAuthorTag { AuthorId = ada.Id, TagId = math.Id });

            Guid linkNode = backend.GetNodeGuid<CfAuthorTag>(link.Id);
            List<Guid> targets = new List<Guid>();
            await foreach (Edge edge in backend.Client.Edge.ReadEdgesFromNode(backend.TenantGuid, backend.GraphGuid, linkNode))
            {
                targets.Add(edge.To);
            }

            Assert.Equal(2, targets.Count);
            Assert.Contains(backend.GetNodeGuid<CfAuthor>(ada.Id), targets);
            Assert.Contains(backend.GetNodeGuid<CfTag>(math.Id), targets);

            List<Guid> parentsOfTag = new List<Guid>();
            await foreach (Node parent in backend.Client.Node.ReadParents(backend.TenantGuid, backend.GraphGuid, backend.GetNodeGuid<CfTag>(math.Id)))
            {
                parentsOfTag.Add(parent.GUID);
            }

            Assert.Equal(new[] { linkNode }, parentsOfTag.ToArray());

            List<CfAuthor> tagged = (await authors.Query().Where(a => a.Tags.Any(t => t.Label == "math")).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Ada" }, tagged.Select(a => a.Name).ToArray());
            CfAuthor withTags = (await authors.Query().Where(a => a.Id == ada.Id).Include(a => a.Tags).ExecuteAsync()).Single();
            Assert.Equal(new[] { "math" }, withTags.Tags.Select(t => t.Label).ToArray());
            Assert.Empty((await authors.Query().Where(a => a.Id == grace.Id).Include(a => a.Tags).ExecuteAsync()).Single().Tags);
        }

        /// <summary>
        /// LiteGraph's own traversal APIs follow Durable relationships: routes from a book through its author to the
        /// publisher, children (principals) and parents (dependents).
        /// </summary>
        [Fact]
        public async Task LiteGraphTraversalFollowsDurableRelationships()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            LiteGraphBackend backend = store.Backend;
            CfPublisher publisher = await backend.CreateRepository<CfPublisher>().CreateAsync(new CfPublisher { Name = "Acme" });
            CfAuthor author = await backend.CreateRepository<CfAuthor>().CreateAsync(new CfAuthor { Name = "Ada", PublisherId = publisher.Id });
            CfBook book = await backend.CreateRepository<CfBook>().CreateAsync(new CfBook { Title = "Notes", AuthorId = author.Id, Pages = 10 });

            Guid bookNode = backend.GetNodeGuid<CfBook>(book.Id);
            Guid authorNode = backend.GetNodeGuid<CfAuthor>(author.Id);
            Guid publisherNode = backend.GetNodeGuid<CfPublisher>(publisher.Id);

            List<RouteDetail> routes = new List<RouteDetail>();
            await foreach (RouteDetail route in backend.Client.Node.ReadRoutes(SearchTypeEnum.DepthFirstSearch, backend.TenantGuid, backend.GraphGuid, bookNode, publisherNode))
            {
                routes.Add(route);
            }

            RouteDetail twoHops = Assert.Single(routes);
            Assert.Equal(new[] { bookNode, authorNode }, twoHops.Edges.Select(e => e.From).ToArray());
            Assert.Equal(new[] { authorNode, publisherNode }, twoHops.Edges.Select(e => e.To).ToArray());

            List<Guid> children = new List<Guid>();
            await foreach (Node child in backend.Client.Node.ReadChildren(backend.TenantGuid, backend.GraphGuid, bookNode))
            {
                children.Add(child.GUID);
            }

            Assert.Equal(new[] { authorNode }, children.ToArray());

            List<string> parentNames = new List<string>();
            await foreach (Node parent in backend.Client.Node.ReadParents(backend.TenantGuid, backend.GraphGuid, publisherNode))
            {
                parentNames.Add(parent.Name);
            }

            Assert.Equal(new[] { "cf_authors:" + author.Id }, parentNames.ToArray());
        }

        /// <summary>
        /// Data, edges, the tenant and graph (found again by name) and the key sequence survive closing and reopening the
        /// database file.
        /// </summary>
        [Fact]
        public async Task DataPersistsAcrossReopen()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            Guid tenant = store.Backend.TenantGuid;
            Guid graph = store.Backend.GraphGuid;
            CfPublisher publisher = await store.Backend.CreateRepository<CfPublisher>().CreateAsync(new CfPublisher { Name = "Acme" });
            CfAuthor first = await store.Backend.CreateRepository<CfAuthor>().CreateAsync(new CfAuthor { Name = "Ada", PublisherId = publisher.Id });
            CfAuthor second = await store.Backend.CreateRepository<CfAuthor>().CreateAsync(new CfAuthor { Name = "Grace", PublisherId = publisher.Id });

            await store.ReopenAsync();
            LiteGraphBackend reopened = store.Backend;
            Assert.Equal(tenant, reopened.TenantGuid);
            Assert.Equal(graph, reopened.GraphGuid);
            IRepository<CfAuthor> authors = reopened.CreateRepository<CfAuthor>();
            Assert.Equal(new[] { "Ada", "Grace" }, authors.ReadAll().Select(a => a.Name).ToArray());
            Assert.Equal("Grace", (await authors.ReadByIdAsync(second.Id))!.Name);
            Assert.Equal(2, await CountEdgesAsync(reopened, reopened.GetNodeGuid<CfPublisher>(publisher.Id), false));

            CfAuthor third = await authors.CreateAsync(new CfAuthor { Name = "Hedy" });
            Assert.True(third.Id > second.Id && third.Id > first.Id, "Generated keys continue after the stored keys (got " + third.Id + ").");
        }

        /// <summary>
        /// Every scalar type round-trips exactly (kinds, offsets, ticks, decimal scale, non-finite doubles, extremes,
        /// bytes, enums both ways, JSON) and can be queried.
        /// </summary>
        [Fact]
        public async Task ValuesRoundTripExactly()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            IRepository<LgAllTypes> repository = store.Backend.CreateRepository<LgAllTypes>();
            Guid uid = Guid.NewGuid();
            string text = "It's \"quoted\" -- ' OR 1=1; é中\U0001F600\nline\ttab";
            LgAllTypes full = new LgAllTypes
            {
                Text = text,
                Letter = 'é',
                Tiny = byte.MaxValue,
                Small = short.MinValue,
                Big = long.MinValue,
                UnsignedBig = ulong.MaxValue,
                Amount = 1.2300m,
                Ratio = double.NaN,
                Single = float.Epsilon,
                Flag = true,
                Utc = new DateTime(2024, 2, 29, 23, 59, 58, DateTimeKind.Utc).AddTicks(1234567),
                Local = new DateTime(2023, 7, 1, 12, 0, 0, DateTimeKind.Local).AddTicks(7),
                Unspecified = new DateTime(1999, 12, 31, 1, 2, 3).AddTicks(9999999),
                MaybeTime = null,
                Offset = new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.FromMinutes(330)).AddTicks(42),
                Duration = TimeSpan.FromTicks(-123456789012345),
                Day = new DateOnly(2000, 2, 29),
                Time = new TimeOnly(13, 14, 15).Add(TimeSpan.FromTicks(1234567)),
                Uid = uid,
                Blob = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray(),
                Status = Status.Pending,
                Priority = Status.Inactive,
                Maybe = null,
                Tags = new List<string> { "a", "b'c" }
            };

            LgAllTypes created = await repository.CreateAsync(full);
            LgAllTypes? stored = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(stored);
            Assert.Equal(text, stored!.Text);
            Assert.Equal('é', stored.Letter);
            Assert.Equal(byte.MaxValue, stored.Tiny);
            Assert.Equal(short.MinValue, stored.Small);
            Assert.Equal(long.MinValue, stored.Big);
            Assert.Equal(ulong.MaxValue, stored.UnsignedBig);
            Assert.Equal("1.2300", stored.Amount.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Assert.True(double.IsNaN(stored.Ratio));
            Assert.Equal(float.Epsilon, stored.Single);
            Assert.True(stored.Flag);
            AssertSameDateTime(full.Utc, stored.Utc);
            AssertSameDateTime(full.Local, stored.Local);
            AssertSameDateTime(full.Unspecified, stored.Unspecified);
            Assert.Null(stored.MaybeTime);
            Assert.Equal(full.Offset, stored.Offset);
            Assert.Equal(full.Offset.Offset, stored.Offset.Offset);
            Assert.Equal(full.Duration, stored.Duration);
            Assert.Equal(full.Day, stored.Day);
            Assert.Equal(full.Time, stored.Time);
            Assert.Equal(uid, stored.Uid);
            Assert.Equal(full.Blob, stored.Blob);
            Assert.Equal(Status.Pending, stored.Status);
            Assert.Equal(Status.Inactive, stored.Priority);
            Assert.Null(stored.Maybe);
            Assert.Equal(new[] { "a", "b'c" }, stored.Tags);

            LgAllTypes extremes = await repository.CreateAsync(new LgAllTypes { Ratio = double.NegativeInfinity, Single = float.PositiveInfinity, Big = long.MaxValue, Amount = -0.0000000001m, Text = string.Empty, MaybeTime = DateTime.MaxValue, Maybe = int.MinValue });
            LgAllTypes? second = await repository.ReadByIdAsync(extremes.Id);
            Assert.Equal(double.NegativeInfinity, second!.Ratio);
            Assert.Equal(float.PositiveInfinity, second.Single);
            Assert.Equal(long.MaxValue, second.Big);
            Assert.Equal(-0.0000000001m, second.Amount);
            Assert.Equal(string.Empty, second.Text);
            Assert.Equal(DateTime.MaxValue, second.MaybeTime);
            Assert.Equal(int.MinValue, second.Maybe);
            Assert.Null(second.Tags);

            Assert.Equal(created.Id, (await repository.ReadFirstAsync(x => x.Text == text))!.Id);
            Assert.Equal(created.Id, (await repository.ReadFirstAsync(x => x.Uid == uid))!.Id);
            Assert.Equal(created.Id, (await repository.ReadFirstAsync(x => x.Amount == 1.23m))!.Id);
            Assert.Equal(created.Id, (await repository.ReadFirstAsync(x => x.Big == long.MinValue))!.Id);
            Assert.Equal(created.Id, (await repository.ReadFirstAsync(x => x.Day == new DateOnly(2000, 2, 29)))!.Id);
            Assert.Equal(created.Id, (await repository.ReadFirstAsync(x => x.Status == Status.Pending && x.Priority == Status.Inactive))!.Id);
            Assert.Equal(extremes.Id, (await repository.ReadFirstAsync(x => x.Maybe == int.MinValue))!.Id);
            Assert.Equal(2L, await repository.CountAsync(x => x.Small <= 0));
        }

        /// <summary>
        /// Key equality is a lookup by node GUID; exact string and non-negative integer equality become data filters;
        /// anything not provably identical stays client-side; the plan is raised and logged.
        /// </summary>
        [Fact]
        public async Task PushDownPlansAreReported()
        {
            InMemoryLogger logger = new InMemoryLogger { MinimumLevel = LogLevel.Debug };
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync(s => s.Logger = logger);
            LiteGraphBackend backend = store.Backend;
            List<LiteGraphQueryPlan> plans = new List<LiteGraphQueryPlan>();
            backend.QueryPlanned += (sender, plan) => plans.Add(plan);
            IRepository<CfItem> items = backend.CreateRepository<CfItem>();
            CfItem hammer = await items.CreateAsync(new CfItem { Name = "Hammer", Category = "Tools", Quantity = 5, Status = CfStatus.Active, Priority = CfPriority.High });
            await items.CreateAsync(new CfItem { Name = "Kite", Category = "Toys", Quantity = 1, Status = CfStatus.Draft, Priority = CfPriority.Low });

            LiteGraphQueryPlan byKey = await PlanOfAsync(plans, () => items.ReadByIdAsync(hammer.Id));
            Assert.Equal(LiteGraphReadStrategy.KeyLookup, byKey.Strategy);
            Assert.Equal(new[] { backend.GetNodeGuid<CfItem>(hammer.Id) }, byKey.NodeGuids.ToArray());
            Assert.Equal("cf_items", byKey.Label);

            LiteGraphQueryPlan byKeys = await PlanOfAsync(plans, () => items.Query().Where(x => new[] { hammer.Id, 12345 }.Contains(x.Id)).ExecuteAsync());
            Assert.Equal(LiteGraphReadStrategy.KeyLookup, byKeys.Strategy);
            Assert.Equal(2, byKeys.NodeGuids.Count);

            await AssertPushedAsync(plans, items, x => x.Category == "Tools", "category", "Hammer");
            await AssertPushedAsync(plans, items, x => x.Quantity == 1, "quantity", "Kite");
            await AssertPushedAsync(plans, items, x => x.Status == CfStatus.Active, "status", "Hammer");
            await AssertPushedAsync(plans, items, x => x.Priority == CfPriority.Low, "priority", "Kite");
            await AssertPushedAsync(plans, items, x => x.Category == "Toys" && x.Name.StartsWith("K"), "category", "Kite");
            await AssertPushedAsync(plans, items, x => new[] { "Toys", "Garden" }.Contains(x.Category), "category", "Kite");

            await AssertNotPushedAsync(plans, items, x => x.Quantity == -5);
            await AssertNotPushedAsync(plans, items, x => x.Name.ToUpper() == "KITE", "Kite");
            await AssertNotPushedAsync(plans, items, x => x.Category == "Tools" || x.Category == "Toys", "Hammer", "Kite");
            await AssertNotPushedAsync(plans, items, x => x.Code == null, "Hammer", "Kite");
            await AssertNotPushedAsync(plans, items, x => x.Name == "Ki\tte");
            await AssertNotPushedAsync(plans, items, x => x.Quantity > 2, "Hammer");

            IRepository<CfItem> ignoreCase = backend.CreateRepository<CfItem>(new RepositoryOptions { StringMatching = StringMatchMode.IgnoreCase });
            LiteGraphQueryPlan insensitive = await PlanOfAsync(plans, () => ignoreCase.Query().Where(x => x.Category == "tools").ExecuteAsync());
            Assert.Null(insensitive.DataFilter);
            Assert.Equal(new[] { "Hammer" }, (await ignoreCase.Query().Where(x => x.Category == "tools").ExecuteAsync()).Select(x => x.Name).ToArray());

            Assert.Contains(logger.Snapshot(), entry => entry.Level == LogLevel.Debug && entry.Message.Contains("LiteGraph read") && entry.Message.Contains("cf_items"));
        }

        /// <summary>
        /// Push-down never changes results: a backend with data push-down disabled, reading the same graph, returns the
        /// same rows for predicates around nulls, case, control characters, enums, keys and combinations.
        /// </summary>
        [Fact]
        public async Task PushDownMatchesClientSideEvaluation()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            LiteGraphBackend pushing = store.Backend;
            await using LiteGraphBackend clientSide = await LiteGraphBackend.CreateAsync(new LiteGraphBackendSettings
            {
                Client = pushing.Client,
                TenantGuid = pushing.TenantGuid,
                GraphGuid = pushing.GraphGuid,
                PushDownDataFilters = false
            });

            IRepository<CfItem> items = pushing.CreateRepository<CfItem>();
            string[] names = { "alpha", "Alpha", "ALPHA", "al\tpha", "beta", "", "gamma", "Gamma" };
            for (int i = 0; i < names.Length; i++)
            {
                await items.CreateAsync(new CfItem
                {
                    Name = names[i],
                    Code = i % 3 == 0 ? null : "c" + (i % 2),
                    Category = i % 2 == 0 ? "Even" : "Odd",
                    Quantity = i - 2,
                    Status = (CfStatus)(i % 4),
                    Priority = (CfPriority)(1 + (i % 4)),
                    OwnerId = i % 2 == 0 ? (int?)null : i
                });
            }

            IRepository<CfItem> reference = clientSide.CreateRepository<CfItem>();
            int firstId = items.ReadAll().Min(x => x.Id);
            List<Expression<Func<CfItem, bool>>> predicates = new List<Expression<Func<CfItem, bool>>>
            {
                x => x.Name == "alpha",
                x => x.Name == "ALPHA",
                x => x.Name == "al\tpha",
                x => x.Name == "",
                x => x.Code == "c1",
                x => x.Code != "c1",
                x => x.Code == null,
                x => x.Quantity == 0,
                x => x.Quantity == 3,
                x => x.Quantity == -1,
                x => x.OwnerId == 3,
                x => x.Status == CfStatus.Suspended,
                x => x.Priority == CfPriority.Critical,
                x => x.Category == "Even" && x.Quantity == 2,
                x => x.Category == "Odd" && x.Name == "alpha",
                x => new[] { "alpha", "beta", "missing" }.Contains(x.Name),
                x => new[] { 1, 3, 5 }.Contains(x.Quantity),
                x => x.Id == firstId && x.Name == "alpha",
                x => x.Id == firstId && x.Name == "nope",
                x => x.Id == firstId + 100,
                x => x.Name == "Gamma" || x.Quantity == 0
            };

            foreach (Expression<Func<CfItem, bool>> predicate in predicates)
            {
                string[] expected = (await reference.Query().Where(predicate).ExecuteAsync()).Select(x => x.Id + ":" + x.Name).ToArray();
                string[] actual = (await items.Query().Where(predicate).ExecuteAsync()).Select(x => x.Id + ":" + x.Name).ToArray();
                Assert.True(expected.SequenceEqual(actual), "Predicate " + predicate + " returned [" + string.Join(", ", actual) + "], expected [" + string.Join(", ", expected) + "].");
                Assert.Equal(await reference.CountAsync(predicate), await items.CountAsync(predicate));
            }
        }

        /// <summary>
        /// A transaction's writes are invisible to LiteGraph until commit and then appear together (nodes and edges);
        /// rollback writes nothing; a commit after another writer changed a written row fails and rolls back.
        /// </summary>
        [Fact]
        public async Task TransactionsApplyAtomicallyAtCommit()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            LiteGraphBackend backend = store.Backend;
            IRepository<CfPublisher> publishers = backend.CreateRepository<CfPublisher>();
            IRepository<CfAuthor> authors = backend.CreateRepository<CfAuthor>();

            CfAuthor author;
            await using (ITransaction transaction = await authors.BeginTransactionAsync())
            {
                CfPublisher publisher = await publishers.CreateAsync(new CfPublisher { Name = "Acme" }, transaction);
                author = await authors.CreateAsync(new CfAuthor { Name = "Ada", PublisherId = publisher.Id }, transaction);
                Assert.Equal(3, ((LiteGraphTransaction)transaction).PendingOperationCount);
                Assert.False(await backend.Client.Node.ExistsByGuid(backend.TenantGuid, backend.GetNodeGuid<CfAuthor>(author.Id)));
                Assert.Equal(0L, await authors.CountAsync());
                Assert.Equal(1L, await authors.CountAsync(x => x.Publisher!.Name == "Acme", transaction));
                await transaction.CommitAsync();
            }

            Assert.True(await backend.Client.Node.ExistsByGuid(backend.TenantGuid, backend.GetNodeGuid<CfAuthor>(author.Id)));
            Assert.Equal(1, await CountEdgesAsync(backend, backend.GetNodeGuid<CfAuthor>(author.Id), true));

            using (ITransaction transaction = authors.BeginTransaction())
            {
                authors.Create(new CfAuthor { Name = "Rolled back" }, transaction);
                Assert.True(authors.Delete(author, transaction));
                transaction.Rollback();
            }

            Assert.Equal(new[] { "Ada" }, authors.ReadAll().Select(a => a.Name).ToArray());

            ITransaction conflicting = authors.BeginTransaction();
            CfAuthor inTransaction = authors.ReadById(author.Id, conflicting)!;
            inTransaction.Name = "From transaction";
            authors.Update(inTransaction, conflicting);
            CfAuthor outside = authors.ReadById(author.Id)!;
            outside.Name = "From outside";
            authors.Update(outside);
            InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => conflicting.Commit());
            Assert.Contains("changed by another writer", error.Message);
            Assert.True(conflicting.IsCompleted);
            Assert.Equal("From outside", authors.ReadById(author.Id)!.Name);

            IRepository<CfUpsertItem> codes = backend.CreateRepository<CfUpsertItem>();
            ITransaction duplicate = codes.BeginTransaction();
            codes.Create(new CfUpsertItem { Code = "K", Name = "in transaction" }, duplicate);
            codes.Create(new CfUpsertItem { Code = "K", Name = "outside" });
            Assert.Throws<InvalidOperationException>(() => duplicate.Commit());
            Assert.Equal("outside", codes.ReadById("K")!.Name);
            Assert.Throws<InvalidOperationException>(() => codes.Create(new CfUpsertItem { Code = "K", Name = "again" }, duplicate));
        }

        /// <summary>
        /// A transaction that needs more graph operations than MaxOperationsPerTransaction fails at commit and writes
        /// nothing (CreateMany runs in such a transaction); a set-based delete outside a transaction is applied in chunks.
        /// </summary>
        [Fact]
        public async Task TransactionSizeIsLimited()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync(s => s.MaxOperationsPerTransaction = 5);
            IRepository<CfTenantNote> notes = store.Backend.CreateRepository<CfTenantNote>();
            Assert.Equal(5, (await notes.CreateManyAsync(Enumerable.Range(0, 5).Select(i => new CfTenantNote { Title = "ok" + i }).ToList())).Count());

            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => notes.CreateManyAsync(Enumerable.Range(0, 6).Select(i => new CfTenantNote { Title = "big" + i }).ToList()));
            Assert.Contains("MaxOperationsPerTransaction", error.Message);
            Assert.Equal(5L, await notes.CountAsync());

            for (int i = 0; i < 3; i++) await notes.CreateAsync(new CfTenantNote { Title = "single" + i });
            Assert.Equal(8, await notes.DeleteManyAsync(x => x.Id > 0));
            Assert.Equal(0L, await notes.CountAsync());
        }

        /// <summary>
        /// A query's rows are read before the first is returned, so deleting each row while enumerating (beyond one
        /// LiteGraph page) deletes every row.
        /// </summary>
        [Fact]
        public async Task WritingWhileEnumeratingSeesAStableResult()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            IRepository<CfTenantNote> notes = store.Backend.CreateRepository<CfTenantNote>();
            for (int batch = 0; batch < 3; batch++)
            {
                await notes.CreateManyAsync(Enumerable.Range(0, 100).Select(i => new CfTenantNote { Title = "n" + batch + "-" + i, Amount = i }).ToList());
            }

            int deleted = 0;
            foreach (CfTenantNote note in notes.ReadMany(x => x.Amount >= 0))
            {
                if (notes.Delete(note)) deleted++;
            }

            Assert.Equal(300, deleted);
            Assert.Equal(0L, await notes.CountAsync());
        }

        /// <summary>
        /// Concurrent creates from many threads get unique generated keys and all rows are stored.
        /// </summary>
        [Fact]
        public async Task ConcurrentCreatesGetUniqueKeys()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            IRepository<CfTenantNote> notes = store.Backend.CreateRepository<CfTenantNote>();
            await notes.CreateAsync(new CfTenantNote { Title = "seed" });

            List<Task<List<int>>> workers = new List<Task<List<int>>>();
            for (int w = 0; w < 8; w++)
            {
                int worker = w;
                workers.Add(Task.Run(async () =>
                {
                    List<int> ids = new List<int>();
                    for (int i = 0; i < 15; i++)
                    {
                        ids.Add((await notes.CreateAsync(new CfTenantNote { TenantId = worker, Title = worker + "-" + i, Amount = i })).Id);
                    }

                    ids.AddRange((await notes.CreateManyAsync(new List<CfTenantNote> { new CfTenantNote { TenantId = worker, Title = "many-a" }, new CfTenantNote { TenantId = worker, Title = "many-b" } })).Select(n => n.Id));
                    return ids;
                }));
            }

            List<int> all = (await Task.WhenAll(workers)).SelectMany(ids => ids).ToList();
            Assert.Equal(8 * 17, all.Count);
            Assert.Equal(all.Count, all.Distinct().Count());
            Assert.Equal(8L * 17 + 1, await notes.CountAsync());
            Assert.Equal(all.OrderBy(i => i).ToArray(), notes.ReadAll().Where(n => n.Title != "seed").Select(n => n.Id).OrderBy(i => i).ToArray());
        }

        /// <summary>
        /// Labels, tags and vectors added to a Durable node with LiteGraph's client survive Durable updates.
        /// </summary>
        [Fact]
        public async Task LiteGraphAnnotationsSurviveUpdates()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            LiteGraphBackend backend = store.Backend;
            IRepository<CfAuthor> authors = backend.CreateRepository<CfAuthor>();
            CfAuthor author = await authors.CreateAsync(new CfAuthor { Name = "Ada" });
            Guid nodeGuid = backend.GetNodeGuid<CfAuthor>(author.Id);

            Node node = await ReadNodeAsync(backend, nodeGuid);
            node.Labels.Add("Featured");
            node.Tags = new NameValueCollection { { "tier", "gold" } };
            await backend.Client.Node.Update(node);

            author.Name = "Ada Lovelace";
            await authors.UpdateAsync(author);
            Assert.Equal(1, await authors.UpdateFieldAsync(x => x.Id == author.Id, x => x.Name, "Countess"));

            Node updated = await ReadNodeAsync(backend, nodeGuid);
            Assert.Contains("cf_authors", updated.Labels);
            Assert.Contains("Featured", updated.Labels);
            Assert.Equal("gold", updated.Tags["tier"]);
            Assert.Equal("Countess", ((JsonElement)updated.Data).GetProperty("name").GetString());
        }

        /// <summary>
        /// With edges disabled nothing is written to edges; RebuildEdgesAsync creates the missing edges, is idempotent and
        /// repairs edges deleted outside Durable.
        /// </summary>
        [Fact]
        public async Task RebuildEdgesRepairsTheGraph()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync(s => s.MaintainEdges = false);
            LiteGraphBackend plain = store.Backend;
            Assert.False(plain.MaintainEdges);
            CfPublisher publisher = await plain.CreateRepository<CfPublisher>().CreateAsync(new CfPublisher { Name = "Acme" });
            CfAuthor author = await plain.CreateRepository<CfAuthor>().CreateAsync(new CfAuthor { Name = "Ada", PublisherId = publisher.Id });
            await plain.CreateRepository<CfBook>().CreateAsync(new CfBook { Title = "Notes", AuthorId = author.Id, PublisherId = publisher.Id });
            Guid publisherNode = plain.GetNodeGuid<CfPublisher>(publisher.Id);
            Assert.Equal(0, await CountEdgesAsync(plain, publisherNode, false));

            await using LiteGraphBackend maintaining = await LiteGraphBackend.CreateAsync(LiteGraphBackendSettings.ForClient(plain.Client, plain.TenantGuid, plain.GraphGuid));
            Assert.Equal(3, await maintaining.RebuildEdgesAsync(new[] { typeof(CfBook) }));
            Assert.Equal(2, await CountEdgesAsync(maintaining, publisherNode, false));
            Assert.Equal(0, await maintaining.RebuildEdgesAsync());

            LiteGraphRelationship relationship = maintaining.GetRelationships(typeof(CfAuthor)).Single(r => r.Principal.EntityType == typeof(CfPublisher));
            await maintaining.Client.Edge.DeleteByGuid(maintaining.TenantGuid, maintaining.GraphGuid, maintaining.GetEdgeGuid(relationship, maintaining.GetNodeGuid<CfAuthor>(author.Id)));
            Assert.Equal(1, await maintaining.RebuildEdgesAsync());
            Assert.Equal(2, await CountEdgesAsync(maintaining, publisherNode, false));
        }

        /// <summary>
        /// Settings are validated before anything is opened.
        /// </summary>
        [Fact]
        public async Task SettingsAreValidated()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => LiteGraphBackend.CreateAsync(null!));
            Assert.Throws<ArgumentNullException>(() => LiteGraphBackend.Create(null!));
            await Assert.ThrowsAsync<ArgumentException>(() => LiteGraphBackend.CreateAsync(new LiteGraphBackendSettings()));
            Assert.Throws<ArgumentException>(() => LiteGraphBackendSettings.ForFile(string.Empty));
            Assert.Throws<ArgumentNullException>(() => LiteGraphBackendSettings.ForClient(null!));
            Assert.Throws<ArgumentException>(() => new LiteGraphBackendSettings { TenantName = string.Empty });
            Assert.Throws<ArgumentException>(() => new LiteGraphBackendSettings { GraphName = null! });
            Assert.Throws<ArgumentOutOfRangeException>(() => new LiteGraphBackendSettings { MaxOperationsPerTransaction = 0 });
            Assert.Throws<ArgumentOutOfRangeException>(() => new LiteGraphBackendSettings { MaxOperationsPerTransaction = 10001 });
            Assert.Throws<ArgumentOutOfRangeException>(() => new LiteGraphBackendSettings { TransactionTimeoutSeconds = 0 });
            Assert.Throws<ArgumentException>(() => new LiteGraphBackendSettings { InMemory = true, GraphGuid = Guid.Empty }.Validate());
            Assert.Throws<ArgumentException>(() => new LiteGraphBackendSettings { InMemory = true, TenantGuid = Guid.Empty }.Validate());

            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            Assert.Throws<ArgumentException>(() => new LiteGraphBackendSettings { Client = store.Backend.Client, Filename = "other.db" }.Validate());
            new LiteGraphBackendSettings { Client = store.Backend.Client }.Validate();
            new LiteGraphBackendSettings { InMemory = true }.Validate();
        }

        /// <summary>
        /// A backend never disposes a client it was given; a backend that created its client disposes it, after which the
        /// backend and its repositories throw ObjectDisposedException; disposing twice is harmless; transactions of
        /// another backend are rejected.
        /// </summary>
        [Fact]
        public async Task DisposalSemantics()
        {
            await using LiteGraphTestStore store = await LiteGraphTestStore.CreateAsync();
            Assert.True(store.Backend.OwnsClient);
            LiteGraphBackend borrowing = await LiteGraphBackend.CreateAsync(LiteGraphBackendSettings.ForClient(store.Backend.Client, store.Backend.TenantGuid, store.Backend.GraphGuid));
            Assert.False(borrowing.OwnsClient);
            await borrowing.CreateRepository<CfTenantNote>().CreateAsync(new CfTenantNote { Title = "borrowed" });
            await borrowing.DisposeAsync();
            borrowing.Dispose();
            Assert.Equal("borrowed", store.Backend.CreateRepository<CfTenantNote>().ReadAll().Single().Title);
            Assert.NotNull(borrowing.Client);
            Assert.Throws<ObjectDisposedException>(() => borrowing.CreateRepository<CfTenantNote>());

            LiteGraphBackend owning = LiteGraphBackend.Create(LiteGraphBackendSettings.ForInMemory());
            IRepository<CfTenantNote> notes = owning.CreateRepository<CfTenantNote>();
            await notes.CreateAsync(new CfTenantNote { Title = "ephemeral" });
            Assert.Equal(1L, await notes.CountAsync());
            Assert.ThrowsAny<ArgumentException>(() => notes.Create(new CfTenantNote { Title = "x" }, store.Backend.BeginTransaction()));
            owning.Dispose();
            owning.Dispose();
            await owning.DisposeAsync();
            Assert.Throws<ObjectDisposedException>(() => owning.Client);
            Assert.Throws<ObjectDisposedException>(() => owning.CreateRepository<CfTenantNote>());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => notes.CreateAsync(new CfTenantNote { Title = "late" }));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => notes.CountAsync());
        }

        /// <summary>
        /// LiteGraph's in-memory mode works, and two in-memory backends are isolated (each gets its own graph).
        /// </summary>
        [Fact]
        public async Task InMemoryBackendsAreIsolated()
        {
            await using LiteGraphBackend first = await LiteGraphBackend.CreateAsync(LiteGraphBackendSettings.ForInMemory());
            await using LiteGraphBackend second = await LiteGraphBackend.CreateAsync(LiteGraphBackendSettings.ForInMemory());
            Assert.NotEqual(first.GraphGuid, second.GraphGuid);
            IRepository<CfPublisher> firstPublishers = first.CreateRepository<CfPublisher>();
            CfPublisher publisher = await firstPublishers.CreateAsync(new CfPublisher { Name = "Acme" });
            await first.CreateRepository<CfAuthor>().CreateAsync(new CfAuthor { Name = "Ada", PublisherId = publisher.Id });
            Assert.Equal(1, await CountEdgesAsync(first, first.GetNodeGuid<CfPublisher>(publisher.Id), false));
            Assert.Equal(0L, await second.CreateRepository<CfPublisher>().CountAsync());

            await using (ITransaction transaction = await firstPublishers.BeginTransactionAsync())
            {
                await firstPublishers.CreateAsync(new CfPublisher { Name = "Committed" }, transaction);
                await transaction.CommitAsync();
            }

            Assert.Equal(2L, await firstPublishers.CountAsync());
        }

        #endregion

        #region Private-Methods

        private static async Task<Node> ReadNodeAsync(LiteGraphBackend backend, Guid guid)
        {
            Node? node = await backend.Client.Node.ReadByGuid(backend.TenantGuid, backend.GraphGuid, guid, true, true);
            Assert.True(node != null, "Node " + guid + " should exist.");
            return node!;
        }

        private static async Task<Edge> ReadEdgeAsync(LiteGraphBackend backend, Guid guid)
        {
            Edge? edge = await backend.Client.Edge.ReadByGuid(backend.TenantGuid, backend.GraphGuid, guid, true, true);
            Assert.True(edge != null, "Edge " + guid + " should exist.");
            return edge!;
        }

        private static async Task<int> CountEdgesAsync(LiteGraphBackend backend, Guid node, bool outgoing)
        {
            int count = 0;
            IAsyncEnumerable<Edge> edges = outgoing
                ? backend.Client.Edge.ReadEdgesFromNode(backend.TenantGuid, backend.GraphGuid, node)
                : backend.Client.Edge.ReadEdgesToNode(backend.TenantGuid, backend.GraphGuid, node);
            await foreach (Edge edge in edges) count++;
            return count;
        }

        private static async Task<LiteGraphQueryPlan> PlanOfAsync<TResult>(List<LiteGraphQueryPlan> plans, Func<Task<TResult>> action)
        {
            plans.Clear();
            await action();
            LiteGraphQueryPlan? plan = plans.FirstOrDefault(p => p.Operation == "Query");
            Assert.True(plan != null, "A Query plan should have been raised.");
            return plan!;
        }

        private static async Task AssertPushedAsync(List<LiteGraphQueryPlan> plans, IRepository<CfItem> items, Expression<Func<CfItem, bool>> predicate, string column, params string[] expected)
        {
            LiteGraphQueryPlan plan = await PlanOfAsync(plans, () => items.Query().Where(predicate).ExecuteAsync());
            Assert.Equal(LiteGraphReadStrategy.LabelScan, plan.Strategy);
            Assert.True(plan.DataFilter != null && plan.DataFilter.Contains(column), "Predicate " + predicate + " should push down a filter on " + column + " (plan: " + plan + ").");
            Assert.Equal(expected, (await items.Query().Where(predicate).ExecuteAsync()).Select(x => x.Name).ToArray());
        }

        private static async Task AssertNotPushedAsync(List<LiteGraphQueryPlan> plans, IRepository<CfItem> items, Expression<Func<CfItem, bool>> predicate, params string[] expected)
        {
            LiteGraphQueryPlan plan = await PlanOfAsync(plans, () => items.Query().Where(predicate).ExecuteAsync());
            Assert.Equal(LiteGraphReadStrategy.LabelScan, plan.Strategy);
            Assert.True(plan.DataFilter == null, "Predicate " + predicate + " must not be pushed down (plan: " + plan + ").");
            Assert.Equal(expected, (await items.Query().Where(predicate).ExecuteAsync()).Select(x => x.Name).ToArray());
        }

        private static void AssertSameDateTime(DateTime expected, DateTime actual)
        {
            Assert.Equal(expected.Ticks, actual.Ticks);
            Assert.Equal(expected.Kind, actual.Kind);
        }

        #endregion
    }
}
