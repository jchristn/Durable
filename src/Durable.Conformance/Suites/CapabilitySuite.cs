namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// One case per <see cref="RepositoryCapabilities"/> flag, always run. When the target supports the flag, the
    /// repository must report it and the gated call sites must work. When it does not, the repository must not report it
    /// and every gated call site must throw <see cref="NotSupportedException"/> immediately (at the builder call or the
    /// repository call, before any result is produced), never fail later or silently ignore the request.
    /// </summary>
    internal sealed class CapabilitySuite : KitSuite
    {
        public CapabilitySuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "Repositories report exactly the target's capabilities")]
        public void RepositoriesReportTargetCapabilities()
        {
            Assert.Equal(Target.Capabilities, Repository<CfItem>().Capabilities);
            Assert.Equal(Target.Capabilities, Repository<CfAuthor>().Capabilities);
            Assert.Equal(Target.Capabilities, Repository<CfTextItem>().Capabilities);
            Assert.Equal(Target.Capabilities, Repository<CfConventionWidget>().Capabilities);
            Assert.Equal(RepositoryCapabilities.None, Target.Capabilities & ~RepositoryCapabilities.All);
        }

        [ConformanceTest(Description = "Transactions: BeginTransaction works, or throws NotSupportedException")]
        public async Task Transactions()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.Transactions;
            await ResetAsync(typeof(CfTenantNote));
            IRepository<CfTenantNote> repository = Repository<CfTenantNote>();
            AssertReported(flag, repository.Capabilities);
            if (Supports(flag))
            {
                using (ITransaction transaction = repository.BeginTransaction())
                {
                    Assert.NotNull(transaction);
                    Assert.False(transaction.IsCompleted);
                    transaction.Commit();
                    Assert.True(transaction.IsCompleted);
                }

                await using (ITransaction transaction = await repository.BeginTransactionAsync(Token))
                {
                    await transaction.RollbackAsync(Token);
                }

                return;
            }

            ConformanceAssert.NotSupported(() => repository.BeginTransaction().Dispose(), "BeginTransaction()", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(async () => (await repository.BeginTransactionAsync(Token)).Dispose(), "BeginTransactionAsync()", flag.ToString());
        }

        [ConformanceTest(Description = "Include: Include/ThenInclude work, or Include throws NotSupportedException")]
        public async Task Include()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.Include;
            await ResetAsync(ConformanceEntities.Library.ToArray());
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            IRepository<CfBook> books = Repository<CfBook>();
            AssertReported(flag, authors.Capabilities);
            if (Supports(flag))
            {
                Assert.Empty(authors.Query().Include(a => a.Books).ThenInclude((CfBook b) => b.Publisher).Execute());
                Assert.Empty(books.Query().Include(b => b.Author).Execute());
                return;
            }

            ConformanceAssert.NotSupported(() => authors.Query().Include(a => a.Books), "Query().Include(a => a.Books)", flag.ToString());
            ConformanceAssert.NotSupported(() => authors.Query().Include(a => a.Publisher), "Query().Include(a => a.Publisher)", flag.ToString());
            ConformanceAssert.NotSupported(() => books.Query().Include(b => b.Author), "Query().Include(b => b.Author)", flag.ToString());
        }

        [ConformanceTest(Description = "ManyToMany: many-to-many includes and predicates work, or throw NotSupportedException")]
        public async Task ManyToMany()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.ManyToMany;
            await ResetAsync(ConformanceEntities.Library.ToArray());
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            AssertReported(flag, authors.Capabilities);
            if (Supports(flag))
            {
                if (Supports(RepositoryCapabilities.Include)) Assert.Empty(authors.Query().Include(a => a.Tags).Execute());
                if (Supports(RepositoryCapabilities.NavigationPredicates)) Assert.Equal(0L, authors.Count(a => a.Tags.Any()));
                return;
            }

            ConformanceAssert.NotSupported(() => authors.Query().Include(a => a.Tags), "Query().Include(a => a.Tags)", flag.ToString());
            ConformanceAssert.NotSupported(() => authors.Query().Where(a => a.Tags.Any()), "Query().Where(a => a.Tags.Any())", flag.ToString());
            ConformanceAssert.NotSupported(() => authors.Count(a => a.Tags.Any(t => t.Label == "x")), "Count(a => a.Tags.Any(...))", flag.ToString());
        }

        [ConformanceTest(Description = "NavigationPredicates: navigation members and collection predicates work, or throw NotSupportedException")]
        public async Task NavigationPredicates()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.NavigationPredicates;
            await ResetAsync(ConformanceEntities.Library.ToArray());
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            IRepository<CfBook> books = Repository<CfBook>();
            AssertReported(flag, books.Capabilities);
            if (Supports(flag))
            {
                Assert.Empty(books.Query().Where(b => b.Author!.Name == "x").Execute());
                Assert.Equal(0L, authors.Count(a => a.Books.Any()));
                return;
            }

            ConformanceAssert.NotSupported(() => books.Query().Where(b => b.Author!.Name == "x"), "Query().Where(b => b.Author.Name == ...)", flag.ToString());
            ConformanceAssert.NotSupported(() => authors.Query().Where(a => a.Books.Any()), "Query().Where(a => a.Books.Any())", flag.ToString());
            ConformanceAssert.NotSupported(() => authors.Query().Where(a => a.Books.Count() > 1), "Query().Where(a => a.Books.Count() > 1)", flag.ToString());
            ConformanceAssert.NotSupported(() => books.Count(b => b.Author!.Name == "x"), "Count(b => b.Author.Name == ...)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => authors.ExistsAsync(a => a.Books.All(b => b.Pages > 1), null, Token), "ExistsAsync(a => a.Books.All(...))", flag.ToString());
        }

        [ConformanceTest(Description = "Grouping: GroupBy works, or throws NotSupportedException")]
        public async Task Grouping()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.Grouping;
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> items = Repository<CfItem>();
            AssertReported(flag, items.Capabilities);
            if (Supports(flag))
            {
                Assert.Equal(0L, items.Query().GroupBy(x => x.Category).Having(g => g.Count() > 1).Count());
                return;
            }

            ConformanceAssert.NotSupported(() => items.Query().GroupBy(x => x.Category), "Query().GroupBy(x => x.Category)", flag.ToString());
        }

        [ConformanceTest(Description = "Projection: Select works, or throws NotSupportedException")]
        public async Task Projection()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.Projection;
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> items = Repository<CfItem>();
            AssertReported(flag, items.Capabilities);
            if (Supports(flag))
            {
                Assert.Empty(items.Query().Select(x => new CfItemSummary { Name = x.Name }).Execute());
                return;
            }

            ConformanceAssert.NotSupported(() => items.Query().Select(x => new CfItemSummary { Name = x.Name }), "Query().Select(...)", flag.ToString());
        }

        [ConformanceTest(Description = "Distinct: Distinct works, or throws NotSupportedException")]
        public async Task Distinct()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.Distinct;
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> items = Repository<CfItem>();
            AssertReported(flag, items.Capabilities);
            if (Supports(flag))
            {
                Assert.Empty(items.Query().Distinct().Execute());
                return;
            }

            ConformanceAssert.NotSupported(() => items.Query().Distinct(), "Query().Distinct()", flag.ToString());
        }

        [ConformanceTest(Description = "Aggregates: Sum/Average/Min/Max work, or throw NotSupportedException (Count and Any always work)")]
        public async Task Aggregates()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.Aggregates;
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> items = Repository<CfItem>();
            items.Create(new CfItem { Name = "one", Category = "c", Price = 2m, Quantity = 3 });
            AssertReported(flag, items.Capabilities);
            Assert.Equal(1L, items.Query().Count());
            Assert.True(items.Query().Any());
            if (Supports(flag))
            {
                Assert.Equal(2m, items.Sum(x => x.Price));
                Assert.Equal(3m, items.Query().Average(x => x.Quantity));
                Assert.Equal(3, await items.MaxAsync(x => x.Quantity, null, null, Token));
                return;
            }

            ConformanceAssert.NotSupported(() => items.Sum(x => x.Price), "Sum(x => x.Price)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Average(x => x.Price), "Average(x => x.Price)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Min(x => x.Quantity), "Min(x => x.Quantity)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Max(x => x.Quantity), "Max(x => x.Quantity)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => items.SumAsync(x => x.Price, null, null, Token), "SumAsync(x => x.Price)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => items.MaxAsync(x => x.Quantity, null, null, Token), "MaxAsync(x => x.Quantity)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Query().Sum(x => x.Price), "Query().Sum(x => x.Price)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => items.Query().AverageAsync(x => x.Price, Token), "Query().AverageAsync(x => x.Price)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Query().Min(x => x.Price), "Query().Min(x => x.Price)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => items.Query().MaxAsync(x => x.Price, Token), "Query().MaxAsync(x => x.Price)", flag.ToString());
        }

        [ConformanceTest(Description = "Functions: string/date/math functions in predicates work, or throw NotSupportedException")]
        public async Task Functions()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.Functions;
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> items = Repository<CfItem>();
            AssertReported(flag, items.Capabilities);
            if (Supports(flag))
            {
                Assert.Empty(items.Query().Where(x => x.Name.ToUpper() == "A" || x.CreatedUtc.Year == 1 || Math.Abs(x.Ratio) > 1e9).Execute());
                return;
            }

            ConformanceAssert.NotSupported(() => items.Query().Where(x => x.Name.ToUpper() == "A"), "Query().Where(x => x.Name.ToUpper() == ...)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Query().Where(x => x.Name.Length > 3), "Query().Where(x => x.Name.Length > 3)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Query().Where(x => x.CreatedUtc.Year == 2024), "Query().Where(x => x.CreatedUtc.Year == 2024)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Query().Where(x => Math.Abs(x.Ratio) > 1), "Query().Where(x => Math.Abs(x.Ratio) > 1)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.Count(x => x.Name.Trim() == "A"), "Count(x => x.Name.Trim() == ...)", flag.ToString());
        }

        [ConformanceTest(Description = "StringMatchModes: Ordinal/IgnoreCase matching works, or throws NotSupportedException")]
        public async Task StringMatchModes()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.StringMatchModes;
            await ResetAsync(typeof(CfTextItem));
            IRepository<CfTextItem> texts = Repository<CfTextItem>();
            AssertReported(flag, texts.Capabilities);
            if (Supports(flag))
            {
                Assert.Empty(texts.Query().Where(x => x.Text!.Equals("a", StringComparison.OrdinalIgnoreCase)).Execute());
                Assert.Equal(0L, Repository<CfTextItem>(new RepositoryOptions { StringMatching = StringMatchMode.Ordinal }).Count(x => x.Text == "a"));
                return;
            }

            ConformanceAssert.NotSupported(() => texts.Query().Where(x => x.Text!.Equals("a", StringComparison.OrdinalIgnoreCase)), "Where(Equals(..., OrdinalIgnoreCase))", flag.ToString());
            ConformanceAssert.NotSupported(() => texts.Query().Where(x => x.Text!.Contains("a", StringComparison.Ordinal)), "Where(Contains(..., Ordinal))", flag.ToString());
            ConformanceAssert.NotSupported(() => texts.Count(x => string.Equals(x.Text, "a", StringComparison.Ordinal)), "Count(string.Equals(..., Ordinal))", flag.ToString());
            foreach (StringMatchMode mode in new[] { StringMatchMode.Ordinal, StringMatchMode.IgnoreCase })
            {
                IRepository<CfTextItem> configured;
                try
                {
                    configured = Repository<CfTextItem>(new RepositoryOptions { StringMatching = mode });
                }
                catch (NotSupportedException)
                {
                    continue;
                }

                ConformanceAssert.NotSupported(() => configured.Query().Where(x => x.Text == "a"), "Where(x => x.Text == ...) with StringMatching = " + mode, flag.ToString());
            }
        }

        [ConformanceTest(Description = "CompositeKeys: composite-key entities work, or throw NotSupportedException")]
        public async Task CompositeKeys()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.CompositeKeys;
            if (Supports(flag))
            {
                await ResetAsync(typeof(CfCompositeItem));
                IRepository<CfCompositeItem> repository = Repository<CfCompositeItem>();
                AssertReported(flag, repository.Capabilities);
                repository.Create(new CfCompositeItem { TenantId = 1, Sku = "A", Name = "x" });
                Assert.Equal("x", repository.ReadById(new object[] { 1, "A" })?.Name);
                return;
            }

            await AssertEntityShapeRejectedAsync<CfCompositeItem>(
                flag,
                repository => repository.Create(new CfCompositeItem { TenantId = 1, Sku = "A", Name = "x" }),
                repository => repository.ReadById(new object[] { 1, "A" }),
                "ReadById(new object[] { 1, \"A\" })");
        }

        [ConformanceTest(Description = "OptimisticConcurrency: versioned entities work, or throw NotSupportedException")]
        public async Task OptimisticConcurrency()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.OptimisticConcurrency;
            if (Supports(flag))
            {
                await ResetAsync(typeof(CfVersionedItem));
                IRepository<CfVersionedItem> repository = Repository<CfVersionedItem>();
                AssertReported(flag, repository.Capabilities);
                CfVersionedItem item = repository.Create(new CfVersionedItem { Name = "v" });
                Assert.Equal(1, item.Version);
                Assert.Equal(2, repository.Update(item).Version);
                return;
            }

            await AssertEntityShapeRejectedAsync<CfVersionedItem>(
                flag,
                repository => repository.Create(new CfVersionedItem { Name = "v" }),
                repository => repository.Update(new CfVersionedItem { Id = 1, Name = "v", Version = 1 }),
                "Update(versioned entity)");
        }

        [ConformanceTest(Description = "BatchUpdate: UpdateField/BatchUpdate work, or throw NotSupportedException")]
        public async Task BatchUpdate()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.BatchUpdate;
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> items = Repository<CfItem>();
            items.Create(new CfItem { Name = "one", Category = "c", Quantity = 1 });
            AssertReported(flag, items.Capabilities);
            if (Supports(flag))
            {
                Assert.Equal(1, items.UpdateField(x => x.Quantity == 1, x => x.Quantity, 2));
                Assert.Equal(1, items.BatchUpdate(x => x.Quantity == 2, x => new CfItem { Quantity = x.Quantity + 1 }));
                Assert.Equal(3, items.ReadSingle(x => x.Name == "one").Quantity);
                return;
            }

            ConformanceAssert.NotSupported(() => items.UpdateField(x => x.Quantity == 1, x => x.Quantity, 2), "UpdateField(...)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => items.UpdateFieldAsync(x => x.Quantity == 1, x => x.Quantity, 2, null, Token), "UpdateFieldAsync(...)", flag.ToString());
            ConformanceAssert.NotSupported(() => items.BatchUpdate(x => x.Quantity == 1, x => new CfItem { Quantity = 2 }), "BatchUpdate(...)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => items.BatchUpdateAsync(x => x.Quantity == 1, x => new CfItem { Quantity = 2 }, null, Token), "BatchUpdateAsync(...)", flag.ToString());
            Assert.Equal(1, items.ReadSingle(x => x.Name == "one").Quantity);
            Assert.Equal(1, items.UpdateMany(x => x.Quantity == 1, x => x.Quantity = 5));
        }

        [ConformanceTest(Description = "Upsert: Upsert/UpsertMany work, or throw NotSupportedException")]
        public async Task Upsert()
        {
            const RepositoryCapabilities flag = RepositoryCapabilities.Upsert;
            await ResetAsync(typeof(CfUpsertItem));
            IRepository<CfUpsertItem> repository = Repository<CfUpsertItem>();
            AssertReported(flag, repository.Capabilities);
            if (Supports(flag))
            {
                repository.Upsert(new CfUpsertItem { Code = "A", Name = "a" });
                repository.Upsert(new CfUpsertItem { Code = "A", Name = "b" });
                Assert.Equal("b", Assert.Single(repository.ReadAll()).Name);
                return;
            }

            ConformanceAssert.NotSupported(() => repository.Upsert(new CfUpsertItem { Code = "A", Name = "a" }), "Upsert(...)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => repository.UpsertAsync(new CfUpsertItem { Code = "A", Name = "a" }, null, Token), "UpsertAsync(...)", flag.ToString());
            ConformanceAssert.NotSupported(() => repository.UpsertMany(new List<CfUpsertItem> { new CfUpsertItem { Code = "A" } }).ToList(), "UpsertMany(...)", flag.ToString());
            await ConformanceAssert.NotSupportedAsync(() => repository.UpsertManyAsync(new List<CfUpsertItem> { new CfUpsertItem { Code = "A" } }, null, Token), "UpsertManyAsync(...)", flag.ToString());
            Assert.Equal(0L, repository.Count());
        }

        private void AssertReported(RepositoryCapabilities flag, RepositoryCapabilities reported)
        {
            bool expected = Supports(flag);
            bool actual = (reported & flag) == flag;
            Assert.True(expected == actual,
                "Target '" + Target.Name + "' " + (expected ? "supports" : "does not support") + " " + flag + " but the repository " + (actual ? "reports" : "does not report") + " it.");
        }

        private async Task AssertEntityShapeRejectedAsync<T>(RepositoryCapabilities flag, Action<IRepository<T>> create, Action<IRepository<T>> use, string useDescription) where T : class, new()
        {
            IRepository<T> repository;
            try
            {
                repository = Repository<T>();
            }
            catch (NotSupportedException)
            {
                return;
            }

            AssertReported(flag, repository.Capabilities);
            try
            {
                await ResetAsync(typeof(T));
            }
            catch (NotSupportedException)
            {
                return;
            }

            try
            {
                create(repository);
            }
            catch (NotSupportedException)
            {
                return;
            }

            ConformanceAssert.NotSupported(() => use(repository), "Create succeeded, so " + useDescription, flag.ToString());
        }
    }
}
