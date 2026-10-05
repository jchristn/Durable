namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Navigations inside predicates (<see cref="RepositoryCapabilities.NavigationPredicates"/>): reference members,
    /// collection Any / All / Count, many-to-many collections (<see cref="RepositoryCapabilities.ManyToMany"/>), chained
    /// references, and navigation predicates in Count / Exists / DeleteMany. Soft-deleted related rows are invisible.
    /// </summary>
    internal sealed class NavigationPredicateSuite : KitSuite
    {
        public NavigationPredicateSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "Reference navigation members in predicates")]
        public async Task ReferenceNavigationMembers()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertNamesAsync(f.Items.Query().Where(i => i.Owner!.Name == "Acme"), "Where(Owner.Name == Acme)", ItemFixture.Alpha, ItemFixture.Beta);
            await AssertNamesAsync(f.Items.Query().Where(i => i.Owner!.City == "Austin"), "Where(Owner.City == Austin)", ItemFixture.Foxtrot);
            await AssertNamesAsync(f.Items.Query().Where(i => i.Owner!.Name.StartsWith("Glo") && i.Price > 50), "Where(Owner.Name.StartsWith(Glo) && Price > 50)", ItemFixture.Delta);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "Collection Any() with and without a predicate, and its negation")]
        public async Task CollectionAny()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Any()), "Where(Items.Any())", "Acme", "Globex", "Initech");
            await AssertOwnersAsync(f.Owners.Query().Where(o => !o.Items.Any()), "Where(!Items.Any())", "Umbrella");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Any(i => i.Price > 50)), "Where(Items.Any(Price > 50))", "Globex");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Any(i => i.Name == "O'Brien" || i.Code!.Contains("%"))), "Where(Items.Any(O'Brien or Code contains %))", "Acme", "Globex", "Initech");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "Collection Count() / Count property / Count(predicate)")]
        public async Task CollectionCount()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Count() > 1), "Where(Items.Count() > 1)", "Acme", "Globex");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Count == 0), "Where(Items.Count == 0)", "Umbrella");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.Count(i => i.IsActive) == 1), "Where(Items.Count(IsActive) == 1)", "Acme", "Initech");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "Collection All(predicate) is vacuously true for parents without children")]
        public async Task CollectionAll()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.All(i => i.IsActive)), "Where(Items.All(IsActive))", "Globex", "Initech", "Umbrella");
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.All(i => i.Price < 50) && o.Items.Any()), "Where(Items.All(Price < 50) && Items.Any())", "Acme", "Initech");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "All(predicate) treats a comparison with NULL as false, so a NULL child violates it")]
        public async Task CollectionAllWithNullableChildren()
        {
            ItemFixture f = await SeedItemsAsync();
            await AssertOwnersAsync(f.Owners.Query().Where(o => o.Items.All(i => i.Discount > 0)), "Where(Items.All(Discount > 0))", "Initech", "Umbrella");
            await AssertOwnersAsync(f.Owners.Query().Where(o => !o.Items.All(i => i.Discount > 0)), "Where(!Items.All(Discount > 0))", "Acme", "Globex");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "A reference navigation to a soft-deleted row reads as missing")]
        public async Task ReferenceToSoftDeletedRowIsMissing()
        {
            await ResetAsync(typeof(CfFolder), typeof(CfDocument));
            IRepository<CfFolder> folders = Repository<CfFolder>();
            IRepository<CfDocument> documents = Repository<CfDocument>();
            CfFolder live = await folders.CreateAsync(new CfFolder { Name = "live" }, null, Token);
            CfFolder gone = await folders.CreateAsync(new CfFolder { Name = "gone" }, null, Token);
            await documents.CreateAsync(new CfDocument { Title = "a", FolderId = live.Id }, null, Token);
            await documents.CreateAsync(new CfDocument { Title = "b", FolderId = gone.Id }, null, Token);
            Assert.True(await folders.DeleteAsync(gone, null, Token));

            List<string> named = (await ExecuteAsync(documents.Query().Where(d => d.Folder!.Name == "gone"), "Where(Folder.Name == gone)")).Select(d => d.Title).ToList();
            Assert.True(named.Count == 0, "Where(Folder.Name == gone) must not see the soft-deleted folder, but returned [" + string.Join(", ", named) + "].");
            List<string> missing = (await ExecuteAsync(documents.Query().Where(d => d.Folder!.Name == null), "Where(Folder.Name == null)")).Select(d => d.Title).ToList();
            Assert.True(missing.SequenceEqual(new[] { "b" }), "Where(Folder.Name == null) must return [b], but returned [" + string.Join(", ", missing) + "].");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "Reference navigations on the book/author graph, including a chained reference")]
        public async Task LibraryReferenceNavigations()
        {
            await SeedLibraryAsync();
            IRepository<CfBook> books = Repository<CfBook>();
            await AssertTitlesAsync(books.Query().Where(b => b.Author!.Name == "Austen"), "Where(Author.Name == Austen)", "Emma", "Persuasion", "Sanditon");
            await AssertTitlesAsync(books.Query().Where(b => b.Publisher!.Name == "Penguin"), "Where(Publisher.Name == Penguin)", "Emma", "Labyrinths");
            await AssertTitlesAsync(books.Query().Where(b => b.Author!.Publisher!.Name == "Vintage"), "Where(Author.Publisher.Name == Vintage)", "Ficciones", "Labyrinths");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates | RepositoryCapabilities.ManyToMany, Description = "Many-to-many collections in predicates")]
        public async Task ManyToManyPredicates()
        {
            await SeedLibraryAsync();
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            await AssertAuthorsAsync(authors.Query().Where(a => a.Tags.Any(t => t.Label == "Classic")), "Where(Tags.Any(Label == Classic))", "Austen", "Dickens");
            await AssertAuthorsAsync(authors.Query().Where(a => !a.Tags.Any()), "Where(!Tags.Any())", "Calvino");
            await AssertAuthorsAsync(authors.Query().Where(a => a.Tags.Count() == 2), "Where(Tags.Count() == 2)", "Austen", "Dickens");
            List<CfTag> unused = await ExecuteAsync(Repository<CfTag>().Query().Where(t => !t.Authors.Any()), "tags.Where(!Authors.Any())");
            Assert.Equal("Unused", Assert.Single(unused).Label);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "Soft-deleted related rows do not satisfy collection predicates")]
        public async Task SoftDeletedChildrenAreInvisible()
        {
            await SeedLibraryAsync();
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            await AssertAuthorsAsync(authors.Query().Where(a => a.Notes.Any()), "Where(Notes.Any())", "Austen", "Dickens");
            await AssertAuthorsAsync(authors.Query().Where(a => a.Notes.Any(n => n.Text.StartsWith("drop"))), "Where(Notes.Any(Text starts with drop))");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates, Description = "Navigation predicates in Count, Exists, ReadMany and DeleteMany")]
        public async Task NavigationPredicatesInRepositoryMethods()
        {
            ItemFixture f = await SeedItemsAsync();
            Assert.Equal(2L, f.Items.Count(i => i.Owner!.Name == "Globex"));
            Assert.True(await f.Items.ExistsAsync(i => i.Owner!.City == "Berlin", null, Token));
            Assert.False(f.Items.Exists(i => i.Owner!.Name == "Umbrella"));
            ConformanceAssert.NameSet(f.Owners.ReadMany(o => o.Items.Any(i => i.Quantity > 10)).Select(o => o.Name), new[] { "Globex" }, "ReadMany(Items.Any(Quantity > 10))");
            Assert.Equal(2, f.Items.DeleteMany(i => i.Owner!.Name == "Acme"));
            Assert.Equal(4L, f.Items.Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.NavigationPredicates | RepositoryCapabilities.Include, Description = "A collection predicate filters roots without trimming included children")]
        public async Task NavigationPredicateWithInclude()
        {
            await SeedLibraryAsync();
            List<CfAuthor> authors = await ExecuteAsync(Repository<CfAuthor>().Query().Include(a => a.Books).Where(a => a.Books.Any(b => b.Pages > 300)).OrderBy(a => a.Name),
                "Include(Books).Where(Books.Any(Pages > 300))");
            Assert.Equal(new[] { "Austen", "Dickens" }, authors.Select(a => a.Name).ToArray());
            Assert.Equal(3, authors[0].Books.Count);
            Assert.Single(authors[1].Books);
        }

        private async Task AssertOwnersAsync(IQueryBuilder<CfOwner> query, string context, params string[] expected)
        {
            List<CfOwner> rows = await ExecuteAsync(query, context);
            ConformanceAssert.NameSet(rows.Select(r => r.Name), expected, context);
        }

        private async Task AssertAuthorsAsync(IQueryBuilder<CfAuthor> query, string context, params string[] expected)
        {
            List<CfAuthor> rows = await ExecuteAsync(query, context);
            ConformanceAssert.NameSet(rows.Select(r => r.Name), expected, context);
        }

        private async Task AssertTitlesAsync(IQueryBuilder<CfBook> query, string context, params string[] expected)
        {
            List<CfBook> rows = await ExecuteAsync(query, context);
            ConformanceAssert.NameSet(rows.Select(r => r.Title), expected, context);
        }
    }
}
