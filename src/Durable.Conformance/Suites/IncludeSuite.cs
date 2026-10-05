namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Eager loading (<see cref="RepositoryCapabilities.Include"/>): reference and collection navigations, ThenInclude
    /// chains, sibling includes, many-to-many (<see cref="RepositoryCapabilities.ManyToMany"/>), paging applied to roots,
    /// soft-deleted related rows excluded, streaming, and include argument validation.
    /// </summary>
    internal sealed class IncludeSuite : KitSuite
    {
        public IncludeSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Reference navigations load the right parent; a null key leaves the navigation null")]
        public async Task IncludeReferenceNavigations()
        {
            await SeedLibraryAsync();
            List<CfBook> books = await ExecuteAsync(Repository<CfBook>().Query().Include(b => b.Author).Include(b => b.Publisher).OrderBy(b => b.Title),
                "books.Include(Author).Include(Publisher)");
            Assert.Equal(6, books.Count);
            Dictionary<string, CfBook> byTitle = books.ToDictionary(b => b.Title, StringComparer.Ordinal);
            Assert.Equal("Austen", byTitle["Emma"].Author?.Name);
            Assert.Equal("Penguin", byTitle["Emma"].Publisher?.Name);
            Assert.Equal("Vintage", byTitle["Persuasion"].Publisher?.Name);
            Assert.Equal("Borges", byTitle["Labyrinths"].Author?.Name);
            Assert.Equal("Penguin", byTitle["Labyrinths"].Publisher?.Name);
            Assert.Equal("Dickens", byTitle["Hard Times"].Author?.Name);
            Assert.Null(byTitle["Hard Times"].Publisher);
            Assert.Null(byTitle["Sanditon"].Publisher);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Collection navigations give each parent exactly its own children; no children gives an empty list")]
        public async Task IncludeCollectionNavigation()
        {
            LibraryFixture library = await SeedLibraryAsync();
            List<CfAuthor> authors = await ExecuteAsync(Repository<CfAuthor>().Query().Include(a => a.Books).OrderBy(a => a.Name), "authors.Include(Books)");
            Assert.Equal(new[] { "Austen", "Borges", "Calvino", "Dickens" }, authors.Select(a => a.Name).ToArray());
            Assert.Equal(new[] { "Emma", "Persuasion", "Sanditon" }, LibraryFixture.Titles(authors[0].Books));
            Assert.Equal(new[] { "Ficciones", "Labyrinths" }, LibraryFixture.Titles(authors[1].Books));
            Assert.NotNull(authors[2].Books);
            Assert.Empty(authors[2].Books);
            Assert.Equal(new[] { "Hard Times" }, LibraryFixture.Titles(authors[3].Books));
            Assert.All(authors, a => Assert.All(a.Books, b => Assert.Equal(a.Id, b.AuthorId)));
            Assert.Equal(library.Authors["Austen"].Id, authors[0].Id);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Paging applies to root entities and every root keeps all of its children")]
        public async Task IncludeWithPaging()
        {
            await ResetAsync(ConformanceEntities.Library.ToArray());
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            IRepository<CfBook> books = Repository<CfBook>();
            List<CfAuthor> created = (await authors.CreateManyAsync(Enumerable.Range(0, 10)
                .Select(i => new CfAuthor { Name = "Author " + i.ToString("00", CultureInfo.InvariantCulture) }).ToList(), null, Token)).ToList();
            List<CfBook> rows = new List<CfBook>();
            foreach (CfAuthor author in created)
            {
                for (int b = 0; b < 3; b++) rows.Add(new CfBook { Title = author.Name + " Book " + b, AuthorId = author.Id, Pages = b });
            }

            await books.CreateManyAsync(rows, null, Token);
            List<CfAuthor> page = await ExecuteAsync(authors.Query().Include(a => a.Books).OrderBy(a => a.Name).Skip(3).Take(4), "Include(Books).OrderBy(Name).Skip(3).Take(4)");
            Assert.Equal(new[] { "Author 03", "Author 04", "Author 05", "Author 06" }, page.Select(a => a.Name).ToArray());
            Assert.All(page, a => Assert.Equal(3, a.Books.Count));
            Assert.All(page, a => Assert.All(a.Books, b => Assert.StartsWith(a.Name + " Book ", b.Title)));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Include combined with Where and OrderByDescending")]
        public async Task IncludeWithWhereAndOrder()
        {
            await SeedLibraryAsync();
            List<CfAuthor> authors = await ExecuteAsync(Repository<CfAuthor>().Query().Include(a => a.Books).Where(a => a.Name != "Calvino").OrderByDescending(a => a.Name),
                "authors.Include(Books).Where(Name != Calvino).OrderByDescending(Name)");
            Assert.Equal(new[] { "Dickens", "Borges", "Austen" }, authors.Select(a => a.Name).ToArray());
            Assert.Equal(new[] { 1, 2, 3 }, authors.Select(a => a.Books.Count).ToArray());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Reference include on authors (one null)")]
        public async Task IncludeReferenceFromCollectionSide()
        {
            await SeedLibraryAsync();
            List<CfAuthor> authors = await ExecuteAsync(Repository<CfAuthor>().Query().Include(a => a.Publisher).OrderBy(a => a.Name), "authors.Include(Publisher)");
            Assert.Equal(new[] { "Penguin", "Vintage", "Penguin", null }, authors.Select(a => a.Publisher?.Name).ToArray());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "ThenInclude: collection then reference")]
        public async Task ThenIncludeCollectionThenReference()
        {
            await SeedLibraryAsync();
            List<CfAuthor> authors = await ExecuteAsync(Repository<CfAuthor>().Query()
                .Include(a => a.Books)
                .ThenInclude((CfBook b) => b.Publisher)
                .Where(a => a.Name == "Austen"), "authors.Include(Books).ThenInclude(Publisher)");
            CfAuthor austen = Assert.Single(authors);
            Dictionary<string, CfBook> books = austen.Books.ToDictionary(b => b.Title, StringComparer.Ordinal);
            Assert.Equal(3, books.Count);
            Assert.Equal("Penguin", books["Emma"].Publisher?.Name);
            Assert.Equal("Vintage", books["Persuasion"].Publisher?.Name);
            Assert.Null(books["Sanditon"].Publisher);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "ThenInclude: reference then reference")]
        public async Task ThenIncludeReferenceThenReference()
        {
            await SeedLibraryAsync();
            List<CfBook> books = await ExecuteAsync(Repository<CfBook>().Query()
                .Include(b => b.Author)
                .ThenInclude((CfAuthor a) => a.Publisher), "books.Include(Author).ThenInclude(Publisher)");
            Dictionary<string, CfBook> byTitle = books.ToDictionary(b => b.Title, StringComparer.Ordinal);
            Assert.Equal("Penguin", byTitle["Emma"].Author?.Publisher?.Name);
            Assert.Equal("Vintage", byTitle["Ficciones"].Author?.Publisher?.Name);
            Assert.NotNull(byTitle["Hard Times"].Author);
            Assert.Null(byTitle["Hard Times"].Author!.Publisher);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "ThenInclude: collection then collection")]
        public async Task ThenIncludeCollectionThenCollection()
        {
            await SeedLibraryAsync();
            List<CfPublisher> publishers = await ExecuteAsync(Repository<CfPublisher>().Query()
                .Include(p => p.Authors)
                .ThenInclude((CfAuthor a) => a.Books)
                .OrderBy(p => p.Name), "publishers.Include(Authors).ThenInclude(Books)");
            Assert.Equal(new[] { "Empty Press", "Penguin", "Vintage" }, publishers.Select(p => p.Name).ToArray());
            Assert.Empty(publishers[0].Authors);
            Dictionary<string, CfAuthor> penguin = publishers[1].Authors.ToDictionary(a => a.Name, StringComparer.Ordinal);
            Assert.Equal(new[] { "Austen", "Calvino" }, penguin.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
            Assert.Equal(3, penguin["Austen"].Books.Count);
            Assert.Empty(penguin["Calvino"].Books);
            CfAuthor borges = Assert.Single(publishers[2].Authors);
            Assert.Equal(new[] { "Ficciones", "Labyrinths" }, LibraryFixture.Titles(borges.Books));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Include(x => x.A.B) is shorthand for Include + ThenInclude")]
        public async Task ChainedIncludeShorthand()
        {
            await SeedLibraryAsync();
            List<CfBook> books = await ExecuteAsync(Repository<CfBook>().Query().Include(b => b.Author!.Publisher), "books.Include(b => b.Author.Publisher)");
            Dictionary<string, CfBook> byTitle = books.ToDictionary(b => b.Title, StringComparer.Ordinal);
            Assert.Equal("Austen", byTitle["Persuasion"].Author?.Name);
            Assert.Equal("Penguin", byTitle["Persuasion"].Author?.Publisher?.Name);
            Assert.Equal("Vintage", byTitle["Labyrinths"].Author?.Publisher?.Name);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Sibling includes all load")]
        public async Task SiblingIncludes()
        {
            await SeedLibraryAsync();
            List<CfAuthor> authors = await ExecuteAsync(Repository<CfAuthor>().Query()
                .Include(a => a.Books)
                .Include(a => a.Publisher)
                .Include(a => a.Notes)
                .Where(a => a.Name == "Dickens"), "authors.Include(Books).Include(Publisher).Include(Notes)");
            CfAuthor dickens = Assert.Single(authors);
            Assert.Equal("Hard Times", Assert.Single(dickens.Books).Title);
            Assert.Null(dickens.Publisher);
            Assert.Equal("keep-d", Assert.Single(dickens.Notes).Text);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Soft-deleted related rows are not loaded")]
        public async Task IncludeExcludesSoftDeletedChildren()
        {
            await SeedLibraryAsync();
            List<CfAuthor> authors = await ExecuteAsync(Repository<CfAuthor>().Query().Include(a => a.Notes).OrderBy(a => a.Name), "authors.Include(Notes)");
            Assert.Equal(new[] { "keep-a" }, authors[0].Notes.Select(n => n.Text).ToArray());
            Assert.Empty(authors[1].Notes);
            Assert.Empty(authors[2].Notes);
            Assert.Equal(new[] { "keep-d" }, authors[3].Notes.Select(n => n.Text).ToArray());
            Assert.All(authors.SelectMany(a => a.Notes), n => Assert.False(n.IsDeleted));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include | RepositoryCapabilities.ManyToMany, Description = "Many-to-many loads in both directions; no links gives an empty list")]
        public async Task IncludeManyToMany()
        {
            await SeedLibraryAsync();
            List<CfAuthor> authors = await ExecuteAsync(Repository<CfAuthor>().Query().Include(a => a.Tags).OrderBy(a => a.Name), "authors.Include(Tags)");
            Assert.Equal(new[] { "Classic", "Satire" }, Labels(authors[0].Tags));
            Assert.Equal(new[] { "Fantasy" }, Labels(authors[1].Tags));
            Assert.NotNull(authors[2].Tags);
            Assert.Empty(authors[2].Tags);
            Assert.Equal(new[] { "Classic", "Satire" }, Labels(authors[3].Tags));

            List<CfTag> tags = await ExecuteAsync(Repository<CfTag>().Query().Include(t => t.Authors).OrderBy(t => t.Label), "tags.Include(Authors)");
            Assert.Equal(new[] { "Classic", "Fantasy", "Satire", "Unused" }, tags.Select(t => t.Label).ToArray());
            Assert.Equal(new[] { "Austen", "Dickens" }, Names(tags[0].Authors));
            Assert.Equal(new[] { "Borges" }, Names(tags[1].Authors));
            Assert.Equal(new[] { "Austen", "Dickens" }, Names(tags[2].Authors));
            Assert.Empty(tags[3].Authors);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Includes load with synchronous Execute and with streaming")]
        public async Task IncludeSyncAndStreaming()
        {
            await SeedLibraryAsync();
            IRepository<CfAuthor> repository = Repository<CfAuthor>();
            List<CfAuthor> sync = repository.Query().Include(a => a.Books).OrderBy(a => a.Name).Execute().ToList();
            Assert.Equal(new[] { 3, 2, 0, 1 }, sync.Select(a => a.Books.Count).ToArray());

            List<CfAuthor> streamed = new List<CfAuthor>();
            await foreach (CfAuthor author in repository.Query().Include(a => a.Books).ThenInclude((CfBook b) => b.Publisher).OrderBy(a => a.Name).ExecuteAsyncEnumerable(Token))
                streamed.Add(author);
            Assert.Equal(new[] { "Austen", "Borges", "Calvino", "Dickens" }, streamed.Select(a => a.Name).ToArray());
            Assert.Equal(new[] { 3, 2, 0, 1 }, streamed.Select(a => a.Books.Count).ToArray());
            Assert.Equal("Penguin", streamed[0].Books.Single(b => b.Title == "Emma").Publisher?.Name);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Include over an empty result and Count with includes")]
        public async Task IncludeEdgeCases()
        {
            await SeedLibraryAsync();
            IRepository<CfAuthor> repository = Repository<CfAuthor>();
            Assert.Empty(await repository.Query().Include(a => a.Books).Where(a => a.Name == "nobody").ExecuteAsync(Token));
            Assert.Equal(4L, repository.Query().Include(a => a.Books).Count());
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Many roots (250) each get their own children")]
        public async Task IncludeManyRoots()
        {
            await ResetAsync(ConformanceEntities.Library.ToArray());
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            IRepository<CfBook> books = Repository<CfBook>();
            List<CfAuthor> created = (await authors.CreateManyAsync(Enumerable.Range(0, 250)
                .Select(i => new CfAuthor { Name = "Bulk " + i.ToString("000", CultureInfo.InvariantCulture) }).ToList(), null, Token)).ToList();
            List<CfBook> rows = created.Select(a => new CfBook { Title = a.Name + " Book", AuthorId = a.Id }).ToList();
            rows.AddRange(created.Where((a, i) => i % 5 == 0).Select(a => new CfBook { Title = a.Name + " Extra", AuthorId = a.Id }));
            await books.CreateManyAsync(rows, null, Token);

            List<CfAuthor> loaded = await ExecuteAsync(authors.Query().Include(a => a.Books), "Include(Books) over 250 roots");
            Assert.Equal(250, loaded.Count);
            foreach (CfAuthor author in loaded)
            {
                int index = int.Parse(author.Name.Substring(5), CultureInfo.InvariantCulture);
                Assert.True(author.Books.Count == (index % 5 == 0 ? 2 : 1), author.Name + " has " + author.Books.Count + " books");
                Assert.All(author.Books, b => Assert.Equal(author.Id, b.AuthorId));
            }
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include, Description = "Include of a non-navigation member throws ArgumentException; ThenInclude without Include throws InvalidOperationException")]
        public void IncludeArgumentValidation()
        {
            IRepository<CfAuthor> repository = Repository<CfAuthor>();
            ConformanceAssert.Throws<ArgumentException>(() => repository.Query().Include(a => a.Name), "Include(a => a.Name)");
            ConformanceAssert.Throws<InvalidOperationException>(() => repository.Query().ThenInclude((CfBook b) => b.Publisher), "ThenInclude without Include");
            ConformanceAssert.Throws<ArgumentNullException>(() => repository.Query().Include<CfPublisher?>(null!), "Include(null)");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Include | RepositoryCapabilities.Transactions, Description = "Includes see uncommitted rows inside the same transaction")]
        public async Task IncludeInsideTransaction()
        {
            await ResetAsync(ConformanceEntities.Library.ToArray());
            IRepository<CfAuthor> authors = Repository<CfAuthor>();
            IRepository<CfBook> books = Repository<CfBook>();
            using (ITransaction transaction = await authors.BeginTransactionAsync(Token))
            {
                for (int i = 0; i < 3; i++)
                {
                    CfAuthor author = await authors.CreateAsync(new CfAuthor { Name = "Tx " + i }, transaction, Token);
                    await books.CreateAsync(new CfBook { Title = "Tx " + i + " Book", AuthorId = author.Id }, transaction, Token);
                }

                List<CfAuthor> loaded = await ExecuteAsync(authors.Query(transaction).Include(a => a.Books).OrderBy(a => a.Name), "Query(transaction).Include(Books)");
                Assert.Equal(3, loaded.Count);
                Assert.All(loaded, a => Assert.Equal(a.Name + " Book", Assert.Single(a.Books).Title));
                await transaction.RollbackAsync(Token);
            }

            Assert.Equal(0L, await authors.CountAsync(null, null, Token));
            Assert.Equal(0L, await books.CountAsync(null, null, Token));
        }

        private static string[] Labels(IEnumerable<CfTag> tags)
        {
            return tags.Select(t => t.Label).OrderBy(l => l, StringComparer.Ordinal).ToArray();
        }

        private static string[] Names(IEnumerable<CfAuthor> authors)
        {
            return authors.Select(a => a.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        }
    }
}
