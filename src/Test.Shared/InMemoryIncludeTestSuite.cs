namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;
    using Xunit;

    /// <summary>
    /// Split-query Include/ThenInclude through the backend-neutral include loader: reference, collection and many-to-many
    /// navigations, nested includes, paging of roots, chunked key lists, streaming, transactions and soft-deleted related
    /// rows (excluded from includes and from collection predicates).
    /// </summary>
    public class InMemoryIncludeTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// Reference includes assign the related entity, or null when the foreign key is null or matches nothing.
        /// </summary>
        [Fact]
        public async Task ReferenceIncludeAssignsRelatedEntity()
        {
            InMemoryLibraryData d = await InMemoryLibraryData.CreateAsync();
            List<Author> authors = (await d.Authors.Query().Include(a => a.Company).OrderBy(a => a.Name).ExecuteAsync()).ToList();
            Assert.Equal("Penguin", authors[0].Company!.Name);
            Assert.Equal("Orbit", authors[1].Company!.Name);
            Assert.Null(authors[2].Company);
            Assert.Null(authors[3].Company);

            List<Book> books = d.Books.Query().Include(b => b.Author).Include(b => b.Publisher).Execute().ToList();
            Assert.All(books, b => Assert.NotNull(b.Author));
            Assert.Null(books.Single(b => b.Title == "Persuasion").Publisher);
            Assert.Equal("Orbit", books.Single(b => b.Title == "Excession").Publisher!.Name);
            Assert.Null(d.Books.ReadFirst()!.Author);
        }

        /// <summary>
        /// Collection includes assign lists ordered by related key, and empty lists when nothing matches.
        /// </summary>
        [Fact]
        public async Task CollectionIncludeAssignsLists()
        {
            InMemoryLibraryData d = await InMemoryLibraryData.CreateAsync();
            List<Author> authors = (await d.Authors.Query().Include(a => a.Books).OrderBy(a => a.Name).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Emma", "Persuasion" }, authors[0].Books.Select(b => b.Title).ToArray());
            Assert.Equal(new[] { "Excession", "Player of Games" }, authors[1].Books.Select(b => b.Title).ToArray());
            Assert.Empty(authors[2].Books);
            Assert.Empty(authors[3].Books);

            Company orbit = (await d.Companies.Query().Include(c => c.Employees).Include(c => c.PublishedBooks).Where(c => c.Name == "Orbit").ExecuteAsync()).Single();
            Assert.Equal("Banks", orbit.Employees.Single().Name);
            Assert.Equal(2, orbit.PublishedBooks.Count);
        }

        /// <summary>
        /// Many-to-many includes read the junction by owner key and the related rows by remote key, in both directions.
        /// </summary>
        [Fact]
        public async Task ManyToManyIncludeUsesJunction()
        {
            InMemoryLibraryData d = await InMemoryLibraryData.CreateAsync();
            List<Author> authors = (await d.Authors.Query().Include(a => a.Categories).OrderBy(a => a.Name).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Classic" }, authors[0].Categories.Select(c => c.Name).ToArray());
            Assert.Equal(new[] { "Classic", "SciFi" }, authors[1].Categories.Select(c => c.Name).ToArray());
            Assert.Equal(new[] { "Classic" }, authors[2].Categories.Select(c => c.Name).ToArray());
            Assert.Empty(authors[3].Categories);

            List<Category> categories = (await d.Categories.Query().Include(c => c.Authors).OrderBy(c => c.Name).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Austen", "Banks", "Christie" }, categories[0].Authors.Select(a => a.Name).ToArray());
            Assert.Empty(categories[1].Authors);
            Assert.Equal(new[] { "Banks" }, categories[2].Authors.Select(a => a.Name).ToArray());
        }

        /// <summary>
        /// ThenInclude and dotted paths load nested navigations, including through collections and many-to-many.
        /// </summary>
        [Fact]
        public async Task NestedIncludes()
        {
            InMemoryLibraryData d = await InMemoryLibraryData.CreateAsync();
            List<Book> books = (await d.Books.Query().Include(b => b.Author).ThenInclude<Author, Company?>(a => a.Company).ExecuteAsync()).ToList();
            Assert.Equal("Penguin", books.Single(b => b.Title == "Emma").Author!.Company!.Name);

            List<Book> dotted = (await d.Books.Query().Include(b => b.Author!.Company).ExecuteAsync()).ToList();
            Assert.Equal("Orbit", dotted.Single(b => b.Title == "Excession").Author!.Company!.Name);

            Author banks = (await d.Authors.Query().Where(a => a.Name == "Banks")
                .Include(a => a.Books).ThenInclude<Book, Company?>(b => b.Publisher)
                .Include(a => a.Categories).ThenInclude<Category, List<Author>>(c => c.Authors)
                .ExecuteAsync()).Single();
            Assert.All(banks.Books, b => Assert.Equal("Orbit", b.Publisher!.Name));
            Assert.Equal(new[] { "Austen", "Banks", "Christie" }, banks.Categories.Single(c => c.Name == "Classic").Authors.Select(a => a.Name).ToArray());

            Assert.Throws<InvalidOperationException>(() => d.Books.Query().ThenInclude<Author, Company?>(a => a.Company));
            Assert.Throws<ArgumentException>(() => d.Books.Query().Include(b => b.Title));
        }

        /// <summary>
        /// Paging applies to roots; includes are loaded in chunks of IncludeChunkSize keys; streaming with includes loads
        /// per batch of IncludeStreamingBatchSize roots.
        /// </summary>
        [Fact]
        public async Task PagingChunkingAndStreaming()
        {
            InMemoryLibraryData d = await InMemoryLibraryData.CreateAsync();
            List<Author> page = (await d.Authors.Query().Include(a => a.Books).OrderBy(a => a.Name).Take(1).ExecuteAsync()).ToList();
            Assert.Single(page);
            Assert.Equal(2, page[0].Books.Count);

            d.Authors.IncludeChunkSize = 1;
            d.Authors.IncludeStreamingBatchSize = 3;
            Assert.Throws<ArgumentOutOfRangeException>(() => d.Authors.IncludeChunkSize = 0);
            List<Author> streamed = new List<Author>();
            await foreach (Author author in d.Authors.Query().Include(a => a.Books).Include(a => a.Categories).OrderBy(a => a.Name).ExecuteAsyncEnumerable()) streamed.Add(author);
            Assert.Equal(4, streamed.Count);
            Assert.Equal(new[] { 2, 2, 0, 0 }, streamed.Select(a => a.Books.Count).ToArray());
            Assert.Equal(new[] { 1, 2, 1, 0 }, streamed.Select(a => a.Categories.Count).ToArray());
        }

        /// <summary>
        /// Soft-deleted related rows are excluded from includes and from collection predicates.
        /// </summary>
        [Fact]
        public async Task SoftDeletedRelatedRowsAreExcluded()
        {
            InMemoryBackend backend = new InMemoryBackend();
            InMemoryRepository<RelSoftParent> parents = backend.CreateRepository<RelSoftParent>();
            InMemoryRepository<RelSoftNote> notes = backend.CreateRepository<RelSoftNote>();
            RelSoftParent p1 = await parents.CreateAsync(new RelSoftParent { Name = "P1" });
            RelSoftParent p2 = await parents.CreateAsync(new RelSoftParent { Name = "P2" });
            await notes.CreateManyAsync(new[]
            {
                new RelSoftNote { ParentId = p1.Id, Text = "keep" },
                new RelSoftNote { ParentId = p1.Id, Text = "drop" },
                new RelSoftNote { ParentId = p2.Id, Text = "drop" }
            });
            Assert.Equal(2, await notes.DeleteManyAsync(x => x.Text == "drop"));

            List<RelSoftParent> loaded = (await parents.Query().Include(p => p.Notes).OrderBy(p => p.Name).ExecuteAsync()).ToList();
            Assert.Equal("keep", loaded[0].Notes!.Single().Text);
            Assert.NotNull(loaded[1].Notes);
            Assert.Empty(loaded[1].Notes!);

            Assert.Equal(new[] { "P1" }, (await parents.Query().Where(p => p.Notes!.Any()).ExecuteAsync()).Select(p => p.Name).ToArray());
            Assert.Equal(1, await parents.CountAsync(p => p.Notes!.Count() == 0));
        }

        /// <summary>
        /// Includes inside a transaction see the transaction's uncommitted related rows.
        /// </summary>
        [Fact]
        public async Task IncludeInsideTransactionSeesOwnWrites()
        {
            InMemoryLibraryData d = await InMemoryLibraryData.CreateAsync();
            await using ITransaction transaction = await d.Authors.BeginTransactionAsync();
            await d.Books.CreateAsync(new Book { Title = "Uncommitted", AuthorId = d.Dick.Id }, transaction);

            Author inside = (await d.Authors.Query(transaction).Include(a => a.Books).Where(a => a.Name == "Dick").ExecuteAsync()).Single();
            Assert.Equal("Uncommitted", inside.Books.Single().Title);
            Author outside = (await d.Authors.Query().Include(a => a.Books).Where(a => a.Name == "Dick").ExecuteAsync()).Single();
            Assert.Empty(outside.Books);
        }

        #endregion
    }
}
