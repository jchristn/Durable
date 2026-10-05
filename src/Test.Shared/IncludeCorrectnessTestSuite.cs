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
    /// Exhaustive Include / ThenInclude correctness coverage (split-query loading) for every provider: reference,
    /// collection and many-to-many navigations, nested and sibling includes, paging, filtering, streaming, empty
    /// children and IN-list chunking with more than 2000 parents.
    /// </summary>
    public class IncludeCorrectnessTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="IncludeCorrectnessTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public IncludeCorrectnessTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Two sibling reference includes on Book load the author and the (optional) publisher; a null foreign key
        /// leaves the reference null.
        /// </summary>
        [Fact]
        public async Task IncludeReferenceNavigationsLoadsAuthorAndPublisher()
        {
            await ResetAsync();
            ISqlRepository<Company> companies = _Provider.CreateRepository<Company>();
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> books = _Provider.CreateRepository<Book>();

            Company publisher = await companies.CreateAsync(new Company { Name = "Penguin", Industry = "Publishing" });
            Author orwell = await authors.CreateAsync(new Author { Name = "Orwell" });
            Author huxley = await authors.CreateAsync(new Author { Name = "Huxley" });
            await books.CreateAsync(new Book { Title = "1984", AuthorId = orwell.Id, PublisherId = publisher.Id });
            await books.CreateAsync(new Book { Title = "Brave New World", AuthorId = huxley.Id, PublisherId = null });

            List<Book> loaded = (await books.Query()
                .Include(b => b.Author)
                .Include(b => b.Publisher)
                .OrderBy(b => b.Title)
                .ExecuteAsync()).ToList();

            Assert.Equal(2, loaded.Count);
            Book nineteen = loaded.Single(b => b.Title == "1984");
            Book brave = loaded.Single(b => b.Title == "Brave New World");
            Assert.NotNull(nineteen.Author);
            Assert.Equal("Orwell", nineteen.Author.Name);
            Assert.NotNull(nineteen.Publisher);
            Assert.Equal("Penguin", nineteen.Publisher.Name);
            Assert.NotNull(brave.Author);
            Assert.Equal("Huxley", brave.Author.Name);
            Assert.Null(brave.Publisher);
        }

        /// <summary>
        /// A collection include assigns each parent exactly its own children (no cross-contamination).
        /// </summary>
        [Fact]
        public async Task IncludeCollectionAssignsEachParentItsOwnChildren()
        {
            await ResetAsync();
            List<Author> seeded = await SeedAuthorsWithBooksAsync(5, 3);
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();

            List<Author> loaded = (await authors.Query().Include(a => a.Books).ExecuteAsync()).ToList();

            Assert.Equal(5, loaded.Count);
            foreach (Author author in loaded)
            {
                Assert.NotNull(author.Books);
                Assert.Equal(3, author.Books.Count);
                Assert.All(author.Books, b => Assert.Equal(author.Id, b.AuthorId));
                Assert.All(author.Books, b => Assert.StartsWith(author.Name + " Book ", b.Title));
            }
        }

        /// <summary>
        /// Many-to-many include loads through the junction table in both directions.
        /// </summary>
        [Fact]
        public async Task IncludeManyToManyLoadsBothDirections()
        {
            await ResetAsync();
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();
            ISqlRepository<Category> categories = _Provider.CreateRepository<Category>();
            ISqlRepository<AuthorCategory> links = _Provider.CreateRepository<AuthorCategory>();

            Author a1 = await authors.CreateAsync(new Author { Name = "A1" });
            Author a2 = await authors.CreateAsync(new Author { Name = "A2" });
            Author a3 = await authors.CreateAsync(new Author { Name = "A3" });
            Category fiction = await categories.CreateAsync(new Category { Name = "Fiction", Description = "F" });
            Category history = await categories.CreateAsync(new Category { Name = "History", Description = "H" });
            Category poetry = await categories.CreateAsync(new Category { Name = "Poetry", Description = "P" });

            await links.CreateManyAsync(new List<AuthorCategory>
            {
                new AuthorCategory { AuthorId = a1.Id, CategoryId = fiction.Id },
                new AuthorCategory { AuthorId = a1.Id, CategoryId = history.Id },
                new AuthorCategory { AuthorId = a2.Id, CategoryId = fiction.Id }
            });

            List<Author> loadedAuthors = (await authors.Query().Include(a => a.Categories).OrderBy(a => a.Name).ExecuteAsync()).ToList();
            Assert.Equal(3, loadedAuthors.Count);
            Assert.Equal(new[] { "Fiction", "History" }, loadedAuthors[0].Categories.Select(c => c.Name).OrderBy(n => n).ToArray());
            Assert.Equal(new[] { "Fiction" }, loadedAuthors[1].Categories.Select(c => c.Name).ToArray());
            Assert.NotNull(loadedAuthors[2].Categories);
            Assert.Empty(loadedAuthors[2].Categories);

            List<Category> loadedCategories = (await categories.Query().Include(c => c.Authors).OrderBy(c => c.Name).ExecuteAsync()).ToList();
            Assert.Equal(3, loadedCategories.Count);
            Assert.Equal(new[] { "A1", "A2" }, loadedCategories[0].Authors.Select(a => a.Name).OrderBy(n => n).ToArray());
            Assert.Equal(new[] { "A1" }, loadedCategories[1].Authors.Select(a => a.Name).ToArray());
            Assert.Empty(loadedCategories[2].Authors);
        }

        /// <summary>
        /// ThenInclude two levels deep: Author -> Books -> Publisher.
        /// </summary>
        [Fact]
        public async Task ThenIncludeAuthorBooksPublisher()
        {
            await ResetAsync();
            ISqlRepository<Company> companies = _Provider.CreateRepository<Company>();
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> books = _Provider.CreateRepository<Book>();

            Company p1 = await companies.CreateAsync(new Company { Name = "Pub One", Industry = "Publishing" });
            Company p2 = await companies.CreateAsync(new Company { Name = "Pub Two", Industry = "Publishing" });
            Author author = await authors.CreateAsync(new Author { Name = "Nested" });
            await books.CreateManyAsync(new List<Book>
            {
                new Book { Title = "B1", AuthorId = author.Id, PublisherId = p1.Id },
                new Book { Title = "B2", AuthorId = author.Id, PublisherId = p2.Id },
                new Book { Title = "B3", AuthorId = author.Id, PublisherId = null }
            });

            List<Author> loaded = (await authors.Query()
                .Include(a => a.Books)
                .ThenInclude((Book b) => b.Publisher)
                .ExecuteAsync()).ToList();

            Author single = Assert.Single(loaded);
            Assert.Equal(3, single.Books.Count);
            Assert.Equal("Pub One", single.Books.Single(b => b.Title == "B1").Publisher?.Name);
            Assert.Equal("Pub Two", single.Books.Single(b => b.Title == "B2").Publisher?.Name);
            Assert.Null(single.Books.Single(b => b.Title == "B3").Publisher);
        }

        /// <summary>
        /// ThenInclude two levels deep through collections: Company -> Employees (authors) -> Books, using the
        /// synchronous Execute path.
        /// </summary>
        [Fact]
        public async Task ThenIncludeCompanyEmployeesBooksSync()
        {
            await ResetAsync();
            ISqlRepository<Company> companies = _Provider.CreateRepository<Company>();
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> books = _Provider.CreateRepository<Book>();

            Company acme = await companies.CreateAsync(new Company { Name = "Acme", Industry = "Tech" });
            Company empty = await companies.CreateAsync(new Company { Name = "Empty Co", Industry = "None" });
            Author e1 = await authors.CreateAsync(new Author { Name = "E1", CompanyId = acme.Id });
            Author e2 = await authors.CreateAsync(new Author { Name = "E2", CompanyId = acme.Id });
            await authors.CreateAsync(new Author { Name = "Freelancer", CompanyId = null });
            await books.CreateManyAsync(new List<Book>
            {
                new Book { Title = "E1-a", AuthorId = e1.Id },
                new Book { Title = "E1-b", AuthorId = e1.Id },
                new Book { Title = "E2-a", AuthorId = e2.Id }
            });

            List<Company> loaded = companies.Query()
                .Include(c => c.Employees)
                .ThenInclude((Author a) => a.Books)
                .OrderBy(c => c.Name)
                .Execute()
                .ToList();

            Assert.Equal(2, loaded.Count);
            Company loadedAcme = loaded.Single(c => c.Id == acme.Id);
            Company loadedEmpty = loaded.Single(c => c.Id == empty.Id);
            Assert.Equal(2, loadedAcme.Employees.Count);
            Assert.Equal(2, loadedAcme.Employees.Single(a => a.Name == "E1").Books.Count);
            Assert.Equal("E2-a", Assert.Single(loadedAcme.Employees.Single(a => a.Name == "E2").Books).Title);
            Assert.NotNull(loadedEmpty.Employees);
            Assert.Empty(loadedEmpty.Employees);
        }

        /// <summary>
        /// Several sibling includes (collection, reference, many-to-many) on the same root are all populated.
        /// </summary>
        [Fact]
        public async Task MultipleSiblingIncludesAllLoad()
        {
            await ResetAsync();
            ISqlRepository<Company> companies = _Provider.CreateRepository<Company>();
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> books = _Provider.CreateRepository<Book>();
            ISqlRepository<Category> categories = _Provider.CreateRepository<Category>();
            ISqlRepository<AuthorCategory> links = _Provider.CreateRepository<AuthorCategory>();

            Company company = await companies.CreateAsync(new Company { Name = "Sibling Co", Industry = "Books" });
            Author author = await authors.CreateAsync(new Author { Name = "Sibling", CompanyId = company.Id });
            Category category = await categories.CreateAsync(new Category { Name = "Drama", Description = "D" });
            await links.CreateAsync(new AuthorCategory { AuthorId = author.Id, CategoryId = category.Id });
            await books.CreateManyAsync(new List<Book>
            {
                new Book { Title = "S1", AuthorId = author.Id },
                new Book { Title = "S2", AuthorId = author.Id }
            });

            Author loaded = Assert.Single(await authors.Query()
                .Include(a => a.Books)
                .Include(a => a.Company)
                .Include(a => a.Categories)
                .Where(a => a.Id == author.Id)
                .ExecuteAsync());

            Assert.Equal(2, loaded.Books.Count);
            Assert.NotNull(loaded.Company);
            Assert.Equal("Sibling Co", loaded.Company.Name);
            Assert.Equal("Drama", Assert.Single(loaded.Categories).Name);
        }

        /// <summary>
        /// Include combined with Skip/Take returns exactly Take root entities, each with its complete collection
        /// (paging must apply to roots, not to joined child rows).
        /// </summary>
        [Fact]
        public async Task IncludeWithSkipTakeReturnsExactRootsWithCompleteChildren()
        {
            await ResetAsync();
            await SeedAuthorsWithBooksAsync(10, 4);
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();

            List<Author> page = (await authors.Query()
                .Include(a => a.Books)
                .OrderBy(a => a.Name)
                .Skip(3)
                .Take(4)
                .ExecuteAsync()).ToList();

            Assert.Equal(4, page.Count);
            Assert.Equal(new[] { "Author 03", "Author 04", "Author 05", "Author 06" }, page.Select(a => a.Name).ToArray());
            Assert.All(page, a => Assert.Equal(4, a.Books.Count));
            Assert.All(page, a => Assert.All(a.Books, b => Assert.Equal(a.Id, b.AuthorId)));
        }

        /// <summary>
        /// Include combined with Where and OrderByDescending filters and orders roots and loads their children.
        /// </summary>
        [Fact]
        public async Task IncludeWithWhereAndOrderBy()
        {
            await ResetAsync();
            await SeedAuthorsWithBooksAsync(6, 2);
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();

            List<Author> loaded = (await authors.Query()
                .Include(a => a.Books)
                .Where(a => a.Name != "Author 00" && a.Name != "Author 05")
                .OrderByDescending(a => a.Name)
                .ExecuteAsync()).ToList();

            Assert.Equal(new[] { "Author 04", "Author 03", "Author 02", "Author 01" }, loaded.Select(a => a.Name).ToArray());
            Assert.All(loaded, a => Assert.Equal(2, a.Books.Count));
        }

        /// <summary>
        /// ExecuteAsyncEnumerable with Include and no transaction streams every root with its children, across
        /// multiple include batches (batch size 3).
        /// </summary>
        [Fact]
        public async Task IncludeStreamingWithoutTransaction()
        {
            await ResetAsync();
            await SeedAuthorsWithBooksAsync(10, 2);
            SqlRepositoryOptions options = new SqlRepositoryOptions { IncludeStreamingBatchSize = 3 };
            ISqlRepository<Author> authors = RelTestHelpers.CreateRepository<Author>(_Provider, options);

            List<Author> streamed = new List<Author>();
            await foreach (Author author in authors.Query().Include(a => a.Books).ThenInclude((Book b) => b.Publisher).OrderBy(a => a.Name).ExecuteAsyncEnumerable())
            {
                streamed.Add(author);
            }

            Assert.Equal(10, streamed.Count);
            Assert.Equal(Enumerable.Range(0, 10).Select(i => "Author " + i.ToString("00", CultureInfo.InvariantCulture)).ToArray(), streamed.Select(a => a.Name).ToArray());
            Assert.All(streamed, a => Assert.Equal(2, a.Books.Count));
            Assert.All(streamed, a => Assert.All(a.Books, b => Assert.Equal(a.Id, b.AuthorId)));
        }

        /// <summary>
        /// ExecuteAsyncEnumerable with Include inside a transaction sees uncommitted rows (roots and children) written
        /// on that transaction; the rollback removes them.
        /// </summary>
        [Fact]
        public async Task IncludeStreamingWithinTransaction()
        {
            await ResetAsync();
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> books = _Provider.CreateRepository<Book>();

            using (ISqlTransaction transaction = await authors.BeginTransactionAsync())
            {
                List<Author> created = new List<Author>();
                for (int i = 0; i < 4; i++)
                {
                    Author author = await authors.CreateAsync(new Author { Name = "Tx " + i }, transaction);
                    created.Add(author);
                    await books.CreateAsync(new Book { Title = "Tx " + i + " Book", AuthorId = author.Id }, transaction);
                }

                List<Author> streamed = new List<Author>();
                await foreach (Author author in authors.Query(transaction).Include(a => a.Books).OrderBy(a => a.Name).ExecuteAsyncEnumerable())
                {
                    streamed.Add(author);
                }

                Assert.Equal(4, streamed.Count);
                Assert.All(streamed, a => Assert.Equal(a.Name + " Book", Assert.Single(a.Books).Title));
                transaction.Rollback();
            }

            Assert.Equal(0, await authors.CountAsync());
        }

        /// <summary>
        /// A collection include on parents without children assigns an empty list (not null), even when the property
        /// has no initializer.
        /// </summary>
        [Fact]
        public async Task IncludeWithNoChildrenAssignsEmptyList()
        {
            ISqlRepository<RelSoftParent> parents = _Provider.CreateRepository<RelSoftParent>();
            ISqlRepository<RelSoftNote> notes = _Provider.CreateRepository<RelSoftNote>();
            await RelTestHelpers.RecreateTableAsync(parents);
            await RelTestHelpers.RecreateTableAsync(notes);

            RelSoftParent withNotes = await parents.CreateAsync(new RelSoftParent { Name = "With" });
            RelSoftParent without = await parents.CreateAsync(new RelSoftParent { Name = "Without" });
            await notes.CreateAsync(new RelSoftNote { ParentId = withNotes.Id, Text = "n1" });

            RelSoftParent raw = Assert.Single(await parents.Query().Where(p => p.Id == without.Id).ExecuteAsync());
            Assert.Null(raw.Notes);

            List<RelSoftParent> loaded = (await parents.Query().Include(p => p.Notes).OrderBy(p => p.Name).ExecuteAsync()).ToList();
            Assert.Equal(2, loaded.Count);
            Assert.NotNull(loaded[0].Notes);
            Assert.Single(loaded[0].Notes!);
            Assert.NotNull(loaded[1].Notes);
            Assert.Empty(loaded[1].Notes!);
        }

        /// <summary>
        /// Include with more than 2000 parents splits the IN list into chunks of (MaxParameters - 50) keys and still
        /// assigns every child to the right parent.
        /// </summary>
        [Fact]
        public async Task IncludeWithMoreThanTwoThousandParentsChunksInList()
        {
            await ResetAsync();
            const int parentCount = 2100;
            ISqlRepository<Author> seedAuthors = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> seedBooks = _Provider.CreateRepository<Book>();

            List<Author> created = (await seedAuthors.CreateManyAsync(
                Enumerable.Range(0, parentCount).Select(i => new Author { Name = "Bulk " + i.ToString("0000", CultureInfo.InvariantCulture) }).ToList())).ToList();
            Assert.Equal(parentCount, created.Select(a => a.Id).Distinct().Count());

            List<Book> childRows = created.Select(a => new Book { Title = a.Name + " Book", AuthorId = a.Id }).ToList();
            childRows.AddRange(created.Where((a, i) => i % 7 == 0).Select(a => new Book { Title = a.Name + " Extra", AuthorId = a.Id }));
            await seedBooks.BulkInsertAsync(childRows);

            CountingCommandInterceptor counter = new CountingCommandInterceptor();
            SqlRepositoryOptions options = new SqlRepositoryOptions();
            options.Interceptors.Add(counter);
            ISqlRepository<Author> authors = RelTestHelpers.CreateRepository<Author>(_Provider, options);

            List<Author> loaded = (await authors.Query().Include(a => a.Books).ExecuteAsync()).ToList();

            Assert.Equal(parentCount, loaded.Count);
            for (int i = 0; i < loaded.Count; i++)
            {
                Author author = loaded[i];
                int index = int.Parse(author.Name.Substring(5), CultureInfo.InvariantCulture);
                int expected = index % 7 == 0 ? 2 : 1;
                Assert.Equal(expected, author.Books.Count);
                Assert.All(author.Books, b => Assert.Equal(author.Id, b.AuthorId));
            }

            int chunkSize = Math.Max(1, authors.Dialect.MaxParameters - 50);
            int expectedStatements = (parentCount + chunkSize - 1) / chunkSize;
            Assert.Equal(expectedStatements, counter.CountOf("INCLUDE"));
            Console.WriteLine("     " + parentCount + " parents loaded with " + counter.CountOf("INCLUDE") + " include statement(s)");
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private async Task ResetAsync()
        {
            ISqlRepository<Author> repository = _Provider.CreateRepository<Author>();
            await repository.ExecuteSqlAsync("DELETE FROM author_categories");
            await repository.ExecuteSqlAsync("DELETE FROM books");
            await repository.ExecuteSqlAsync("DELETE FROM authors");
            await repository.ExecuteSqlAsync("DELETE FROM categories");
            await repository.ExecuteSqlAsync("DELETE FROM companies");
        }

        private async Task<List<Author>> SeedAuthorsWithBooksAsync(int authorCount, int booksPerAuthor)
        {
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> books = _Provider.CreateRepository<Book>();

            List<Author> created = (await authors.CreateManyAsync(Enumerable.Range(0, authorCount)
                .Select(i => new Author { Name = "Author " + i.ToString("00", CultureInfo.InvariantCulture) })
                .ToList())).ToList();

            List<Book> rows = new List<Book>();
            foreach (Author author in created)
            {
                for (int b = 0; b < booksPerAuthor; b++) rows.Add(new Book { Title = author.Name + " Book " + b, AuthorId = author.Id });
            }

            await books.CreateManyAsync(rows);
            return created;
        }

        #endregion
    }
}
