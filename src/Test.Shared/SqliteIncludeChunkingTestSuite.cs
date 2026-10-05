namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// SQLite-only coverage for Include IN-list chunking. SQLite's real parameter limit (32000) is too large to chunk
    /// with a reasonable row count, so these tests use <see cref="ChunkingSqliteRepository{T}"/> whose dialect allows
    /// only 60 parameters (10 keys per include statement).
    /// </summary>
    public class SqliteIncludeChunkingTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SqliteIncludeChunkingTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The SQLite repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public SqliteIncludeChunkingTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Collection and nested reference includes over 25 parents are split into three IN-list chunks and every
        /// child lands on its own parent.
        /// </summary>
        [Fact]
        public async Task CollectionIncludeIsChunkedAcrossStatements()
        {
            await ResetAsync();
            ISqlRepository<Author> seed = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> books = _Provider.CreateRepository<Book>();
            ISqlRepository<Company> companies = _Provider.CreateRepository<Company>();

            Company publisher = await companies.CreateAsync(new Company { Name = "Chunk Pub", Industry = "P" });
            List<Author> created = (await seed.CreateManyAsync(Enumerable.Range(0, 25)
                .Select(i => new Author { Name = "C" + i.ToString("00", CultureInfo.InvariantCulture) }).ToList())).ToList();
            List<Book> rows = new List<Book>();
            foreach (Author author in created)
            {
                rows.Add(new Book { Title = author.Name + "-1", AuthorId = author.Id, PublisherId = publisher.Id });
                rows.Add(new Book { Title = author.Name + "-2", AuthorId = author.Id });
            }

            await books.CreateManyAsync(rows);

            CountingCommandInterceptor counter = new CountingCommandInterceptor();
            SqlRepositoryOptions options = new SqlRepositoryOptions();
            options.Interceptors.Add(counter);
            ChunkingSqliteRepository<Author> authors = new ChunkingSqliteRepository<Author>(seed.ConnectionFactory, options);

            List<Author> loaded = (await authors.Query().Include(a => a.Books).ThenInclude((Book b) => b.Publisher).ExecuteAsync()).ToList();

            Assert.Equal(25, loaded.Count);
            Assert.All(loaded, a => Assert.Equal(2, a.Books.Count));
            Assert.All(loaded, a => Assert.All(a.Books, b => Assert.Equal(a.Id, b.AuthorId)));
            Assert.All(loaded, a => Assert.Equal("Chunk Pub", a.Books.Single(b => b.Title.EndsWith("-1", StringComparison.Ordinal)).Publisher?.Name));
            Assert.All(loaded, a => Assert.Null(a.Books.Single(b => b.Title.EndsWith("-2", StringComparison.Ordinal)).Publisher));

            // 25 author keys -> 3 chunks for Books; 1 distinct publisher key -> 1 statement for Publisher.
            Assert.Equal(4, counter.CountOf("INCLUDE"));
        }

        /// <summary>
        /// Many-to-many include over 23 parents is chunked and correct.
        /// </summary>
        [Fact]
        public async Task ManyToManyIncludeIsChunkedAcrossStatements()
        {
            await ResetAsync();
            ISqlRepository<Author> seed = _Provider.CreateRepository<Author>();
            ISqlRepository<Category> categories = _Provider.CreateRepository<Category>();
            ISqlRepository<AuthorCategory> links = _Provider.CreateRepository<AuthorCategory>();

            Category even = await categories.CreateAsync(new Category { Name = "Even", Description = "E" });
            Category odd = await categories.CreateAsync(new Category { Name = "Odd", Description = "O" });
            List<Author> created = (await seed.CreateManyAsync(Enumerable.Range(0, 23)
                .Select(i => new Author { Name = "M" + i.ToString("00", CultureInfo.InvariantCulture) }).ToList())).ToList();
            await links.CreateManyAsync(created.Select((a, i) => new AuthorCategory { AuthorId = a.Id, CategoryId = i % 2 == 0 ? even.Id : odd.Id }).ToList());

            CountingCommandInterceptor counter = new CountingCommandInterceptor();
            SqlRepositoryOptions options = new SqlRepositoryOptions();
            options.Interceptors.Add(counter);
            ChunkingSqliteRepository<Author> authors = new ChunkingSqliteRepository<Author>(seed.ConnectionFactory, options);

            List<Author> loaded = (await authors.Query().Include(a => a.Categories).OrderBy(a => a.Name).ExecuteAsync()).ToList();

            Assert.Equal(23, loaded.Count);
            for (int i = 0; i < loaded.Count; i++)
            {
                Assert.Equal(i % 2 == 0 ? "Even" : "Odd", Assert.Single(loaded[i].Categories).Name);
            }

            Assert.Equal(3, counter.CountOf("INCLUDE"));
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

        #endregion
    }
}
