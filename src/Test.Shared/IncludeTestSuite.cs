namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Test suite for validating Include/Join functionality for related entity loading.
    /// </summary>
    public class IncludeTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="IncludeTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public IncludeTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests Include functionality for one-to-many relationships.
        /// </summary>
        [Fact]
        public async Task CanIncludeOneToManyRelationships()
        {
            IRepository<Author> authorRepo = _Provider.CreateRepository<Author>();
            IRepository<Book> bookRepo = _Provider.CreateRepository<Book>();

            await authorRepo.ExecuteSqlAsync("DELETE FROM author_categories");
            await authorRepo.ExecuteSqlAsync("DELETE FROM books");
            await authorRepo.ExecuteSqlAsync("DELETE FROM authors");

            Author author = new Author
            {
                Name = "J.K. Rowling"
            };

            Author createdAuthor = await authorRepo.CreateAsync(author);

            Book[] books = new[]
            {
                new Book { Title = "Harry Potter 1", AuthorId = createdAuthor.Id },
                new Book { Title = "Harry Potter 2", AuthorId = createdAuthor.Id }
            };

            await bookRepo.CreateManyAsync(books);

            Author[] authorsWithBooks = (await authorRepo.Query()
                .Include(a => a.Books)
                .ExecuteAsync())
                .ToArray();

            Assert.Single(authorsWithBooks);
            Assert.NotNull(authorsWithBooks[0].Books);
            Assert.Equal(2, authorsWithBooks[0].Books.Count);

            // Check that both books exist (order-independent)
            List<string> bookTitles = authorsWithBooks[0].Books.Select(b => b.Title).ToList();
            Assert.Contains("Harry Potter 1", bookTitles);
            Assert.Contains("Harry Potter 2", bookTitles);

            Console.WriteLine($"     Loaded author with {authorsWithBooks[0].Books.Count} books");
        }

        /// <summary>
        /// Tests Include functionality for many-to-one relationships.
        /// </summary>
        [Fact]
        public async Task CanIncludeManyToOneRelationships()
        {
            IRepository<Author> authorRepo = _Provider.CreateRepository<Author>();
            IRepository<Book> bookRepo = _Provider.CreateRepository<Book>();

            await authorRepo.ExecuteSqlAsync("DELETE FROM author_categories");
            await authorRepo.ExecuteSqlAsync("DELETE FROM books");
            await authorRepo.ExecuteSqlAsync("DELETE FROM authors");

            Author author = new Author
            {
                Name = "George Orwell"
            };

            Author createdAuthor = await authorRepo.CreateAsync(author);

            Book book = new Book
            {
                Title = "1984",
                AuthorId = createdAuthor.Id
            };

            await bookRepo.CreateAsync(book);

            Book[] booksWithAuthor = (await bookRepo.Query()
                .Include(b => b.Author)
                .ExecuteAsync())
                .ToArray();

            Assert.Single(booksWithAuthor);
            Assert.NotNull(booksWithAuthor[0].Author);
            Assert.True(ValidationHelpers.AreStringsEqual("George Orwell", booksWithAuthor[0].Author.Name));

            Console.WriteLine($"     Loaded book with author: {booksWithAuthor[0].Author.Name}");
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
