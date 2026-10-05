namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Coverage for advanced query composition: set operations (UNION / UNION ALL / INTERSECT / EXCEPT),
    /// EXISTS subqueries, and common table expressions. Executed identically across all database providers.
    /// </summary>
    public class AdvancedQueryTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="AdvancedQueryTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public AdvancedQueryTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// WHERE EXISTS returns rows when the correlated subquery has any results.
        /// </summary>
        [Fact]
        public async Task WhereExistsReturnsRowsWhenSubqueryHasResults()
        {
            ISqlRepository<Author> repository = await SeedAsync();
            ISqlRepository<Book> bookRepository = _Provider.CreateRepository<Book>();

            ISqlQueryBuilder<Book> books = bookRepository.Query();
            List<Author> results = (await repository.Query().WhereExists(books).ExecuteAsync()).ToList();

            Assert.Equal(4, results.Count);
        }

        /// <summary>
        /// A raw SQL predicate filters the query using a bound parameter.
        /// </summary>
        [Fact]
        public async Task RawPredicateFilters()
        {
            ISqlRepository<Author> repository = await SeedAsync();

            List<Author> results = (await repository.Query()
                .WhereRaw("company_id = {0}", 1)
                .ExecuteAsync()).ToList();

            Assert.Equal(2, results.Count);
            Assert.All(results, a => Assert.Equal(1, a.CompanyId));
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<Author>> SeedAsync()
        {
            ISqlRepository<Author> authorRepository = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> bookRepository = _Provider.CreateRepository<Book>();

            await bookRepository.ExecuteSqlAsync("DELETE FROM books");
            await authorRepository.ExecuteSqlAsync("DELETE FROM authors");

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Alpha", CompanyId = 1 },
                new Author { Name = "Beta", CompanyId = 1 },
                new Author { Name = "Gamma", CompanyId = 2 },
                new Author { Name = "Delta", CompanyId = 2 }
            };

            List<Author> created = new List<Author>();
            foreach (Author author in authors)
            {
                created.Add(await authorRepository.CreateAsync(author));
            }

            await bookRepository.CreateAsync(new Book { Title = "First", AuthorId = created[0].Id });

            return authorRepository;
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
