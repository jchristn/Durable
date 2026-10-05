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
    /// Coverage for SQL set operations (UNION, UNION ALL, INTERSECT, EXCEPT). Executed identically across all
    /// database providers.
    /// </summary>
    public class SetOperationTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SetOperationTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public SetOperationTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// UNION combines two result sets and removes duplicates.
        /// </summary>
        [Fact]
        public async Task UnionCombinesDistinct()
        {
            ISqlRepository<Author> repository = await SeedAsync();

            ISqlQueryBuilder<Author> company1 = repository.Query().Where(a => a.CompanyId == 1);
            ISqlQueryBuilder<Author> company2 = repository.Query().Where(a => a.CompanyId == 2);

            List<Author> results = (await company1.Union(company2).ExecuteAsync()).ToList();
            Assert.Equal(4, results.Count);
        }

        /// <summary>
        /// UNION ALL keeps duplicate rows.
        /// </summary>
        [Fact]
        public async Task UnionAllKeepsDuplicates()
        {
            ISqlRepository<Author> repository = await SeedAsync();

            ISqlQueryBuilder<Author> company1 = repository.Query().Where(a => a.CompanyId == 1);
            ISqlQueryBuilder<Author> company1Again = repository.Query().Where(a => a.CompanyId == 1);

            List<Author> results = (await company1.UnionAll(company1Again).ExecuteAsync()).ToList();
            Assert.Equal(4, results.Count);
        }

        /// <summary>
        /// INTERSECT returns only rows common to both queries.
        /// </summary>
        [Fact]
        public async Task IntersectReturnsCommonRows()
        {
            ISqlRepository<Author> repository = await SeedAsync();

            ISqlQueryBuilder<Author> company1 = repository.Query().Where(a => a.CompanyId == 1);
            ISqlQueryBuilder<Author> named = repository.Query().Where(a => a.Name == "Alpha" || a.Name == "Gamma");

            List<Author> results = (await company1.Intersect(named).ExecuteAsync()).ToList();
            Assert.Single(results);
            Assert.Equal("Alpha", results[0].Name);
        }

        /// <summary>
        /// EXCEPT returns rows in the first query not present in the second.
        /// </summary>
        [Fact]
        public async Task ExceptRemovesSecondQueryRows()
        {
            ISqlRepository<Author> repository = await SeedAsync();

            ISqlQueryBuilder<Author> company1 = repository.Query().Where(a => a.CompanyId == 1);
            ISqlQueryBuilder<Author> named = repository.Query().Where(a => a.Name == "Alpha" || a.Name == "Gamma");

            List<Author> results = (await company1.Except(named).ExecuteAsync()).ToList();
            Assert.Single(results);
            Assert.Equal("Beta", results[0].Name);
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

            foreach (Author author in authors)
            {
                await authorRepository.CreateAsync(author);
            }

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
