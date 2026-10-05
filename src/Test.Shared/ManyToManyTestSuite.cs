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
    /// Coverage for many-to-many navigation loading via Include. Runs on every provider.
    /// </summary>
    public class ManyToManyTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ManyToManyTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public ManyToManyTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Include loads a many-to-many navigation collection through the join entity.
        /// </summary>
        [Fact]
        public async Task IncludeLoadsManyToManyRelationships()
        {
            ISqlRepository<Author> authorRepository = _Provider.CreateRepository<Author>();
            ISqlRepository<Category> categoryRepository = _Provider.CreateRepository<Category>();
            ISqlRepository<AuthorCategory> linkRepository = _Provider.CreateRepository<AuthorCategory>();

            await linkRepository.ExecuteSqlAsync("DELETE FROM author_categories");
            await linkRepository.ExecuteSqlAsync("DELETE FROM categories");
            await linkRepository.ExecuteSqlAsync("DELETE FROM authors");

            Author author = await authorRepository.CreateAsync(new Author { Name = "Prolific", CompanyId = 1 });
            Category fiction = await categoryRepository.CreateAsync(new Category { Name = "Fiction", Description = "Fiction works" });
            Category history = await categoryRepository.CreateAsync(new Category { Name = "History", Description = "History works" });

            await linkRepository.CreateAsync(new AuthorCategory { AuthorId = author.Id, CategoryId = fiction.Id });
            await linkRepository.CreateAsync(new AuthorCategory { AuthorId = author.Id, CategoryId = history.Id });

            List<Author> results = (await authorRepository.Query()
                .Include(a => a.Categories)
                .Where(a => a.Id == author.Id)
                .ExecuteAsync()).ToList();

            Assert.Single(results);
            Assert.NotNull(results[0].Categories);
            Assert.Equal(2, results[0].Categories.Count);
            Assert.Contains(results[0].Categories, c => c.Name == "Fiction");
            Assert.Contains(results[0].Categories, c => c.Name == "History");
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
