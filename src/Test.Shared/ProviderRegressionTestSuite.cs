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
    /// Regression coverage for provider bugs reported against the pre-0.3 per-provider code: chained Where/Having
    /// grouping, enums stored as text, binary columns and literals, custom-typed and Guid keys, synchronous Include,
    /// empty IN lists and schema-qualified tables.
    /// </summary>
    public class ProviderRegressionTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ProviderRegressionTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public ProviderRegressionTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Where(a).Where(b || c) keeps the OR inside its own group (a AND (b OR c)), sync and async.
        /// </summary>
        [Fact]
        public async Task ChainedWhereWithOrIsGrouped()
        {
            ISqlRepository<RegEnumItem> repository = await SeedEnumItemsAsync();

            List<RegEnumItem> matches = (await repository.Query()
                .Where(i => i.Score > 10)
                .Where(i => i.Name == "a" || i.Name == "c")
                .ExecuteAsync()).ToList();
            Assert.Equal(new[] { "c" }, matches.Select(i => i.Name).ToArray());

            List<RegEnumItem> syncMatches = repository.Query()
                .Where(i => i.Score > 10)
                .Where(i => i.Name == "a" || i.Name == "c")
                .Execute()
                .ToList();
            Assert.Equal(new[] { "c" }, syncMatches.Select(i => i.Name).ToArray());
        }

        /// <summary>
        /// Two Having calls, the second containing OR, keep their grouping.
        /// </summary>
        [Fact]
        public async Task ChainedHavingWithOrIsGrouped()
        {
            ISqlRepository<RegEnumItem> repository = await SeedEnumItemsAsync();

            List<IGrouping<RegStatus, RegEnumItem>> groups = (await repository.Query()
                .GroupBy(i => i.Status)
                .Having(g => g.Count() > 1)
                .Having(g => g.Sum(i => i.Score) < 0 || g.Sum(i => i.Score) > 25)
                .ExecuteAsync()).ToList();

            Assert.Equal(new[] { RegStatus.Active }, groups.Select(g => g.Key).ToArray());
        }

        /// <summary>
        /// Comparisons on an enum stored as text bind the member name (equality, inequality, Contains), and an enum
        /// stored as an integer still compares numerically.
        /// </summary>
        [Fact]
        public async Task EnumStoredAsTextComparesByName()
        {
            ISqlRepository<RegEnumItem> repository = await SeedEnumItemsAsync();
            RegStatus archived = RegStatus.Archived;
            List<RegStatus> wanted = new List<RegStatus> { RegStatus.Draft, RegStatus.Archived };

            Assert.Equal(new[] { "a", "c" }, (await repository.Query().Where(i => i.Status == RegStatus.Active).OrderBy(i => i.Name).ExecuteAsync()).Select(i => i.Name).ToArray());
            Assert.Equal(new[] { "d" }, repository.Query().Where(i => i.Status == archived).Execute().Select(i => i.Name).ToArray());
            Assert.Equal(3, await repository.CountAsync(i => i.Status != RegStatus.Draft));
            Assert.Equal(new[] { "b", "d" }, (await repository.Query().Where(i => wanted.Contains(i.Status)).OrderBy(i => i.Name).ExecuteAsync()).Select(i => i.Name).ToArray());
            Assert.Equal(new[] { "a" }, (await repository.Query().Where(i => i.Rank == RegStatus.Draft).ExecuteAsync()).Select(i => i.Name).ToArray());
            Assert.Equal(new[] { "c", "d" }, (await repository.Query().Where(i => i.Rank > RegStatus.Draft).OrderBy(i => i.Name).ExecuteAsync()).Select(i => i.Name).ToArray());
        }

        /// <summary>
        /// Binary values round-trip, filter by equality, and render as a binary literal in the captured SQL.
        /// </summary>
        [Fact]
        public async Task BinaryValuesBindAndRenderAsBinary()
        {
            ISqlRepository<RegBinaryItem> repository = _Provider.CreateRepository<RegBinaryItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            byte[] first = new byte[] { 0x00, 0x01, 0xAB, 0xFF };
            byte[] second = new byte[] { 0x10, 0x20 };
            await repository.CreateAsync(new RegBinaryItem { Payload = first });
            await repository.CreateAsync(new RegBinaryItem { Payload = second });

            repository.CaptureSql = true;
            List<RegBinaryItem> found = (await repository.Query().Where(i => i.Payload == first).ExecuteAsync()).ToList();

            Assert.Single(found);
            Assert.Equal(first, found[0].Payload);
            string withParameters = repository.LastExecutedSqlWithParameters ?? string.Empty;
            Assert.DoesNotContain("System.Byte[]", withParameters, StringComparison.Ordinal);
            Assert.Contains("ab", withParameters.ToLowerInvariant(), StringComparison.Ordinal);
            Assert.Equal(second, repository.ReadById(found[0].Id + 1)!.Payload);
        }

        /// <summary>
        /// A key whose CLR type goes through a value converter works with every by-key operation, sync and async.
        /// </summary>
        [Fact]
        public async Task CustomTypedKeyWorksWithByKeyOperations()
        {
            ISqlRepository<RegCodedItem> repository = _Provider.CreateRepository<RegCodedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            RegCode alpha = new RegCode("alpha");
            RegCode beta = new RegCode("beta");
            await repository.CreateAsync(new RegCodedItem { Code = alpha, Name = "Alpha" });
            repository.Create(new RegCodedItem { Code = beta, Name = "Beta" });

            Assert.Equal("Alpha", (await repository.ReadByIdAsync(alpha))!.Name);
            Assert.Equal("Beta", repository.ReadById(beta)!.Name);
            Assert.True(await repository.ExistsByIdAsync(alpha));
            Assert.False(repository.ExistsById(new RegCode("gamma")));
            Assert.Equal("Alpha", (await repository.ReadFirstAsync(i => i.Code == alpha))!.Name);

            RegCodedItem loaded = (await repository.ReadByIdAsync(beta))!;
            loaded.Name = "Beta 2";
            await repository.UpdateAsync(loaded);
            Assert.Equal("Beta 2", repository.ReadById(beta)!.Name);

            Assert.True(await repository.DeleteByIdAsync(alpha));
            await repository.DeleteAsync(loaded);
            Assert.Equal(0, await repository.CountAsync());
        }

        /// <summary>
        /// A Guid key works with every by-key operation.
        /// </summary>
        [Fact]
        public async Task GuidKeyWorksWithByKeyOperations()
        {
            ISqlRepository<RegGuidItem> repository = _Provider.CreateRepository<RegGuidItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            Guid id = Guid.NewGuid();
            await repository.CreateAsync(new RegGuidItem { Id = id, Name = "One" });

            Assert.Equal("One", repository.ReadById(id)!.Name);
            Assert.True(await repository.ExistsByIdAsync(id));
            Assert.Single(await repository.Query().Where(i => i.Id == id).ExecuteAsync());
            Assert.True(repository.DeleteById(id));
            Assert.False(await repository.ExistsByIdAsync(id));
        }

        /// <summary>
        /// Include loads reference, collection and many-to-many navigations on the synchronous path too.
        /// </summary>
        [Fact]
        public async Task SynchronousExecuteLoadsIncludes()
        {
            ISqlRepository<Author> authors = _Provider.CreateRepository<Author>();
            ISqlRepository<Book> books = _Provider.CreateRepository<Book>();
            ISqlRepository<Category> categories = _Provider.CreateRepository<Category>();
            ISqlRepository<AuthorCategory> links = _Provider.CreateRepository<AuthorCategory>();
            await authors.ExecuteSqlRawAsync("DELETE FROM author_categories");
            await authors.ExecuteSqlRawAsync("DELETE FROM books");
            await authors.ExecuteSqlRawAsync("DELETE FROM authors");
            await authors.ExecuteSqlRawAsync("DELETE FROM categories");

            Author author = await authors.CreateAsync(new Author { Name = "Orwell" });
            Category category = await categories.CreateAsync(new Category { Name = "Fiction", Description = "F" });
            await books.CreateAsync(new Book { Title = "1984", AuthorId = author.Id });
            await links.CreateAsync(new AuthorCategory { AuthorId = author.Id, CategoryId = category.Id });

            Book book = books.Query().Include(b => b.Author).Execute().Single();
            Assert.NotNull(book.Author);
            Assert.Equal("Orwell", book.Author!.Name);

            Author loaded = authors.Query().Include(a => a.Books).Include(a => a.Categories).Execute().Single();
            Assert.Equal(new[] { "1984" }, loaded.Books.Select(b => b.Title).ToArray());
            Assert.Equal(new[] { "Fiction" }, loaded.Categories.Select(c => c.Name).ToArray());
        }

        /// <summary>
        /// Contains over an empty list matches nothing (and its negation matches everything) instead of emitting
        /// invalid SQL.
        /// </summary>
        [Fact]
        public async Task EmptyContainsListMatchesNothing()
        {
            ISqlRepository<RegEnumItem> repository = await SeedEnumItemsAsync();
            List<int> none = new List<int>();

            Assert.Empty(await repository.Query().Where(i => none.Contains(i.Id)).ExecuteAsync());
            Assert.Equal(4, await repository.CountAsync(i => !none.Contains(i.Id)));
        }

        /// <summary>
        /// A schema-qualified table name works for table creation, validation, CRUD and queries (PostgreSQL and SQL
        /// Server; MySQL schemas are databases and SQLite has none).
        /// </summary>
        [Fact]
        public async Task SchemaQualifiedTableSupportsCrud()
        {
            RepositoryType type = RelTestHelpers.TypeOf(_Provider);
            if (type != RepositoryType.Postgres && type != RepositoryType.SqlServer) return;

            ISqlRepository<RegSchemaItem> repository = _Provider.CreateRepository<RegSchemaItem>();
            if (type == RepositoryType.Postgres) await repository.ExecuteSqlRawAsync("CREATE SCHEMA IF NOT EXISTS durable_reg");
            else await repository.ExecuteSqlRawAsync("IF SCHEMA_ID('durable_reg') IS NULL EXEC('CREATE SCHEMA durable_reg')");
            await RelTestHelpers.RecreateTableAsync(repository);
            await repository.InitializeTableAsync(typeof(RegSchemaItem));

            RegSchemaItem created = await repository.CreateAsync(new RegSchemaItem { Name = "one" });
            Assert.True(created.Id > 0);
            created.Name = "two";
            await repository.UpdateAsync(created);
            Assert.Equal("two", (await repository.ReadByIdAsync(created.Id))!.Name);
            Assert.Single(await repository.Query().Where(i => i.Name == "two").ExecuteAsync());
            Assert.True((await repository.ValidateTableAsync(typeof(RegSchemaItem))).IsValid);
            Assert.True(await repository.DeleteByIdAsync(created.Id));
        }

        /// <summary>
        /// Releases resources.
        /// </summary>
        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<RegEnumItem>> SeedEnumItemsAsync()
        {
            ISqlRepository<RegEnumItem> repository = _Provider.CreateRepository<RegEnumItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            await repository.CreateManyAsync(new List<RegEnumItem>
            {
                new RegEnumItem { Name = "a", Status = RegStatus.Active, Rank = RegStatus.Draft, Score = 5 },
                new RegEnumItem { Name = "b", Status = RegStatus.Draft, Rank = RegStatus.Active, Score = 20 },
                new RegEnumItem { Name = "c", Status = RegStatus.Active, Rank = RegStatus.Archived, Score = 30 },
                new RegEnumItem { Name = "d", Status = RegStatus.Archived, Rank = RegStatus.Archived, Score = 40 }
            });
            return repository;
        }

        #endregion
    }
}
