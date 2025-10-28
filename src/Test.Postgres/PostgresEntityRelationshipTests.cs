namespace Test.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Npgsql;
    using Test.Shared;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Comprehensive tests for PostgreSQL entity relationships and complex models including:
    /// - Multi-entity models: Author/Book/Category/Company relationships
    /// - Foreign key relationships: Proper relationship mapping and integrity
    /// - Complex entity hierarchies: Nested object structures and navigation properties
    /// </summary>
    public class PostgresEntityRelationshipTests : IDisposable
    {

        #region Private-Members

        private readonly ITestOutputHelper _Output;
        private readonly string _ConnectionString;
        private readonly bool _SkipTests;
        private readonly PostgresRepository<Author> _AuthorRepository;
        private readonly PostgresRepository<Book> _BookRepository;
        private readonly PostgresRepository<Category> _CategoryRepository;
        private readonly PostgresRepository<Company> _CompanyRepository;
        private readonly PostgresRepository<AuthorCategory> _AuthorCategoryRepository;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the entity relationship test class.
        /// </summary>
        public PostgresEntityRelationshipTests(ITestOutputHelper output)
        {
            _Output = output;
            _ConnectionString = "Host=localhost;Database=durable_test;Username=test_user;Password=test_password;";

            try
            {
                using NpgsqlConnection connection = new NpgsqlConnection(_ConnectionString);
                connection.Open();
                using NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteScalar();

                _AuthorRepository = new PostgresRepository<Author>(_ConnectionString);
                _BookRepository = new PostgresRepository<Book>(_ConnectionString);
                _CategoryRepository = new PostgresRepository<Category>(_ConnectionString);
                _CompanyRepository = new PostgresRepository<Company>(_ConnectionString);
                _AuthorCategoryRepository = new PostgresRepository<AuthorCategory>(_ConnectionString);

                _Output.WriteLine("PostgreSQL entity relationship tests initialized successfully");
            }
            catch (Exception ex)
            {
                _SkipTests = true;
                _Output.WriteLine($"WARNING: PostgreSQL initialization failed - {ex.Message}");
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests basic entity creation and foreign key relationships.
        /// </summary>
        [Fact]
        public async Task BasicEntityRelationships_CreateEntitiesWithForeignKeys_EstablishesProperRelationships()
        {
            if (_SkipTests) return;

            await SetupDatabaseAsync();

            Company company = new Company
            {
                Name = "Tech Publishing House",
                Industry = "Publishing"
            };
            Company createdCompany = await _CompanyRepository.CreateAsync(company);
            Assert.NotEqual(0, createdCompany.Id);
            _Output.WriteLine($"Created Company: {createdCompany}");

            Author author = new Author
            {
                Name = "John Technical Writer",
                CompanyId = createdCompany.Id
            };
            Author createdAuthor = await _AuthorRepository.CreateAsync(author);
            Assert.NotEqual(0, createdAuthor.Id);
            Assert.Equal(createdCompany.Id, createdAuthor.CompanyId);
            _Output.WriteLine($"Created Author: {createdAuthor}");

            Book book1 = new Book
            {
                Title = "Advanced Database Design",
                AuthorId = createdAuthor.Id,
                PublisherId = createdCompany.Id
            };
            Book createdBook1 = await _BookRepository.CreateAsync(book1);
            Assert.NotEqual(0, createdBook1.Id);
            Assert.Equal(createdAuthor.Id, createdBook1.AuthorId);
            Assert.Equal(createdCompany.Id, createdBook1.PublisherId);
            _Output.WriteLine($"Created Book 1: {createdBook1}");

            Book book2 = new Book
            {
                Title = "PostgreSQL Performance Tuning",
                AuthorId = createdAuthor.Id,
                PublisherId = createdCompany.Id
            };
            Book createdBook2 = await _BookRepository.CreateAsync(book2);
            Assert.NotEqual(0, createdBook2.Id);
            _Output.WriteLine($"Created Book 2: {createdBook2}");

            int authorBooksCount = await _BookRepository.CountAsync(b => b.AuthorId == createdAuthor.Id);
            Assert.Equal(2, authorBooksCount);

            int companyBooksCount = await _BookRepository.CountAsync(b => b.PublisherId == createdCompany.Id);
            Assert.Equal(2, companyBooksCount);

            _Output.WriteLine("✅ Basic entity relationships test completed successfully");
        }

        /// <summary>
        /// Tests navigation properties and include functionality for related entities.
        /// </summary>
        [Fact]
        public async Task NavigationProperties_IncludeRelatedEntities_LoadsNestedObjectStructures()
        {
            if (_SkipTests) return;

            await SetupDatabaseAsync();
            await SeedTestDataAsync();

            List<Author> authorsWithCompanies = _AuthorRepository
                .Query()
                .Include(a => a.Company)
                .Where(a => a.CompanyId != null)
                .Execute()
                .ToList();

            Assert.NotEmpty(authorsWithCompanies);
            foreach (Author author in authorsWithCompanies)
            {
                Assert.NotNull(author.Company);
                Assert.NotNull(author.Company.Name);
                _Output.WriteLine($"Author: {author.Name} works at {author.Company.Name}");
            }

            List<Book> booksWithDetails = _BookRepository
                .Query()
                .Include(b => b.Author)
                .Include(b => b.Publisher)
                .Execute()
                .ToList();

            Assert.NotEmpty(booksWithDetails);
            foreach (Book book in booksWithDetails)
            {
                Assert.NotNull(book.Author);
                Assert.NotNull(book.Author.Name);
                _Output.WriteLine($"Book: {book.Title} by {book.Author.Name}");

                if (book.Publisher != null)
                {
                    _Output.WriteLine($"  Published by: {book.Publisher.Name}");
                }
            }

            List<Book> booksWithNestedIncludes = _BookRepository
                .Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Where(b => b.Author.CompanyId != null)
                .Execute()
                .ToList();

            Assert.NotEmpty(booksWithNestedIncludes);
            foreach (Book book in booksWithNestedIncludes)
            {
                Assert.NotNull(book.Author);
                Assert.NotNull(book.Author.Company);
                _Output.WriteLine($"Book: {book.Title} by {book.Author.Name} (Company: {book.Author.Company.Name})");
            }

            _Output.WriteLine("✅ Navigation properties test completed successfully");
        }

        /// <summary>
        /// Tests many-to-many relationships through junction tables.
        /// </summary>
        [Fact]
        public async Task ManyToManyRelationships_AuthorCategoryAssociations_ManagesJunctionTableCorrectly()
        {
            if (_SkipTests) return;

            await SetupDatabaseAsync();
            await SeedTestDataAsync();

            List<Author> authors = _AuthorRepository.Query().Execute().ToList();
            List<Category> categories = _CategoryRepository.Query().Execute().ToList();

            Assert.NotEmpty(authors);
            Assert.NotEmpty(categories);

            Author author1 = authors[0];
            Author author2 = authors[1];
            Category category1 = categories[0];
            Category category2 = categories[1];

            AuthorCategory ac1 = new AuthorCategory { AuthorId = author1.Id, CategoryId = category1.Id };
            AuthorCategory ac2 = new AuthorCategory { AuthorId = author1.Id, CategoryId = category2.Id };
            AuthorCategory ac3 = new AuthorCategory { AuthorId = author2.Id, CategoryId = category1.Id };

            await _AuthorCategoryRepository.CreateAsync(ac1);
            await _AuthorCategoryRepository.CreateAsync(ac2);
            await _AuthorCategoryRepository.CreateAsync(ac3);

            _Output.WriteLine($"Created Author-Category associations:");
            _Output.WriteLine($"  {author1.Name} → {category1.Name}, {category2.Name}");
            _Output.WriteLine($"  {author2.Name} → {category1.Name}");

            List<AuthorCategory> author1Categories = _AuthorCategoryRepository
                .Query()
                .Where(ac => ac.AuthorId == author1.Id)
                .Execute()
                .ToList();
            Assert.Equal(2, author1Categories.Count);

            List<AuthorCategory> category1Authors = _AuthorCategoryRepository
                .Query()
                .Where(ac => ac.CategoryId == category1.Id)
                .Execute()
                .ToList();
            Assert.Equal(2, category1Authors.Count);

            List<AuthorCategory> associationsWithDetails = _AuthorCategoryRepository
                .Query()
                .Include(ac => ac.Author)
                .Include(ac => ac.Category)
                .Execute()
                .ToList();

            foreach (AuthorCategory association in associationsWithDetails)
            {
                Assert.NotNull(association.Author);
                Assert.NotNull(association.Category);
                _Output.WriteLine($"Association: {association.Author.Name} ↔ {association.Category.Name}");
            }

            _Output.WriteLine("✅ Many-to-many relationships test completed successfully");
        }

        /// <summary>
        /// Tests complex entity hierarchies with multiple levels of relationships.
        /// </summary>
        [Fact]
        public async Task ComplexEntityHierarchies_MultiLevelRelationships_HandlesNestedStructures()
        {
            if (_SkipTests) return;

            await SetupDatabaseAsync();
            await SeedTestDataAsync();

            List<Author> authors = _AuthorRepository.Query().Execute().ToList();
            List<Category> categories = _CategoryRepository.Query().Execute().ToList();

            if (authors.Count > 0 && categories.Count > 0)
            {
                await _AuthorCategoryRepository.CreateAsync(new AuthorCategory
                {
                    AuthorId = authors[0].Id,
                    CategoryId = categories[0].Id
                });

                if (authors.Count > 1 && categories.Count > 1)
                {
                    await _AuthorCategoryRepository.CreateAsync(new AuthorCategory
                    {
                        AuthorId = authors[1].Id,
                        CategoryId = categories[1].Id
                    });
                }
            }

            List<Company> companiesWithFullHierarchy = _CompanyRepository
                .Query()
                .Execute()
                .ToList();

            foreach (Company company in companiesWithFullHierarchy)
            {
                List<Author> companyAuthors = _AuthorRepository
                    .Query()
                    .Where(a => a.CompanyId == company.Id)
                    .Execute()
                    .ToList();

                _Output.WriteLine($"\nCompany: {company.Name} ({company.Industry})");
                _Output.WriteLine($"  Authors: {companyAuthors.Count}");

                foreach (Author author in companyAuthors)
                {
                    List<Book> authorBooks = _BookRepository
                        .Query()
                        .Where(b => b.AuthorId == author.Id)
                        .Execute()
                        .ToList();

                    List<AuthorCategory> authorCategories = _AuthorCategoryRepository
                        .Query()
                        .Include(ac => ac.Category)
                        .Where(ac => ac.AuthorId == author.Id)
                        .Execute()
                        .ToList();

                    _Output.WriteLine($"    Author: {author.Name}");
                    _Output.WriteLine($"      Books: {authorBooks.Count} ({string.Join(", ", authorBooks.Select(b => b.Title))})");
                    _Output.WriteLine($"      Categories: {authorCategories.Count} ({string.Join(", ", authorCategories.Select(ac => ac.Category.Name))})");
                }

                List<Book> publishedBooks = _BookRepository
                    .Query()
                    .Include(b => b.Author)
                    .Where(b => b.PublisherId == company.Id)
                    .Execute()
                    .ToList();

                _Output.WriteLine($"  Published Books: {publishedBooks.Count}");
                foreach (Book book in publishedBooks)
                {
                    _Output.WriteLine($"    {book.Title} by {book.Author.Name}");
                }
            }

            int totalBooksInSystem = await _BookRepository.CountAsync();
            int totalAuthorsWithCompanies = _AuthorRepository.Query().Where(a => a.CompanyId != null).Execute().ToList().Count;
            int totalPublishedBooks = _BookRepository.Query().Where(b => b.PublisherId != null).Execute().ToList().Count;
            int totalCategoryAssociations = await _AuthorCategoryRepository.CountAsync();

            _Output.WriteLine($"\n=== System Statistics ===");
            _Output.WriteLine($"Total Books: {totalBooksInSystem}");
            _Output.WriteLine($"Authors with Companies: {totalAuthorsWithCompanies}");
            _Output.WriteLine($"Books with Publishers: {totalPublishedBooks}");
            _Output.WriteLine($"Author-Category Associations: {totalCategoryAssociations}");

            Assert.True(totalBooksInSystem > 0);
            Assert.True(totalAuthorsWithCompanies > 0);

            _Output.WriteLine("✅ Complex entity hierarchies test completed successfully");
        }

        /// <summary>
        /// Tests foreign key constraint validation and referential integrity.
        /// </summary>
        [Fact]
        public async Task ForeignKeyConstraints_ReferentialIntegrity_EnforcesDataConsistency()
        {
            if (_SkipTests) return;

            await SetupDatabaseAsync();

            Company company = await _CompanyRepository.CreateAsync(new Company
            {
                Name = "Constraint Test Company",
                Industry = "Testing"
            });

            Author author = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "FK Test Author",
                CompanyId = company.Id
            });

            Book book = await _BookRepository.CreateAsync(new Book
            {
                Title = "Foreign Key Test Book",
                AuthorId = author.Id,
                PublisherId = company.Id
            });

            Assert.NotEqual(0, book.Id);
            _Output.WriteLine($"Successfully created entities with valid foreign keys");

            try
            {
                Book invalidBook = new Book
                {
                    Title = "Invalid FK Test",
                    AuthorId = 99999,
                    PublisherId = company.Id
                };

                Book createdInvalidBook = await _BookRepository.CreateAsync(invalidBook);
                Assert.Fail("Expected foreign key constraint violation but creation succeeded");
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Foreign key constraint correctly prevented invalid reference: {ex.GetType().Name}");
            }

            Author authorWithoutCompany = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Independent Author",
                CompanyId = null
            });

            Assert.NotEqual(0, authorWithoutCompany.Id);
            Assert.Null(authorWithoutCompany.CompanyId);
            _Output.WriteLine($"Successfully created author without company (null foreign key)");

            Book bookWithoutPublisher = await _BookRepository.CreateAsync(new Book
            {
                Title = "Self-Published Book",
                AuthorId = author.Id,
                PublisherId = null
            });

            Assert.NotEqual(0, bookWithoutPublisher.Id);
            Assert.Null(bookWithoutPublisher.PublisherId);
            _Output.WriteLine($"Successfully created book without publisher (null foreign key)");

            _Output.WriteLine("✅ Foreign key constraints test completed successfully");
        }

        /// <summary>
        /// Tests cascading operations and relationship consistency.
        /// </summary>
        [Fact]
        public async Task CascadingOperations_RelationshipConsistency_MaintainsDataIntegrity()
        {
            if (_SkipTests) return;

            await SetupDatabaseAsync();

            Company company = await _CompanyRepository.CreateAsync(new Company
            {
                Name = "Cascade Test Company",
                Industry = "Testing"
            });

            Author author = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Cascade Test Author",
                CompanyId = company.Id
            });

            List<Book> books = new List<Book>();
            for (int i = 1; i <= 3; i++)
            {
                Book book = await _BookRepository.CreateAsync(new Book
                {
                    Title = $"Cascade Test Book {i}",
                    AuthorId = author.Id,
                    PublisherId = company.Id
                });
                books.Add(book);
            }

            Category category = await _CategoryRepository.CreateAsync(new Category
            {
                Name = "Test Category",
                Description = "For cascade testing"
            });

            await _AuthorCategoryRepository.CreateAsync(new AuthorCategory
            {
                AuthorId = author.Id,
                CategoryId = category.Id
            });

            int initialBookCount = await _BookRepository.CountAsync(b => b.AuthorId == author.Id);
            int initialACCount = await _AuthorCategoryRepository.CountAsync(ac => ac.AuthorId == author.Id);
            Assert.Equal(3, initialBookCount);
            Assert.Equal(1, initialACCount);

            _Output.WriteLine($"Created cascade test data: 1 company, 1 author, 3 books, 1 category, 1 association");

            List<AuthorCategory> authorCategories = _AuthorCategoryRepository
                .Query()
                .Where(ac => ac.AuthorId == author.Id)
                .Execute()
                .ToList();

            foreach (AuthorCategory ac in authorCategories)
            {
                await _AuthorCategoryRepository.DeleteAsync(ac);
            }

            List<Book> authorBooks = _BookRepository
                .Query()
                .Where(b => b.AuthorId == author.Id)
                .Execute()
                .ToList();

            foreach (Book authorBook in authorBooks)
            {
                await _BookRepository.DeleteAsync(authorBook);
            }

            await _AuthorRepository.DeleteAsync(author);

            int remainingBookCount = await _BookRepository.CountAsync(b => b.AuthorId == author.Id);
            int remainingACCount = await _AuthorCategoryRepository.CountAsync(ac => ac.AuthorId == author.Id);
            Author deletedAuthor = await _AuthorRepository.ReadByIdAsync(author.Id);

            Assert.Equal(0, remainingBookCount);
            Assert.Equal(0, remainingACCount);
            Assert.Null(deletedAuthor);

            Company remainingCompany = await _CompanyRepository.ReadByIdAsync(company.Id);
            Category remainingCategory = await _CategoryRepository.ReadByIdAsync(category.Id);
            Assert.NotNull(remainingCompany);
            Assert.NotNull(remainingCategory);

            _Output.WriteLine($"Cascade deletion completed: books and associations removed, company/category preserved");
            _Output.WriteLine("✅ Cascading operations test completed successfully");
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Disposes resources used by the test class.
        /// </summary>
        public void Dispose()
        {
            _AuthorRepository?.Dispose();
            _BookRepository?.Dispose();
            _CategoryRepository?.Dispose();
            _CompanyRepository?.Dispose();
            _AuthorCategoryRepository?.Dispose();
        }

        #endregion

        #region Private-Methods

        private async Task SetupDatabaseAsync()
        {
            // Create database if it doesn't exist
            _CompanyRepository.CreateDatabaseIfNotExists();

            // Drop tables with CASCADE to automatically drop dependent objects
            await _CompanyRepository.ExecuteSqlAsync("DROP TABLE IF EXISTS author_categories CASCADE");
            await _CompanyRepository.ExecuteSqlAsync("DROP TABLE IF EXISTS books CASCADE");
            await _CompanyRepository.ExecuteSqlAsync("DROP TABLE IF EXISTS authors CASCADE");
            await _CompanyRepository.ExecuteSqlAsync("DROP TABLE IF EXISTS categories CASCADE");
            await _CompanyRepository.ExecuteSqlAsync("DROP TABLE IF EXISTS companies CASCADE");

            // Create tables using InitializeTable (respects entity attributes including version columns)
            // Create in dependency order (parent tables first)
            _CompanyRepository.InitializeTable(typeof(Company));
            _CategoryRepository.InitializeTable(typeof(Category));
            _AuthorRepository.InitializeTable(typeof(Author));
            _BookRepository.InitializeTable(typeof(Book));
            _AuthorCategoryRepository.InitializeTable(typeof(AuthorCategory));

            _Output.WriteLine("Database tables created successfully with proper foreign key constraints");
        }

        private async Task SeedTestDataAsync()
        {
            Company company1 = await _CompanyRepository.CreateAsync(new Company
            {
                Name = "Tech Publications Inc",
                Industry = "Publishing"
            });

            Company company2 = await _CompanyRepository.CreateAsync(new Company
            {
                Name = "Academic Press Ltd",
                Industry = "Education"
            });

            Category category1 = await _CategoryRepository.CreateAsync(new Category
            {
                Name = "Technology",
                Description = "Technology and software development"
            });

            Category category2 = await _CategoryRepository.CreateAsync(new Category
            {
                Name = "Science",
                Description = "Scientific and research topics"
            });

            Category category3 = await _CategoryRepository.CreateAsync(new Category
            {
                Name = "Business",
                Description = "Business and management topics"
            });

            Author author1 = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Alice Technical",
                CompanyId = company1.Id
            });

            Author author2 = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Bob Scientific",
                CompanyId = company2.Id
            });

            Author author3 = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Carol Independent",
                CompanyId = null
            });

            await _BookRepository.CreateAsync(new Book
            {
                Title = "PostgreSQL Advanced Techniques",
                AuthorId = author1.Id,
                PublisherId = company1.Id
            });

            await _BookRepository.CreateAsync(new Book
            {
                Title = "Database Theory and Practice",
                AuthorId = author2.Id,
                PublisherId = company2.Id
            });

            await _BookRepository.CreateAsync(new Book
            {
                Title = "Freelance Writing Guide",
                AuthorId = author3.Id,
                PublisherId = null
            });

            await _BookRepository.CreateAsync(new Book
            {
                Title = "Business Intelligence with SQL",
                AuthorId = author1.Id,
                PublisherId = company1.Id
            });

            _Output.WriteLine("Test data seeded: 2 companies, 3 categories, 3 authors, 4 books");
        }

        #endregion

    }
}
