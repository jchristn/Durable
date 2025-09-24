namespace Test.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.MySql;
    using MySqlConnector;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Comprehensive tests for Include and Join operations for related entity handling in MySQL.
    /// Tests loading navigation properties, nested relationships, and complex entity graphs.
    /// </summary>
    public class MySqlIncludeTests : IDisposable
    {
        #region Private-Members

        private const string TestConnectionString = "Server=localhost;Database=durable_include_test;User=test_user;Password=test_password;";
        private const string TestDatabaseName = "durable_include_test";
        private static readonly object _TestLock = new object();
        private static bool _DatabaseSetupComplete = false;
        private static bool _SkipTests = false;

        private readonly MySqlRepository<Author> _AuthorRepository;
        private readonly MySqlRepository<Book> _BookRepository;
        private readonly MySqlRepository<Category> _CategoryRepository;
        private readonly MySqlRepository<Company> _CompanyRepository;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the include tests by setting up the test database and repositories.
        /// </summary>
        public MySqlIncludeTests()
        {
            lock (_TestLock)
            {
                if (!_DatabaseSetupComplete)
                {
                    SetupTestDatabase();
                    _DatabaseSetupComplete = true;
                }
            }

            if (_SkipTests) return;

            // Initialize repositories
            _AuthorRepository = new MySqlRepository<Author>(TestConnectionString);
            _BookRepository = new MySqlRepository<Book>(TestConnectionString);
            _CategoryRepository = new MySqlRepository<Category>(TestConnectionString);
            _CompanyRepository = new MySqlRepository<Company>(TestConnectionString);

            // Create tables and insert test data
            CreateTables();
            InsertTestData();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests simple include functionality for loading related entities.
        /// </summary>
        [Fact]
        public void TestSimpleInclude()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing simple include functionality...");

            // Load books with their authors
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .Execute()
                .ToList();

            Assert.True(books.Count >= 3);
            Console.WriteLine($"Retrieved {books.Count} books with authors");

            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");

                // Verify that authors are loaded
                Assert.NotNull(book.Author);
                Assert.True(book.Author.Id > 0);
                Assert.False(string.IsNullOrEmpty(book.Author.Name));
            }

            Console.WriteLine("✅ Simple Include test passed!");
        }

        /// <summary>
        /// Tests nested include functionality using ThenInclude for deep relationships.
        /// </summary>
        [Fact]
        public void TestNestedInclude()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing nested include (ThenInclude) functionality...");

            // Load books with authors and their companies
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Execute()
                .ToList();

            Assert.True(books.Count >= 3);
            Console.WriteLine($"Retrieved {books.Count} books with authors and companies");

            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
                Console.WriteLine($"    Author's Company: {book.Author?.Company?.Name ?? "null"}");

                // Verify that authors are loaded
                Assert.NotNull(book.Author);
                Assert.True(book.Author.Id > 0);

                // Verify company is loaded when author has a company
                if (book.Author.CompanyId.HasValue)
                {
                    Assert.NotNull(book.Author.Company);
                    Assert.True(book.Author.Company.Id > 0);
                    Assert.False(string.IsNullOrEmpty(book.Author.Company.Name));
                }
            }

            Console.WriteLine("✅ Nested Include test passed!");
        }

        /// <summary>
        /// Tests multiple includes for loading multiple navigation properties.
        /// </summary>
        [Fact]
        public void TestMultipleIncludes()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing multiple includes functionality...");

            // Load books with both authors and publishers
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .Include(b => b.Publisher)
                .Execute()
                .ToList();

            Assert.True(books.Count >= 3);
            Console.WriteLine($"Retrieved {books.Count} books with authors and publishers");

            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
                Console.WriteLine($"    Publisher: {book.Publisher?.Name ?? "null"}");

                // Verify that authors are always loaded
                Assert.NotNull(book.Author);
                Assert.True(book.Author.Id > 0);

                // Verify publisher is loaded when book has a publisher
                if (book.PublisherId.HasValue)
                {
                    Assert.NotNull(book.Publisher);
                    Assert.True(book.Publisher.Id > 0);
                    Assert.False(string.IsNullOrEmpty(book.Publisher.Name));
                }
            }

            Console.WriteLine("✅ Multiple Includes test passed!");
        }

        /// <summary>
        /// Tests include functionality with WHERE clause filtering.
        /// </summary>
        [Fact]
        public void TestIncludeWithWhere()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing include with WHERE clause...");

            // Get specific author's books with author included
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .Where(b => b.Author.Name.Contains("Orwell"))
                .Execute()
                .ToList();

            Assert.True(books.Count >= 1);
            Console.WriteLine($"Retrieved {books.Count} books by authors containing 'Orwell'");

            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title} by {book.Author?.Name}");

                Assert.NotNull(book.Author);
                Assert.Contains("Orwell", book.Author.Name);
            }

            Console.WriteLine("✅ Include with WHERE test passed!");
        }

        /// <summary>
        /// Tests include functionality with ORDER BY clause.
        /// </summary>
        [Fact]
        public void TestIncludeWithOrderBy()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing include with ORDER BY...");

            // Load books with authors, ordered by book title
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .OrderBy(b => b.Title)
                .Execute()
                .ToList();

            Assert.True(books.Count >= 3);
            Console.WriteLine($"Retrieved {books.Count} books ordered by title");

            string previousTitle = "";
            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title} by {book.Author?.Name}");

                Assert.NotNull(book.Author);

                // Verify ordering
                if (!string.IsNullOrEmpty(previousTitle))
                {
                    Assert.True(string.Compare(previousTitle, book.Title, StringComparison.Ordinal) <= 0);
                }
                previousTitle = book.Title;
            }

            Console.WriteLine("✅ Include with ORDER BY test passed!");
        }

        /// <summary>
        /// Tests include functionality with pagination (Take/Skip).
        /// </summary>
        [Fact]
        public void TestIncludeWithPagination()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing include with pagination...");

            // Get first page
            List<Book> page1 = _BookRepository.Query()
                .Include(b => b.Author)
                .OrderBy(b => b.Id)
                .Take(2)
                .Execute()
                .ToList();

            Console.WriteLine($"Page 1: Retrieved {page1.Count} books");
            foreach (Book book in page1)
            {
                Console.WriteLine($"  Book: {book.Title} by {book.Author?.Name}");
                Assert.NotNull(book.Author);
            }

            // Get second page
            List<Book> page2 = _BookRepository.Query()
                .Include(b => b.Author)
                .OrderBy(b => b.Id)
                .Skip(2)
                .Take(2)
                .Execute()
                .ToList();

            Console.WriteLine($"Page 2: Retrieved {page2.Count} books");
            foreach (Book book in page2)
            {
                Console.WriteLine($"  Book: {book.Title} by {book.Author?.Name}");
                Assert.NotNull(book.Author);
            }

            // Verify no overlap between pages
            Assert.False(page1.Any(p1 => page2.Any(p2 => p2.Id == p1.Id)));

            Console.WriteLine("✅ Include with Pagination test passed!");
        }

        /// <summary>
        /// Tests ThenInclude chain with multiple levels of relationships.
        /// </summary>
        [Fact]
        public void TestThenIncludeChain()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing ThenInclude chain...");

            // Load books with full relationship chain: Book -> Author -> Company + Book -> Publisher
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Include(b => b.Publisher)
                .Execute()
                .ToList();

            Assert.True(books.Count >= 3);
            Console.WriteLine($"Retrieved {books.Count} books with full relationship chain");

            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
                Console.WriteLine($"    Author's Company: {book.Author?.Company?.Name ?? "null"}");
                Console.WriteLine($"    Publisher: {book.Publisher?.Name ?? "null"}");

                // Verify author is always loaded
                Assert.NotNull(book.Author);

                // Verify author's company is loaded when exists
                if (book.Author.CompanyId.HasValue)
                {
                    Assert.NotNull(book.Author.Company);
                    Assert.Equal(book.Author.CompanyId.Value, book.Author.Company.Id);
                }

                // Verify publisher is loaded when exists
                if (book.PublisherId.HasValue)
                {
                    Assert.NotNull(book.Publisher);
                    Assert.Equal(book.PublisherId.Value, book.Publisher.Id);
                }
            }

            Console.WriteLine("✅ ThenInclude Chain test passed!");
        }

        /// <summary>
        /// Tests complex query combining includes with advanced filtering and aggregation.
        /// </summary>
        [Fact]
        public void TestComplexQueryWithIncludes()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing complex query with includes...");

            // Complex query: Books with publishers, including author and company relationships,
            // filtered by company industry, ordered by publication order
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Include(b => b.Publisher)
                .Where(b => b.PublisherId != null)
                .Where(b => b.Author.Company.Industry == "Publishing")
                .OrderBy(b => b.Id)
                .Take(3)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} published books from publishing companies");

            foreach (Book book in books)
            {
                Console.WriteLine($"  Book ID {book.Id}: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
                Console.WriteLine($"    Author's Company: {book.Author?.Company?.Name ?? "null"} ({book.Author?.Company?.Industry ?? "null"})");
                Console.WriteLine($"    Publisher: {book.Publisher?.Name ?? "null"}");

                // Verify all relationships are properly loaded
                Assert.NotNull(book.Author);
                Assert.NotNull(book.Author.Company);
                Assert.Equal("Publishing", book.Author.Company.Industry);
                Assert.NotNull(book.Publisher);
                Assert.NotNull(book.PublisherId);
            }

            Console.WriteLine("✅ Complex Query test passed!");
        }

        /// <summary>
        /// Tests include functionality with async operations.
        /// </summary>
        [Fact]
        public async Task TestAsyncIncludeOperations()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing async include operations...");

            // Test async include with single navigation property
            IEnumerable<Book> books1 = await _BookRepository.Query()
                .Include(b => b.Author)
                .ExecuteAsync();

            List<Book> bookList1 = books1.ToList();
            Assert.True(bookList1.Count >= 3);

            foreach (Book book in bookList1)
            {
                Assert.NotNull(book.Author);
                Console.WriteLine($"  Async: {book.Title} by {book.Author.Name}");
            }

            // Test async include with nested relationships
            IEnumerable<Book> books2 = await _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Where(b => b.Author.CompanyId != null)
                .ExecuteAsync();

            List<Book> bookList2 = books2.ToList();
            Assert.True(bookList2.Count >= 1);

            foreach (Book book in bookList2)
            {
                Assert.NotNull(book.Author);
                Assert.NotNull(book.Author.Company);
                Console.WriteLine($"  Async Nested: {book.Title} by {book.Author.Name} ({book.Author.Company.Name})");
            }

            Console.WriteLine("✅ Async Include Operations test passed!");
        }

        /// <summary>
        /// Tests reverse navigation - loading authors with their books.
        /// </summary>
        [Fact]
        public void TestReverseNavigation()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing reverse navigation (Authors with Books)...");

            // Load authors with their books (reverse navigation)
            List<Author> authors = _AuthorRepository.Query()
                .Include(a => a.Books)
                .Execute()
                .ToList();

            Assert.True(authors.Count >= 3);
            Console.WriteLine($"Retrieved {authors.Count} authors with their books");

            foreach (Author author in authors)
            {
                Console.WriteLine($"  Author: {author.Name}");
                Console.WriteLine($"    Books count: {author.Books?.Count ?? 0}");

                if (author.Books != null)
                {
                    foreach (Book book in author.Books)
                    {
                        Console.WriteLine($"      - {book.Title}");
                        Assert.Equal(author.Id, book.AuthorId);
                    }
                }
            }

            // Verify that prolific authors have multiple books
            Author prolificAuthor = authors.FirstOrDefault(a => a.Books != null && a.Books.Count > 1);
            Assert.NotNull(prolificAuthor);
            Console.WriteLine($"  Prolific author: {prolificAuthor.Name} with {prolificAuthor.Books.Count} books");

            Console.WriteLine("✅ Reverse Navigation test passed!");
        }

        /// <summary>
        /// Tests include with filtering on related entities.
        /// </summary>
        [Fact]
        public void TestIncludeWithRelatedEntityFiltering()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing include with related entity filtering...");

            // Find books by authors from specific companies
            List<Book> booksFromPublishingCompanies = _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Where(b => b.Author.Company.Industry == "Publishing")
                .Execute()
                .ToList();

            Assert.True(booksFromPublishingCompanies.Count >= 1);
            Console.WriteLine($"Retrieved {booksFromPublishingCompanies.Count} books from publishing companies");

            foreach (Book book in booksFromPublishingCompanies)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author.Name}");
                Console.WriteLine($"    Company: {book.Author.Company.Name} ({book.Author.Company.Industry})");

                Assert.NotNull(book.Author);
                Assert.NotNull(book.Author.Company);
                Assert.Equal("Publishing", book.Author.Company.Industry);
            }

            Console.WriteLine("✅ Include with Related Entity Filtering test passed!");
        }

        /// <summary>
        /// Tests SQL generation and execution verification for include operations.
        /// </summary>
        [Fact]
        public void TestIncludeSqlGeneration()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing SQL generation for include operations...");

            _BookRepository.CaptureSql = true;

            // Execute include query with SQL capture
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Where(b => b.Title.Contains("1984"))
                .Execute()
                .ToList();

            Assert.NotNull(_BookRepository.LastExecutedSql);
            Assert.Contains("JOIN", _BookRepository.LastExecutedSql.ToUpper());
            Assert.Contains("authors", _BookRepository.LastExecutedSql.ToLower());

            Console.WriteLine($"Generated SQL: {_BookRepository.LastExecutedSql}");
            Console.WriteLine($"Retrieved {books.Count} books with includes");

            // Verify the query executed successfully and relationships are loaded
            if (books.Count > 0)
            {
                Book book = books.First();
                Assert.NotNull(book.Author);
                Console.WriteLine($"  Book: {book.Title} by {book.Author.Name}");
            }

            Console.WriteLine("✅ Include SQL Generation test passed!");
        }

        /// <summary>
        /// Disposes test resources.
        /// </summary>
        public void Dispose()
        {
            _AuthorRepository?.Dispose();
            _BookRepository?.Dispose();
            _CategoryRepository?.Dispose();
            _CompanyRepository?.Dispose();
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Sets up the test database by ensuring it exists and is accessible.
        /// </summary>
        private void SetupTestDatabase()
        {
            try
            {
                // Test connection to database
                using MySqlConnection connection = new MySqlConnection(TestConnectionString);
                connection.Open();

                using MySqlCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteScalar();

                Console.WriteLine("✅ MySQL include tests enabled - database connection successful");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  MySQL include tests disabled - could not connect to database: {ex.Message}");
                Console.WriteLine("To enable MySQL include tests:");
                Console.WriteLine("1. Start MySQL server on localhost:3306");
                Console.WriteLine($"2. Create database: CREATE DATABASE {TestDatabaseName};");
                Console.WriteLine("3. Create user: CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'test_password';");
                Console.WriteLine($"4. Grant permissions: GRANT ALL PRIVILEGES ON {TestDatabaseName}.* TO 'test_user'@'localhost';");
                _SkipTests = true;
            }
        }

        /// <summary>
        /// Creates the necessary tables for include testing with proper foreign key relationships.
        /// </summary>
        private void CreateTables()
        {
            if (_SkipTests) return;

            // Drop tables if they exist (in correct order due to foreign keys)
            DropTablesIfExist();

            // Create companies table
            _CompanyRepository.ExecuteSql(@"
                CREATE TABLE companies (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    industry VARCHAR(50),
                    INDEX idx_industry (industry),
                    INDEX idx_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

            // Create authors table with foreign key to companies
            _AuthorRepository.ExecuteSql(@"
                CREATE TABLE authors (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    company_id INT NULL,
                    INDEX idx_company_id (company_id),
                    INDEX idx_name (name),
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

            // Create categories table
            _CategoryRepository.ExecuteSql(@"
                CREATE TABLE categories (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    description VARCHAR(255),
                    INDEX idx_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

            // Create books table with foreign keys to authors and companies (as publishers)
            _BookRepository.ExecuteSql(@"
                CREATE TABLE books (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    author_id INT NOT NULL,
                    publisher_id INT NULL,
                    INDEX idx_author_id (author_id),
                    INDEX idx_publisher_id (publisher_id),
                    INDEX idx_title (title),
                    FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE,
                    FOREIGN KEY (publisher_id) REFERENCES companies(id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

            Console.WriteLine("✅ Tables created successfully with foreign key relationships");
        }

        /// <summary>
        /// Drops existing tables if they exist.
        /// </summary>
        private void DropTablesIfExist()
        {
            try
            {
                _BookRepository.ExecuteSql("DROP TABLE IF EXISTS books");
                _CategoryRepository.ExecuteSql("DROP TABLE IF EXISTS categories");
                _AuthorRepository.ExecuteSql("DROP TABLE IF EXISTS authors");
                _CompanyRepository.ExecuteSql("DROP TABLE IF EXISTS companies");
            }
            catch
            {
                // Tables might not exist, ignore errors
            }
        }

        /// <summary>
        /// Inserts comprehensive test data for include relationship testing.
        /// </summary>
        private void InsertTestData()
        {
            if (_SkipTests) return;

            Console.WriteLine("Seeding test data for include functionality...");

            // Create companies with different industries
            Company orwellPublishing = _CompanyRepository.Create(new Company
            {
                Name = "Orwell Publishing House",
                Industry = "Publishing"
            });

            Company asimovPress = _CompanyRepository.Create(new Company
            {
                Name = "Asimov Science Press",
                Industry = "Publishing"
            });

            Company techCorp = _CompanyRepository.Create(new Company
            {
                Name = "TechCorp Solutions",
                Industry = "Technology"
            });

            Company literaryHouse = _CompanyRepository.Create(new Company
            {
                Name = "Classic Literary House",
                Industry = "Publishing"
            });

            // Create authors with company relationships
            Author georgeOrwell = _AuthorRepository.Create(new Author
            {
                Name = "George Orwell",
                CompanyId = orwellPublishing.Id
            });

            Author isaacAsimov = _AuthorRepository.Create(new Author
            {
                Name = "Isaac Asimov",
                CompanyId = asimovPress.Id
            });

            Author janeAusten = _AuthorRepository.Create(new Author
            {
                Name = "Jane Austen",
                CompanyId = null // Independent author
            });

            Author techWriter = _AuthorRepository.Create(new Author
            {
                Name = "Tech Writer",
                CompanyId = techCorp.Id
            });

            // Create categories
            Category fiction = _CategoryRepository.Create(new Category
            {
                Name = "Fiction",
                Description = "Fictional literature"
            });

            Category sciFi = _CategoryRepository.Create(new Category
            {
                Name = "Science Fiction",
                Description = "Science fiction literature"
            });

            Category classic = _CategoryRepository.Create(new Category
            {
                Name = "Classic",
                Description = "Classic literature"
            });

            // Create books with author and publisher relationships
            _BookRepository.Create(new Book
            {
                Title = "1984",
                AuthorId = georgeOrwell.Id,
                PublisherId = orwellPublishing.Id
            });

            _BookRepository.Create(new Book
            {
                Title = "Animal Farm",
                AuthorId = georgeOrwell.Id,
                PublisherId = orwellPublishing.Id
            });

            _BookRepository.Create(new Book
            {
                Title = "Foundation",
                AuthorId = isaacAsimov.Id,
                PublisherId = asimovPress.Id
            });

            _BookRepository.Create(new Book
            {
                Title = "I, Robot",
                AuthorId = isaacAsimov.Id,
                PublisherId = asimovPress.Id
            });

            _BookRepository.Create(new Book
            {
                Title = "The Robots of Dawn",
                AuthorId = isaacAsimov.Id,
                PublisherId = asimovPress.Id
            });

            _BookRepository.Create(new Book
            {
                Title = "Pride and Prejudice",
                AuthorId = janeAusten.Id,
                PublisherId = literaryHouse.Id
            });

            _BookRepository.Create(new Book
            {
                Title = "Sense and Sensibility",
                AuthorId = janeAusten.Id,
                PublisherId = literaryHouse.Id
            });

            _BookRepository.Create(new Book
            {
                Title = "Technical Manual",
                AuthorId = techWriter.Id,
                PublisherId = techCorp.Id
            });

            _BookRepository.Create(new Book
            {
                Title = "Independent Work",
                AuthorId = janeAusten.Id,
                PublisherId = null // Self-published
            });

            Console.WriteLine("✅ Test data seeded successfully");
            Console.WriteLine($"   Companies: 4, Authors: 4, Books: 9, Categories: 3");
        }

        #endregion
    }
}