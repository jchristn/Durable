namespace Test.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using Durable.MySql;
    using MySqlConnector;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Comprehensive tests for advanced MySQL query builder features including set operations,
    /// subqueries, CTEs, window functions, and custom SQL fragments.
    /// </summary>
    public class MySqlAdvancedQueryBuilderTests : IDisposable
    {
        #region Private-Members

        private const string TestConnectionString = "Server=localhost;Database=durable_advanced_test;User=test_user;Password=test_password;";
        private const string TestDatabaseName = "durable_advanced_test";
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
        /// Initializes the advanced query builder test class by setting up the test database and repositories.
        /// </summary>
        public MySqlAdvancedQueryBuilderTests()
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
        /// Tests UNION operation between two query builders.
        /// </summary>
        [Fact]
        public void TestUnionOperation()
        {
            if (_SkipTests) return;

            // Get UK authors
            IQueryBuilder<Author> ukAuthors = _AuthorRepository.Query()
                .Where(a => a.Name == "George Orwell" || a.Name == "Jane Austen");

            // Get authors from companies with ID 1
            IQueryBuilder<Author> company1Authors = _AuthorRepository.Query()
                .Where(a => a.CompanyId == 1);

            // Union the two queries
            IEnumerable<Author> results = ukAuthors.Union(company1Authors).Execute();
            List<Author> resultList = results.ToList();

            Assert.True(resultList.Count >= 2); // Should have at least George Orwell and Jane Austen
            Assert.Contains(resultList, a => a.Name == "George Orwell");
            Assert.Contains(resultList, a => a.Name == "Jane Austen");
        }

        /// <summary>
        /// Tests UNION ALL operation which includes duplicates.
        /// </summary>
        [Fact]
        public void TestUnionAllOperation()
        {
            if (_SkipTests) return;

            // Get books by author 1
            IQueryBuilder<Book> authorBooks = _BookRepository.Query()
                .Where(b => b.AuthorId == 1);

            // Get books published before 1950
            IQueryBuilder<Book> oldBooks = _BookRepository.Query()
                .Where(b => b.Title.Contains("1984") || b.Title.Contains("Animal"));

            // Union All the two queries
            IEnumerable<Book> results = authorBooks.UnionAll(oldBooks).Execute();
            List<Book> resultList = results.ToList();

            // Should include duplicates if books match both criteria
            Assert.True(resultList.Count >= 2);
        }

        /// <summary>
        /// Tests INTERSECT operation to find common records.
        /// </summary>
        [Fact]
        public void TestIntersectOperation()
        {
            if (_SkipTests) return;

            // Get books by author 1
            IQueryBuilder<Book> authorBooks = _BookRepository.Query()
                .Where(b => b.AuthorId == 1);

            // Get books with specific titles
            IQueryBuilder<Book> specificBooks = _BookRepository.Query()
                .Where(b => b.Title.Contains("1984") || b.Title.Contains("Animal"));

            // Intersect the two queries
            IEnumerable<Book> results = authorBooks.Intersect(specificBooks).Execute();
            List<Book> resultList = results.ToList();

            Assert.True(resultList.Count >= 1); // Should have at least one book that matches both criteria
        }

        /// <summary>
        /// Tests EXCEPT operation to find records in first set but not in second.
        /// </summary>
        [Fact]
        public void TestExceptOperation()
        {
            if (_SkipTests) return;

            // Get all authors
            IQueryBuilder<Author> allAuthors = _AuthorRepository.Query();

            // Get authors from specific company
            IQueryBuilder<Author> companyAuthors = _AuthorRepository.Query()
                .Where(a => a.CompanyId == 1);

            // Except operation
            IEnumerable<Author> results = allAuthors.Except(companyAuthors).Execute();
            List<Author> resultList = results.ToList();

            Assert.True(resultList.Count >= 1); // Should have authors not in company 1
            Assert.True(resultList.All(a => a.CompanyId != 1)); // None should be from company 1
        }

        /// <summary>
        /// Tests WHERE IN subquery functionality.
        /// </summary>
        [Fact]
        public void TestWhereInSubquery()
        {
            if (_SkipTests) return;

            // Test using raw SQL for subquery (since full scalar projection needs more implementation)
            string query = _AuthorRepository.Query()
                .WhereRaw("company_id IN (SELECT id FROM companies WHERE industry = 'Publishing')")
                .BuildSql();

            Assert.Contains("company_id IN (SELECT id FROM companies WHERE industry = 'Publishing')", query);

            // Execute the query to verify it works
            IEnumerable<Author> results = _AuthorRepository.Query()
                .WhereRaw("company_id IN (SELECT id FROM companies WHERE industry = 'Publishing')")
                .Execute();

            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests WHERE NOT IN subquery functionality.
        /// </summary>
        [Fact]
        public void TestWhereNotInSubquery()
        {
            if (_SkipTests) return;

            // Test NOT IN subquery
            string query = _AuthorRepository.Query()
                .WhereRaw("id NOT IN (SELECT author_id FROM books WHERE title LIKE '%Fiction%')")
                .BuildSql();

            Assert.Contains("id NOT IN (SELECT author_id FROM books WHERE title LIKE '%Fiction%')", query);

            // Execute the query to verify it works
            IEnumerable<Author> results = _AuthorRepository.Query()
                .WhereRaw("id NOT IN (SELECT author_id FROM books WHERE title LIKE '%Fiction%')")
                .Execute();

            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests WHERE EXISTS subquery functionality.
        /// </summary>
        [Fact]
        public void TestWhereExists()
        {
            if (_SkipTests) return;

            // Get authors who have written at least one book
            IQueryBuilder<Book> bookQuery = _BookRepository.Query();

            IEnumerable<Author> results = _AuthorRepository.Query()
                .WhereExists(bookQuery)
                .Execute();

            List<Author> resultList = results.ToList();

            // Since EXISTS without correlation returns all if any exist
            Assert.True(resultList.Count >= 1);
        }

        /// <summary>
        /// Tests Common Table Expression (CTE) functionality.
        /// </summary>
        [Fact]
        public void TestCte()
        {
            if (_SkipTests) return;

            string cteQuery = "SELECT id, name, company_id FROM authors WHERE company_id IS NOT NULL";

            IEnumerable<Author> results = _AuthorRepository.Query()
                .WithCte("company_authors", cteQuery)
                .FromRaw("company_authors")
                .Execute();

            List<Author> resultList = results.ToList();
            Assert.True(resultList.Count >= 1);
            Assert.True(resultList.All(a => a.CompanyId != null));
        }

        /// <summary>
        /// Tests Recursive Common Table Expression functionality.
        /// </summary>
        [Fact]
        public void TestRecursiveCte()
        {
            if (_SkipTests) return;

            // Example of a recursive CTE for generating a number sequence
            string anchorQuery = "SELECT 1 as n";
            string recursiveQuery = "SELECT n + 1 FROM numbers WHERE n < 5";

            string query = _AuthorRepository.Query()
                .WithRecursiveCte("numbers", anchorQuery, recursiveQuery)
                .BuildSql();

            Assert.Contains("WITH RECURSIVE", query);
            Assert.Contains("`numbers` AS", query);
        }

        /// <summary>
        /// Tests ROW_NUMBER window function.
        /// </summary>
        [Fact]
        public void TestWindowFunctionRowNumber()
        {
            if (_SkipTests) return;

            // Test ROW_NUMBER window function SQL generation
            string query = _BookRepository.Query()
                .WithWindowFunction("ROW_NUMBER")
                .RowNumber("row_num")
                .OrderBy(b => b.Id)
                .EndWindow()
                .BuildSql();

            Assert.Contains("ROW_NUMBER()", query);
            Assert.Contains("OVER", query);
            Assert.Contains("ORDER BY", query);
        }

        /// <summary>
        /// Tests window function with PARTITION BY clause.
        /// </summary>
        [Fact]
        public void TestWindowFunctionWithPartition()
        {
            if (_SkipTests) return;

            string query = _BookRepository.Query()
                .WithWindowFunction("RANK")
                .Rank("author_rank")
                .PartitionBy(b => b.AuthorId)
                .OrderBy(b => b.Id)
                .EndWindow()
                .BuildSql();

            Assert.Contains("RANK() OVER (PARTITION BY", query);
            Assert.Contains("ORDER BY", query);
        }

        /// <summary>
        /// Tests window function with frame specification.
        /// </summary>
        [Fact]
        public void TestWindowFunctionWithFrame()
        {
            if (_SkipTests) return;

            string query = _BookRepository.Query()
                .WithWindowFunction("SUM")
                .Sum(b => b.Id, "running_total")
                .OrderBy(b => b.Id)
                .RowsUnboundedPreceding()
                .EndWindow()
                .BuildSql();

            Assert.Contains("SUM(`Id`) OVER", query);
            Assert.Contains("ORDER BY", query);
        }

        /// <summary>
        /// Tests custom SQL fragments including WhereRaw, SelectRaw, and JoinRaw.
        /// </summary>
        [Fact]
        public void TestCustomSqlFragments()
        {
            if (_SkipTests) return;

            // Test WhereRaw with parameters
            IEnumerable<Author> results1 = _AuthorRepository.Query()
                .WhereRaw("id BETWEEN {0} AND {1}", 1, 10)
                .Execute();

            Assert.NotNull(results1);
            Assert.True(results1.Count() >= 1);

            // Test SelectRaw
            string selectQuery = _BookRepository.Query()
                .SelectRaw("title, CONCAT('Book: ', title) as formatted_title")
                .BuildSql();

            Assert.Contains("SELECT title, CONCAT('Book: ', title) as formatted_title", selectQuery);

            // Test JoinRaw
            string joinQuery = _BookRepository.Query()
                .JoinRaw("INNER JOIN authors a ON books.author_id = a.id")
                .BuildSql();

            Assert.Contains("INNER JOIN authors a ON books.author_id = a.id", joinQuery);
        }

        /// <summary>
        /// Tests complex query combining multiple advanced features.
        /// </summary>
        [Fact]
        public void TestComplexQueryWithMultipleFeatures()
        {
            if (_SkipTests) return;

            // Complex query combining CTEs, window functions, and set operations
            string cteQuery = "SELECT id FROM companies WHERE industry = 'Publishing'";

            IQueryBuilder<Author> publishingAuthors = _AuthorRepository.Query()
                .WithCte("publishing_companies", cteQuery)
                .Where(a => a.CompanyId != null);

            IQueryBuilder<Author> prolificAuthors = _AuthorRepository.Query()
                .WhereRaw("id IN (SELECT author_id FROM books GROUP BY author_id HAVING COUNT(*) > 1)");

            string complexQuery = publishingAuthors
                .Union(prolificAuthors)
                .BuildSql();

            Assert.Contains("WITH `publishing_companies`", complexQuery);
            Assert.Contains("UNION", complexQuery);

            // Execute to verify it works
            IEnumerable<Author> results = publishingAuthors.Union(prolificAuthors).Execute();
            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests multiple chained set operations.
        /// </summary>
        [Fact]
        public void TestMultipleSetOperations()
        {
            if (_SkipTests) return;

            IQueryBuilder<Book> query1 = _BookRepository.Query().Where(b => b.AuthorId == 1);
            IQueryBuilder<Book> query2 = _BookRepository.Query().Where(b => b.Title.Contains("Science"));
            IQueryBuilder<Book> query3 = _BookRepository.Query().Where(b => b.PublisherId == 1);

            string sql = query1
                .Union(query2)
                .Except(query3)
                .BuildSql();

            // Just verify the SQL is generated and can be executed
            Assert.False(string.IsNullOrEmpty(sql));

            // Execute to verify it works
            IEnumerable<Book> results = query1.Union(query2).Except(query3).Execute();
            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests advanced aggregation with window functions.
        /// </summary>
        [Fact]
        public void TestAdvancedAggregationWithWindows()
        {
            if (_SkipTests) return;

            // Test multiple window functions in same query
            string query = _BookRepository.Query()
                .WithWindowFunction("ROW_NUMBER")
                .RowNumber("book_number")
                .PartitionBy(b => b.AuthorId)
                .OrderBy(b => b.Id)
                .EndWindow()
                .WithWindowFunction("COUNT")
                .Count("total_books")
                .PartitionBy(b => b.AuthorId)
                .EndWindow()
                .BuildSql();

            Assert.Contains("ROW_NUMBER() OVER (PARTITION", query);
            Assert.Contains("COUNT(*) OVER (PARTITION", query);
            Assert.Contains("ORDER BY", query);
        }

        /// <summary>
        /// Tests SQL capture with advanced query features.
        /// </summary>
        [Fact]
        public void TestSqlCaptureWithAdvancedFeatures()
        {
            if (_SkipTests) return;

            // Test SQL generation for complex query with CTE and window function
            string cteQuery = "SELECT id FROM companies WHERE industry = 'Publishing'";

            string sql = _AuthorRepository.Query()
                .WithCte("publishing_companies", cteQuery)
                .WithWindowFunction("ROW_NUMBER")
                .RowNumber("row_num")
                .OrderBy(a => a.Name)
                .EndWindow()
                .Where(a => a.CompanyId != null)
                .BuildSql();

            Assert.Contains("WITH `publishing_companies`", sql);
            Assert.Contains("ROW_NUMBER()", sql);
            Assert.Contains("WHERE", sql);
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

                Console.WriteLine("✅ MySQL advanced query builder tests enabled - database connection successful");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  MySQL advanced query builder tests disabled - could not connect to database: {ex.Message}");
                Console.WriteLine("To enable MySQL advanced query builder tests:");
                Console.WriteLine("1. Start MySQL server on localhost:3306");
                Console.WriteLine($"2. Create database: CREATE DATABASE {TestDatabaseName};");
                Console.WriteLine("3. Create user: CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'test_password';");
                Console.WriteLine($"4. Grant permissions: GRANT ALL PRIVILEGES ON {TestDatabaseName}.* TO 'test_user'@'localhost';");
                _SkipTests = true;
            }
        }

        /// <summary>
        /// Creates the necessary tables for testing.
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
                    INDEX idx_industry (industry)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

            // Create authors table
            _AuthorRepository.ExecuteSql(@"
                CREATE TABLE authors (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    company_id INT,
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

            // Create books table
            _BookRepository.ExecuteSql(@"
                CREATE TABLE books (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    author_id INT NOT NULL,
                    publisher_id INT,
                    INDEX idx_author_id (author_id),
                    INDEX idx_publisher_id (publisher_id),
                    INDEX idx_title (title),
                    FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE,
                    FOREIGN KEY (publisher_id) REFERENCES companies(id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");
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
        /// Inserts test data for advanced query testing.
        /// </summary>
        private void InsertTestData()
        {
            if (_SkipTests) return;

            // Create companies
            Company publisher1 = _CompanyRepository.Create(new Company { Name = "Orwell Publishing", Industry = "Publishing" });
            Company publisher2 = _CompanyRepository.Create(new Company { Name = "Asimov Press", Industry = "Publishing" });
            Company techCorp = _CompanyRepository.Create(new Company { Name = "Tech Solutions", Industry = "Technology" });

            // Create authors
            Author orwell = _AuthorRepository.Create(new Author { Name = "George Orwell", CompanyId = publisher1.Id });
            Author asimov = _AuthorRepository.Create(new Author { Name = "Isaac Asimov", CompanyId = publisher2.Id });
            Author austen = _AuthorRepository.Create(new Author { Name = "Jane Austen", CompanyId = null });
            Author techAuthor = _AuthorRepository.Create(new Author { Name = "Tech Writer", CompanyId = techCorp.Id });

            // Create categories
            Category fiction = _CategoryRepository.Create(new Category { Name = "Fiction", Description = "Fiction books" });
            Category sciFi = _CategoryRepository.Create(new Category { Name = "Science Fiction", Description = "Science fiction books" });
            Category classic = _CategoryRepository.Create(new Category { Name = "Classic", Description = "Classic literature" });

            // Create books
            _BookRepository.Create(new Book { Title = "1984", AuthorId = orwell.Id, PublisherId = publisher1.Id });
            _BookRepository.Create(new Book { Title = "Animal Farm", AuthorId = orwell.Id, PublisherId = publisher1.Id });
            _BookRepository.Create(new Book { Title = "Foundation", AuthorId = asimov.Id, PublisherId = publisher2.Id });
            _BookRepository.Create(new Book { Title = "I, Robot", AuthorId = asimov.Id, PublisherId = publisher2.Id });
            _BookRepository.Create(new Book { Title = "Pride and Prejudice", AuthorId = austen.Id, PublisherId = null });
            _BookRepository.Create(new Book { Title = "Tech Manual", AuthorId = techAuthor.Id, PublisherId = techCorp.Id });
        }

        #endregion
    }
}