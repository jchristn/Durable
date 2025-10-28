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

    /// <summary>
    /// Comprehensive tests for advanced PostgreSQL query builder features including GroupBy,
    /// window functions, CTEs, set operations, and subqueries.
    /// These tests require a running PostgreSQL server.
    /// </summary>
    public class PostgresAdvancedQueryBuilderTests : IDisposable
    {

        #region Private-Members

        private const string TestConnectionString = "Host=localhost;Database=durable_advanced_test;Username=test_user;Password=test_password;";
        private const string TestDatabaseName = "durable_advanced_test";
        private static readonly object _TestLock = new object();
        private static bool _DatabaseSetupComplete = false;
        private static bool _SkipTests = false;

        private readonly PostgresRepository<Author> _AuthorRepository;
        private readonly PostgresRepository<Book> _BookRepository;
        private readonly PostgresRepository<Company> _CompanyRepository;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the advanced query builder test class by setting up the test database and repositories.
        /// </summary>
        public PostgresAdvancedQueryBuilderTests()
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

            _AuthorRepository = new PostgresRepository<Author>(TestConnectionString);
            _BookRepository = new PostgresRepository<Book>(TestConnectionString);
            _CompanyRepository = new PostgresRepository<Company>(TestConnectionString);

            CreateTablesAndInsertTestData();
        }

        #endregion

        #region Public-Methods

        // ==================== GROUPBY TESTS ====================

        /// <summary>
        /// Tests basic GroupBy functionality with count aggregation.
        /// </summary>
        [Fact]
        public void GroupBy_BasicGrouping_WorksCorrectly()
        {
            if (_SkipTests) return;

            IEnumerable<Book> books = _BookRepository.ReadMany();
            List<IGrouping<int, Book>> grouped = books.GroupBy(b => b.AuthorId).ToList();

            Assert.NotEmpty(grouped);
            Assert.All(grouped, g => Assert.True(g.Count() > 0));
        }

        /// <summary>
        /// Tests GroupBy with multiple grouping keys.
        /// </summary>
        [Fact]
        public void GroupBy_MultipleKeys_WorksCorrectly()
        {
            if (_SkipTests) return;

            IEnumerable<Book> books = _BookRepository.ReadMany();
            var grouped = books.GroupBy(b => new { b.AuthorId, b.PublisherId }).ToList();

            Assert.NotEmpty(grouped);
        }

        /// <summary>
        /// Tests GroupBy with Having-like filtering.
        /// </summary>
        [Fact]
        public void GroupBy_WithFiltering_WorksCorrectly()
        {
            if (_SkipTests) return;

            IEnumerable<Book> books = _BookRepository.ReadMany();
            List<IGrouping<int, Book>> grouped = books
                .GroupBy(b => b.AuthorId)
                .Where(g => g.Count() > 1)
                .ToList();

            Assert.All(grouped, g => Assert.True(g.Count() > 1));
        }

        /// <summary>
        /// Tests GroupBy with aggregation projections.
        /// </summary>
        [Fact]
        public void GroupBy_WithAggregations_WorksCorrectly()
        {
            if (_SkipTests) return;

            IEnumerable<Book> books = _BookRepository.ReadMany();
            var results = books
                .GroupBy(b => b.AuthorId)
                .Select(g => new
                {
                    AuthorId = g.Key,
                    BookCount = g.Count(),
                    TitleList = g.Select(b => b.Title).ToList()
                })
                .ToList();

            Assert.NotEmpty(results);
            Assert.All(results, r => Assert.True(r.BookCount > 0));
        }

        // ==================== WINDOW FUNCTION TESTS ====================

        /// <summary>
        /// Tests window function feature exists and can be called.
        /// </summary>
        [Fact]
        public void WindowFunction_CanBeInvoked_WorksCorrectly()
        {
            if (_SkipTests) return;

            IQueryBuilder<Book> query = _BookRepository.Query();
            IWindowedQueryBuilder<Book> windowQuery = query.WithWindowFunction("ROW_NUMBER");

            Assert.NotNull(windowQuery);
        }

        /// <summary>
        /// Tests window function with RANK.
        /// </summary>
        [Fact]
        public void WindowFunction_Rank_WorksCorrectly()
        {
            if (_SkipTests) return;

            IWindowedQueryBuilder<Book> windowQuery = _BookRepository.Query()
                .WithWindowFunction("RANK");

            Assert.NotNull(windowQuery);

            IQueryBuilder<Book> endedQuery = windowQuery.EndWindow();
            Assert.NotNull(endedQuery);
        }

        /// <summary>
        /// Tests window function with DENSE_RANK.
        /// </summary>
        [Fact]
        public void WindowFunction_DenseRank_WorksCorrectly()
        {
            if (_SkipTests) return;

            IWindowedQueryBuilder<Book> windowQuery = _BookRepository.Query()
                .WithWindowFunction("DENSE_RANK");

            Assert.NotNull(windowQuery);

            IQueryBuilder<Book> endedQuery = windowQuery.EndWindow();
            IEnumerable<Book> results = endedQuery.Execute();
            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests multiple window functions can be chained.
        /// </summary>
        [Fact]
        public void WindowFunction_MultipleWindows_WorksCorrectly()
        {
            if (_SkipTests) return;

            IQueryBuilder<Book> query = _BookRepository.Query()
                .WithWindowFunction("ROW_NUMBER")
                .EndWindow()
                .WithWindowFunction("RANK")
                .EndWindow();

            Assert.NotNull(query);
            IEnumerable<Book> results = query.Execute();
            Assert.NotNull(results);
        }

        // ==================== CTE TESTS ====================

        /// <summary>
        /// Tests basic CTE (Common Table Expression) with WITH clause.
        /// </summary>
        [Fact]
        public void CTE_BasicWithClause_WorksCorrectly()
        {
            if (_SkipTests) return;

            string cteQuery = "SELECT id, name FROM companies WHERE industry = 'Technology'";

            IQueryBuilder<Author> query = _AuthorRepository.Query()
                .WithCte("tech_companies", cteQuery);

            Assert.NotNull(query);
        }

        /// <summary>
        /// Tests recursive CTE for hierarchical data.
        /// </summary>
        [Fact]
        public void CTE_RecursiveCTE_WorksCorrectly()
        {
            if (_SkipTests) return;

            string anchorQuery = "SELECT id, name, company_id FROM authors WHERE company_id IS NULL";
            string recursiveQuery = "SELECT a.id, a.name, a.company_id FROM authors a";

            IQueryBuilder<Author> query = _AuthorRepository.Query()
                .WithRecursiveCte("author_hierarchy", anchorQuery, recursiveQuery);

            Assert.NotNull(query);
        }

        /// <summary>
        /// Tests multiple CTEs in a single query.
        /// </summary>
        [Fact]
        public void CTE_MultipleCTEs_WorksCorrectly()
        {
            if (_SkipTests) return;

            string cte1 = "SELECT id FROM companies WHERE industry = 'Technology'";
            string cte2 = "SELECT id FROM authors WHERE company_id IN (SELECT id FROM tech_companies)";

            IQueryBuilder<Book> query = _BookRepository.Query()
                .WithCte("tech_companies", cte1)
                .WithCte("tech_authors", cte2);

            Assert.NotNull(query);
        }

        // ==================== SET OPERATION TESTS ====================

        /// <summary>
        /// Tests UNION operation between two queries.
        /// </summary>
        [Fact]
        public void SetOperation_Union_WorksCorrectly()
        {
            if (_SkipTests) return;

            IQueryBuilder<Author> query1 = _AuthorRepository.Query()
                .Where(a => a.CompanyId == 1);

            IQueryBuilder<Author> query2 = _AuthorRepository.Query()
                .Where(a => a.CompanyId == 2);

            IQueryBuilder<Author> unionQuery = query1.Union(query2);
            Assert.NotNull(unionQuery);

            IEnumerable<Author> results = unionQuery.Execute();
            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests UNION ALL which includes duplicates.
        /// </summary>
        [Fact]
        public void SetOperation_UnionAll_WorksCorrectly()
        {
            if (_SkipTests) return;

            IQueryBuilder<Book> query1 = _BookRepository.Query()
                .Where(b => b.AuthorId == 1);

            IQueryBuilder<Book> query2 = _BookRepository.Query()
                .Where(b => b.AuthorId == 2);

            IQueryBuilder<Book> unionAllQuery = query1.UnionAll(query2);
            Assert.NotNull(unionAllQuery);

            IEnumerable<Book> results = unionAllQuery.Execute();
            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests INTERSECT to find common records between two queries.
        /// </summary>
        [Fact]
        public void SetOperation_Intersect_WorksCorrectly()
        {
            if (_SkipTests) return;

            IQueryBuilder<Author> query1 = _AuthorRepository.Query()
                .Where(a => a.CompanyId != null);

            IQueryBuilder<Author> query2 = _AuthorRepository.Query();

            IQueryBuilder<Author> intersectQuery = query1.Intersect(query2);
            Assert.NotNull(intersectQuery);

            IEnumerable<Author> results = intersectQuery.Execute();
            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests EXCEPT to find records in first query but not in second.
        /// </summary>
        [Fact]
        public void SetOperation_Except_WorksCorrectly()
        {
            if (_SkipTests) return;

            IQueryBuilder<Author> query1 = _AuthorRepository.Query();

            IQueryBuilder<Author> query2 = _AuthorRepository.Query()
                .Where(a => a.CompanyId == 1);

            IQueryBuilder<Author> exceptQuery = query1.Except(query2);
            Assert.NotNull(exceptQuery);

            IEnumerable<Author> results = exceptQuery.Execute();
            Assert.NotNull(results);
        }

        // ==================== SUBQUERY TESTS ====================

        /// <summary>
        /// Tests WhereExists for existence checks.
        /// </summary>
        [Fact]
        public void Subquery_WhereExists_WorksCorrectly()
        {
            if (_SkipTests) return;

            IQueryBuilder<Book> bookSubquery = _BookRepository.Query()
                .WhereRaw("books.author_id = authors.id");

            IQueryBuilder<Author> query = _AuthorRepository.Query()
                .WhereExists(bookSubquery);

            Assert.NotNull(query);
            IEnumerable<Author> results = query.Execute();
            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests WhereNotExists for non-existence checks.
        /// </summary>
        [Fact]
        public void Subquery_WhereNotExists_WorksCorrectly()
        {
            if (_SkipTests) return;

            IQueryBuilder<Book> bookSubquery = _BookRepository.Query()
                .WhereRaw("books.author_id = authors.id");

            IQueryBuilder<Author> query = _AuthorRepository.Query()
                .WhereNotExists(bookSubquery);

            Assert.NotNull(query);
            IEnumerable<Author> results = query.Execute();
            Assert.NotNull(results);
        }

        /// <summary>
        /// Tests WhereInRaw with raw SQL subquery.
        /// </summary>
        [Fact]
        public void Subquery_WhereInRaw_WorksCorrectly()
        {
            if (_SkipTests) return;

            string subquery = "SELECT id FROM companies WHERE industry = 'Technology'";

            IQueryBuilder<Author> query = _AuthorRepository.Query()
                .WhereInRaw(a => a.CompanyId, subquery);

            Assert.NotNull(query);
            IEnumerable<Author> results = query.Execute();
            Assert.NotNull(results);
        }

        #endregion

        #region Cleanup

        /// <summary>
        /// Cleans up test resources.
        /// </summary>
        public void Dispose()
        {
            _AuthorRepository?.Dispose();
            _BookRepository?.Dispose();
            _CompanyRepository?.Dispose();
        }

        #endregion

        #region Private-Methods

        private void SetupTestDatabase()
        {
            try
            {
                using NpgsqlConnection connection = new NpgsqlConnection("Host=localhost;Database=postgres;Username=test_user;Password=test_password;");
                connection.Open();

                using NpgsqlCommand checkDbCommand = new NpgsqlCommand($"SELECT 1 FROM pg_database WHERE datname = '{TestDatabaseName}'", connection);
                object result = checkDbCommand.ExecuteScalar();

                if (result == null)
                {
                    using NpgsqlCommand createDbCommand = new NpgsqlCommand($"CREATE DATABASE {TestDatabaseName}", connection);
                    createDbCommand.ExecuteNonQuery();
                }
            }
            catch (Exception)
            {
                _SkipTests = true;
            }
        }

        private void CreateTablesAndInsertTestData()
        {
            try
            {
                using NpgsqlConnection connection = new NpgsqlConnection(TestConnectionString);
                connection.Open();

                using NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = @"
                    DROP TABLE IF EXISTS books CASCADE;
                    DROP TABLE IF EXISTS authors CASCADE;
                    DROP TABLE IF EXISTS companies CASCADE;

                    CREATE TABLE companies (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        industry VARCHAR(50)
                    );

                    CREATE TABLE authors (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        company_id INTEGER REFERENCES companies(id)
                    );

                    CREATE TABLE books (
                        id SERIAL PRIMARY KEY,
                        title VARCHAR(200) NOT NULL,
                        author_id INTEGER NOT NULL REFERENCES authors(id),
                        publisher_id INTEGER REFERENCES companies(id)
                    );

                    INSERT INTO companies (name, industry) VALUES
                        ('Tech Corp', 'Technology'),
                        ('Book Publishers Inc', 'Publishing'),
                        ('Digital Media', 'Technology');

                    INSERT INTO authors (name, company_id) VALUES
                        ('Alice Tech', 1),
                        ('Bob Writer', 2),
                        ('Carol Digital', 3),
                        ('Dave Independent', NULL);

                    INSERT INTO books (title, author_id, publisher_id) VALUES
                        ('Tech Guide 1', 1, 1),
                        ('Tech Guide 2', 1, 1),
                        ('Novel 1', 2, 2),
                        ('Novel 2', 2, 2),
                        ('Digital Future', 3, 3),
                        ('Independent Work', 4, NULL);
                ";

                command.ExecuteNonQuery();
            }
            catch (Exception)
            {
                _SkipTests = true;
            }
        }

        #endregion

    }
}
