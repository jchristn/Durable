namespace Test.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Npgsql;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Comprehensive tests for async methods that currently have zero or minimal test coverage.
    /// Tests ReadFirstAsync, ReadSingleAsync, DeleteAsync, CreateManyAsync, UpdateManyAsync,
    /// DeleteManyAsync, CountAsync, ExistsAsync, and ExecuteSqlAsync.
    /// </summary>
    public class PostgresAsyncMethodTests : IDisposable
    {

        #region Private-Members

        private const string TestConnectionString = "Host=localhost;Database=durable_async_test;Username=test_user;Password=test_password;";
        private const string TestDatabaseName = "durable_async_test";
        private static readonly object _TestLock = new object();
        private static bool _DatabaseSetupComplete = false;
        private static bool _SkipTests = false;

        private readonly PostgresRepository<Author> _AuthorRepository;
        private readonly PostgresRepository<Book> _BookRepository;
        private readonly PostgresRepository<Company> _CompanyRepository;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the async method test class by setting up the test database and repositories.
        /// </summary>
        public PostgresAsyncMethodTests()
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

        // ==================== READFIRSTASYNC / READSINGLEASYNC TESTS ====================

        /// <summary>
        /// Tests ReadFirstAsync returns first matching record.
        /// </summary>
        [Fact]
        public async Task ReadFirstAsync_WithMatchingRecords_ReturnsFirst()
        {
            if (_SkipTests) return;

            Author author = await _AuthorRepository.ReadFirstAsync(a => a.CompanyId == 1);

            Assert.NotNull(author);
            Assert.Equal(1, author.CompanyId);
        }

        /// <summary>
        /// Tests ReadFirstAsync returns null when no records match.
        /// </summary>
        [Fact]
        public async Task ReadFirstAsync_WithNoMatches_ReturnsNull()
        {
            if (_SkipTests) return;

            Author author = await _AuthorRepository.ReadFirstAsync(a => a.CompanyId == 999);

            Assert.Null(author);
        }

        /// <summary>
        /// Tests ReadFirstAsync with ordering and multiple matches.
        /// </summary>
        [Fact]
        public async Task ReadFirstAsync_WithMultipleMatches_ReturnsFirst()
        {
            if (_SkipTests) return;

            Author author = await _AuthorRepository.ReadFirstAsync(a => a.CompanyId != null);

            Assert.NotNull(author);
            Assert.True(author.Id > 0);
        }

        /// <summary>
        /// Tests ReadFirstAsync supports cancellation.
        /// </summary>
        [Fact]
        public async Task ReadFirstAsync_SupportsCancellation()
        {
            if (_SkipTests) return;

            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await _AuthorRepository.ReadFirstAsync(a => a.Id > 0, token: cts.Token));
        }

        /// <summary>
        /// Tests ReadSingleAsync returns exactly one record when match exists.
        /// </summary>
        [Fact]
        public async Task ReadSingleAsync_WithSingleMatch_ReturnsRecord()
        {
            if (_SkipTests) return;

            Author author = await _AuthorRepository.ReadSingleAsync(a => a.Name == "Alice Tech");

            Assert.NotNull(author);
            Assert.Equal("Alice Tech", author.Name);
        }

        /// <summary>
        /// Tests ReadSingleAsync throws when no records match.
        /// </summary>
        [Fact]
        public async Task ReadSingleAsync_WithNoMatches_ThrowsException()
        {
            if (_SkipTests) return;

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await _AuthorRepository.ReadSingleAsync(a => a.Name == "NonExistent"));
        }

        /// <summary>
        /// Tests ReadSingleAsync throws when multiple records match.
        /// </summary>
        [Fact]
        public async Task ReadSingleAsync_WithMultipleMatches_ThrowsException()
        {
            if (_SkipTests) return;

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await _AuthorRepository.ReadSingleAsync(a => a.CompanyId == 1));
        }

        // ==================== DELETEASYNC TESTS ====================

        /// <summary>
        /// Tests DeleteAsync removes a single entity.
        /// </summary>
        [Fact]
        public async Task DeleteAsync_WithValidEntity_DeletesRecord()
        {
            if (_SkipTests) return;

            Author newAuthor = new Author
            {
                Name = "Delete Test Author",
                CompanyId = 1
            };

            Author created = await _AuthorRepository.CreateAsync(newAuthor);
            Assert.True(created.Id > 0);

            bool deleted = await _AuthorRepository.DeleteAsync(created);
            Assert.True(deleted);

            Author retrieved = await _AuthorRepository.ReadFirstAsync(a => a.Id == created.Id);
            Assert.Null(retrieved);
        }

        /// <summary>
        /// Tests DeleteAsync with transaction support.
        /// </summary>
        [Fact]
        public async Task DeleteAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            Author newAuthor = new Author
            {
                Name = "Transaction Delete Test",
                CompanyId = 2
            };

            Author created = await _AuthorRepository.CreateAsync(newAuthor);

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();
            bool deleted = await _AuthorRepository.DeleteAsync(created, transaction);
            Assert.True(deleted);

            await transaction.CommitAsync();

            Author retrieved = await _AuthorRepository.ReadFirstAsync(a => a.Id == created.Id);
            Assert.Null(retrieved);
        }

        /// <summary>
        /// Tests DeleteAsync supports cancellation.
        /// </summary>
        [Fact]
        public async Task DeleteAsync_SupportsCancellation()
        {
            if (_SkipTests) return;

            Author newAuthor = new Author
            {
                Name = "Cancellation Test",
                CompanyId = 1
            };

            Author created = await _AuthorRepository.CreateAsync(newAuthor);

            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await _AuthorRepository.DeleteAsync(created, token: cts.Token));
        }

        // ==================== BULK ASYNC OPERATION TESTS ====================

        /// <summary>
        /// Tests CreateManyAsync inserts multiple entities.
        /// </summary>
        [Fact]
        public async Task CreateManyAsync_WithMultipleEntities_InsertsAll()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Bulk Author 1", CompanyId = 1 },
                new Author { Name = "Bulk Author 2", CompanyId = 2 },
                new Author { Name = "Bulk Author 3", CompanyId = 3 }
            };

            IEnumerable<Author> created = await _AuthorRepository.CreateManyAsync(authors);
            List<Author> createdList = created.ToList();

            Assert.Equal(3, createdList.Count);
            Assert.All(createdList, a => Assert.True(a.Id > 0));
        }

        /// <summary>
        /// Tests CreateManyAsync with empty list.
        /// </summary>
        [Fact]
        public async Task CreateManyAsync_WithEmptyList_ReturnsEmpty()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>();
            IEnumerable<Author> created = await _AuthorRepository.CreateManyAsync(authors);

            Assert.Empty(created);
        }

        /// <summary>
        /// Tests CreateManyAsync with transaction support.
        /// </summary>
        [Fact]
        public async Task CreateManyAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Transaction Bulk 1", CompanyId = 1 },
                new Author { Name = "Transaction Bulk 2", CompanyId = 2 }
            };

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();
            IEnumerable<Author> created = await _AuthorRepository.CreateManyAsync(authors, transaction);
            Assert.Equal(2, created.Count());

            await transaction.CommitAsync();

            int count = await _AuthorRepository.CountAsync(a => a.Name.StartsWith("Transaction Bulk"));
            Assert.Equal(2, count);
        }

        /// <summary>
        /// Tests UpdateManyAsync updates multiple entities.
        /// </summary>
        [Fact]
        public async Task UpdateManyAsync_WithMultipleEntities_UpdatesAll()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Update Test 1", CompanyId = 1 },
                new Author { Name = "Update Test 2", CompanyId = 1 }
            };

            IEnumerable<Author> created = await _AuthorRepository.CreateManyAsync(authors);
            List<Author> createdList = created.ToList();

            int updated = await _AuthorRepository.UpdateManyAsync(
                a => a.Name.StartsWith("Update Test"),
                async (a) => { a.CompanyId = 3; await Task.CompletedTask; });

            Assert.Equal(2, updated);

            IEnumerable<Author> retrieved = _AuthorRepository.ReadMany(a => a.Name.StartsWith("Update Test"));
            Assert.All(retrieved, a => Assert.Equal(3, a.CompanyId));
        }

        /// <summary>
        /// Tests UpdateManyAsync with no matching records.
        /// </summary>
        [Fact]
        public async Task UpdateManyAsync_WithNoMatches_ReturnsZero()
        {
            if (_SkipTests) return;

            int updated = await _AuthorRepository.UpdateManyAsync(
                a => a.Name == "NonExistentAuthor",
                async (a) => { a.CompanyId = 1; await Task.CompletedTask; });

            Assert.Equal(0, updated);
        }

        /// <summary>
        /// Tests DeleteManyAsync deletes multiple entities.
        /// </summary>
        [Fact]
        public async Task DeleteManyAsync_WithMultipleEntities_DeletesAll()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Delete Bulk 1", CompanyId = 1 },
                new Author { Name = "Delete Bulk 2", CompanyId = 1 },
                new Author { Name = "Delete Bulk 3", CompanyId = 1 }
            };

            await _AuthorRepository.CreateManyAsync(authors);

            int deleted = await _AuthorRepository.DeleteManyAsync(a => a.Name.StartsWith("Delete Bulk"));
            Assert.Equal(3, deleted);

            int remaining = await _AuthorRepository.CountAsync(a => a.Name.StartsWith("Delete Bulk"));
            Assert.Equal(0, remaining);
        }

        /// <summary>
        /// Tests DeleteManyAsync with transaction and rollback.
        /// </summary>
        [Fact]
        public async Task DeleteManyAsync_WithTransactionRollback_KeepsRecords()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Rollback Delete 1", CompanyId = 1 },
                new Author { Name = "Rollback Delete 2", CompanyId = 1 }
            };

            await _AuthorRepository.CreateManyAsync(authors);
            int initialCount = await _AuthorRepository.CountAsync(a => a.Name.StartsWith("Rollback Delete"));

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();
            int deleted = await _AuthorRepository.DeleteManyAsync(a => a.Name.StartsWith("Rollback Delete"), transaction);
            Assert.Equal(2, deleted);

            await transaction.RollbackAsync();

            int finalCount = await _AuthorRepository.CountAsync(a => a.Name.StartsWith("Rollback Delete"));
            Assert.Equal(initialCount, finalCount);
        }

        // ==================== COUNTASYNC / EXISTSASYNC TESTS ====================

        /// <summary>
        /// Tests CountAsync returns correct count.
        /// </summary>
        [Fact]
        public async Task CountAsync_WithMatchingRecords_ReturnsCount()
        {
            if (_SkipTests) return;

            int count = await _AuthorRepository.CountAsync(a => a.CompanyId == 1);
            Assert.True(count > 0);
        }

        /// <summary>
        /// Tests CountAsync with no matches returns zero.
        /// </summary>
        [Fact]
        public async Task CountAsync_WithNoMatches_ReturnsZero()
        {
            if (_SkipTests) return;

            int count = await _AuthorRepository.CountAsync(a => a.CompanyId == 999);
            Assert.Equal(0, count);
        }

        /// <summary>
        /// Tests CountAsync without predicate returns total count.
        /// </summary>
        [Fact]
        public async Task CountAsync_WithoutPredicate_ReturnsTotalCount()
        {
            if (_SkipTests) return;

            int count = await _AuthorRepository.CountAsync();
            Assert.True(count >= 4);
        }

        /// <summary>
        /// Tests CountAsync with transaction.
        /// </summary>
        [Fact]
        public async Task CountAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();

            Author newAuthor = new Author { Name = "Count Transaction Test", CompanyId = 1 };
            await _AuthorRepository.CreateAsync(newAuthor, transaction);

            int count = await _AuthorRepository.CountAsync(a => a.Name == "Count Transaction Test", transaction);
            Assert.Equal(1, count);

            await transaction.RollbackAsync();

            int countAfterRollback = await _AuthorRepository.CountAsync(a => a.Name == "Count Transaction Test");
            Assert.Equal(0, countAfterRollback);
        }

        /// <summary>
        /// Tests ExistsAsync returns true when record exists.
        /// </summary>
        [Fact]
        public async Task ExistsAsync_WithMatchingRecord_ReturnsTrue()
        {
            if (_SkipTests) return;

            bool exists = await _AuthorRepository.ExistsAsync(a => a.Name == "Alice Tech");
            Assert.True(exists);
        }

        /// <summary>
        /// Tests ExistsAsync returns false when record does not exist.
        /// </summary>
        [Fact]
        public async Task ExistsAsync_WithNoMatch_ReturnsFalse()
        {
            if (_SkipTests) return;

            bool exists = await _AuthorRepository.ExistsAsync(a => a.Name == "NonExistent Author");
            Assert.False(exists);
        }

        /// <summary>
        /// Tests ExistsAsync with transaction.
        /// </summary>
        [Fact]
        public async Task ExistsAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();

            Author newAuthor = new Author { Name = "Exists Transaction Test", CompanyId = 1 };
            await _AuthorRepository.CreateAsync(newAuthor, transaction);

            bool existsInTransaction = await _AuthorRepository.ExistsAsync(a => a.Name == "Exists Transaction Test", transaction);
            Assert.True(existsInTransaction);

            await transaction.RollbackAsync();

            bool existsAfterRollback = await _AuthorRepository.ExistsAsync(a => a.Name == "Exists Transaction Test");
            Assert.False(existsAfterRollback);
        }

        // ==================== EXECUTESQLASYNC TESTS ====================

        /// <summary>
        /// Tests ExecuteSqlAsync executes INSERT statements.
        /// </summary>
        [Fact]
        public async Task ExecuteSqlAsync_WithInsert_InsertsRecord()
        {
            if (_SkipTests) return;

            string sql = "INSERT INTO authors (name, company_id) VALUES ('SQL Insert Test', 1)";
            int rowsAffected = await _AuthorRepository.ExecuteSqlAsync(sql);

            Assert.Equal(1, rowsAffected);

            Author author = await _AuthorRepository.ReadFirstAsync(a => a.Name == "SQL Insert Test");
            Assert.NotNull(author);
        }

        /// <summary>
        /// Tests ExecuteSqlAsync executes UPDATE statements.
        /// </summary>
        [Fact]
        public async Task ExecuteSqlAsync_WithUpdate_UpdatesRecords()
        {
            if (_SkipTests) return;

            Author newAuthor = new Author { Name = "SQL Update Test", CompanyId = 1 };
            await _AuthorRepository.CreateAsync(newAuthor);

            string sql = $"UPDATE authors SET company_id = 2 WHERE name = 'SQL Update Test'";
            int rowsAffected = await _AuthorRepository.ExecuteSqlAsync(sql);

            Assert.Equal(1, rowsAffected);

            Author updated = await _AuthorRepository.ReadFirstAsync(a => a.Name == "SQL Update Test");
            Assert.Equal(2, updated.CompanyId);
        }

        /// <summary>
        /// Tests ExecuteSqlAsync executes DELETE statements.
        /// </summary>
        [Fact]
        public async Task ExecuteSqlAsync_WithDelete_DeletesRecords()
        {
            if (_SkipTests) return;

            Author newAuthor = new Author { Name = "SQL Delete Test", CompanyId = 1 };
            await _AuthorRepository.CreateAsync(newAuthor);

            string sql = "DELETE FROM authors WHERE name = 'SQL Delete Test'";
            int rowsAffected = await _AuthorRepository.ExecuteSqlAsync(sql);

            Assert.Equal(1, rowsAffected);

            Author deleted = await _AuthorRepository.ReadFirstAsync(a => a.Name == "SQL Delete Test");
            Assert.Null(deleted);
        }

        /// <summary>
        /// Tests ExecuteSqlAsync with transaction.
        /// </summary>
        [Fact]
        public async Task ExecuteSqlAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();

            string sql = "INSERT INTO authors (name, company_id) VALUES ('SQL Transaction Test', 1)";
            int rowsAffected = await _AuthorRepository.ExecuteSqlAsync(sql, transaction);
            Assert.Equal(1, rowsAffected);

            await transaction.CommitAsync();

            Author author = await _AuthorRepository.ReadFirstAsync(a => a.Name == "SQL Transaction Test");
            Assert.NotNull(author);
        }

        /// <summary>
        /// Tests ExecuteSqlAsync with transaction rollback.
        /// </summary>
        [Fact]
        public async Task ExecuteSqlAsync_WithRollback_RevertsChanges()
        {
            if (_SkipTests) return;

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();

            string sql = "INSERT INTO authors (name, company_id) VALUES ('SQL Rollback Test', 1)";
            await _AuthorRepository.ExecuteSqlAsync(sql, transaction);

            await transaction.RollbackAsync();

            Author author = await _AuthorRepository.ReadFirstAsync(a => a.Name == "SQL Rollback Test");
            Assert.Null(author);
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
