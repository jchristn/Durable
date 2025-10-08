using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Durable;
using Durable.SqlServer;
using Test.Shared;
using Microsoft.Data.SqlClient;

namespace Test.SqlServer
{
    /// <summary>
    /// Comprehensive tests for SQL Server async repository methods with focus on:
    /// - ReadSingleAsync: Single record retrieval with exceptions
    /// - UpsertAsync/UpsertManyAsync: Insert-or-update operations
    /// - ExistsAsync: Existence checking
    /// - UpdateManyAsync/DeleteManyAsync: Bulk operations
    /// - Edge cases, error conditions, and cancellation scenarios
    /// </summary>
    public class SqlServerAsyncMethodTests : IDisposable
    {

        #region Private-Members

        private const string TestConnectionString = "Server=view.homedns.org,1433;Database=durable_async_test;User=sa;Password=P@ssw0rd4Sql;TrustServerCertificate=true;Encrypt=false;";
        private const string TestDatabaseName = "durable_async_test";
        private static readonly object _TestLock = new object();
        private static bool _DatabaseSetupComplete = false;
        private static bool _SkipTests = false;

        private readonly SqlServerRepository<Author> _AuthorRepository;
        private readonly SqlServerRepository<Book> _BookRepository;
        private readonly SqlServerRepository<Company> _CompanyRepository;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the async method test class by setting up the test database and repositories.
        /// </summary>
        public SqlServerAsyncMethodTests()
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

            _AuthorRepository = new SqlServerRepository<Author>(TestConnectionString);
            _BookRepository = new SqlServerRepository<Book>(TestConnectionString);
            _CompanyRepository = new SqlServerRepository<Company>(TestConnectionString);

            CreateTablesAndInsertTestData();
        }

        #endregion

        #region Public-Methods

        // ==================== READSINGLEASYNC TESTS ====================

        /// <summary>
        /// Tests ReadSingleAsync returns the single matching record.
        /// </summary>
        [Fact]
        public async Task ReadSingleAsync_WithSingleMatch_ReturnsRecord()
        {
            if (_SkipTests) return;

            Author newAuthor = new Author
            {
                Name = "Unique Single Author",
                CompanyId = 1
            };

            Author created = await _AuthorRepository.CreateAsync(newAuthor);
            Author single = await _AuthorRepository.ReadSingleAsync(a => a.Id == created.Id);

            Assert.NotNull(single);
            Assert.Equal(created.Id, single.Id);
            Assert.Equal("Unique Single Author", single.Name);

            await _AuthorRepository.DeleteAsync(created);
        }

        /// <summary>
        /// Tests ReadSingleAsync throws when no matches found.
        /// </summary>
        [Fact]
        public async Task ReadSingleAsync_WithNoMatches_ThrowsException()
        {
            if (_SkipTests) return;

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await _AuthorRepository.ReadSingleAsync(a => a.Id == 999999);
            });
        }

        /// <summary>
        /// Tests ReadSingleAsync throws when multiple matches found.
        /// </summary>
        [Fact]
        public async Task ReadSingleAsync_WithMultipleMatches_ThrowsException()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Duplicate Test", CompanyId = 1 },
                new Author { Name = "Duplicate Test", CompanyId = 1 }
            };

            IEnumerable<Author> created = await _AuthorRepository.CreateManyAsync(authors);
            List<Author> createdList = created.ToList();

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await _AuthorRepository.ReadSingleAsync(a => a.Name == "Duplicate Test");
            });

            foreach (Author author in createdList)
            {
                await _AuthorRepository.DeleteAsync(author);
            }
        }

        /// <summary>
        /// Tests ReadSingleAsync with transaction support.
        /// </summary>
        [Fact]
        public async Task ReadSingleAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();

            Author newAuthor = new Author
            {
                Name = "Transaction Single Test",
                CompanyId = 1
            };

            Author created = await _AuthorRepository.CreateAsync(newAuthor, transaction);
            Author single = await _AuthorRepository.ReadSingleAsync(a => a.Id == created.Id, transaction);

            Assert.NotNull(single);
            Assert.Equal(created.Id, single.Id);

            await transaction.RollbackAsync();

            Author shouldBeNull = await _AuthorRepository.ReadFirstAsync(a => a.Id == created.Id);
            Assert.Null(shouldBeNull);
        }

        // ==================== UPSERTASYNC TESTS ====================

        /// <summary>
        /// Tests UpsertAsync inserts when entity does not exist.
        /// </summary>
        [Fact]
        public async Task UpsertAsync_WhenNotExists_InsertsRecord()
        {
            if (_SkipTests) return;

            Author newAuthor = new Author
            {
                Name = "Upsert Insert Test",
                CompanyId = 1
            };

            Author upserted = await _AuthorRepository.UpsertAsync(newAuthor);

            Assert.True(upserted.Id > 0);
            Assert.Equal("Upsert Insert Test", upserted.Name);

            await _AuthorRepository.DeleteAsync(upserted);
        }

        /// <summary>
        /// Tests UpsertAsync updates when entity exists.
        /// </summary>
        [Fact]
        public async Task UpsertAsync_WhenExists_UpdatesRecord()
        {
            if (_SkipTests) return;

            Author author = new Author
            {
                Name = "Original Name",
                CompanyId = 1
            };

            Author created = await _AuthorRepository.CreateAsync(author);
            created.Name = "Updated Name";

            Author upserted = await _AuthorRepository.UpsertAsync(created);

            Assert.Equal(created.Id, upserted.Id);
            Assert.Equal("Updated Name", upserted.Name);

            await _AuthorRepository.DeleteAsync(upserted);
        }

        /// <summary>
        /// Tests UpsertAsync with transaction support.
        /// </summary>
        [Fact]
        public async Task UpsertAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();

            Author newAuthor = new Author
            {
                Name = "Upsert Transaction Test",
                CompanyId = 1
            };

            Author upserted = await _AuthorRepository.UpsertAsync(newAuthor, transaction);
            Assert.True(upserted.Id > 0);

            await transaction.RollbackAsync();

            Author shouldBeNull = await _AuthorRepository.ReadFirstAsync(a => a.Id == upserted.Id);
            Assert.Null(shouldBeNull);
        }

        // ==================== UPSERTMANYASYNC TESTS ====================

        /// <summary>
        /// Tests UpsertManyAsync with multiple new entities.
        /// </summary>
        [Fact]
        public async Task UpsertManyAsync_WithNewEntities_InsertsAll()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Bulk Upsert 1", CompanyId = 1 },
                new Author { Name = "Bulk Upsert 2", CompanyId = 2 },
                new Author { Name = "Bulk Upsert 3", CompanyId = 1 }
            };

            IEnumerable<Author> upserted = await _AuthorRepository.UpsertManyAsync(authors);
            List<Author> upsertedList = upserted.ToList();

            Assert.Equal(3, upsertedList.Count);
            Assert.All(upsertedList, a => Assert.True(a.Id > 0));

            foreach (Author author in upsertedList)
            {
                await _AuthorRepository.DeleteAsync(author);
            }
        }

        /// <summary>
        /// Tests UpsertManyAsync with mix of new and existing entities.
        /// </summary>
        [Fact]
        public async Task UpsertManyAsync_WithMixedEntities_UpsertsCorrectly()
        {
            if (_SkipTests) return;

            Author existing = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Existing Author",
                CompanyId = 1
            });

            List<Author> authors = new List<Author>
            {
                existing,
                new Author { Name = "New Author 1", CompanyId = 2 },
                new Author { Name = "New Author 2", CompanyId = 1 }
            };

            existing.Name = "Updated Existing";

            IEnumerable<Author> upserted = await _AuthorRepository.UpsertManyAsync(authors);
            List<Author> upsertedList = upserted.ToList();

            Assert.Equal(3, upsertedList.Count);

            Author updatedExisting = await _AuthorRepository.ReadByIdAsync(existing.Id);
            Assert.Equal("Updated Existing", updatedExisting.Name);

            foreach (Author author in upsertedList)
            {
                await _AuthorRepository.DeleteAsync(author);
            }
        }

        /// <summary>
        /// Tests UpsertManyAsync with transaction rollback.
        /// </summary>
        [Fact]
        public async Task UpsertManyAsync_WithTransactionRollback_DiscardsChanges()
        {
            if (_SkipTests) return;

            int initialCount = await _AuthorRepository.CountAsync();

            using (ITransaction transaction = await _AuthorRepository.BeginTransactionAsync())
            {
                List<Author> authors = new List<Author>
                {
                    new Author { Name = "Rollback Test 1", CompanyId = 1 },
                    new Author { Name = "Rollback Test 2", CompanyId = 2 }
                };

                await _AuthorRepository.UpsertManyAsync(authors, transaction);
                await transaction.RollbackAsync();
            }

            int finalCount = await _AuthorRepository.CountAsync();
            Assert.Equal(initialCount, finalCount);
        }

        // ==================== EXISTSASYNC TESTS ====================

        /// <summary>
        /// Tests ExistsAsync returns true when record exists.
        /// </summary>
        [Fact]
        public async Task ExistsAsync_WithMatchingRecord_ReturnsTrue()
        {
            if (_SkipTests) return;

            Author author = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Exists Test Author",
                CompanyId = 1
            });

            bool exists = await _AuthorRepository.ExistsAsync(a => a.Id == author.Id);
            Assert.True(exists);

            await _AuthorRepository.DeleteAsync(author);
        }

        /// <summary>
        /// Tests ExistsAsync returns false when no match.
        /// </summary>
        [Fact]
        public async Task ExistsAsync_WithNoMatch_ReturnsFalse()
        {
            if (_SkipTests) return;

            bool exists = await _AuthorRepository.ExistsAsync(a => a.Id == 999999);
            Assert.False(exists);
        }

        /// <summary>
        /// Tests ExistsAsync with complex predicate.
        /// </summary>
        [Fact]
        public async Task ExistsAsync_WithComplexPredicate_WorksCorrectly()
        {
            if (_SkipTests) return;

            Author author = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Complex Exists Test",
                CompanyId = 1
            });

            bool exists = await _AuthorRepository.ExistsAsync(a =>
                a.Name.Contains("Complex") && a.CompanyId == 1);
            Assert.True(exists);

            bool notExists = await _AuthorRepository.ExistsAsync(a =>
                a.Name.Contains("Complex") && a.CompanyId == 999);
            Assert.False(notExists);

            await _AuthorRepository.DeleteAsync(author);
        }

        /// <summary>
        /// Tests ExistsAsync with transaction support.
        /// </summary>
        [Fact]
        public async Task ExistsAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            using ITransaction transaction = await _AuthorRepository.BeginTransactionAsync();

            Author author = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Transaction Exists Test",
                CompanyId = 1
            }, transaction);

            bool existsInTransaction = await _AuthorRepository.ExistsAsync(
                a => a.Id == author.Id, transaction);
            Assert.True(existsInTransaction);

            await transaction.RollbackAsync();

            bool existsAfterRollback = await _AuthorRepository.ExistsAsync(
                a => a.Id == author.Id);
            Assert.False(existsAfterRollback);
        }

        // ==================== UPDATEMANYASYNC TESTS ====================

        /// <summary>
        /// Tests UpdateManyAsync updates multiple matching records.
        /// </summary>
        [Fact]
        public async Task UpdateManyAsync_WithMultipleMatches_UpdatesAll()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Update Many 1", CompanyId = 1 },
                new Author { Name = "Update Many 2", CompanyId = 1 },
                new Author { Name = "Update Many 3", CompanyId = 1 }
            };

            IEnumerable<Author> created = await _AuthorRepository.CreateManyAsync(authors);
            List<Author> createdList = created.ToList();

            int updatedCount = await _AuthorRepository.UpdateManyAsync(
                a => a.CompanyId == 1,
                async a => { a.CompanyId = 2; await Task.CompletedTask; }
            );

            Assert.Equal(3, updatedCount);

            foreach (Author author in createdList)
            {
                Author updated = await _AuthorRepository.ReadByIdAsync(author.Id);
                Assert.Equal(2, updated.CompanyId);
                await _AuthorRepository.DeleteAsync(updated);
            }
        }

        /// <summary>
        /// Tests UpdateManyAsync returns zero when no matches.
        /// </summary>
        [Fact]
        public async Task UpdateManyAsync_WithNoMatches_ReturnsZero()
        {
            if (_SkipTests) return;

            int updatedCount = await _AuthorRepository.UpdateManyAsync(
                a => a.Id == 999999,
                async a => { a.Name = "Updated"; await Task.CompletedTask; }
            );

            Assert.Equal(0, updatedCount);
        }

        /// <summary>
        /// Tests UpdateManyAsync with transaction support.
        /// </summary>
        [Fact]
        public async Task UpdateManyAsync_WithTransaction_WorksCorrectly()
        {
            if (_SkipTests) return;

            Author author = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Update Transaction Test",
                CompanyId = 1
            });

            using (ITransaction transaction = await _AuthorRepository.BeginTransactionAsync())
            {
                await _AuthorRepository.UpdateManyAsync(
                    a => a.Id == author.Id,
                    async a => { a.CompanyId = 999; await Task.CompletedTask; },
                    transaction
                );

                await transaction.CommitAsync();
            }

            Author updated = await _AuthorRepository.ReadByIdAsync(author.Id);
            Assert.Equal(999, updated.CompanyId);

            await _AuthorRepository.DeleteAsync(updated);
        }

        // ==================== DELETEMANYASYNC TESTS ====================

        /// <summary>
        /// Tests DeleteManyAsync deletes multiple matching records.
        /// </summary>
        [Fact]
        public async Task DeleteManyAsync_WithMultipleMatches_DeletesAll()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Delete Many 1", CompanyId = 100 },
                new Author { Name = "Delete Many 2", CompanyId = 100 },
                new Author { Name = "Delete Many 3", CompanyId = 100 }
            };

            await _AuthorRepository.CreateManyAsync(authors);

            int deletedCount = await _AuthorRepository.DeleteManyAsync(a => a.CompanyId == 100);

            Assert.Equal(3, deletedCount);

            bool anyRemaining = await _AuthorRepository.ExistsAsync(a => a.CompanyId == 100);
            Assert.False(anyRemaining);
        }

        /// <summary>
        /// Tests DeleteManyAsync returns zero when no matches.
        /// </summary>
        [Fact]
        public async Task DeleteManyAsync_WithNoMatches_ReturnsZero()
        {
            if (_SkipTests) return;

            int deletedCount = await _AuthorRepository.DeleteManyAsync(a => a.Id == 999999);

            Assert.Equal(0, deletedCount);
        }

        /// <summary>
        /// Tests DeleteManyAsync with transaction rollback.
        /// </summary>
        [Fact]
        public async Task DeleteManyAsync_WithTransactionRollback_KeepsRecords()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Rollback Delete 1", CompanyId = 200 },
                new Author { Name = "Rollback Delete 2", CompanyId = 200 }
            };

            IEnumerable<Author> created = await _AuthorRepository.CreateManyAsync(authors);
            List<Author> createdList = created.ToList();

            using (ITransaction transaction = await _AuthorRepository.BeginTransactionAsync())
            {
                int deletedCount = await _AuthorRepository.DeleteManyAsync(
                    a => a.CompanyId == 200,
                    transaction
                );
                Assert.Equal(2, deletedCount);

                await transaction.RollbackAsync();
            }

            bool stillExists = await _AuthorRepository.ExistsAsync(a => a.CompanyId == 200);
            Assert.True(stillExists);

            foreach (Author author in createdList)
            {
                await _AuthorRepository.DeleteAsync(author);
            }
        }

        /// <summary>
        /// Tests DeleteManyAsync with transaction commit.
        /// </summary>
        [Fact]
        public async Task DeleteManyAsync_WithTransactionCommit_DeletesRecords()
        {
            if (_SkipTests) return;

            List<Author> authors = new List<Author>
            {
                new Author { Name = "Commit Delete 1", CompanyId = 300 },
                new Author { Name = "Commit Delete 2", CompanyId = 300 }
            };

            await _AuthorRepository.CreateManyAsync(authors);

            using (ITransaction transaction = await _AuthorRepository.BeginTransactionAsync())
            {
                int deletedCount = await _AuthorRepository.DeleteManyAsync(
                    a => a.CompanyId == 300,
                    transaction
                );
                Assert.Equal(2, deletedCount);

                await transaction.CommitAsync();
            }

            bool anyRemaining = await _AuthorRepository.ExistsAsync(a => a.CompanyId == 300);
            Assert.False(anyRemaining);
        }

        // ==================== CANCELLATION TESTS ====================

        /// <summary>
        /// Tests ReadSingleAsync supports cancellation.
        /// </summary>
        [Fact]
        public async Task ReadSingleAsync_SupportsCancellation()
        {
            if (_SkipTests) return;

            Author author = await _AuthorRepository.CreateAsync(new Author
            {
                Name = "Cancellation Test",
                CompanyId = 1
            });

            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await _AuthorRepository.ReadSingleAsync(a => a.Id == author.Id, null, cts.Token);
            });

            await _AuthorRepository.DeleteAsync(author);
        }

        /// <summary>
        /// Tests ExistsAsync supports cancellation.
        /// </summary>
        [Fact]
        public async Task ExistsAsync_SupportsCancellation()
        {
            if (_SkipTests) return;

            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await _AuthorRepository.ExistsAsync(a => a.Id == 1, null, cts.Token);
            });
        }

        #endregion

        #region Public-Methods

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
                using SqlConnection connection = new SqlConnection("Server=view.homedns.org,1433;Database=master;User=sa;Password=P@ssw0rd4Sql;TrustServerCertificate=true;Encrypt=false;");
                connection.Open();

                using SqlCommand checkDbCommand = new SqlCommand($"SELECT database_id FROM sys.databases WHERE name = '{TestDatabaseName}'", connection);
                object result = checkDbCommand.ExecuteScalar();

                if (result == null)
                {
                    using SqlCommand createDbCommand = new SqlCommand($"CREATE DATABASE {TestDatabaseName}", connection);
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
                using SqlConnection connection = new SqlConnection(TestConnectionString);
                connection.Open();

                using SqlCommand command = connection.CreateCommand();
                command.CommandText = @"
                    IF OBJECT_ID('books', 'U') IS NOT NULL DROP TABLE books;
                    IF OBJECT_ID('authors', 'U') IS NOT NULL DROP TABLE authors;
                    IF OBJECT_ID('companies', 'U') IS NOT NULL DROP TABLE companies;

                    CREATE TABLE companies (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(100) NOT NULL,
                        industry NVARCHAR(50)
                    );

                    CREATE TABLE authors (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(100) NOT NULL,
                        company_id INT NULL FOREIGN KEY REFERENCES companies(id)
                    );

                    CREATE TABLE books (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        title NVARCHAR(200) NOT NULL,
                        author_id INT NOT NULL FOREIGN KEY REFERENCES authors(id),
                        publisher_id INT NULL FOREIGN KEY REFERENCES companies(id)
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
