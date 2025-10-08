namespace Test.Postgres
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Durable.ConcurrencyConflictResolvers;
    using Test.Shared;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Comprehensive integration tests for PostgreSQL concurrency control and conflict resolution including:
    /// - Optimistic concurrency: Version column support
    /// - Conflict detection: OptimisticConcurrencyException handling
    /// - Conflict resolution: Client Wins, Database Wins, Merge Changes strategies
    /// - Version management: Automatic version incrementing
    /// </summary>
    public class PostgresConcurrencyIntegrationTests : IDisposable
    {

        #region Private-Members

        private readonly ITestOutputHelper _Output;
        private readonly string _ConnectionString;
        private readonly bool _SkipTests;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the concurrency integration test class.
        /// </summary>
        public PostgresConcurrencyIntegrationTests(ITestOutputHelper output)
        {
            _Output = output;
            _ConnectionString = "Host=localhost;Database=durable_concurrency_test;Username=test_user;Password=test_password;";

            try
            {
                using Npgsql.NpgsqlConnection connection = new Npgsql.NpgsqlConnection(_ConnectionString);
                connection.Open();
                using Npgsql.NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteScalar();
                _Output.WriteLine("PostgreSQL concurrency integration tests initialized successfully");
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
        /// Tests basic concurrency functionality with version column for create and update operations.
        /// </summary>
        [Fact]
        public void BasicConcurrencyTest_CreateAndUpdate_WithVersionColumn()
        {
            if (_SkipTests) return;

            try
            {
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Test Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Creating author...");
                    AuthorWithVersion created = repo.Create(author);
                    _Output.WriteLine($"Created: ID={created.Id}, Version={created.Version}, Name={created.Name}");

                    Assert.True(created.Id > 0);
                    Assert.Equal(1, created.Version);
                    Assert.Equal("Test Author", created.Name);

                    _Output.WriteLine("Updating author...");
                    created.Name = "Updated Author";
                    AuthorWithVersion updated = repo.Update(created);
                    _Output.WriteLine($"Updated: ID={updated.Id}, Version={updated.Version}, Name={updated.Name}");

                    Assert.Equal(2, updated.Version);
                    Assert.Equal("Updated Author", updated.Name);

                    _Output.WriteLine("✅ Basic concurrency test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Tests that concurrency conflicts throw appropriate exceptions.
        /// </summary>
        [Fact]
        public void ConcurrencyConflict_ThrowsException()
        {
            if (_SkipTests) return;

            try
            {
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Conflict Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Creating author for conflict test...");
                    AuthorWithVersion created = repo.Create(author);
                    _Output.WriteLine($"Created: ID={created.Id}, Version={created.Version}");

                    // Get two copies of the same entity
                    AuthorWithVersion copy1 = repo.ReadById(created.Id);
                    AuthorWithVersion copy2 = repo.ReadById(created.Id);

                    Assert.Equal(copy1.Version, copy2.Version);

                    // Update the first copy successfully
                    copy1.Name = "Update 1";
                    _Output.WriteLine("Performing first update...");
                    AuthorWithVersion updated1 = repo.Update(copy1);
                    _Output.WriteLine($"First update successful: Version={updated1.Version}");

                    // Attempt to update the second copy - this should throw
                    copy2.Name = "Update 2";
                    _Output.WriteLine("Attempting second update (expecting OptimisticConcurrencyException)...");

                    OptimisticConcurrencyException thrownException = Assert.Throws<OptimisticConcurrencyException>(() => repo.Update(copy2));

                    _Output.WriteLine($"✅ Expected exception thrown: {thrownException.Message}");
                    Assert.Contains("concurrency conflict", thrownException.Message.ToLower());
                }
            }
            catch (Exception ex)
            {
                if (!(ex is OptimisticConcurrencyException))
                {
                    _Output.WriteLine($"Test failed with unexpected exception: {ex.Message}");
                    _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                    throw;
                }
                throw;
            }
        }

        /// <summary>
        /// Tests Client Wins conflict resolution strategy.
        /// </summary>
        [Fact]
        public void ClientWinsResolver_ResolvesConflict()
        {
            if (_SkipTests) return;

            try
            {
                IConcurrencyConflictResolver<AuthorWithVersion> resolver = new ClientWinsResolver<AuthorWithVersion>();
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString, null, null, resolver))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Conflict Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Setting up Client Wins conflict resolution test...");
                    AuthorWithVersion created = repo.Create(author);

                    AuthorWithVersion copy1 = repo.ReadById(created.Id);
                    AuthorWithVersion copy2 = repo.ReadById(created.Id);

                    // Update first copy
                    copy1.Name = "Database Update";
                    AuthorWithVersion updated1 = repo.Update(copy1);
                    _Output.WriteLine($"Database update: Name='{updated1.Name}', Version={updated1.Version}");

                    // Update second copy - should use Client Wins resolver
                    copy2.Name = "Client Update";
                    _Output.WriteLine("Attempting client update with Client Wins resolver...");
                    AuthorWithVersion resolved = repo.Update(copy2);

                    _Output.WriteLine($"Client Wins result: Name='{resolved.Name}', Version={resolved.Version}");

                    // Client should win - the client's changes should be preserved
                    Assert.Equal("Client Update", resolved.Name);
                    Assert.True(resolved.Version > updated1.Version);

                    _Output.WriteLine("✅ Client Wins resolver test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Client Wins test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Tests Database Wins conflict resolution strategy.
        /// </summary>
        [Fact]
        public void DatabaseWinsResolver_ResolvesConflict()
        {
            if (_SkipTests) return;

            try
            {
                IConcurrencyConflictResolver<AuthorWithVersion> resolver = new DatabaseWinsResolver<AuthorWithVersion>();
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString, null, null, resolver))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Conflict Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Setting up Database Wins conflict resolution test...");
                    AuthorWithVersion created = repo.Create(author);

                    AuthorWithVersion copy1 = repo.ReadById(created.Id);
                    AuthorWithVersion copy2 = repo.ReadById(created.Id);

                    // Update first copy
                    copy1.Name = "Database Update";
                    AuthorWithVersion updated1 = repo.Update(copy1);
                    _Output.WriteLine($"Database update: Name='{updated1.Name}', Version={updated1.Version}");

                    // Update second copy - should use Database Wins resolver
                    copy2.Name = "Client Update";
                    _Output.WriteLine("Attempting client update with Database Wins resolver...");
                    AuthorWithVersion resolved = repo.Update(copy2);

                    _Output.WriteLine($"Database Wins result: Name='{resolved.Name}', Version={resolved.Version}");

                    // Database should win - the database's changes should be preserved
                    Assert.Equal("Database Update", resolved.Name);
                    Assert.Equal(updated1.Version + 1, resolved.Version);

                    _Output.WriteLine("✅ Database Wins resolver test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Database Wins test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Tests Merge Changes conflict resolution strategy.
        /// </summary>
        [Fact]
        public void MergeChangesResolver_ResolvesConflict()
        {
            if (_SkipTests) return;

            try
            {
                IConcurrencyConflictResolver<AuthorWithVersion> resolver = new MergeChangesResolver<AuthorWithVersion>("Id", "Version");
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString, null, null, resolver))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Original Name",
                        CompanyId = 50
                    };

                    _Output.WriteLine("Setting up Merge Changes conflict resolution test...");
                    AuthorWithVersion created = repo.Create(author);
                    _Output.WriteLine($"Created: Name='{created.Name}', CompanyId={created.CompanyId}, Version={created.Version}");

                    AuthorWithVersion copy1 = repo.ReadById(created.Id);
                    AuthorWithVersion copy2 = repo.ReadById(created.Id);

                    // Update different fields in each copy
                    copy1.Name = "Updated Name";
                    AuthorWithVersion updated1 = repo.Update(copy1);
                    _Output.WriteLine($"First update (Name): Name='{updated1.Name}', CompanyId={updated1.CompanyId}, Version={updated1.Version}");

                    copy2.CompanyId = 100;
                    _Output.WriteLine("Attempting second update (CompanyId) with Merge Changes resolver...");
                    AuthorWithVersion resolved = repo.Update(copy2);

                    _Output.WriteLine($"Merge result: Name='{resolved.Name}', CompanyId={resolved.CompanyId}, Version={resolved.Version}");

                    Assert.Equal(100, resolved.CompanyId);
                    Assert.Equal("Original Name", resolved.Name);
                    Assert.True(resolved.Version > updated1.Version);

                    _Output.WriteLine("✅ Merge Changes resolver test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Merge Changes test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Tests async concurrency operations.
        /// </summary>
        [Fact]
        public async Task AsyncConcurrency_WorksCorrectly()
        {
            if (_SkipTests) return;

            try
            {
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Async Test Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Testing async concurrency operations...");
                    AuthorWithVersion created = await repo.CreateAsync(author);
                    _Output.WriteLine($"Async created: ID={created.Id}, Version={created.Version}");

                    Assert.Equal(1, created.Version);

                    created.Name = "Async Updated Author";
                    AuthorWithVersion updated = await repo.UpdateAsync(created);
                    _Output.WriteLine($"Async updated: Version={updated.Version}, Name='{updated.Name}'");

                    Assert.Equal(2, updated.Version);
                    Assert.Equal("Async Updated Author", updated.Name);

                    _Output.WriteLine("✅ Async concurrency test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Async concurrency test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Tests concurrent updates from multiple threads.
        /// </summary>
        [Fact]
        public async Task ConcurrentUpdates_HandleConflictsCorrectly()
        {
            if (_SkipTests) return;

            try
            {
                IConcurrencyConflictResolver<AuthorWithVersion> resolver = new ClientWinsResolver<AuthorWithVersion>();
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString, null, null, resolver))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Concurrent Test Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Setting up concurrent updates test...");
                    AuthorWithVersion created = repo.Create(author);

                    // Create multiple tasks that will attempt concurrent updates
                    Task<AuthorWithVersion> task1 = Task.Run(async () =>
                    {
                        using PostgresRepository<AuthorWithVersion> repo1 = new PostgresRepository<AuthorWithVersion>(_ConnectionString, null, null, resolver);
                        AuthorWithVersion copy = repo1.ReadById(created.Id);
                        copy.Name = "Update from Task 1";
                        await Task.Delay(50);
                        return repo1.Update(copy);
                    });

                    Task<AuthorWithVersion> task2 = Task.Run(async () =>
                    {
                        using PostgresRepository<AuthorWithVersion> repo2 = new PostgresRepository<AuthorWithVersion>(_ConnectionString, null, null, resolver);
                        AuthorWithVersion copy = repo2.ReadById(created.Id);
                        copy.Name = "Update from Task 2";
                        await Task.Delay(50);
                        return repo2.Update(copy);
                    });

                    Task<AuthorWithVersion> task3 = Task.Run(async () =>
                    {
                        using PostgresRepository<AuthorWithVersion> repo3 = new PostgresRepository<AuthorWithVersion>(_ConnectionString, null, null, resolver);
                        AuthorWithVersion copy = repo3.ReadById(created.Id);
                        copy.Name = "Update from Task 3";
                        await Task.Delay(50);
                        return repo3.Update(copy);
                    });

                    AuthorWithVersion[] results = await Task.WhenAll(task1, task2, task3);

                    _Output.WriteLine("Concurrent updates completed:");
                    for (int i = 0; i < results.Length; i++)
                    {
                        _Output.WriteLine($"  Task {i + 1} result: Name='{results[i].Name}', Version={results[i].Version}");
                    }

                    Assert.All(results, result => Assert.True(result.Version > created.Version));

                    AuthorWithVersion final = repo.ReadById(created.Id);
                    _Output.WriteLine($"Final state: Name='{final.Name}', Version={final.Version}");

                    _Output.WriteLine("✅ Concurrent updates test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Concurrent updates test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Tests that entities without version columns behave normally without concurrency control.
        /// </summary>
        [Fact]
        public void NoVersionColumn_BehavesNormally()
        {
            if (_SkipTests) return;

            try
            {
                using (PostgresRepository<AuthorWithoutVersion> repo = new PostgresRepository<AuthorWithoutVersion>(_ConnectionString))
                {
                    repo.DeleteAll();

                    AuthorWithoutVersion author = new AuthorWithoutVersion
                    {
                        Name = "No Version Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Testing entity without version column...");
                    AuthorWithoutVersion created = repo.Create(author);
                    _Output.WriteLine($"Created: ID={created.Id}, Name='{created.Name}'");

                    AuthorWithoutVersion copy1 = repo.ReadById(created.Id);
                    AuthorWithoutVersion copy2 = repo.ReadById(created.Id);

                    copy1.Name = "Update 1";
                    AuthorWithoutVersion updated1 = repo.Update(copy1);
                    _Output.WriteLine($"First update: Name='{updated1.Name}'");

                    copy2.Name = "Update 2";
                    AuthorWithoutVersion updated2 = repo.Update(copy2);
                    _Output.WriteLine($"Second update: Name='{updated2.Name}'");

                    Assert.Equal("Update 2", updated2.Name);

                    _Output.WriteLine("✅ No version column test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"No version column test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Tests version increment behavior with multiple updates.
        /// </summary>
        [Fact]
        public void VersionIncrement_WorksCorrectly()
        {
            if (_SkipTests) return;

            try
            {
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Version Test Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Testing version increment behavior...");
                    AuthorWithVersion created = repo.Create(author);
                    _Output.WriteLine($"Initial version: {created.Version}");
                    Assert.Equal(1, created.Version);

                    for (int i = 2; i <= 10; i++)
                    {
                        created.Name = $"Update {i}";
                        created = repo.Update(created);
                        _Output.WriteLine($"Update {i}: Version={created.Version}, Name='{created.Name}'");
                        Assert.Equal(i, created.Version);
                    }

                    _Output.WriteLine("✅ Version increment test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Version increment test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Tests concurrency with delete operations.
        /// </summary>
        [Fact]
        public void ConcurrencyWithDelete_HandledCorrectly()
        {
            if (_SkipTests) return;

            try
            {
                using (PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString))
                {
                    CleanupTestData(repo);

                    AuthorWithVersion author = new AuthorWithVersion
                    {
                        Name = "Delete Test Author",
                        CompanyId = null
                    };

                    _Output.WriteLine("Testing concurrency with delete operations...");
                    AuthorWithVersion created = repo.Create(author);

                    AuthorWithVersion copy1 = repo.ReadById(created.Id);
                    AuthorWithVersion copy2 = repo.ReadById(created.Id);

                    bool deleted = repo.Delete(copy1);
                    Assert.True(deleted);
                    _Output.WriteLine("Entity deleted using first copy");

                    copy2.Name = "Updated after delete";

                    OptimisticConcurrencyException exception = Assert.Throws<OptimisticConcurrencyException>(() => repo.Update(copy2));
                    _Output.WriteLine($"Expected exception on update after delete: {exception.Message}");

                    OptimisticConcurrencyException deleteException = Assert.Throws<OptimisticConcurrencyException>(() => repo.Delete(copy2));
                    _Output.WriteLine($"Expected exception on delete after delete: {deleteException.Message}");

                    _Output.WriteLine("✅ Concurrency with delete test completed successfully");
                }
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Concurrency with delete test failed with exception: {ex.Message}");
                _Output.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Disposes resources used by the test class.
        /// </summary>
        public void Dispose()
        {
            if (!_SkipTests)
            {
                try
                {
                    using PostgresRepository<AuthorWithVersion> repo = new PostgresRepository<AuthorWithVersion>(_ConnectionString);
                    CleanupTestData(repo);
                }
                catch (Exception ex)
                {
                    _Output.WriteLine($"Warning: Could not clean up test data during disposal: {ex.Message}");
                }
            }
        }

        #endregion

        #region Private-Methods

        private void CleanupTestData(PostgresRepository<AuthorWithVersion> repo)
        {
            try
            {
                repo.DeleteAll();
                _Output.WriteLine("Test data cleaned up");
            }
            catch (Exception ex)
            {
                _Output.WriteLine($"Warning: Could not clean up test data: {ex.Message}");
            }
        }

        #endregion

    }
}
