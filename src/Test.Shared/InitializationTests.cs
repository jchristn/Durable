namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Durable;
    using Durable.Sql;
    using Durable.DefaultValueProviders;
    using Durable.Sqlite;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Tests for table initialization system including database creation, table creation,
    /// validation, and default value providers
    /// </summary>
    public class InitializationTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _testDatabasePath;
        private readonly List<string> _createdDatabases;

        /// <summary>
        /// Initializes a new instance of the InitializationTests class.
        /// </summary>
        /// <param name="output">Test output helper for logging test results.</param>
        public InitializationTests(ITestOutputHelper output)
        {
            _output = output;
            _testDatabasePath = Path.Combine(Path.GetTempPath(), $"init_test_{Guid.NewGuid()}.db");
            _createdDatabases = new List<string>();
        }

        /// <summary>
        /// Disposes resources and cleans up test database files.
        /// </summary>
        public void Dispose()
        {
            // Clean up test databases
            foreach (string dbPath in _createdDatabases)
            {
                try
                {
                    using (Microsoft.Data.Sqlite.SqliteConnection connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=" + dbPath))
                    {
                        Microsoft.Data.Sqlite.SqliteConnection.ClearPool(connection);
                    }

                    if (File.Exists(dbPath))
                    {
                        File.Delete(dbPath);
                        _output.WriteLine($"Cleaned up test database: {dbPath}");
                    }
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"Warning: Failed to delete test database {dbPath}: {ex.Message}");
                }
            }
        }

        #region Database Creation Tests

        /// <summary>
        /// Tests that CreateDatabaseIfNotExists creates a new database file.
        /// </summary>
        [Fact]
        public void CreateDatabaseIfNotExists_CreatesNewDatabase()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"new_db_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);

            // Act - Database creation should be implicit when using file-based database
            // For SQLite, the database file is created when the connection is opened
            repository.CreateDatabaseIfNotExists();

            // Assert
            Assert.True(File.Exists(dbPath), "Database file should be created");
            _output.WriteLine($"âœ“ Database created successfully at: {dbPath}");
        }

        #endregion

        #region Table Initialization Tests

        /// <summary>
        /// Tests that InitializeTable creates a new table in the database.
        /// </summary>
        [Fact]
        public void InitializeTable_CreatesNewTable()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"init_table_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);

            // Act
            repository.InitializeTable(typeof(InitProduct));

            // Assert - Try to query the table to verify it exists
            IEnumerable<InitProduct> products = repository.Query().Execute();
            Assert.NotNull(products);
            Assert.Empty(products);
            _output.WriteLine("âœ“ Table 'test_products' created successfully");
        }

        /// <summary>
        /// Tests that InitializeTable is idempotent and does not fail when called multiple times.
        /// </summary>
        [Fact]
        public void InitializeTable_DoesNotFailIfTableExists()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"init_exists_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);

            // Act - Initialize twice
            repository.InitializeTable(typeof(InitProduct));
            repository.InitializeTable(typeof(InitProduct)); // Should not throw

            // Assert
            IEnumerable<InitProduct> products = repository.Query().Execute();
            Assert.NotNull(products);
            _output.WriteLine("âœ“ InitializeTable is idempotent");
        }

        /// <summary>
        /// Tests that InitializeTables can create multiple tables in a single call.
        /// </summary>
        [Fact]
        public void InitializeTables_CreatesMultipleTables()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"init_multi_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> productRepo = new SqliteRepository<InitProduct>(settings);

            Type[] entityTypes = new[] { typeof(InitProduct), typeof(InitCategory) };

            // Act
            productRepo.InitializeTables(entityTypes);

            // Assert - Verify both tables exist
            IEnumerable<InitProduct> products = productRepo.Query().Execute();
            Assert.NotNull(products);

            SqliteRepository<InitCategory> categoryRepo = new SqliteRepository<InitCategory>(settings);
            IEnumerable<InitCategory> categories = categoryRepo.Query().Execute();
            Assert.NotNull(categories);

            _output.WriteLine("âœ“ Multiple tables created successfully");
        }

        /// <summary>
        /// Tests that InitializeTable properly creates tables with foreign key relationships.
        /// </summary>
        [Fact]
        public void InitializeTable_WithForeignKey_CreatesRelatedTables()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"init_fk_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> productRepo = new SqliteRepository<InitProduct>(settings);
            SqliteRepository<InitOrder> orderRepo = new SqliteRepository<InitOrder>(settings);

            // Act - Create parent table first, then child table with foreign key
            productRepo.InitializeTable(typeof(InitProduct));
            orderRepo.InitializeTable(typeof(InitOrder));

            // Assert - Create a product and an order referencing it
            InitProduct product = new InitProduct { Name = "Test InitProduct", Price = 99.99m };
            product = productRepo.Create(product);

            InitOrder order = new InitOrder { ProductId = product.Id, Quantity = 5 };
            order = orderRepo.Create(order);

            Assert.True(order.Id > 0);
            _output.WriteLine("âœ“ Foreign key relationship established successfully");
        }

        #endregion

        #region Validation Tests

        /// <summary>
        /// Tests that ValidateTable returns true for a properly configured entity.
        /// </summary>
        [Fact]
        public void ValidateTable_WithValidEntity_ReturnsTrue()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"validate_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);

            // Act
            TableValidationResult result = repository.ValidateTable(typeof(InitProduct));

            // Assert
            Assert.True(result.IsValid, "InitProduct entity should be valid");
            Assert.Empty(result.Errors);
            Assert.False(result.TableExists);
            Assert.Equal(typeof(InitProduct), result.EntityType);
            _output.WriteLine("âœ“ ValidateTable returned true for valid entity");
        }

        /// <summary>
        /// Tests that ValidateTable returns false for an entity missing required attributes.
        /// </summary>
        [Fact]
        public void ValidateTable_WithInvalidEntity_ReturnsFalse()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"validate_inv_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            // Use a valid repository to call ValidateTable on an invalid entity type
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);

            // Act
            TableValidationResult result = repository.ValidateTable(typeof(InitInvalidEntity));

            // Assert
            Assert.False(result.IsValid, "InitInvalidEntity should not be valid");
            Assert.NotEmpty(result.Errors);
            Assert.Contains(result.Errors, e => e.Contains("Entity"));
            _output.WriteLine($"âœ“ ValidateTable returned false with errors: {string.Join(", ", result.Errors)}");
        }

        /// <summary>
        /// Tests that ValidateTable returns false for an entity without a primary key.
        /// </summary>
        [Fact]
        public void ValidateTable_WithNoPrimaryKey_ReturnsFalse()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"validate_no_pk_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            // Use a valid repository to call ValidateTable on an invalid entity type
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);

            // Act
            TableValidationResult result = repository.ValidateTable(typeof(InitInvalidNoPrimaryKey));

            // Assert
            Assert.False(result.IsValid, "Entity without primary key should not be valid");
            Assert.NotEmpty(result.Errors);
            Assert.Contains(result.Errors, e => e.Contains("primary key"));
            _output.WriteLine($"âœ“ ValidateTable correctly identified missing primary key");
        }

        /// <summary>
        /// Tests that ValidateTables validates multiple entities and detects invalid ones.
        /// </summary>
        [Fact]
        public void ValidateTables_ValidatesMultipleEntities()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"validate_multi_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);

            Type[] entityTypes = new[] { typeof(InitProduct), typeof(InitCategory), typeof(InitInvalidEntity) };

            // Act
            SchemaValidationResult result = repository.ValidateTables(entityTypes);

            // Assert
            Assert.False(result.IsValid, "Should fail because InitInvalidEntity is included");
            Assert.NotEmpty(result.Errors);
            Assert.Equal(3, result.Tables.Count);
            Assert.True(result.Tables[0].IsValid);
            Assert.False(result.Tables[2].IsValid);
            Assert.All(result.Errors, e => Assert.StartsWith(nameof(InitInvalidEntity) + ": ", e));
            _output.WriteLine($"âœ“ ValidateTables found errors in mixed entity types: {result.Errors.Count} errors");
        }

        #endregion

        #region Default Value Provider Tests

        /// <summary>
        /// Tests that the CurrentDateTimeUtc default value provider sets the current UTC timestamp.
        /// </summary>
        [Fact]
        public void DefaultValueProvider_CurrentDateTimeUtc_SetsValue()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"default_datetime_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);
            repository.InitializeTable(typeof(InitProduct));

            DateTime beforeCreate = DateTime.UtcNow.AddSeconds(-1);

            // Act
            InitProduct product = new InitProduct { Name = "Test InitProduct", Price = 99.99m };
            // Don't set CreatedUtc - it should be set automatically
            product = repository.Create(product);

            DateTime afterCreate = DateTime.UtcNow.AddSeconds(1);

            // Assert
            Assert.True(product.CreatedUtc >= beforeCreate && product.CreatedUtc <= afterCreate,
                $"CreatedUtc should be set to current UTC time. Got: {product.CreatedUtc}");
            _output.WriteLine($"âœ“ CurrentDateTimeUtc provider set value: {product.CreatedUtc}");
        }

        /// <summary>
        /// Tests that the NewGuid default value provider generates a new GUID.
        /// </summary>
        [Fact]
        public void DefaultValueProvider_NewGuid_SetsValue()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"default_guid_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);
            repository.InitializeTable(typeof(InitProduct));

            // Act
            InitProduct product = new InitProduct { Name = "Test InitProduct", Price = 99.99m };
            // Don't set ProductGuid - it should be set automatically
            product = repository.Create(product);

            // Assert
            Assert.NotEqual(Guid.Empty, product.ProductGuid);
            _output.WriteLine($"âœ“ NewGuid provider set value: {product.ProductGuid}");
        }

        /// <summary>
        /// Tests that the SequentialGuid default value provider generates sequential GUIDs.
        /// </summary>
        [Fact]
        public void DefaultValueProvider_SequentialGuid_SetsValue()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"default_seqguid_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitCategory> repository = new SqliteRepository<InitCategory>(settings);
            repository.InitializeTable(typeof(InitCategory));

            // Act - Create multiple categories to test sequential nature
            InitCategory category1 = new InitCategory { Name = "InitCategory 1" };
            category1 = repository.Create(category1);

            InitCategory category2 = new InitCategory { Name = "InitCategory 2" };
            category2 = repository.Create(category2);

            // Assert
            Assert.NotEqual(Guid.Empty, category1.CategoryGuid);
            Assert.NotEqual(Guid.Empty, category2.CategoryGuid);
            Assert.NotEqual(category1.CategoryGuid, category2.CategoryGuid);
            _output.WriteLine($"âœ“ SequentialGuid provider set values: {category1.CategoryGuid}, {category2.CategoryGuid}");
        }

        /// <summary>
        /// Tests that default value providers only apply when property values are at their default.
        /// </summary>
        [Fact]
        public void DefaultValueProvider_OnlyAppliesWhenValueIsDefault()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"default_conditional_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<InitProduct> repository = new SqliteRepository<InitProduct>(settings);
            repository.InitializeTable(typeof(InitProduct));

            Guid customGuid = Guid.NewGuid();
            DateTime customDate = new DateTime(2023, 1, 1, 12, 0, 0, DateTimeKind.Utc);

            // Act - Set values explicitly
            InitProduct product = new InitProduct
            {
                Name = "Test InitProduct",
                Price = 99.99m,
                ProductGuid = customGuid,
                CreatedUtc = customDate
            };
            product = repository.Create(product);

            // Assert - Custom values should be preserved
            Assert.Equal(customGuid, product.ProductGuid);
            Assert.Equal(customDate, product.CreatedUtc);
            _output.WriteLine("âœ“ Default value providers did not override explicitly set values");
        }

        #endregion

        #region Integration Tests

        /// <summary>
        /// Tests a full workflow of database initialization, table creation, validation, and data operations.
        /// </summary>
        [Fact]
        public void FullWorkflow_InitializeAndUseDatabase()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"full_workflow_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);

            // Act - Full workflow
            // Step 1: Create database (implicit for SQLite)
            SqliteRepository<InitProduct> productRepo = new SqliteRepository<InitProduct>(settings);
            productRepo.CreateDatabaseIfNotExists();

            // Step 2: Initialize tables
            productRepo.InitializeTables(new[] { typeof(InitProduct), typeof(InitCategory), typeof(InitOrder) });

            // Step 3: Validate tables
            SchemaValidationResult validation = productRepo.ValidateTables(new[] { typeof(InitProduct), typeof(InitCategory), typeof(InitOrder) });
            Assert.True(validation.IsValid, $"Tables should be valid. Errors: {string.Join(", ", validation.Errors)}");
            Assert.All(validation.Tables, t => Assert.True(t.TableExists));

            // Step 4: Create data with default values
            InitProduct product = productRepo.Create(new InitProduct { Name = "Widget", Price = 19.99m });
            Assert.NotEqual(Guid.Empty, product.ProductGuid);
            Assert.NotEqual(default(DateTime), product.CreatedUtc);

            SqliteRepository<InitCategory> categoryRepo = new SqliteRepository<InitCategory>(settings);
            InitCategory category = categoryRepo.Create(new InitCategory { Name = "Electronics" });
            Assert.NotEqual(Guid.Empty, category.CategoryGuid);

            // Step 5: Create order with foreign key
            SqliteRepository<InitOrder> orderRepo = new SqliteRepository<InitOrder>(settings);
            InitOrder order = orderRepo.Create(new InitOrder { ProductId = product.Id, Quantity = 3 });
            Assert.True(order.Id > 0);
            Assert.NotEqual(default(DateTime), order.OrderDate);

            // Step 6: Query data
            List<InitProduct> allProducts = productRepo.Query().Execute().ToList();
            Assert.Single(allProducts);
            Assert.Equal("Widget", allProducts[0].Name);

            List<InitOrder> allOrders = orderRepo.Query().Where(o => o.ProductId == product.Id).Execute().ToList();
            Assert.Single(allOrders);
            Assert.Equal(3, allOrders[0].Quantity);

            _output.WriteLine("âœ“ Full initialization workflow completed successfully");
            _output.WriteLine($"  - Created {allProducts.Count} products");
            _output.WriteLine($"  - Created {allOrders.Count} orders");
            _output.WriteLine($"  - All default values applied correctly");
        }

        #endregion
    }
}

