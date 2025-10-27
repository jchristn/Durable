namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Durable;
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

        public InitializationTests(ITestOutputHelper output)
        {
            _output = output;
            _testDatabasePath = Path.Combine(Path.GetTempPath(), $"init_test_{Guid.NewGuid()}.db");
            _createdDatabases = new List<string>();
        }

        public void Dispose()
        {
            // Clean up test databases
            foreach (string dbPath in _createdDatabases)
            {
                try
                {
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

        #region Test Entities

        [Entity("test_products")]
        public class Product
        {
            [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
            public int Id { get; set; }

            [Property("name")]
            public string Name { get; set; } = string.Empty;

            [Property("created_utc")]
            [DefaultValue(DefaultValueType.CurrentDateTimeUtc)]
            public DateTime CreatedUtc { get; set; }

            [Property("guid")]
            [DefaultValue(DefaultValueType.NewGuid)]
            public Guid ProductGuid { get; set; }

            [Property("price")]
            public decimal Price { get; set; }
        }

        [Entity("test_categories")]
        public class Category
        {
            [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
            public int Id { get; set; }

            [Property("name")]
            public string Name { get; set; } = string.Empty;

            [Property("guid")]
            [DefaultValue(DefaultValueType.SequentialGuid)]
            public Guid CategoryGuid { get; set; }
        }

        [Entity("test_orders")]
        public class Order
        {
            [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
            public int Id { get; set; }

            [Property("product_id")]
            [ForeignKey(typeof(Product), nameof(Product.Id))]
            public int ProductId { get; set; }

            [Property("quantity")]
            public int Quantity { get; set; }

            [Property("order_date")]
            [DefaultValue(DefaultValueType.CurrentDateTimeUtc)]
            public DateTime OrderDate { get; set; }
        }

        // Entity without proper attributes (for validation testing)
        public class InvalidEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }

        // Entity with attributes but no primary key (for validation testing)
        [Entity("invalid_no_pk")]
        public class InvalidNoPrimaryKey
        {
            [Property("id")]
            public int Id { get; set; }

            [Property("name")]
            public string Name { get; set; } = string.Empty;
        }

        #endregion

        #region Database Creation Tests

        [Fact]
        public void CreateDatabaseIfNotExists_CreatesNewDatabase()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"new_db_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);

            // Act - Database creation should be implicit when using file-based database
            // For SQLite, the database file is created when the connection is opened
            repository.CreateDatabaseIfNotExists();

            // Assert
            Assert.True(File.Exists(dbPath), "Database file should be created");
            _output.WriteLine($"✓ Database created successfully at: {dbPath}");
        }

        #endregion

        #region Table Initialization Tests

        [Fact]
        public void InitializeTable_CreatesNewTable()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"init_table_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);

            // Act
            repository.InitializeTable(typeof(Product));

            // Assert - Try to query the table to verify it exists
            IEnumerable<Product> products = repository.Query().Execute();
            Assert.NotNull(products);
            Assert.Empty(products);
            _output.WriteLine("✓ Table 'test_products' created successfully");
        }

        [Fact]
        public void InitializeTable_DoesNotFailIfTableExists()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"init_exists_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);

            // Act - Initialize twice
            repository.InitializeTable(typeof(Product));
            repository.InitializeTable(typeof(Product)); // Should not throw

            // Assert
            IEnumerable<Product> products = repository.Query().Execute();
            Assert.NotNull(products);
            _output.WriteLine("✓ InitializeTable is idempotent");
        }

        [Fact]
        public void InitializeTables_CreatesMultipleTables()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"init_multi_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> productRepo = new SqliteRepository<Product>(settings);

            Type[] entityTypes = new[] { typeof(Product), typeof(Category) };

            // Act
            productRepo.InitializeTables(entityTypes);

            // Assert - Verify both tables exist
            IEnumerable<Product> products = productRepo.Query().Execute();
            Assert.NotNull(products);

            SqliteRepository<Category> categoryRepo = new SqliteRepository<Category>(settings);
            IEnumerable<Category> categories = categoryRepo.Query().Execute();
            Assert.NotNull(categories);

            _output.WriteLine("✓ Multiple tables created successfully");
        }

        [Fact]
        public void InitializeTable_WithForeignKey_CreatesRelatedTables()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"init_fk_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> productRepo = new SqliteRepository<Product>(settings);
            SqliteRepository<Order> orderRepo = new SqliteRepository<Order>(settings);

            // Act - Create parent table first, then child table with foreign key
            productRepo.InitializeTable(typeof(Product));
            orderRepo.InitializeTable(typeof(Order));

            // Assert - Create a product and an order referencing it
            Product product = new Product { Name = "Test Product", Price = 99.99m };
            product = productRepo.Create(product);

            Order order = new Order { ProductId = product.Id, Quantity = 5 };
            order = orderRepo.Create(order);

            Assert.True(order.Id > 0);
            _output.WriteLine("✓ Foreign key relationship established successfully");
        }

        #endregion

        #region Validation Tests

        [Fact]
        public void ValidateTable_WithValidEntity_ReturnsTrue()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"validate_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);

            // Act
            bool isValid = repository.ValidateTable(typeof(Product), out List<string> errors, out List<string> warnings);

            // Assert
            Assert.True(isValid, "Product entity should be valid");
            Assert.Empty(errors);
            _output.WriteLine("✓ ValidateTable returned true for valid entity");
        }

        [Fact]
        public void ValidateTable_WithInvalidEntity_ReturnsFalse()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"validate_inv_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            // Use a valid repository to call ValidateTable on an invalid entity type
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);

            // Act
            bool isValid = repository.ValidateTable(typeof(InvalidEntity), out List<string> errors, out List<string> warnings);

            // Assert
            Assert.False(isValid, "InvalidEntity should not be valid");
            Assert.NotEmpty(errors);
            Assert.Contains(errors, e => e.Contains("Entity"));
            _output.WriteLine($"✓ ValidateTable returned false with errors: {string.Join(", ", errors)}");
        }

        [Fact]
        public void ValidateTable_WithNoPrimaryKey_ReturnsFalse()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"validate_no_pk_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            // Use a valid repository to call ValidateTable on an invalid entity type
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);

            // Act
            bool isValid = repository.ValidateTable(typeof(InvalidNoPrimaryKey), out List<string> errors, out List<string> warnings);

            // Assert
            Assert.False(isValid, "Entity without primary key should not be valid");
            Assert.NotEmpty(errors);
            Assert.Contains(errors, e => e.Contains("primary key"));
            _output.WriteLine($"✓ ValidateTable correctly identified missing primary key");
        }

        [Fact]
        public void ValidateTables_ValidatesMultipleEntities()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"validate_multi_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);

            Type[] entityTypes = new[] { typeof(Product), typeof(Category), typeof(InvalidEntity) };

            // Act
            bool isValid = repository.ValidateTables(entityTypes, out List<string> errors, out List<string> warnings);

            // Assert
            Assert.False(isValid, "Should fail because InvalidEntity is included");
            Assert.NotEmpty(errors);
            _output.WriteLine($"✓ ValidateTables found errors in mixed entity types: {errors.Count} errors");
        }

        #endregion

        #region Default Value Provider Tests

        [Fact]
        public void DefaultValueProvider_CurrentDateTimeUtc_SetsValue()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"default_datetime_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);
            repository.InitializeTable(typeof(Product));

            DateTime beforeCreate = DateTime.UtcNow.AddSeconds(-1);

            // Act
            Product product = new Product { Name = "Test Product", Price = 99.99m };
            // Don't set CreatedUtc - it should be set automatically
            product = repository.Create(product);

            DateTime afterCreate = DateTime.UtcNow.AddSeconds(1);

            // Assert
            Assert.True(product.CreatedUtc >= beforeCreate && product.CreatedUtc <= afterCreate,
                $"CreatedUtc should be set to current UTC time. Got: {product.CreatedUtc}");
            _output.WriteLine($"✓ CurrentDateTimeUtc provider set value: {product.CreatedUtc}");
        }

        [Fact]
        public void DefaultValueProvider_NewGuid_SetsValue()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"default_guid_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);
            repository.InitializeTable(typeof(Product));

            // Act
            Product product = new Product { Name = "Test Product", Price = 99.99m };
            // Don't set ProductGuid - it should be set automatically
            product = repository.Create(product);

            // Assert
            Assert.NotEqual(Guid.Empty, product.ProductGuid);
            _output.WriteLine($"✓ NewGuid provider set value: {product.ProductGuid}");
        }

        [Fact]
        public void DefaultValueProvider_SequentialGuid_SetsValue()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"default_seqguid_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Category> repository = new SqliteRepository<Category>(settings);
            repository.InitializeTable(typeof(Category));

            // Act - Create multiple categories to test sequential nature
            Category category1 = new Category { Name = "Category 1" };
            category1 = repository.Create(category1);

            Category category2 = new Category { Name = "Category 2" };
            category2 = repository.Create(category2);

            // Assert
            Assert.NotEqual(Guid.Empty, category1.CategoryGuid);
            Assert.NotEqual(Guid.Empty, category2.CategoryGuid);
            Assert.NotEqual(category1.CategoryGuid, category2.CategoryGuid);
            _output.WriteLine($"✓ SequentialGuid provider set values: {category1.CategoryGuid}, {category2.CategoryGuid}");
        }

        [Fact]
        public void DefaultValueProvider_OnlyAppliesWhenValueIsDefault()
        {
            // Arrange
            string dbPath = Path.Combine(Path.GetTempPath(), $"default_conditional_{Guid.NewGuid()}.db");
            _createdDatabases.Add(dbPath);
            string connectionString = $"Data Source={dbPath}";

            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            SqliteRepository<Product> repository = new SqliteRepository<Product>(settings);
            repository.InitializeTable(typeof(Product));

            Guid customGuid = Guid.NewGuid();
            DateTime customDate = new DateTime(2023, 1, 1, 12, 0, 0, DateTimeKind.Utc);

            // Act - Set values explicitly
            Product product = new Product
            {
                Name = "Test Product",
                Price = 99.99m,
                ProductGuid = customGuid,
                CreatedUtc = customDate
            };
            product = repository.Create(product);

            // Assert - Custom values should be preserved
            Assert.Equal(customGuid, product.ProductGuid);
            Assert.Equal(customDate, product.CreatedUtc);
            _output.WriteLine("✓ Default value providers did not override explicitly set values");
        }

        #endregion

        #region Integration Tests

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
            SqliteRepository<Product> productRepo = new SqliteRepository<Product>(settings);
            productRepo.CreateDatabaseIfNotExists();

            // Step 2: Initialize tables
            productRepo.InitializeTables(new[] { typeof(Product), typeof(Category), typeof(Order) });

            // Step 3: Validate tables
            bool isValid = productRepo.ValidateTables(
                new[] { typeof(Product), typeof(Category), typeof(Order) },
                out List<string> errors,
                out List<string> warnings);
            Assert.True(isValid, $"Tables should be valid. Errors: {string.Join(", ", errors)}");

            // Step 4: Create data with default values
            Product product = productRepo.Create(new Product { Name = "Widget", Price = 19.99m });
            Assert.NotEqual(Guid.Empty, product.ProductGuid);
            Assert.NotEqual(default(DateTime), product.CreatedUtc);

            SqliteRepository<Category> categoryRepo = new SqliteRepository<Category>(settings);
            Category category = categoryRepo.Create(new Category { Name = "Electronics" });
            Assert.NotEqual(Guid.Empty, category.CategoryGuid);

            // Step 5: Create order with foreign key
            SqliteRepository<Order> orderRepo = new SqliteRepository<Order>(settings);
            Order order = orderRepo.Create(new Order { ProductId = product.Id, Quantity = 3 });
            Assert.True(order.Id > 0);
            Assert.NotEqual(default(DateTime), order.OrderDate);

            // Step 6: Query data
            List<Product> allProducts = productRepo.Query().Execute().ToList();
            Assert.Single(allProducts);
            Assert.Equal("Widget", allProducts[0].Name);

            List<Order> allOrders = orderRepo.Query().Where(o => o.ProductId == product.Id).Execute().ToList();
            Assert.Single(allOrders);
            Assert.Equal(3, allOrders[0].Quantity);

            _output.WriteLine("✓ Full initialization workflow completed successfully");
            _output.WriteLine($"  - Created {allProducts.Count} products");
            _output.WriteLine($"  - Created {allOrders.Count} orders");
            _output.WriteLine($"  - All default values applied correctly");
        }

        #endregion
    }
}
