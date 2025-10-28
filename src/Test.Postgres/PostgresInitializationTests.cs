namespace Test.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using Durable.DefaultValueProviders;
    using Durable.Postgres;
    using Npgsql;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Tests for PostgreSQL table initialization system including database creation, table creation,
    /// validation, and default value providers
    /// </summary>
    public class PostgresInitializationTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _connectionString;
        private readonly string _testDatabaseName;
        private readonly List<string> _createdDatabases;
        private readonly bool _skipTests;

        public PostgresInitializationTests(ITestOutputHelper output)
        {
            _output = output;
            _testDatabaseName = $"durable_init_test_{Guid.NewGuid():N}";
            _connectionString = $"Host=localhost;Database={_testDatabaseName};Username=postgres;Password=postgres;";
            _createdDatabases = new List<string>();

            // Check if PostgreSQL is available
            try
            {
                using NpgsqlConnection connection = new NpgsqlConnection("Host=localhost;Username=postgres;Password=postgres;");
                connection.Open();
                _output.WriteLine("✓ PostgreSQL server is available");
            }
            catch (Exception ex)
            {
                _skipTests = true;
                _output.WriteLine($"⚠️  PostgreSQL server not available - tests will be skipped: {ex.Message}");
            }
        }

        public void Dispose()
        {
            if (_skipTests) return;

            // Clean up test databases
            foreach (string dbName in _createdDatabases)
            {
                try
                {
                    using NpgsqlConnection connection = new NpgsqlConnection("Host=localhost;Username=postgres;Password=postgres;");
                    connection.Open();
                    using NpgsqlCommand command = connection.CreateCommand();
                    command.CommandText = $"DROP DATABASE IF EXISTS \"{dbName}\"";
                    command.ExecuteNonQuery();
                    _output.WriteLine($"Cleaned up test database: {dbName}");
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"Warning: Failed to delete test database {dbName}: {ex.Message}");
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

            [Property("created_utc")]
            [DefaultValue(DefaultValueType.CurrentDateTimeUtc)]
            public DateTime CreatedUtc { get; set; }
        }

        // Test entity without Entity attribute
        public class InvalidEntity
        {
            public int Id { get; set; }
        }

        // Test entity without primary key
        [Entity("no_pk_entity")]
        public class NoPrimaryKeyEntity
        {
            [Property("name")]
            public string Name { get; set; } = string.Empty;
        }

        #endregion

        #region Database Creation Tests

        [Fact]
        public void CreateDatabaseIfNotExists_CreatesNewDatabase()
        {
            if (_skipTests) return;

            string dbName = $"init_create_db_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();

            // Verify database was created
            using NpgsqlConnection connection = new NpgsqlConnection("Host=localhost;Username=postgres;Password=postgres;");
            connection.Open();
            using NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = $"SELECT datname FROM pg_database WHERE datname = '{dbName}'";
            object? result = command.ExecuteScalar();

            Assert.NotNull(result);
            Assert.Equal(dbName, result.ToString());
            _output.WriteLine($"✓ Database created successfully: {dbName}");
        }

        #endregion

        #region Table Initialization Tests

        [Fact]
        public void InitializeTable_CreatesNewTable()
        {
            if (_skipTests) return;

            string dbName = $"init_table_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();
            repository.InitializeTable(typeof(Product));

            // Verify table was created
            using NpgsqlConnection connection = new NpgsqlConnection(connString);
            connection.Open();
            using NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'test_products'";
            object? result = command.ExecuteScalar();

            Assert.NotNull(result);
            Assert.Equal("test_products", result.ToString());
            _output.WriteLine("✓ Table 'test_products' created successfully");
        }

        [Fact]
        public void InitializeTable_DoesNotFailIfTableExists()
        {
            if (_skipTests) return;

            string dbName = $"init_exists_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();
            repository.InitializeTable(typeof(Product));

            // Call again - should not throw
            repository.InitializeTable(typeof(Product));

            _output.WriteLine("✓ InitializeTable is idempotent");
        }

        [Fact]
        public void InitializeTable_WithForeignKey_CreatesRelatedTables()
        {
            if (_skipTests) return;

            string dbName = $"init_fk_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Order> repository = new PostgresRepository<Order>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();

            // Create the Product table first (parent table for the foreign key)
            repository.InitializeTable(typeof(Product));

            // Now create the Order table with foreign key to Product
            repository.InitializeTable(typeof(Order));

            // Verify both tables were created
            using NpgsqlConnection connection = new NpgsqlConnection(connString);
            connection.Open();

            using NpgsqlCommand command1 = connection.CreateCommand();
            command1.CommandText = "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'test_products'";
            object? result1 = command1.ExecuteScalar();
            Assert.NotNull(result1);

            using NpgsqlCommand command2 = connection.CreateCommand();
            command2.CommandText = "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'test_orders'";
            object? result2 = command2.ExecuteScalar();
            Assert.NotNull(result2);

            _output.WriteLine("✓ Foreign key relationship established successfully");
        }

        [Fact]
        public void InitializeTables_CreatesMultipleTables()
        {
            if (_skipTests) return;

            string dbName = $"init_multi_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();

            // Order tables so that parent tables (Product) are created before child tables (Order)
            repository.InitializeTables(new[] { typeof(Product), typeof(Category), typeof(Order) });

            // Verify all tables were created
            using NpgsqlConnection connection = new NpgsqlConnection(connString);
            connection.Open();

            List<string> tables = new List<string>();
            using NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'";
            using NpgsqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                tables.Add(reader.GetString(0));
            }

            Assert.Contains("test_products", tables);
            Assert.Contains("test_categories", tables);
            Assert.Contains("test_orders", tables);
            _output.WriteLine("✓ Multiple tables created successfully");
        }

        #endregion

        #region Validation Tests

        [Fact]
        public void ValidateTable_WithValidEntity_ReturnsTrue()
        {
            if (_skipTests) return;

            string dbName = $"validate_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            bool isValid = repository.ValidateTable(typeof(Product), out List<string> errors, out List<string> warnings);

            Assert.True(isValid);
            Assert.Empty(errors);
            _output.WriteLine("✓ ValidateTable returned true for valid entity");
        }

        [Fact]
        public void ValidateTable_WithInvalidEntity_ReturnsFalse()
        {
            if (_skipTests) return;

            string dbName = $"validate_invalid_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            // Use a valid repository to call ValidateTable with an invalid entity type
            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            bool isValid = repository.ValidateTable(typeof(InvalidEntity), out List<string> errors, out List<string> warnings);

            Assert.False(isValid);
            Assert.NotEmpty(errors);
            _output.WriteLine($"✓ ValidateTable returned false with errors: {string.Join(", ", errors)}");
        }

        [Fact]
        public void ValidateTable_WithNoPrimaryKey_ReturnsFalse()
        {
            if (_skipTests) return;

            string dbName = $"validate_no_pk_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            // Use a valid repository to call ValidateTable with an entity type that has no primary key
            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            bool isValid = repository.ValidateTable(typeof(NoPrimaryKeyEntity), out List<string> errors, out List<string> warnings);

            Assert.False(isValid);
            Assert.Contains(errors, e => e.Contains("primary key") || e.Contains("Primary key") || e.Contains("PrimaryKey"));
            _output.WriteLine("✓ ValidateTable correctly identified missing primary key");
        }

        [Fact]
        public void ValidateTables_ValidatesMultipleEntities()
        {
            if (_skipTests) return;

            string dbName = $"validate_multi_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            bool isValid = repository.ValidateTables(
                new[] { typeof(Product), typeof(InvalidEntity) },
                out List<string> errors,
                out List<string> warnings);

            Assert.False(isValid); // Should be false because InvalidEntity is invalid
            Assert.NotEmpty(errors);
            _output.WriteLine($"✓ ValidateTables found errors in mixed entity types: {errors.Count} errors");
        }

        [Fact]
        public void ValidateTable_DetectsMissingColumns()
        {
            if (_skipTests) return;

            string dbName = $"validate_missing_col_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();

            // Create a table with only some columns
            using NpgsqlConnection connection = new NpgsqlConnection(connString);
            connection.Open();
            using NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = @"
                CREATE TABLE test_products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255)
                )";
            command.ExecuteNonQuery();

            // Now validate - should detect missing columns (price, created_utc, guid)
            bool isValid = repository.ValidateTable(typeof(Product), out List<string> errors, out List<string> warnings);

            Assert.False(isValid);
            Assert.Contains(errors, e => e.Contains("price"));
            Assert.Contains(errors, e => e.Contains("created_utc"));
            Assert.Contains(errors, e => e.Contains("guid"));
            _output.WriteLine($"✓ ValidateTable detected missing columns: {string.Join(", ", errors)}");
        }

        [Fact]
        public void ValidateTable_PassesWhenSchemaMatches()
        {
            if (_skipTests) return;

            string dbName = $"validate_matching_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();
            repository.InitializeTable(typeof(Product));

            // Validate after creating table - should pass
            bool isValid = repository.ValidateTable(typeof(Product), out List<string> errors, out List<string> warnings);

            Assert.True(isValid);
            Assert.Empty(errors);
            _output.WriteLine("✓ ValidateTable passed when schema matches entity definition");
        }

        [Fact]
        public void ValidateTable_WarnsAboutExtraColumns()
        {
            if (_skipTests) return;

            string dbName = $"validate_extra_col_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();
            repository.InitializeTable(typeof(Product));

            // Add an extra column to the table
            using NpgsqlConnection connection = new NpgsqlConnection(connString);
            connection.Open();
            using NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = "ALTER TABLE test_products ADD COLUMN extra_field VARCHAR(100)";
            command.ExecuteNonQuery();

            // Validate - should pass but generate warning about extra column
            bool isValid = repository.ValidateTable(typeof(Product), out List<string> errors, out List<string> warnings);

            Assert.True(isValid); // Should still be valid (warning, not error)
            Assert.Empty(errors);
            Assert.Contains(warnings, w => w.Contains("extra_field"));
            _output.WriteLine($"✓ ValidateTable warned about extra column: {string.Join(", ", warnings)}");
        }

        #endregion

        #region Default Value Provider Tests

        [Fact]
        public void DefaultValueProvider_CurrentDateTimeUtc_SetsValue()
        {
            if (_skipTests) return;

            string dbName = $"default_datetime_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();
            repository.InitializeTable(typeof(Product));

            Product product = new Product
            {
                Name = "Test Product",
                Price = 19.99m
                // CreatedUtc not set - should be auto-populated
            };

            Product created = repository.Create(product);

            Assert.NotEqual(default(DateTime), created.CreatedUtc);
            Assert.True(created.CreatedUtc <= DateTime.UtcNow);
            Assert.True(created.CreatedUtc >= DateTime.UtcNow.AddSeconds(-5));
            _output.WriteLine($"✓ CurrentDateTimeUtc provider set value: {created.CreatedUtc}");
        }

        [Fact]
        public void DefaultValueProvider_NewGuid_SetsValue()
        {
            if (_skipTests) return;

            string dbName = $"default_guid_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();
            repository.InitializeTable(typeof(Product));

            Product product = new Product
            {
                Name = "Test Product",
                Price = 19.99m
                // ProductGuid not set - should be auto-populated
            };

            Product created = repository.Create(product);

            Assert.NotEqual(Guid.Empty, created.ProductGuid);
            _output.WriteLine($"✓ NewGuid provider set value: {created.ProductGuid}");
        }

        [Fact]
        public void DefaultValueProvider_SequentialGuid_SetsValue()
        {
            if (_skipTests) return;

            string dbName = $"default_seqguid_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Category> repository = new PostgresRepository<Category>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();
            repository.InitializeTable(typeof(Category));

            Category cat1 = new Category { Name = "Category 1" };
            Category cat2 = new Category { Name = "Category 2" };

            Category created1 = repository.Create(cat1);
            Category created2 = repository.Create(cat2);

            Assert.NotEqual(Guid.Empty, created1.CategoryGuid);
            Assert.NotEqual(Guid.Empty, created2.CategoryGuid);
            Assert.NotEqual(created1.CategoryGuid, created2.CategoryGuid);
            _output.WriteLine($"✓ SequentialGuid provider set values: {created1.CategoryGuid}, {created2.CategoryGuid}");
        }

        [Fact]
        public void DefaultValueProvider_OnlyAppliesWhenValueIsDefault()
        {
            if (_skipTests) return;

            string dbName = $"default_conditional_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> repository = new PostgresRepository<Product>(settings);
            _createdDatabases.Add(dbName);

            repository.CreateDatabaseIfNotExists();
            repository.InitializeTable(typeof(Product));

            Guid explicitGuid = Guid.NewGuid();
            DateTime explicitTime = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            Product product = new Product
            {
                Name = "Test Product",
                Price = 19.99m,
                ProductGuid = explicitGuid,
                CreatedUtc = explicitTime
            };

            Product created = repository.Create(product);

            Assert.Equal(explicitGuid, created.ProductGuid);
            Assert.Equal(explicitTime, created.CreatedUtc);
            _output.WriteLine("✓ Default value providers did not override explicitly set values");
        }

        #endregion

        #region Full Workflow Tests

        [Fact]
        public void FullWorkflow_InitializeAndUseDatabase()
        {
            if (_skipTests) return;

            string dbName = $"full_workflow_{Guid.NewGuid():N}";
            string connString = $"Host=localhost;Database={dbName};Username=postgres;Password=postgres;";

            PostgresRepositorySettings settingsproductRepo = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Product> productRepo = new PostgresRepository<Product>(settingsproductRepo);
            PostgresRepositorySettings settingsorderRepo = PostgresRepositorySettings.Parse(connString);
            PostgresRepository<Order> orderRepo = new PostgresRepository<Order>(settingsorderRepo);
            _createdDatabases.Add(dbName);

            // 1. Create database
            productRepo.CreateDatabaseIfNotExists();

            // 2. Initialize tables
            productRepo.InitializeTables(new[] { typeof(Product), typeof(Order) });

            // 3. Validate tables
            bool isValid = productRepo.ValidateTables(
                new[] { typeof(Product), typeof(Order) },
                out List<string> errors,
                out List<string> warnings);

            Assert.True(isValid);
            Assert.Empty(errors);

            // 4. Create product with default values
            Product product = new Product
            {
                Name = "Test Product",
                Price = 29.99m
            };

            Product createdProduct = productRepo.Create(product);
            Assert.True(createdProduct.Id > 0);
            Assert.NotEqual(default(DateTime), createdProduct.CreatedUtc);
            Assert.NotEqual(Guid.Empty, createdProduct.ProductGuid);

            // 5. Create order
            Order order = new Order
            {
                ProductId = createdProduct.Id,
                Quantity = 5
            };

            Order createdOrder = orderRepo.Create(order);
            Assert.True(createdOrder.Id > 0);
            Assert.NotEqual(default(DateTime), createdOrder.CreatedUtc);

            _output.WriteLine("✓ Full initialization workflow completed successfully");
            _output.WriteLine($"  - Created {productRepo.Count()} products");
            _output.WriteLine($"  - Created {orderRepo.Count()} orders");
            _output.WriteLine("  - All default values applied correctly");
        }

        #endregion
    }
}
