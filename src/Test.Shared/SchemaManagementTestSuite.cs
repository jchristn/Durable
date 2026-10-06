namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Test suite for validating schema management functionality including table initialization,
    /// validation, and index management operations.
    /// </summary>
    public class SchemaManagementTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaManagementTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public SchemaManagementTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests that InitializeTable creates a table successfully.
        /// </summary>
        [Fact]
        public async Task CanInitializeTable()
        {
            ISqlRepository<Product> repository = _Provider.CreateRepository<Product>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");

            repository.InitializeTable(typeof(Product));

            Product product = new Product
            {
                Name = "Test Product",
                Sku = "SKU-001",
                Category = "Electronics",
                Price = 99.99m,
                StockQuantity = 10,
                Description = "A test product"
            };

            Product created = await repository.CreateAsync(product);

            Assert.NotNull(created);
            Assert.True(created.Id > 0);
            Assert.True(ValidationHelpers.AreStringsEqual("Test Product", created.Name));

            Console.WriteLine($"     Table 'products' initialized and record created with ID: {created.Id}");
        }

        /// <summary>
        /// Tests that InitializeTableAsync creates a table successfully.
        /// </summary>
        [Fact]
        public async Task CanInitializeTableAsync()
        {
            ISqlRepository<Employee> repository = _Provider.CreateRepository<Employee>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS employees");

            await repository.InitializeTableAsync(typeof(Employee));

            Employee employee = new Employee
            {
                FirstName = "John",
                LastName = "Doe",
                Email = "john.doe@example.com",
                Department = "Engineering",
                HireDate = DateTime.Now,
                Salary = 75000m
            };

            Employee created = await repository.CreateAsync(employee);

            Assert.NotNull(created);
            Assert.True(created.Id > 0);
            Assert.True(ValidationHelpers.AreStringsEqual("John", created.FirstName));

            Console.WriteLine($"     Table 'employees' initialized async and record created with ID: {created.Id}");
        }

        /// <summary>
        /// Tests that ValidateTable returns true for a valid table schema.
        /// </summary>
        [Fact]
        public async Task ValidateTableReturnsTrueForValidSchema()
        {
            ISqlRepository<Product> repository = _Provider.CreateRepository<Product>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
            repository.InitializeTable(typeof(Product));

            TableValidationResult result = repository.ValidateTable(typeof(Product));
            Assert.True(result.IsValid, $"Table validation failed. Errors: {string.Join(", ", result.Errors)}");
            Assert.Empty(result.Errors);
            Assert.True(result.TableExists);
            Assert.Equal("products", result.TableName);

            TableValidationResult asyncResult = await repository.ValidateTableAsync(typeof(Product));
            Assert.True(asyncResult.IsValid, $"Async table validation failed. Errors: {string.Join(", ", asyncResult.Errors)}");
            Assert.True(asyncResult.TableExists);
            Assert.Equal(result.Warnings.Count, asyncResult.Warnings.Count);
        }

        /// <summary>
        /// ValidateTable[Async] reports a mapped column missing from the table as an error and an unmapped table column
        /// as a warning, and reports a table that does not exist as valid with <c>TableExists</c> false.
        /// </summary>
        [Fact]
        public async Task ValidateTableReportsMissingColumnsAndUnmappedColumns()
        {
            ISqlRepository<Product> repository = _Provider.CreateRepository<Product>();
            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");

            TableValidationResult missing = await repository.ValidateTableAsync(typeof(Product));
            Assert.True(missing.IsValid);
            Assert.False(missing.TableExists);

            await repository.ExecuteSqlAsync("CREATE TABLE products (id INT PRIMARY KEY, extra_column INT)");
            try
            {
                TableValidationResult result = await repository.ValidateTableAsync(typeof(Product));
                Assert.False(result.IsValid);
                Assert.True(result.TableExists);
                Assert.Contains(result.Errors, e => e.Contains("'name'"));
                Assert.Contains(result.Warnings, w => w.Contains("extra_column"));

                TableValidationResult sync = repository.ValidateTable(typeof(Product));
                Assert.Equal(result.Errors.Count, sync.Errors.Count);
                Assert.Equal(result.Warnings.Count, sync.Warnings.Count);
            }
            finally
            {
                await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
            }
        }

        /// <summary>
        /// Tests that ValidateTables validates multiple tables correctly.
        /// </summary>
        [Fact]
        public async Task ValidateTablesValidatesMultipleTables()
        {
            ISqlRepository<Product> productRepo = _Provider.CreateRepository<Product>();
            ISqlRepository<Employee> employeeRepo = _Provider.CreateRepository<Employee>();

            await productRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
            await employeeRepo.ExecuteSqlAsync("DROP TABLE IF EXISTS employees");

            productRepo.InitializeTable(typeof(Product));
            employeeRepo.InitializeTable(typeof(Employee));

            Type[] types = new[] { typeof(Product), typeof(Employee) };
            SchemaValidationResult result = productRepo.ValidateTables(types);
            Assert.True(result.IsValid, $"Tables validation failed. Errors: {string.Join(", ", result.Errors)}");
            Assert.Empty(result.Errors);
            Assert.Equal(2, result.Tables.Count);

            SchemaValidationResult asyncResult = await productRepo.ValidateTablesAsync(types);
            Assert.True(asyncResult.IsValid, $"Async tables validation failed. Errors: {string.Join(", ", asyncResult.Errors)}");
            Assert.All(asyncResult.Tables, t => Assert.True(t.TableExists));
        }

        /// <summary>
        /// Tests that CreateIndexes creates indexes from attributes.
        /// </summary>
        [Fact]
        public async Task CanCreateIndexesFromAttributes()
        {
            ISqlRepository<Product> repository = _Provider.CreateRepository<Product>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
            repository.InitializeTable(typeof(Product));

            repository.CreateIndexes(typeof(Product));

            List<string> indexes = await repository.GetIndexesAsync(typeof(Product));

            Assert.NotNull(indexes);
            Assert.NotEmpty(indexes);

            Console.WriteLine($"     Created {indexes.Count} indexes on 'products' table:");
            foreach (string indexName in indexes)
            {
                Console.WriteLine($"       - {indexName}");
            }

            Assert.Contains(indexes, idx => idx.Contains("idx_product_name", StringComparison.OrdinalIgnoreCase) || idx.Contains("name", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(indexes, idx => idx.Contains("idx_product_sku", StringComparison.OrdinalIgnoreCase) || idx.Contains("sku", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Tests that CreateIndexesAsync creates indexes asynchronously.
        /// </summary>
        [Fact]
        public async Task CanCreateIndexesAsync()
        {
            ISqlRepository<Employee> repository = _Provider.CreateRepository<Employee>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS employees");
            await repository.InitializeTableAsync(typeof(Employee));

            await repository.CreateIndexesAsync(typeof(Employee));

            List<string> indexes = await repository.GetIndexesAsync(typeof(Employee));

            Assert.NotNull(indexes);
            Assert.NotEmpty(indexes);

            Console.WriteLine($"     Created {indexes.Count} indexes async on 'employees' table:");
            foreach (string indexName in indexes)
            {
                Console.WriteLine($"       - {indexName}");
            }

            Assert.Contains(indexes, idx => idx.Contains("idx_full_name", StringComparison.OrdinalIgnoreCase) || idx.Contains("name", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(indexes, idx => idx.Contains("idx_employee_email", StringComparison.OrdinalIgnoreCase) || idx.Contains("email", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Tests that GetIndexes retrieves index metadata.
        /// </summary>
        [Fact]
        public async Task CanGetIndexes()
        {
            ISqlRepository<Product> repository = _Provider.CreateRepository<Product>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
            repository.InitializeTable(typeof(Product));
            repository.CreateIndexes(typeof(Product));

            List<string> indexes = repository.GetIndexes(typeof(Product));

            Assert.NotNull(indexes);
            Assert.NotEmpty(indexes);

            Console.WriteLine($"     Retrieved {indexes.Count} indexes from 'products' table");
        }

        /// <summary>
        /// Tests that GetIndexesAsync retrieves index metadata asynchronously.
        /// </summary>
        [Fact]
        public async Task CanGetIndexesAsync()
        {
            ISqlRepository<Employee> repository = _Provider.CreateRepository<Employee>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS employees");
            await repository.InitializeTableAsync(typeof(Employee));
            await repository.CreateIndexesAsync(typeof(Employee));

            List<string> indexes = await repository.GetIndexesAsync(typeof(Employee));

            Assert.NotNull(indexes);
            Assert.NotEmpty(indexes);

            Console.WriteLine($"     Retrieved {indexes.Count} indexes async from 'employees' table");
        }

        /// <summary>
        /// Tests that DropIndex removes an index successfully.
        /// </summary>
        [Fact]
        public async Task CanDropIndex()
        {
            ISqlRepository<Product> repository = _Provider.CreateRepository<Product>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
            repository.InitializeTable(typeof(Product));
            repository.CreateIndexes(typeof(Product));

            List<string> indexesBeforeDrop = repository.GetIndexes(typeof(Product));
            int initialCount = indexesBeforeDrop.Count;

            Assert.True(initialCount > 0, "No indexes found to drop");

            string? indexToDrop = indexesBeforeDrop.FirstOrDefault(idx =>
                idx.Contains("idx_product_name", StringComparison.OrdinalIgnoreCase) ||
                idx.Contains("name", StringComparison.OrdinalIgnoreCase));

            if (indexToDrop != null)
            {
                repository.DropIndex(indexToDrop);

                List<string> indexesAfterDrop = repository.GetIndexes(typeof(Product));

                Assert.True(indexesAfterDrop.Count < initialCount, $"Index count did not decrease. Before: {initialCount}, After: {indexesAfterDrop.Count}");

                Console.WriteLine($"     Dropped index '{indexToDrop}'. Indexes before: {initialCount}, after: {indexesAfterDrop.Count}");
            }
            else
            {
                Console.WriteLine($"     Could not find 'idx_product_name' to drop. Available indexes: {string.Join(", ", indexesBeforeDrop)}");
            }
        }

        /// <summary>
        /// Tests that DropIndexAsync removes an index asynchronously.
        /// </summary>
        [Fact]
        public async Task CanDropIndexAsync()
        {
            ISqlRepository<Employee> repository = _Provider.CreateRepository<Employee>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS employees");
            await repository.InitializeTableAsync(typeof(Employee));
            await repository.CreateIndexesAsync(typeof(Employee));

            List<string> indexesBeforeDrop = await repository.GetIndexesAsync(typeof(Employee));
            int initialCount = indexesBeforeDrop.Count;

            Assert.True(initialCount > 0, "No indexes found to drop");

            string? indexToDrop = indexesBeforeDrop.FirstOrDefault(idx =>
                idx.Contains("idx_employee_email", StringComparison.OrdinalIgnoreCase) ||
                idx.Contains("email", StringComparison.OrdinalIgnoreCase));

            if (indexToDrop != null)
            {
                await repository.DropIndexAsync(indexToDrop);

                List<string> indexesAfterDrop = await repository.GetIndexesAsync(typeof(Employee));

                Assert.True(indexesAfterDrop.Count < initialCount, $"Index count did not decrease. Before: {initialCount}, After: {indexesAfterDrop.Count}");

                Console.WriteLine($"     Dropped index async '{indexToDrop}'. Indexes before: {initialCount}, after: {indexesAfterDrop.Count}");
            }
            else
            {
                Console.WriteLine($"     Could not find 'idx_employee_email' to drop. Available indexes: {string.Join(", ", indexesBeforeDrop)}");
            }
        }

        /// <summary>
        /// Tests that composite indexes are created correctly using CompositeIndexAttribute.
        /// </summary>
        [Fact]
        public async Task CompositeIndexAttributeCreatesMultiColumnIndexes()
        {
            ISqlRepository<Product> repository = _Provider.CreateRepository<Product>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
            repository.InitializeTable(typeof(Product));
            repository.CreateIndexes(typeof(Product));

            List<string> indexes = repository.GetIndexes(typeof(Product));

            Assert.NotNull(indexes);

            Console.WriteLine($"     Checking for composite indexes on 'products' table:");
            foreach (string indexName in indexes)
            {
                Console.WriteLine($"       - {indexName}");
            }

            bool hasCompositeIndex = indexes.Any(idx =>
                idx.Contains("idx_category_price", StringComparison.OrdinalIgnoreCase) ||
                idx.Contains("idx_name_sku", StringComparison.OrdinalIgnoreCase) ||
                (idx.Contains("category", StringComparison.OrdinalIgnoreCase) && idx.Contains("price", StringComparison.OrdinalIgnoreCase)));

            Assert.True(hasCompositeIndex, "No composite indexes found. Available indexes: " + string.Join(", ", indexes));
        }

        /// <summary>
        /// Tests that composite indexes are created correctly using IndexAttribute with Order.
        /// </summary>
        [Fact]
        public async Task IndexAttributeWithOrderCreatesCompositeIndexes()
        {
            ISqlRepository<Employee> repository = _Provider.CreateRepository<Employee>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS employees");
            await repository.InitializeTableAsync(typeof(Employee));
            await repository.CreateIndexesAsync(typeof(Employee));

            List<string> indexes = await repository.GetIndexesAsync(typeof(Employee));

            Assert.NotNull(indexes);

            Console.WriteLine($"     Checking for composite indexes via IndexAttribute on 'employees' table:");
            foreach (string indexName in indexes)
            {
                Console.WriteLine($"       - {indexName}");
            }

            bool hasFullNameIndex = indexes.Any(idx => idx.Contains("idx_full_name", StringComparison.OrdinalIgnoreCase));
            bool hasDeptHireDateIndex = indexes.Any(idx => idx.Contains("idx_dept_hire_date", StringComparison.OrdinalIgnoreCase));

            Assert.True(hasFullNameIndex || hasDeptHireDateIndex,
                "No composite indexes found via IndexAttribute. Available indexes: " + string.Join(", ", indexes));
        }

        /// <summary>
        /// Tests that unique indexes are created correctly.
        /// </summary>
        [Fact]
        public async Task UniqueIndexesAreEnforced()
        {
            ISqlRepository<Product> repository = _Provider.CreateRepository<Product>();

            await repository.ExecuteSqlAsync("DROP TABLE IF EXISTS products");
            repository.InitializeTable(typeof(Product));
            repository.CreateIndexes(typeof(Product));

            Product product1 = new Product
            {
                Name = "Product 1",
                Sku = "UNIQUE-SKU",
                Category = "Test",
                Price = 10.00m,
                StockQuantity = 5
            };

            await repository.CreateAsync(product1);

            Product product2 = new Product
            {
                Name = "Product 2",
                Sku = "UNIQUE-SKU",  // Same SKU - should fail due to unique index
                Category = "Test",
                Price = 20.00m,
                StockQuantity = 10
            };

            Exception? exception = await Record.ExceptionAsync(async () =>
            {
                await repository.CreateAsync(product2);
            });

            Assert.NotNull(exception);

            Console.WriteLine($"     Unique index constraint enforced. Exception type: {exception.GetType().Name}");
            Console.WriteLine($"     Exception message: {exception.Message}");
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
