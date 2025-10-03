namespace Test.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.SqlServer;
    using Microsoft.Data.SqlClient;
    using Test.Shared;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Comprehensive data type conversion and mapping tests for SQL Server including:
    /// - Complex types: DateTime, DateTimeOffset, TimeSpan, Guid
    /// - Enum handling: String and integer enum storage
    /// - Collections: Arrays, Lists, Dictionaries
    /// - JSON serialization: Complex object persistence
    /// - Nullable types: Comprehensive null value handling
    /// - Type conversion in operations: UpdateField with type conversion
    /// </summary>
    [Collection("SqlServerDataTypeTests")]
    public class SqlServerDataTypeConverterTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _connectionString;
        private readonly bool _skipTests;

        public SqlServerDataTypeConverterTests(ITestOutputHelper output)
        {
            _output = output;
            _connectionString = "Server=view.homedns.org,1433;Database=durable_datatype_test;User=sa;Password=P@ssw0rd4Sql;TrustServerCertificate=true;Encrypt=false;";

            try
            {
                // Test connection availability with a simple query that doesn't require tables
                using var connection = new SqlConnection(_connectionString);
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteScalar();
                _output.WriteLine("SQL Server data type converter tests initialized successfully");
            }
            catch (Exception ex)
            {
                _skipTests = true;
                _output.WriteLine($"WARNING: SQL Server initialization failed - {ex.Message}");
            }
        }

        /// <summary>
        /// Tests basic data type storage and retrieval with complex types.
        /// </summary>
        [Fact]
        public async Task BasicDataTypeStorage_CreateAndRetrieve_HandlesComplexTypes()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            _output.WriteLine("=== Testing Basic Data Type Storage ===");

            // Create entity with all data types
            ComplexEntity testEntity = new ComplexEntity
            {
                Name = "Test Entity",
                CreatedDate = new DateTime(2023, 12, 25, 14, 30, 45, 123, DateTimeKind.Utc),
                UpdatedDate = new DateTimeOffset(2024, 1, 15, 9, 15, 30, TimeSpan.FromHours(-5)),
                UniqueId = Guid.Parse("12345678-1234-5678-9012-123456789012"), // Fixed GUID for testing
                Duration = new TimeSpan(1, 30, 45), // 1 hour, 30 minutes, 45 seconds
                Status = Status.Active,
                StatusAsInt = Status.Pending,
                Tags = new[] { "tag1", "tag2", "important" },
                Scores = new List<int> { 95, 87, 92, 88 },
                Metadata = new Dictionary<string, object>
                {
                    ["version"] = "1.0.0",
                    ["priority"] = 5,
                    ["isPublic"] = true
                },
                Address = new Address
                {
                    Street = "123 Main St",
                    City = "Test City",
                    ZipCode = "12345"
                },
                IsActive = true,
                NullableInt = 42,
                Price = 99.99m
            };

            _output.WriteLine("Inserting complex entity with all data types...");
            ComplexEntity created = await repository.CreateAsync(testEntity);
            _output.WriteLine($"Insert successful: ID = {created.Id}");

            // Test retrieval
            _output.WriteLine("Retrieving entity...");
            ComplexEntity retrieved = await repository.ReadByIdAsync(created.Id);

            Assert.NotNull(retrieved);
            _output.WriteLine("Entity retrieved successfully");

            _output.WriteLine("Verifying data integrity...");
            await VerifyDataIntegrity(testEntity, retrieved);
            _output.WriteLine("✅ Basic data type storage test passed!");
        }

        /// <summary>
        /// Tests handling of null and default values across all data types.
        /// </summary>
        [Fact]
        public async Task NullValueHandling_CreateWithNulls_HandlesNullableTypes()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            _output.WriteLine("=== Testing Null Value Handling ===");

            // Create entity with null values
            ComplexEntity entityWithNulls = new ComplexEntity
            {
                Name = "Null Test Entity",
                CreatedDate = DateTime.UtcNow,
                UpdatedDate = null, // Null DateTimeOffset
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Inactive,
                StatusAsInt = Status.Active,
                Tags = null, // Null array
                Scores = new List<int>(), // Empty list
                Metadata = null, // Null dictionary
                Address = null, // Null complex object
                IsActive = false,
                NullableInt = null, // Null nullable int
                Price = 0m
            };

            _output.WriteLine("Creating entity with null values...");
            ComplexEntity created = await repository.CreateAsync(entityWithNulls);

            _output.WriteLine("Retrieving entity with null values...");
            ComplexEntity retrieved = await repository.ReadByIdAsync(created.Id);

            Assert.NotNull(retrieved);
            Assert.Equal("Null Test Entity", retrieved.Name);
            Assert.Null(retrieved.UpdatedDate);
            Assert.Null(retrieved.Tags);
            Assert.Null(retrieved.Metadata);
            Assert.Null(retrieved.Address);
            Assert.Null(retrieved.NullableInt);
            Assert.NotNull(retrieved.Scores); // Should be empty list, not null
            Assert.Empty(retrieved.Scores);

            _output.WriteLine("✅ Null value handling test passed!");
        }

        /// <summary>
        /// Tests enum storage and retrieval in both string and integer formats.
        /// </summary>
        [Fact]
        public async Task EnumHandling_StorageAndRetrieval_HandlesStringAndIntegerEnums()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            _output.WriteLine("=== Testing Enum Handling ===");

            // Test all enum values
            Status[] enumValues = { Status.Active, Status.Inactive, Status.Pending };

            foreach (Status status in enumValues)
            {
                ComplexEntity entity = new ComplexEntity
                {
                    Name = $"Enum Test - {status}",
                    CreatedDate = DateTime.UtcNow,
                    UniqueId = Guid.NewGuid(),
                    Duration = TimeSpan.Zero,
                    Status = status, // String storage
                    StatusAsInt = status, // Integer storage
                    IsActive = true,
                    Price = 10.00m
                };

                ComplexEntity created = await repository.CreateAsync(entity);
                ComplexEntity retrieved = await repository.ReadByIdAsync(created.Id);

                Assert.Equal(status, retrieved.Status);
                Assert.Equal(status, retrieved.StatusAsInt);

                _output.WriteLine($"✓ Enum {status} stored and retrieved correctly");
            }

            _output.WriteLine("✅ Enum handling test passed!");
        }

        /// <summary>
        /// Tests collection storage and retrieval including arrays and lists.
        /// </summary>
        [Fact]
        public async Task CollectionHandling_ArraysAndLists_HandlesCollectionTypes()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            _output.WriteLine("=== Testing Collection Handling ===");

            ComplexEntity entity = new ComplexEntity
            {
                Name = "Collection Test",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                Tags = new[] { "array", "test", "multiple", "tags" },
                Scores = new List<int> { 100, 95, 87, 92, 78, 85 },
                IsActive = true,
                Price = 50.00m
            };

            _output.WriteLine("Creating entity with collections...");
            ComplexEntity created = await repository.CreateAsync(entity);

            _output.WriteLine("Retrieving entity with collections...");
            ComplexEntity retrieved = await repository.ReadByIdAsync(created.Id);

            Assert.NotNull(retrieved.Tags);
            Assert.NotNull(retrieved.Scores);
            Assert.Equal(4, retrieved.Tags.Length);
            Assert.Equal(6, retrieved.Scores.Count);
            Assert.True(ArraysEqual(entity.Tags, retrieved.Tags));
            Assert.True(ListsEqual(entity.Scores, retrieved.Scores));

            _output.WriteLine($"✓ Tags array: [{string.Join(", ", retrieved.Tags)}]");
            _output.WriteLine($"✓ Scores list: [{string.Join(", ", retrieved.Scores)}]");
            _output.WriteLine("✅ Collection handling test passed!");
        }

        /// <summary>
        /// Tests JSON serialization of complex objects and dictionaries.
        /// </summary>
        [Fact]
        public async Task JsonSerialization_ComplexObjects_HandlesDictionariesAndObjects()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            _output.WriteLine("=== Testing JSON Serialization ===");

            ComplexEntity entity = new ComplexEntity
            {
                Name = "JSON Test",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                Metadata = new Dictionary<string, object>
                {
                    ["version"] = "2.1.0",
                    ["priority"] = 10,
                    ["isPublic"] = false,
                    ["nested"] = new Dictionary<string, object>
                    {
                        ["level"] = 2,
                        ["type"] = "nested"
                    },
                    ["tags"] = new[] { "json", "test" }
                },
                Address = new Address
                {
                    Street = "456 JSON Ave",
                    City = "Serialization City",
                    ZipCode = "54321"
                },
                IsActive = true,
                Price = 75.50m
            };

            _output.WriteLine("Creating entity with JSON-serialized objects...");
            ComplexEntity created = await repository.CreateAsync(entity);

            _output.WriteLine("Retrieving entity with JSON-serialized objects...");
            ComplexEntity retrieved = await repository.ReadByIdAsync(created.Id);

            Assert.NotNull(retrieved.Metadata);
            Assert.NotNull(retrieved.Address);

            // Verify dictionary content
            Assert.Equal("2.1.0", retrieved.Metadata["version"].ToString());

            // Handle JsonElement values from JSON deserialization
            if (retrieved.Metadata["priority"] is System.Text.Json.JsonElement priorityElement)
                Assert.Equal(10L, priorityElement.GetInt64());
            else
                Assert.Equal(10L, Convert.ToInt64(retrieved.Metadata["priority"]));

            if (retrieved.Metadata["isPublic"] is System.Text.Json.JsonElement isPublicElement)
                Assert.False(isPublicElement.GetBoolean());
            else
                Assert.False(Convert.ToBoolean(retrieved.Metadata["isPublic"]));

            // Verify complex object
            Assert.Equal("456 JSON Ave", retrieved.Address.Street);
            Assert.Equal("Serialization City", retrieved.Address.City);
            Assert.Equal("54321", retrieved.Address.ZipCode);

            _output.WriteLine($"✓ Metadata: {retrieved.Metadata.Count} keys");
            _output.WriteLine($"✓ Address: {retrieved.Address.Street}, {retrieved.Address.City}");
            _output.WriteLine("✅ JSON serialization test passed!");
        }

        /// <summary>
        /// Tests DateTime, DateTimeOffset, and TimeSpan precision and handling.
        /// </summary>
        [Fact]
        public async Task DateTimeHandling_VariousPrecisions_HandlesDateTimeTypes()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            _output.WriteLine("=== Testing DateTime Handling ===");

            // Test various DateTime scenarios
            var testCases = new[]
            {
                new
                {
                    Name = "UTC DateTime",
                    CreatedDate = new DateTime(2024, 3, 15, 14, 30, 45, 123, DateTimeKind.Utc),
                    UpdatedDate = (DateTimeOffset?)new DateTimeOffset(2024, 3, 16, 10, 15, 30, TimeSpan.FromHours(2)),
                    Duration = new TimeSpan(2, 15, 30, 45, 500)
                },
                new
                {
                    Name = "Local DateTime",
                    CreatedDate = new DateTime(2024, 6, 20, 8, 45, 15, DateTimeKind.Local),
                    UpdatedDate = (DateTimeOffset?)DateTimeOffset.Now,
                    Duration = TimeSpan.FromDays(1.5)
                },
                new
                {
                    Name = "Minimal DateTime",
                    CreatedDate = DateTime.MinValue.AddYears(1900), // SQL Server doesn't support year 1
                    UpdatedDate = (DateTimeOffset?)null,
                    Duration = TimeSpan.FromTicks(1)
                }
            };

            foreach (var testCase in testCases)
            {
                ComplexEntity entity = new ComplexEntity
                {
                    Name = testCase.Name,
                    CreatedDate = testCase.CreatedDate,
                    UpdatedDate = testCase.UpdatedDate,
                    UniqueId = Guid.NewGuid(),
                    Duration = testCase.Duration,
                    Status = Status.Active,
                    StatusAsInt = Status.Active,
                    IsActive = true,
                    Price = 25.00m
                };

                ComplexEntity created = await repository.CreateAsync(entity);
                ComplexEntity retrieved = await repository.ReadByIdAsync(created.Id);

                // Allow small differences due to database precision
                double timeDiff = Math.Abs((testCase.CreatedDate - retrieved.CreatedDate).TotalMilliseconds);
                Assert.True(timeDiff < 1000, $"DateTime precision issue: expected {testCase.CreatedDate:O}, got {retrieved.CreatedDate:O}");

                if (testCase.UpdatedDate.HasValue)
                {
                    Assert.NotNull(retrieved.UpdatedDate);
                    double offsetDiff = Math.Abs((testCase.UpdatedDate.Value - retrieved.UpdatedDate.Value).TotalSeconds);
                    Assert.True(offsetDiff < 1, $"DateTimeOffset precision issue");
                }
                else
                {
                    Assert.Null(retrieved.UpdatedDate);
                }

                Assert.Equal(testCase.Duration, retrieved.Duration);
                _output.WriteLine($"✓ {testCase.Name} - DateTime handling verified");
            }

            _output.WriteLine("✅ DateTime handling test passed!");
        }

        /// <summary>
        /// Tests GUID storage and retrieval with various GUID formats.
        /// </summary>
        [Fact]
        public async Task GuidHandling_StorageAndRetrieval_HandlesGuidFormats()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            _output.WriteLine("=== Testing GUID Handling ===");

            var testGuids = new[]
            {
                Guid.Empty,
                Guid.NewGuid(),
                Guid.Parse("12345678-1234-5678-9012-123456789012"),
                Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
            };

            foreach (Guid testGuid in testGuids)
            {
                ComplexEntity entity = new ComplexEntity
                {
                    Name = $"GUID Test - {testGuid}",
                    CreatedDate = DateTime.UtcNow,
                    UniqueId = testGuid,
                    Duration = TimeSpan.Zero,
                    Status = Status.Active,
                    StatusAsInt = Status.Active,
                    IsActive = true,
                    Price = 15.00m
                };

                ComplexEntity created = await repository.CreateAsync(entity);
                ComplexEntity retrieved = await repository.ReadByIdAsync(created.Id);

                Assert.Equal(testGuid, retrieved.UniqueId);
                _output.WriteLine($"✓ GUID {testGuid} stored and retrieved correctly");
            }

            _output.WriteLine("✅ GUID handling test passed!");
        }

        /// <summary>
        /// Tests UpdateField operations with complex type conversions.
        /// </summary>
        [Fact]
        public async Task UpdateFieldOperations_TypeConversion_HandlesComplexTypeUpdates()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            _output.WriteLine("=== Testing UpdateField with Type Conversion ===");

            // Create initial entity
            ComplexEntity entity = new ComplexEntity
            {
                Name = "UpdateField Test",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.FromHours(1),
                Status = Status.Active,
                StatusAsInt = Status.Active,
                Tags = new[] { "original", "tags" },
                Scores = new List<int> { 80, 85 },
                IsActive = true,
                Price = 100.00m
            };

            ComplexEntity created = await repository.CreateAsync(entity);
            _output.WriteLine($"Created entity with ID: {created.Id}");

            // Test updating DateTime field
            DateTime newDate = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);
            await repository.UpdateFieldAsync(e => e.Id == created.Id, e => e.CreatedDate, newDate);
            _output.WriteLine("✓ Updated DateTime field");

            // Test updating array field
            string[] newTags = new[] { "updated", "test", "array", "conversion" };
            await repository.UpdateFieldAsync(e => e.Id == created.Id, e => e.Tags, newTags);
            _output.WriteLine("✓ Updated array field");

            // Test updating enum field
            await repository.UpdateFieldAsync(e => e.Id == created.Id, e => e.Status, Status.Pending);
            _output.WriteLine("✓ Updated enum field");

            // Test updating GUID field
            Guid newGuid = Guid.NewGuid();
            await repository.UpdateFieldAsync(e => e.Id == created.Id, e => e.UniqueId, newGuid);
            _output.WriteLine("✓ Updated GUID field");

            // Test updating TimeSpan field
            TimeSpan newDuration = TimeSpan.FromHours(3.5);
            await repository.UpdateFieldAsync(e => e.Id == created.Id, e => e.Duration, newDuration);
            _output.WriteLine("✓ Updated TimeSpan field");

            // Verify all updates
            ComplexEntity updated = await repository.ReadByIdAsync(created.Id);

            Assert.Equal(newDate, updated.CreatedDate);
            Assert.True(ArraysEqual(newTags, updated.Tags));
            Assert.Equal(Status.Pending, updated.Status);
            Assert.Equal(newGuid, updated.UniqueId);
            Assert.Equal(newDuration, updated.Duration);

            _output.WriteLine("✅ UpdateField operations with type conversion test passed!");
        }

        /// <summary>
        /// Tests query operations with complex type filtering and comparisons.
        /// </summary>
        [Fact]
        public async Task QueryOperations_TypeConversion_HandlesComplexTypeQueries()
        {
            if (_skipTests) return;

            using var repository = new SqlServerRepository<ComplexEntity>(_connectionString);
            await CreateTableAsync(repository);

            // Clear existing test data to ensure isolation
            await repository.ExecuteSqlAsync("DELETE FROM complex_entities");
            _output.WriteLine("✓ Cleared existing test data for isolation");

            _output.WriteLine("=== Testing Query Operations with Type Conversion ===");

            // Create test data
            var entities = new[]
            {
                new ComplexEntity
                {
                    Name = "Query Test 1",
                    CreatedDate = new DateTime(2024, 1, 1, 12, 0, 0),
                    UniqueId = Guid.NewGuid(),
                    Duration = TimeSpan.FromHours(1),
                    Status = Status.Active,
                    StatusAsInt = Status.Pending,
                    IsActive = true,
                    Price = 50.00m
                },
                new ComplexEntity
                {
                    Name = "Query Test 2",
                    CreatedDate = new DateTime(2024, 6, 1, 12, 0, 0),
                    UniqueId = Guid.NewGuid(),
                    Duration = TimeSpan.FromHours(2),
                    Status = Status.Pending,
                    StatusAsInt = Status.Active,
                    IsActive = false,
                    Price = 75.00m
                },
                new ComplexEntity
                {
                    Name = "Query Test 3",
                    CreatedDate = new DateTime(2024, 12, 1, 12, 0, 0),
                    UniqueId = Guid.NewGuid(),
                    Duration = TimeSpan.FromHours(3),
                    Status = Status.Inactive,
                    StatusAsInt = Status.Inactive,
                    IsActive = true,
                    Price = 25.00m
                }
            };

            foreach (var entity in entities)
            {
                await repository.CreateAsync(entity);
            }

            _output.WriteLine("Created 3 test entities");

            // Query by enum
            var activeEntities = repository.Query()
                .Where(e => e.Status == Status.Active)
                .Execute()
                .ToList();

            Assert.Single(activeEntities);
            _output.WriteLine($"✓ Query by enum: Found {activeEntities.Count} active entities");

            // Query by DateTime range
            DateTime startDate = new DateTime(2024, 3, 1);
            DateTime endDate = new DateTime(2024, 9, 1);
            var midYearEntities = repository.Query()
                .Where(e => e.CreatedDate > startDate && e.CreatedDate < endDate)
                .Execute()
                .ToList();

            Assert.Single(midYearEntities);
            _output.WriteLine($"✓ Query by DateTime range: Found {midYearEntities.Count} mid-year entities");

            // Query by boolean
            var inactiveEntities = repository.Query()
                .Where(e => e.IsActive == false)
                .Execute()
                .ToList();

            Assert.Single(inactiveEntities);
            _output.WriteLine($"✓ Query by boolean: Found {inactiveEntities.Count} inactive entities");

            // Query by decimal range
            var expensiveEntities = repository.Query()
                .Where(e => e.Price > 60.00m)
                .Execute()
                .ToList();

            Assert.Single(expensiveEntities);
            _output.WriteLine($"✓ Query by decimal: Found {expensiveEntities.Count} expensive entities");

            _output.WriteLine("✅ Query operations with type conversion test passed!");
        }

        /// <summary>
        /// Creates the complex_entities table for testing.
        /// </summary>
        private async Task CreateTableAsync(SqlServerRepository<ComplexEntity> repository)
        {
            // Drop table if it exists to ensure clean state
            try
            {
                await repository.ExecuteSqlAsync("IF OBJECT_ID('dbo.complex_entities', 'U') IS NOT NULL DROP TABLE complex_entities");
                _output.WriteLine("✓ Dropped existing complex_entities table");
                // Give SQL Server time to complete the drop operation
                await Task.Delay(100);
            }
            catch
            {
                // Table might not exist, ignore
            }

            await repository.ExecuteSqlAsync(@"
                CREATE TABLE complex_entities (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    created_date DATETIME2(6) NOT NULL,
                    updated_date DATETIMEOFFSET(6) NULL,
                    unique_id CHAR(36) NOT NULL,
                    duration NVARCHAR(MAX) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    status_int INT NOT NULL,
                    tags NVARCHAR(MAX) NULL,
                    scores NVARCHAR(MAX) NULL,
                    metadata NVARCHAR(MAX) NULL,
                    address NVARCHAR(MAX) NULL,
                    is_active BIT NOT NULL,
                    nullable_int INT NULL,
                    price DECIMAL(10,2) NOT NULL
                )");
            _output.WriteLine("✓ Created complex_entities table");
        }

        /// <summary>
        /// Verifies data integrity between original and retrieved entities.
        /// </summary>
        private Task VerifyDataIntegrity(ComplexEntity original, ComplexEntity retrieved)
        {
            bool success = true;

            // Basic types
            if (original.Name != retrieved.Name)
            {
                _output.WriteLine($"ERROR: Name mismatch. Expected: {original.Name}, Got: {retrieved.Name}");
                success = false;
            }

            // DateTime - allow small difference due to precision
            double timeDiff = Math.Abs((original.CreatedDate - retrieved.CreatedDate).TotalMilliseconds);
            if (timeDiff > 1000) // Allow 1 second difference for SQL Server precision
            {
                _output.WriteLine($"ERROR: CreatedDate mismatch. Expected: {original.CreatedDate:O}, Got: {retrieved.CreatedDate:O}");
                success = false;
            }

            // DateTimeOffset
            if (original.UpdatedDate.HasValue && retrieved.UpdatedDate.HasValue)
            {
                double offsetDiff = Math.Abs((original.UpdatedDate.Value - retrieved.UpdatedDate.Value).TotalSeconds);
                if (offsetDiff > 1)
                {
                    _output.WriteLine($"ERROR: UpdatedDate mismatch. Expected: {original.UpdatedDate:O}, Got: {retrieved.UpdatedDate:O}");
                    success = false;
                }
            }
            else if (original.UpdatedDate != retrieved.UpdatedDate)
            {
                _output.WriteLine($"ERROR: UpdatedDate nullability mismatch. Expected: {original.UpdatedDate}, Got: {retrieved.UpdatedDate}");
                success = false;
            }

            // Guid
            if (original.UniqueId != retrieved.UniqueId)
            {
                _output.WriteLine($"ERROR: UniqueId mismatch. Expected: {original.UniqueId}, Got: {retrieved.UniqueId}");
                success = false;
            }

            // TimeSpan
            if (original.Duration != retrieved.Duration)
            {
                _output.WriteLine($"ERROR: Duration mismatch. Expected: {original.Duration}, Got: {retrieved.Duration}");
                success = false;
            }

            // Enums
            if (original.Status != retrieved.Status)
            {
                _output.WriteLine($"ERROR: Status mismatch. Expected: {original.Status}, Got: {retrieved.Status}");
                success = false;
            }

            if (original.StatusAsInt != retrieved.StatusAsInt)
            {
                _output.WriteLine($"ERROR: StatusAsInt mismatch. Expected: {original.StatusAsInt}, Got: {retrieved.StatusAsInt}");
                success = false;
            }

            // Arrays
            if (!ArraysEqual(original.Tags, retrieved.Tags))
            {
                _output.WriteLine($"ERROR: Tags mismatch. Expected: [{string.Join(", ", original.Tags ?? new string[0])}], Got: [{string.Join(", ", retrieved.Tags ?? new string[0])}]");
                success = false;
            }

            // Lists
            if (!ListsEqual(original.Scores, retrieved.Scores))
            {
                _output.WriteLine($"ERROR: Scores mismatch. Expected: [{string.Join(", ", original.Scores ?? new List<int>())}], Got: [{string.Join(", ", retrieved.Scores ?? new List<int>())}]");
                success = false;
            }

            // Other types
            if (original.IsActive != retrieved.IsActive)
            {
                _output.WriteLine($"ERROR: IsActive mismatch. Expected: {original.IsActive}, Got: {retrieved.IsActive}");
                success = false;
            }

            if (original.NullableInt != retrieved.NullableInt)
            {
                _output.WriteLine($"ERROR: NullableInt mismatch. Expected: {original.NullableInt}, Got: {retrieved.NullableInt}");
                success = false;
            }

            if (Math.Abs(original.Price - retrieved.Price) > 0.001m)
            {
                _output.WriteLine($"ERROR: Price mismatch. Expected: {original.Price}, Got: {retrieved.Price}");
                success = false;
            }

            if (success)
            {
                _output.WriteLine("✓ Data integrity verification passed!");
            }
            else
            {
                throw new Exception("Data integrity verification failed!");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Compares two arrays for equality.
        /// </summary>
        private static bool ArraysEqual<T>(T[] arr1, T[] arr2)
        {
            if (arr1 == null && arr2 == null) return true;
            if (arr1 == null || arr2 == null) return false;
            if (arr1.Length != arr2.Length) return false;

            return arr1.SequenceEqual(arr2);
        }

        /// <summary>
        /// Compares two lists for equality.
        /// </summary>
        private static bool ListsEqual<T>(List<T> list1, List<T> list2)
        {
            if (list1 == null && list2 == null) return true;
            if (list1 == null || list2 == null) return false;
            if (list1.Count != list2.Count) return false;

            return list1.SequenceEqual(list2);
        }

        /// <summary>
        /// Disposes of test resources.
        /// </summary>
        public void Dispose()
        {
            // Cleanup is handled by using statements
        }
    }
}