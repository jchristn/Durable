using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Durable;
using Durable.Sqlite;
using Microsoft.Data.Sqlite;
using Test.Shared;

namespace Test.Sqlite
{
    public static class DataTypeConverterTest
    {
        public static async Task RunDataTypeConverterTest()
        {
            Console.WriteLine("=== DATA TYPE CONVERTER TEST ===");
            
            const string connectionString = "Data Source=DataTypeTest;Mode=Memory;Cache=Shared";
            
            // Keep one connection open to maintain the in-memory database
            using SqliteConnection keepAliveConnection = new SqliteConnection(connectionString);
            keepAliveConnection.Open();
            
            using SqliteRepository<ComplexEntity> repository = new SqliteRepository<ComplexEntity>(connectionString);
            
            Console.WriteLine("Creating table...");
            await CreateTableAsync(repository);
            Console.WriteLine("Table created successfully");
            
            // Test data with various data types
            ComplexEntity testEntity = new ComplexEntity
            {
                Name = "Test Entity",
                CreatedDate = new DateTime(2023, 12, 25, 14, 30, 45, 123, DateTimeKind.Utc),
                UpdatedDate = new DateTimeOffset(2024, 1, 15, 9, 15, 30, TimeSpan.FromHours(-5)),
                UniqueId = Guid.NewGuid(),
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

            Console.WriteLine("Inserting complex entity...");
            ComplexEntity created = await repository.CreateAsync(testEntity);
            Console.WriteLine($"Insert successful: ID = {created.Id}");

            // Test retrieval
            Console.WriteLine("Retrieving entity...");
            ComplexEntity retrieved = await repository.ReadByIdAsync(created.Id);
            
            if (retrieved == null)
            {
                Console.WriteLine("ERROR: Could not retrieve entity!");
                return;
            }

            Console.WriteLine("Verifying data integrity...");
            await VerifyDataIntegrity(created, retrieved);

            // Test with null values
            Console.WriteLine("\nTesting null values...");
            ComplexEntity entityWithNulls = new ComplexEntity
            {
                Name = "Null Test",
                CreatedDate = DateTime.UtcNow,
                UpdatedDate = null, // Null DateTimeOffset
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Inactive,
                StatusAsInt = Status.Active,
                Tags = null, // Null array
                Scores = new List<int>(), // Empty list
                Metadata = null, // Null complex object
                Address = null, // Null complex object
                IsActive = false,
                NullableInt = null, // Null nullable int
                Price = 0m
            };

            ComplexEntity createdNull = await repository.CreateAsync(entityWithNulls);
            ComplexEntity retrievedNull = await repository.ReadByIdAsync(createdNull.Id);
            
            Console.WriteLine($"Null test successful: ID = {createdNull.Id}");

            // Test UpdateField operations with type conversion
            Console.WriteLine("\nTesting UpdateField with type conversion...");
            await TestUpdateFieldOperations(repository, created.Id);

            // Test query operations
            Console.WriteLine("\nTesting query operations...");
            await TestQueryOperations(repository);

            Console.WriteLine("\n=== ALL DATA TYPE CONVERTER TESTS PASSED ===");
        }

        private static async Task CreateTableAsync(SqliteRepository<ComplexEntity> repository)
        {
            await repository.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS complex_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    created_date TEXT NOT NULL,
                    updated_date TEXT,
                    unique_id TEXT NOT NULL,
                    duration TEXT NOT NULL,
                    status TEXT NOT NULL,
                    status_int INTEGER NOT NULL,
                    tags TEXT,
                    scores TEXT,
                    metadata TEXT,
                    address TEXT,
                    is_active INTEGER NOT NULL,
                    nullable_int INTEGER,
                    price REAL NOT NULL
                );");
        }

        private static Task VerifyDataIntegrity(ComplexEntity original, ComplexEntity retrieved)
        {
            bool success = true;

            // Basic types
            if (original.Name != retrieved.Name)
            {
                Console.WriteLine($"ERROR: Name mismatch. Expected: {original.Name}, Got: {retrieved.Name}");
                success = false;
            }

            // DateTime - allow small difference due to precision
            double timeDiff = Math.Abs((original.CreatedDate - retrieved.CreatedDate).TotalMilliseconds);
            if (timeDiff > 1)
            {
                Console.WriteLine($"ERROR: CreatedDate mismatch. Expected: {original.CreatedDate:O}, Got: {retrieved.CreatedDate:O}");
                success = false;
            }

            // DateTimeOffset
            if (original.UpdatedDate != retrieved.UpdatedDate)
            {
                Console.WriteLine($"ERROR: UpdatedDate mismatch. Expected: {original.UpdatedDate:O}, Got: {retrieved.UpdatedDate:O}");
                success = false;
            }

            // Guid
            if (original.UniqueId != retrieved.UniqueId)
            {
                Console.WriteLine($"ERROR: UniqueId mismatch. Expected: {original.UniqueId}, Got: {retrieved.UniqueId}");
                success = false;
            }

            // TimeSpan
            if (original.Duration != retrieved.Duration)
            {
                Console.WriteLine($"ERROR: Duration mismatch. Expected: {original.Duration}, Got: {retrieved.Duration}");
                success = false;
            }

            // Enums
            if (original.Status != retrieved.Status)
            {
                Console.WriteLine($"ERROR: Status mismatch. Expected: {original.Status}, Got: {retrieved.Status}");
                success = false;
            }

            if (original.StatusAsInt != retrieved.StatusAsInt)
            {
                Console.WriteLine($"ERROR: StatusAsInt mismatch. Expected: {original.StatusAsInt}, Got: {retrieved.StatusAsInt}");
                success = false;
            }

            // Arrays
            if (!ArraysEqual(original.Tags, retrieved.Tags))
            {
                Console.WriteLine($"ERROR: Tags mismatch. Expected: [{string.Join(", ", original.Tags ?? new string[0])}], Got: [{string.Join(", ", retrieved.Tags ?? new string[0])}]");
                success = false;
            }

            // Lists
            if (!ListsEqual(original.Scores, retrieved.Scores))
            {
                Console.WriteLine($"ERROR: Scores mismatch. Expected: [{string.Join(", ", original.Scores ?? new List<int>())}], Got: [{string.Join(", ", retrieved.Scores ?? new List<int>())}]");
                success = false;
            }

            // Other types
            if (original.IsActive != retrieved.IsActive)
            {
                Console.WriteLine($"ERROR: IsActive mismatch. Expected: {original.IsActive}, Got: {retrieved.IsActive}");
                success = false;
            }

            if (original.NullableInt != retrieved.NullableInt)
            {
                Console.WriteLine($"ERROR: NullableInt mismatch. Expected: {original.NullableInt}, Got: {retrieved.NullableInt}");
                success = false;
            }

            if (Math.Abs(original.Price - retrieved.Price) > 0.001m)
            {
                Console.WriteLine($"ERROR: Price mismatch. Expected: {original.Price}, Got: {retrieved.Price}");
                success = false;
            }

            if (success)
            {
                Console.WriteLine("✓ Data integrity verification passed!");
            }
            else
            {
                throw new Exception("Data integrity verification failed!");
            }
            
            return Task.CompletedTask;
        }

        private static async Task TestUpdateFieldOperations(SqliteRepository<ComplexEntity> repository, int entityId)
        {
            // Test updating DateTime field
            DateTime newDate = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);
            await repository.UpdateFieldAsync(e => e.Id == entityId, e => e.CreatedDate, newDate);

            // Test updating array field
            string[] newTags = new[] { "updated", "test", "array" };
            await repository.UpdateFieldAsync(e => e.Id == entityId, e => e.Tags, newTags);

            // Test updating enum field
            await repository.UpdateFieldAsync(e => e.Id == entityId, e => e.Status, Status.Pending);

            // Verify updates
            ComplexEntity updated = await repository.ReadByIdAsync(entityId);
            if (updated.CreatedDate != newDate)
            {
                throw new Exception($"UpdateField DateTime failed. Expected: {newDate:O}, Got: {updated.CreatedDate:O}");
            }

            if (!ArraysEqual(updated.Tags, newTags))
            {
                throw new Exception($"UpdateField array failed. Expected: [{string.Join(", ", newTags)}], Got: [{string.Join(", ", updated.Tags ?? new string[0])}]");
            }

            if (updated.Status != Status.Pending)
            {
                throw new Exception($"UpdateField enum failed. Expected: Pending, Got: {updated.Status}");
            }

            Console.WriteLine("✓ UpdateField operations with type conversion passed!");
        }

        private static Task TestQueryOperations(SqliteRepository<ComplexEntity> repository)
        {
            // Test querying by various data types
            List<ComplexEntity> results = repository.Query()
                .Where(e => e.Status == Status.Pending)
                .Execute()
                .ToList();

            if (results.Count == 0)
            {
                throw new Exception("Query by enum failed - no results found");
            }

            Console.WriteLine("✓ Query operations with type conversion passed!");
            
            return Task.CompletedTask;
        }

        private static bool ArraysEqual<T>(T[] arr1, T[] arr2)
        {
            if (arr1 == null && arr2 == null) return true;
            if (arr1 == null || arr2 == null) return false;
            if (arr1.Length != arr2.Length) return false;
            
            return arr1.SequenceEqual(arr2);
        }

        private static bool ListsEqual<T>(List<T> list1, List<T> list2)
        {
            if (list1 == null && list2 == null) return true;
            if (list1 == null || list2 == null) return false;
            if (list1.Count != list2.Count) return false;
            
            return list1.SequenceEqual(list2);
        }
    }
}