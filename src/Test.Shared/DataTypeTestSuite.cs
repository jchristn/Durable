namespace Test.Shared
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Test suite for validating precise data type handling across all database providers.
    /// Tests ensure exact matches for enums, DateTime precision, booleans, strings, decimals, and other types.
    /// </summary>
    public class DataTypeTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="DataTypeTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public DataTypeTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests DateTime storage and retrieval with precision validation.
        /// </summary>
        [Fact]
        public async Task DateTimeValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<ComplexEntity> repository = _Provider.CreateRepository<ComplexEntity>();
            await repository.ExecuteSqlAsync("DELETE FROM complex_entities");

            DateTime testDate = new DateTime(2024, 3, 15, 14, 30, 45, DateTimeKind.Utc);

            ComplexEntity entity = new ComplexEntity
            {
                Name = "DateTimeTest",
                CreatedDate = testDate,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = true,
                Price = 100m
            };

            ComplexEntity created = await repository.CreateAsync(entity);
            Assert.NotNull(created);

            ComplexEntity? retrieved = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(retrieved);

            Assert.True(
                ValidationHelpers.AreDateTimesEqual(testDate, retrieved.CreatedDate, 1000),
                $"Expected: {ValidationHelpers.FormatDateTime(testDate)}, Actual: {ValidationHelpers.FormatDateTime(retrieved.CreatedDate)}"
            );

            Console.WriteLine($"     DateTime - Expected: {ValidationHelpers.FormatDateTime(testDate)}, Actual: {ValidationHelpers.FormatDateTime(retrieved.CreatedDate)}");
        }

        /// <summary>
        /// Tests nullable DateTime storage and retrieval.
        /// </summary>
        [Fact]
        public async Task NullableDateTimeValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<ComplexEntity> repository = _Provider.CreateRepository<ComplexEntity>();
            await repository.ExecuteSqlAsync("DELETE FROM complex_entities");

            DateTimeOffset? testDateTimeOffset = new DateTimeOffset(2024, 3, 15, 14, 30, 45, TimeSpan.Zero);

            ComplexEntity entity = new ComplexEntity
            {
                Name = "NullableDateTimeTest",
                CreatedDate = DateTime.UtcNow,
                UpdatedDate = testDateTimeOffset,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = true,
                Price = 100m
            };

            ComplexEntity created = await repository.CreateAsync(entity);
            Assert.NotNull(created);

            ComplexEntity? retrieved = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(retrieved);

            Assert.True(
                ValidationHelpers.AreDateTimeOffsetsEqual(testDateTimeOffset, retrieved.UpdatedDate, 1000),
                $"Expected: {ValidationHelpers.FormatDateTimeOffset(testDateTimeOffset)}, Actual: {ValidationHelpers.FormatDateTimeOffset(retrieved.UpdatedDate)}"
            );

            Console.WriteLine($"     DateTimeOffset - Expected: {ValidationHelpers.FormatDateTimeOffset(testDateTimeOffset)}, Actual: {ValidationHelpers.FormatDateTimeOffset(retrieved.UpdatedDate)}");

            ComplexEntity entityWithNull = new ComplexEntity
            {
                Name = "NullDateTimeTest",
                CreatedDate = DateTime.UtcNow,
                UpdatedDate = null,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = true,
                Price = 100m
            };

            ComplexEntity createdWithNull = await repository.CreateAsync(entityWithNull);
            ComplexEntity? retrievedWithNull = await repository.ReadByIdAsync(createdWithNull.Id);

            Assert.NotNull(retrievedWithNull);
            Assert.Null(retrievedWithNull.UpdatedDate);

            Console.WriteLine($"     Nullable DateTimeOffset - Expected: NULL, Actual: NULL");
        }

        /// <summary>
        /// Tests enum storage as string (default) and as integer.
        /// </summary>
        [Fact]
        public async Task EnumValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<ComplexEntity> repository = _Provider.CreateRepository<ComplexEntity>();
            await repository.ExecuteSqlAsync("DELETE FROM complex_entities");

            ComplexEntity entity = new ComplexEntity
            {
                Name = "EnumTest",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Pending,
                StatusAsInt = Status.Inactive,
                IsActive = true,
                Price = 100m
            };

            ComplexEntity created = await repository.CreateAsync(entity);
            Assert.NotNull(created);

            ComplexEntity? retrieved = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(retrieved);

            Assert.True(
                ValidationHelpers.AreEnumsEqual(Status.Pending, retrieved.Status),
                $"Expected: {Status.Pending}, Actual: {retrieved.Status}"
            );

            Assert.True(
                ValidationHelpers.AreEnumsEqual(Status.Inactive, retrieved.StatusAsInt),
                $"Expected: {Status.Inactive}, Actual: {retrieved.StatusAsInt}"
            );

            Console.WriteLine($"     Enum (String) - Expected: {Status.Pending}, Actual: {retrieved.Status}");
            Console.WriteLine($"     Enum (Int) - Expected: {Status.Inactive}, Actual: {retrieved.StatusAsInt}");

            Status[] allStatuses = new[] { Status.Active, Status.Inactive, Status.Pending };
            foreach (Status status in allStatuses)
            {
                ComplexEntity statusEntity = new ComplexEntity
                {
                    Name = $"EnumTest_{status}",
                    CreatedDate = DateTime.UtcNow,
                    UniqueId = Guid.NewGuid(),
                    Duration = TimeSpan.Zero,
                    Status = status,
                    StatusAsInt = status,
                    IsActive = true,
                    Price = 100m
                };

                ComplexEntity createdStatus = await repository.CreateAsync(statusEntity);
                ComplexEntity? retrievedStatus = await repository.ReadByIdAsync(createdStatus.Id);

                Assert.NotNull(retrievedStatus);
                Assert.True(ValidationHelpers.AreEnumsEqual(status, retrievedStatus.Status));
                Assert.True(ValidationHelpers.AreEnumsEqual(status, retrievedStatus.StatusAsInt));
            }
        }

        /// <summary>
        /// Tests boolean value storage and retrieval.
        /// </summary>
        [Fact]
        public async Task BooleanValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<ComplexEntity> repository = _Provider.CreateRepository<ComplexEntity>();
            await repository.ExecuteSqlAsync("DELETE FROM complex_entities");

            ComplexEntity entityTrue = new ComplexEntity
            {
                Name = "BooleanTestTrue",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = true,
                Price = 100m
            };

            ComplexEntity createdTrue = await repository.CreateAsync(entityTrue);
            ComplexEntity? retrievedTrue = await repository.ReadByIdAsync(createdTrue.Id);

            Assert.NotNull(retrievedTrue);
            Assert.True(
                ValidationHelpers.AreBooleansEqual(true, retrievedTrue.IsActive),
                $"Expected: true, Actual: {retrievedTrue.IsActive}"
            );

            Console.WriteLine($"     Boolean - Expected: true, Actual: {retrievedTrue.IsActive}");

            ComplexEntity entityFalse = new ComplexEntity
            {
                Name = "BooleanTestFalse",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = false,
                Price = 100m
            };

            ComplexEntity createdFalse = await repository.CreateAsync(entityFalse);
            ComplexEntity? retrievedFalse = await repository.ReadByIdAsync(createdFalse.Id);

            Assert.NotNull(retrievedFalse);
            Assert.True(
                ValidationHelpers.AreBooleansEqual(false, retrievedFalse.IsActive),
                $"Expected: false, Actual: {retrievedFalse.IsActive}"
            );

            Console.WriteLine($"     Boolean - Expected: false, Actual: {retrievedFalse.IsActive}");
        }

        /// <summary>
        /// Tests string value storage and retrieval.
        /// </summary>
        [Fact]
        public async Task StringValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            string[] testStrings = new[]
            {
                "Simple String",
                "String with 'quotes'",
                "String with \"double quotes\"",
                "String with\nnewline",
                "String with\ttab",
                "Unicode: 你好世界 🌍",
                ""
            };

            foreach (string testString in testStrings)
            {
                Person person = new Person
                {
                    FirstName = testString,
                    LastName = "Test",
                    Age = 30,
                    Email = "test@example.com",
                    Salary = 50000m,
                    Department = "Test"
                };

                Person created = await repository.CreateAsync(person);
                Person? retrieved = await repository.ReadByIdAsync(created.Id);

                Assert.NotNull(retrieved);
                Assert.True(
                    ValidationHelpers.AreStringsEqual(testString, retrieved.FirstName),
                    $"Expected: '{testString}', Actual: '{retrieved.FirstName}'"
                );

                Console.WriteLine($"     String - Expected: '{testString}', Actual: '{retrieved.FirstName}'");
            }
        }

        /// <summary>
        /// Tests decimal value storage and retrieval with precision.
        /// </summary>
        [Fact]
        public async Task DecimalValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            decimal[] testDecimals = new[]
            {
                0m,
                0.01m,
                0.99m,
                1.00m,
                1234.56m,
                9999999.99m,
                -1234.56m
            };

            foreach (decimal testDecimal in testDecimals)
            {
                Person person = new Person
                {
                    FirstName = "DecimalTest",
                    LastName = "Test",
                    Age = 30,
                    Email = "test@example.com",
                    Salary = testDecimal,
                    Department = "Test"
                };

                Person created = await repository.CreateAsync(person);
                Person? retrieved = await repository.ReadByIdAsync(created.Id);

                Assert.NotNull(retrieved);
                Assert.True(
                    ValidationHelpers.AreDecimalsEqual(testDecimal, retrieved.Salary),
                    $"Expected: {testDecimal}, Actual: {retrieved.Salary}"
                );

                Console.WriteLine($"     Decimal - Expected: {testDecimal}, Actual: {retrieved.Salary}");
            }
        }

        /// <summary>
        /// Tests integer value storage and retrieval.
        /// </summary>
        [Fact]
        public async Task IntegerValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int[] testIntegers = new[] { 0, 1, 100, 999, -1, -100, int.MaxValue, int.MinValue };

            foreach (int testInteger in testIntegers)
            {
                Person person = new Person
                {
                    FirstName = "IntegerTest",
                    LastName = "Test",
                    Age = testInteger < 0 ? 0 : (testInteger > 150 ? 150 : testInteger),
                    Email = "test@example.com",
                    Salary = 50000m,
                    Department = "Test"
                };

                Person created = await repository.CreateAsync(person);
                Person? retrieved = await repository.ReadByIdAsync(created.Id);

                Assert.NotNull(retrieved);
                int expectedAge = testInteger < 0 ? 0 : (testInteger > 150 ? 150 : testInteger);
                Assert.True(
                    ValidationHelpers.AreIntegersEqual(expectedAge, retrieved.Age),
                    $"Expected: {expectedAge}, Actual: {retrieved.Age}"
                );

                Console.WriteLine($"     Integer - Expected: {expectedAge}, Actual: {retrieved.Age}");
            }
        }

        /// <summary>
        /// Tests nullable integer value storage and retrieval.
        /// </summary>
        [Fact]
        public async Task NullableIntegerValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<ComplexEntity> repository = _Provider.CreateRepository<ComplexEntity>();
            await repository.ExecuteSqlAsync("DELETE FROM complex_entities");

            ComplexEntity entityWithValue = new ComplexEntity
            {
                Name = "NullableIntTest",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = true,
                Price = 100m,
                NullableInt = 42
            };

            ComplexEntity createdWithValue = await repository.CreateAsync(entityWithValue);
            ComplexEntity? retrievedWithValue = await repository.ReadByIdAsync(createdWithValue.Id);

            Assert.NotNull(retrievedWithValue);
            Assert.True(
                ValidationHelpers.AreIntegersEqual(42, retrievedWithValue.NullableInt),
                $"Expected: 42, Actual: {retrievedWithValue.NullableInt}"
            );

            Console.WriteLine($"     Nullable Int - Expected: 42, Actual: {retrievedWithValue.NullableInt}");

            ComplexEntity entityWithNull = new ComplexEntity
            {
                Name = "NullableIntTestNull",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = true,
                Price = 100m,
                NullableInt = null
            };

            ComplexEntity createdWithNull = await repository.CreateAsync(entityWithNull);
            ComplexEntity? retrievedWithNull = await repository.ReadByIdAsync(createdWithNull.Id);

            Assert.NotNull(retrievedWithNull);
            Assert.Null(retrievedWithNull.NullableInt);

            Console.WriteLine($"     Nullable Int - Expected: NULL, Actual: NULL");
        }

        /// <summary>
        /// Tests Guid value storage and retrieval.
        /// </summary>
        [Fact]
        public async Task GuidValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<ComplexEntity> repository = _Provider.CreateRepository<ComplexEntity>();
            await repository.ExecuteSqlAsync("DELETE FROM complex_entities");

            Guid testGuid = Guid.NewGuid();

            ComplexEntity entity = new ComplexEntity
            {
                Name = "GuidTest",
                CreatedDate = DateTime.UtcNow,
                UniqueId = testGuid,
                Duration = TimeSpan.Zero,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = true,
                Price = 100m
            };

            ComplexEntity created = await repository.CreateAsync(entity);
            ComplexEntity? retrieved = await repository.ReadByIdAsync(created.Id);

            Assert.NotNull(retrieved);
            Assert.True(
                ValidationHelpers.AreGuidsEqual(testGuid, retrieved.UniqueId),
                $"Expected: {testGuid}, Actual: {retrieved.UniqueId}"
            );

            Console.WriteLine($"     Guid - Expected: {testGuid}, Actual: {retrieved.UniqueId}");
        }

        /// <summary>
        /// Tests TimeSpan value storage and retrieval.
        /// </summary>
        [Fact]
        public async Task TimeSpanValuesAreStoredAndRetrievedCorrectly()
        {
            IRepository<ComplexEntity> repository = _Provider.CreateRepository<ComplexEntity>();
            await repository.ExecuteSqlAsync("DELETE FROM complex_entities");

            TimeSpan testTimeSpan = new TimeSpan(2, 14, 30, 45);

            ComplexEntity entity = new ComplexEntity
            {
                Name = "TimeSpanTest",
                CreatedDate = DateTime.UtcNow,
                UniqueId = Guid.NewGuid(),
                Duration = testTimeSpan,
                Status = Status.Active,
                StatusAsInt = Status.Active,
                IsActive = true,
                Price = 100m
            };

            ComplexEntity created = await repository.CreateAsync(entity);
            ComplexEntity? retrieved = await repository.ReadByIdAsync(created.Id);

            Assert.NotNull(retrieved);
            Assert.True(
                ValidationHelpers.AreTimeSpansEqual(testTimeSpan, retrieved.Duration),
                $"Expected: {testTimeSpan}, Actual: {retrieved.Duration}"
            );

            Console.WriteLine($"     TimeSpan - Expected: {testTimeSpan}, Actual: {retrieved.Duration}");
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
