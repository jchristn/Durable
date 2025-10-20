namespace Test.Shared
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Integration test suite covering basic CRUD operations and core repository functionality.
    /// These tests are executed identically across all database providers.
    /// </summary>
    public class IntegrationTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="IntegrationTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public IntegrationTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests that a repository can be created successfully.
        /// </summary>
        [Fact]
        public void CanCreateRepository()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();

            Assert.NotNull(repository);
            Assert.IsAssignableFrom<IRepository<Person>>(repository);
        }

        /// <summary>
        /// Tests basic database connectivity.
        /// </summary>
        [Fact]
        public async Task CanConnectToDatabase()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();

            int rowsAffected = await repository.ExecuteSqlAsync(
                "DELETE FROM people WHERE email = 'connectivity-test@example.com'"
            );

            Assert.True(rowsAffected >= 0);
        }

        /// <summary>
        /// Tests complete CRUD operation lifecycle.
        /// </summary>
        [Fact]
        public async Task CanPerformCrudOperations()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person person = new Person
            {
                FirstName = "John",
                LastName = "Doe",
                Age = 30,
                Email = "john.doe@example.com",
                Salary = 75000.50m,
                Department = "Engineering"
            };

            Person created = await repository.CreateAsync(person);

            Assert.NotNull(created);
            Assert.True(created.Id > 0);
            Assert.True(ValidationHelpers.AreStringsEqual("John", created.FirstName));
            Assert.True(ValidationHelpers.AreStringsEqual("Doe", created.LastName));
            Assert.True(ValidationHelpers.AreIntegersEqual(30, created.Age));
            Assert.True(ValidationHelpers.AreStringsEqual("john.doe@example.com", created.Email));
            Assert.True(ValidationHelpers.AreDecimalsEqual(75000.50m, created.Salary));
            Assert.True(ValidationHelpers.AreStringsEqual("Engineering", created.Department));

            int createdId = created.Id;

            Person? retrieved = await repository.ReadByIdAsync(createdId);

            Assert.NotNull(retrieved);
            Assert.True(ValidationHelpers.AreIntegersEqual(createdId, retrieved.Id));
            Assert.True(ValidationHelpers.AreStringsEqual("John", retrieved.FirstName));
            Assert.True(ValidationHelpers.AreStringsEqual("Doe", retrieved.LastName));
            Assert.True(ValidationHelpers.AreIntegersEqual(30, retrieved.Age));

            retrieved.Age = 31;
            retrieved.Salary = 80000.00m;

            Person updated = await repository.UpdateAsync(retrieved);

            Assert.NotNull(updated);
            Assert.True(ValidationHelpers.AreIntegersEqual(createdId, updated.Id));
            Assert.True(ValidationHelpers.AreIntegersEqual(31, updated.Age));
            Assert.True(ValidationHelpers.AreDecimalsEqual(80000.00m, updated.Salary));

            bool deleteResult = await repository.DeleteAsync(updated);

            Assert.True(deleteResult);

            Person? deletedPerson = await repository.ReadByIdAsync(createdId);
            Assert.Null(deletedPerson);
        }

        /// <summary>
        /// Tests querying with WHERE conditions.
        /// </summary>
        [Fact]
        public async Task CanQueryWithWhereConditions()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] testPeople = new[]
            {
                new Person { FirstName = "Alice", LastName = "Smith", Age = 25, Email = "alice@example.com", Salary = 60000m, Department = "Engineering" },
                new Person { FirstName = "Bob", LastName = "Jones", Age = 35, Email = "bob@example.com", Salary = 70000m, Department = "Marketing" },
                new Person { FirstName = "Charlie", LastName = "Brown", Age = 45, Email = "charlie@example.com", Salary = 80000m, Department = "Engineering" }
            };

            await repository.CreateManyAsync(testPeople);

            Person[] engineers = (await repository.Query()
                .Where(p => p.Department == "Engineering")
                .ExecuteAsync())
                .ToArray();

            Assert.Equal(2, engineers.Length);
            Assert.All(engineers, p => Assert.True(ValidationHelpers.AreStringsEqual("Engineering", p.Department)));

            Person[] ageOver30 = (await repository.Query()
                .Where(p => p.Age > 30)
                .ExecuteAsync())
                .ToArray();

            Assert.Equal(2, ageOver30.Length);
            Assert.All(ageOver30, p => Assert.True(p.Age > 30));
        }

        /// <summary>
        /// Tests ordering functionality.
        /// </summary>
        [Fact]
        public async Task CanOrderResults()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] testPeople = new[]
            {
                new Person { FirstName = "Charlie", LastName = "Brown", Age = 45, Email = "charlie@example.com", Salary = 80000m, Department = "Engineering" },
                new Person { FirstName = "Alice", LastName = "Smith", Age = 25, Email = "alice@example.com", Salary = 60000m, Department = "Engineering" },
                new Person { FirstName = "Bob", LastName = "Jones", Age = 35, Email = "bob@example.com", Salary = 70000m, Department = "Marketing" }
            };

            await repository.CreateManyAsync(testPeople);

            Person[] orderedByAge = (await repository.Query()
                .OrderBy(p => p.Age)
                .ExecuteAsync())
                .ToArray();

            Assert.Equal(3, orderedByAge.Length);
            Assert.True(ValidationHelpers.AreIntegersEqual(25, orderedByAge[0].Age));
            Assert.True(ValidationHelpers.AreIntegersEqual(35, orderedByAge[1].Age));
            Assert.True(ValidationHelpers.AreIntegersEqual(45, orderedByAge[2].Age));

            Person[] orderedBySalaryDesc = (await repository.Query()
                .OrderByDescending(p => p.Salary)
                .ExecuteAsync())
                .ToArray();

            Assert.Equal(3, orderedBySalaryDesc.Length);
            Assert.True(ValidationHelpers.AreDecimalsEqual(80000m, orderedBySalaryDesc[0].Salary));
            Assert.True(ValidationHelpers.AreDecimalsEqual(70000m, orderedBySalaryDesc[1].Salary));
            Assert.True(ValidationHelpers.AreDecimalsEqual(60000m, orderedBySalaryDesc[2].Salary));
        }

        /// <summary>
        /// Tests pagination with Skip and Take.
        /// </summary>
        [Fact]
        public async Task CanPaginateResults()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] testPeople = new Person[10];
            for (int i = 0; i < 10; i++)
            {
                testPeople[i] = new Person
                {
                    FirstName = $"Person{i}",
                    LastName = $"Test{i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com",
                    Salary = 50000m + (i * 1000),
                    Department = "Test"
                };
            }

            await repository.CreateManyAsync(testPeople);

            Person[] page1 = (await repository.Query()
                .OrderBy(p => p.Age)
                .Take(3)
                .ExecuteAsync())
                .ToArray();

            Assert.Equal(3, page1.Length);

            Person[] page2 = (await repository.Query()
                .OrderBy(p => p.Age)
                .Skip(3)
                .Take(3)
                .ExecuteAsync())
                .ToArray();

            Assert.Equal(3, page2.Length);
            Assert.NotEqual(page1[0].Id, page2[0].Id);
        }

        /// <summary>
        /// Tests Count functionality.
        /// </summary>
        [Fact]
        public async Task CanCountRecords()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int initialCount = await repository.CountAsync();
            Assert.Equal(0, initialCount);

            Person[] testPeople = new[]
            {
                new Person { FirstName = "Alice", LastName = "Smith", Age = 25, Email = "alice@example.com", Salary = 60000m, Department = "Engineering" },
                new Person { FirstName = "Bob", LastName = "Jones", Age = 35, Email = "bob@example.com", Salary = 70000m, Department = "Marketing" },
                new Person { FirstName = "Charlie", LastName = "Brown", Age = 45, Email = "charlie@example.com", Salary = 80000m, Department = "Engineering" }
            };

            await repository.CreateManyAsync(testPeople);

            int totalCount = await repository.CountAsync();
            Assert.Equal(3, totalCount);

            int engineeringCount = await repository.CountAsync(p => p.Department == "Engineering");
            Assert.Equal(2, engineeringCount);
        }

        /// <summary>
        /// Tests Upsert functionality.
        /// </summary>
        [Fact]
        public async Task CanUpsertRecords()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person person = new Person
            {
                FirstName = "John",
                LastName = "Doe",
                Age = 30,
                Email = "john.doe@example.com",
                Salary = 75000m,
                Department = "Engineering"
            };

            Person created = await repository.CreateAsync(person);
            int originalId = created.Id;

            created.Age = 31;
            created.Salary = 80000m;

            Person upserted = await repository.UpsertAsync(created);

            Assert.True(ValidationHelpers.AreIntegersEqual(originalId, upserted.Id));
            Assert.True(ValidationHelpers.AreIntegersEqual(31, upserted.Age));
            Assert.True(ValidationHelpers.AreDecimalsEqual(80000m, upserted.Salary));

            int count = await repository.CountAsync();
            Assert.Equal(1, count);
        }

        /// <summary>
        /// Tests bulk delete functionality.
        /// </summary>
        [Fact]
        public async Task CanBulkDelete()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] testPeople = new[]
            {
                new Person { FirstName = "Alice", LastName = "Smith", Age = 25, Email = "alice@example.com", Salary = 60000m, Department = "Engineering" },
                new Person { FirstName = "Bob", LastName = "Jones", Age = 35, Email = "bob@example.com", Salary = 70000m, Department = "Marketing" },
                new Person { FirstName = "Charlie", LastName = "Brown", Age = 45, Email = "charlie@example.com", Salary = 80000m, Department = "Engineering" }
            };

            await repository.CreateManyAsync(testPeople);

            int deletedCount = await repository.DeleteManyAsync(p => p.Department == "Engineering");

            Assert.Equal(2, deletedCount);

            int remainingCount = await repository.CountAsync();
            Assert.Equal(1, remainingCount);
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
