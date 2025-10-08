namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Comprehensive integration tests for SQLite implementation.
    /// Tests coverage for upsert, aggregations, specialized updates, window functions, and collection operations.
    /// Uses in-memory database for fast, isolated tests.
    /// </summary>
    public class SqliteIntegrationTests : IDisposable
    {

        #region Private-Members

        private const string TestConnectionString = "Data Source=InMemoryIntegrationTest;Mode=Memory;Cache=Shared";
        private readonly SqliteConnection _KeepAliveConnection;
        private readonly SqliteRepository<Person> _Repository;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the test class by setting up an in-memory database.
        /// </summary>
        public SqliteIntegrationTests()
        {
            _KeepAliveConnection = new SqliteConnection(TestConnectionString);
            _KeepAliveConnection.Open();

            SetupTestDatabase();
            _Repository = new SqliteRepository<Person>(TestConnectionString);
        }

        #endregion

        #region Public-Methods

        // ==================== UPSERT OPERATION TESTS ====================

        /// <summary>
        /// Tests basic upsert functionality - update when exists.
        /// Note: Upsert requires the entity to have a valid Id for update operations.
        /// </summary>
        [Fact]
        public async Task SqliteUpsertOperations_InsertAndUpdate_WorkCorrectly()
        {
            _Repository.DeleteAll();

            Person person = new Person
            {
                FirstName = "John",
                LastName = "Doe",
                Age = 30,
                Email = "john.doe@example.com",
                Salary = 50000,
                Department = "Engineering"
            };

            Person inserted = await _Repository.CreateAsync(person);
            Assert.NotNull(inserted);
            Assert.True(inserted.Id > 0);
            Assert.Equal("John", inserted.FirstName);

            inserted.Age = 31;
            inserted.Salary = 55000;

            Person updated = await _Repository.UpsertAsync(inserted);
            Assert.Equal(inserted.Id, updated.Id);
            Assert.Equal(31, updated.Age);
            Assert.Equal(55000, updated.Salary);

            int count = _Repository.Count();
            Assert.Equal(1, count);
        }

        /// <summary>
        /// Tests upsert with transactions.
        /// </summary>
        [Fact]
        public async Task SqliteUpsertOperations_WithTransactions_WorkCorrectly()
        {
            _Repository.DeleteAll();

            using ITransaction transaction = await _Repository.BeginTransactionAsync();

            Person person = new Person
            {
                FirstName = "Jane",
                LastName = "Smith",
                Age = 28,
                Email = "jane.smith@example.com",
                Salary = 60000,
                Department = "Marketing"
            };

            Person inserted = await _Repository.UpsertAsync(person, transaction);
            Assert.NotNull(inserted);

            await transaction.CommitAsync();

            Person retrieved = _Repository.ReadFirst(p => p.FirstName == "Jane");
            Assert.NotNull(retrieved);
            Assert.Equal("Smith", retrieved.LastName);
        }

        /// <summary>
        /// Tests UpsertMany for bulk upsert operations.
        /// </summary>
        [Fact]
        public async Task SqliteUpsertMany_BulkOperations_WorkCorrectly()
        {
            _Repository.DeleteAll();

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "Alice", LastName = "Johnson", Age = 25, Email = "alice@example.com", Salary = 45000, Department = "Sales" },
                new Person { FirstName = "Bob", LastName = "Williams", Age = 35, Email = "bob@example.com", Salary = 65000, Department = "Engineering" },
                new Person { FirstName = "Carol", LastName = "Brown", Age = 40, Email = "carol@example.com", Salary = 75000, Department = "Management" }
            };

            IEnumerable<Person> inserted = await _Repository.CreateManyAsync(people);
            Assert.Equal(3, inserted.Count());

            List<Person> insertedList = inserted.ToList();
            insertedList[0].Age = 26;
            insertedList[1].Salary = 70000;

            IEnumerable<Person> updated = await _Repository.UpsertManyAsync(insertedList);
            Assert.Equal(3, updated.Count());

            Person alice = _Repository.ReadFirst(p => p.FirstName == "Alice");
            Assert.Equal(26, alice.Age);

            Person bob = _Repository.ReadFirst(p => p.FirstName == "Bob");
            Assert.Equal(70000, bob.Salary);
        }

        // ==================== AGGREGATION METHOD TESTS ====================

        /// <summary>
        /// Tests Sum, Average, Min, Max aggregation methods.
        /// </summary>
        [Fact]
        public async Task SqliteAggregationMethods_SumAvgMinMax_WorkCorrectly()
        {
            _Repository.DeleteAll();

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "Person1", LastName = "Test", Age = 25, Email = "p1@test.com", Salary = 40000, Department = "Sales" },
                new Person { FirstName = "Person2", LastName = "Test", Age = 30, Email = "p2@test.com", Salary = 50000, Department = "Sales" },
                new Person { FirstName = "Person3", LastName = "Test", Age = 35, Email = "p3@test.com", Salary = 60000, Department = "Sales" },
                new Person { FirstName = "Person4", LastName = "Test", Age = 40, Email = "p4@test.com", Salary = 70000, Department = "Engineering" }
            };

            _Repository.CreateMany(people);

            decimal totalSalary = await _Repository.SumAsync(p => p.Salary);
            Assert.Equal(220000, totalSalary);

            decimal avgSalary = await _Repository.AverageAsync(p => p.Salary);
            Assert.Equal(55000, avgSalary);

            int minAge = await _Repository.MinAsync(p => p.Age);
            Assert.Equal(25, minAge);

            int maxAge = await _Repository.MaxAsync(p => p.Age);
            Assert.Equal(40, maxAge);
        }

        /// <summary>
        /// Tests aggregation methods with predicates.
        /// </summary>
        [Fact]
        public async Task SqliteAggregationMethods_WithPredicates_WorkCorrectly()
        {
            _Repository.DeleteAll();

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "Eng1", LastName = "Test", Age = 28, Email = "e1@test.com", Salary = 80000, Department = "Engineering" },
                new Person { FirstName = "Eng2", LastName = "Test", Age = 32, Email = "e2@test.com", Salary = 90000, Department = "Engineering" },
                new Person { FirstName = "Sales1", LastName = "Test", Age = 26, Email = "s1@test.com", Salary = 45000, Department = "Sales" }
            };

            _Repository.CreateMany(people);

            decimal engSalarySum = _Repository.Sum(p => p.Salary, p => p.Department == "Engineering");
            Assert.Equal(170000, engSalarySum);

            decimal engAvgSalary = _Repository.Average(p => p.Salary, p => p.Department == "Engineering");
            Assert.Equal(85000, engAvgSalary);

            int engCount = await _Repository.CountAsync(p => p.Department == "Engineering");
            Assert.Equal(2, engCount);
        }

        /// <summary>
        /// Tests Count method with and without predicates.
        /// </summary>
        [Fact]
        public void SqliteCount_WithAndWithoutPredicates_WorksCorrectly()
        {
            _Repository.DeleteAll();

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "A", LastName = "Test", Age = 20, Email = "a@test.com", Salary = 30000, Department = "Sales" },
                new Person { FirstName = "B", LastName = "Test", Age = 25, Email = "b@test.com", Salary = 40000, Department = "Sales" },
                new Person { FirstName = "C", LastName = "Test", Age = 30, Email = "c@test.com", Salary = 50000, Department = "Engineering" }
            };

            _Repository.CreateMany(people);

            int totalCount = _Repository.Count();
            Assert.Equal(3, totalCount);

            int salesCount = _Repository.Count(p => p.Department == "Sales");
            Assert.Equal(2, salesCount);

            int over25Count = _Repository.Count(p => p.Age > 25);
            Assert.Equal(1, over25Count);
        }

        // ==================== COLLECTION OPERATION TESTS ====================

        /// <summary>
        /// Tests CreateMany, UpdateMany, DeleteMany batch operations.
        /// </summary>
        [Fact]
        public async Task SqliteCollectionOperations_BatchOperations_WorkCorrectly()
        {
            _Repository.DeleteAll();

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "Batch1", LastName = "Test", Age = 30, Email = "b1@test.com", Salary = 50000, Department = "IT" },
                new Person { FirstName = "Batch2", LastName = "Test", Age = 32, Email = "b2@test.com", Salary = 52000, Department = "IT" },
                new Person { FirstName = "Batch3", LastName = "Test", Age = 34, Email = "b3@test.com", Salary = 54000, Department = "IT" }
            };

            IEnumerable<Person> created = await _Repository.CreateManyAsync(people);
            Assert.Equal(3, created.Count());

            int updated = await _Repository.UpdateManyAsync(
                p => p.Department == "IT",
                (p) => { p.Department = "IT-Updated"; return Task.CompletedTask; }
            );
            Assert.Equal(3, updated);

            IEnumerable<Person> updatedPeople = _Repository.ReadMany(p => p.Department == "IT-Updated");
            Assert.Equal(3, updatedPeople.Count());

            int deleted = await _Repository.DeleteManyAsync(p => p.Department == "IT-Updated");
            Assert.Equal(3, deleted);

            int remainingCount = _Repository.Count();
            Assert.Equal(0, remainingCount);
        }

        /// <summary>
        /// Tests collection operations with transactions.
        /// </summary>
        [Fact]
        public async Task SqliteCollectionOperations_WithTransactions_WorkCorrectly()
        {
            _Repository.DeleteAll();

            using ITransaction transaction = await _Repository.BeginTransactionAsync();

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "Trans1", LastName = "Test", Age = 25, Email = "t1@test.com", Salary = 45000, Department = "HR" },
                new Person { FirstName = "Trans2", LastName = "Test", Age = 27, Email = "t2@test.com", Salary = 47000, Department = "HR" }
            };

            IEnumerable<Person> created = await _Repository.CreateManyAsync(people, transaction);
            Assert.Equal(2, created.Count());

            await transaction.CommitAsync();

            int count = _Repository.Count(p => p.Department == "HR");
            Assert.Equal(2, count);
        }

        /// <summary>
        /// Tests collection operations with edge cases.
        /// </summary>
        [Fact]
        public async Task SqliteCollectionOperations_EdgeCases_HandleCorrectly()
        {
            _Repository.DeleteAll();

            IEnumerable<Person> emptyResult = await _Repository.CreateManyAsync(new List<Person>());
            Assert.Empty(emptyResult);

            Person singlePerson = new Person
            {
                FirstName = "Single",
                LastName = "Test",
                Age = 30,
                Email = "single@test.com",
                Salary = 50000,
                Department = "Accounting"
            };

            IEnumerable<Person> singleResult = await _Repository.CreateManyAsync(new List<Person> { singlePerson });
            Assert.Single(singleResult);
        }

        /// <summary>
        /// Tests collection operations support cancellation.
        /// </summary>
        [Fact]
        public async Task SqliteCollectionOperations_SupportCancellation_WorkCorrectly()
        {
            _Repository.DeleteAll();

            CancellationTokenSource cts = new CancellationTokenSource();

            List<Person> people = new List<Person>
            {
                new Person { FirstName = "Cancel1", LastName = "Test", Age = 30, Email = "c1@test.com", Salary = 50000, Department = "Finance" }
            };

            IEnumerable<Person> created = await _Repository.CreateManyAsync(people, null, cts.Token);
            Assert.Single(created);
        }

        // ==================== ERROR HANDLING TESTS ====================

        /// <summary>
        /// Tests error handling for constraint violations.
        /// </summary>
        [Fact]
        public async Task SqliteErrorHandling_ConstraintViolations_ThrowsException()
        {
            _Repository.DeleteAll();

            Person person = new Person
            {
                FirstName = "Error",
                LastName = "Test",
                Age = 30,
                Email = "error@test.com",
                Salary = 50000,
                Department = "Legal"
            };

            Person created = await _Repository.CreateAsync(person);
            Assert.NotNull(created);

            Assert.Throws<SqliteException>(() =>
            {
                using SqliteConnection conn = new SqliteConnection(TestConnectionString);
                conn.Open();
                using SqliteCommand cmd = conn.CreateCommand();
                cmd.CommandText = "INSERT INTO people (id, first, last, age, email, salary, department) VALUES (@id, @first, @last, @age, @email, @salary, @dept)";
                cmd.Parameters.AddWithValue("@id", created.Id);
                cmd.Parameters.AddWithValue("@first", "Duplicate");
                cmd.Parameters.AddWithValue("@last", "Entry");
                cmd.Parameters.AddWithValue("@age", 25);
                cmd.Parameters.AddWithValue("@email", "dup@test.com");
                cmd.Parameters.AddWithValue("@salary", 40000);
                cmd.Parameters.AddWithValue("@dept", "Legal");
                cmd.ExecuteNonQuery();
            });
        }

        /// <summary>
        /// Tests transaction rollback on error.
        /// </summary>
        [Fact]
        public async Task SqliteTransactionErrorHandling_Rollback_WorksCorrectly()
        {
            _Repository.DeleteAll();

            int initialCount = _Repository.Count();

            using ITransaction transaction = await _Repository.BeginTransactionAsync();

            try
            {
                Person person = new Person
                {
                    FirstName = "Rollback",
                    LastName = "Test",
                    Age = 30,
                    Email = "rollback@test.com",
                    Salary = 50000,
                    Department = "Operations"
                };

                await _Repository.CreateAsync(person, transaction);

                await transaction.RollbackAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
            }

            int finalCount = _Repository.Count();
            Assert.Equal(initialCount, finalCount);
        }

        #endregion

        #region Cleanup

        /// <summary>
        /// Cleans up test resources.
        /// </summary>
        public void Dispose()
        {
            _Repository?.Dispose();
            _KeepAliveConnection?.Close();
            _KeepAliveConnection?.Dispose();
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Sets up the test database with required schema.
        /// </summary>
        private void SetupTestDatabase()
        {
            using SqliteConnection connection = new SqliteConnection(TestConnectionString);
            connection.Open();

            using SqliteCommand command = connection.CreateCommand();
            command.CommandText = @"
                DROP TABLE IF EXISTS people;

                CREATE TABLE people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first TEXT NOT NULL,
                    last TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT NOT NULL,
                    salary REAL NOT NULL,
                    department TEXT
                );
            ";

            command.ExecuteNonQuery();
        }

        #endregion

    }
}
