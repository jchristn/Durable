namespace Test.Shared
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Test suite for validating batch insert functionality and performance.
    /// </summary>
    public class BatchInsertTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchInsertTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public BatchInsertTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests batch insert with multiple records.
        /// </summary>
        [Fact]
        public async Task CanBatchInsertMultipleRecords()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int recordCount = 100;
            Person[] people = new Person[recordCount];

            for (int i = 0; i < recordCount; i++)
            {
                people[i] = new Person
                {
                    FirstName = $"FirstName{i}",
                    LastName = $"LastName{i}",
                    Age = 20 + (i % 50),
                    Email = $"person{i}@example.com",
                    Salary = 50000m + (i * 100),
                    Department = $"Dept{i % 5}"
                };
            }

            Stopwatch sw = Stopwatch.StartNew();
            Person[] inserted = (await repository.CreateManyAsync(people)).ToArray();
            sw.Stop();

            Assert.Equal(recordCount, inserted.Length);
            Assert.All(inserted, p => Assert.True(p.Id > 0));

            int count = await repository.CountAsync();
            Assert.Equal(recordCount, count);

            Console.WriteLine($"     Inserted {recordCount} records in {sw.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Tests batch delete functionality.
        /// </summary>
        [Fact]
        public async Task CanBatchDeleteRecords()
        {
            IRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            int recordCount = 50;
            Person[] people = new Person[recordCount];

            for (int i = 0; i < recordCount; i++)
            {
                people[i] = new Person
                {
                    FirstName = $"FirstName{i}",
                    LastName = $"LastName{i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com",
                    Salary = 50000m,
                    Department = "Engineering"
                };
            }

            await repository.CreateManyAsync(people);

            int deletedCount = await repository.DeleteManyAsync(p => p.Department == "Engineering");

            Assert.Equal(recordCount, deletedCount);

            int remainingCount = await repository.CountAsync();
            Assert.Equal(0, remainingCount);

            Console.WriteLine($"     Deleted {deletedCount} records");
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
