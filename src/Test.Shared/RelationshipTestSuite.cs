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
    /// Coverage for asynchronous streaming enumeration over the repository and query builder.
    /// Executed identically across all database providers. Many-to-many navigation loading is covered
    /// separately by <see cref="ManyToManyTestSuite"/> (SQLite only, pending non-SQLite join-builder fixes).
    /// </summary>
    public class RelationshipTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="RelationshipTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public RelationshipTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// ReadManyAsync streams matching entities as an async sequence.
        /// </summary>
        [Fact]
        public async Task ReadManyAsyncStreamsResults()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] people = new[]
            {
                new Person { FirstName = "Stream", LastName = "One", Age = 30, Email = "s1@example.com", Salary = 50000m, Department = "Stream" },
                new Person { FirstName = "Stream", LastName = "Two", Age = 31, Email = "s2@example.com", Salary = 51000m, Department = "Stream" },
                new Person { FirstName = "Stream", LastName = "Three", Age = 32, Email = "s3@example.com", Salary = 52000m, Department = "Stream" }
            };
            await repository.CreateManyAsync(people);

            int streamed = 0;
            await foreach (Person person in repository.ReadManyAsync(p => p.Department == "Stream"))
            {
                Assert.Equal("Stream", person.Department);
                streamed++;
            }

            Assert.Equal(3, streamed);
        }

        /// <summary>
        /// The query builder exposes an async streaming enumerator.
        /// </summary>
        [Fact]
        public async Task QueryExecuteAsyncEnumerableStreamsResults()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] people = new[]
            {
                new Person { FirstName = "Q", LastName = "One", Age = 40, Email = "q1@example.com", Salary = 60000m, Department = "Q" },
                new Person { FirstName = "Q", LastName = "Two", Age = 41, Email = "q2@example.com", Salary = 61000m, Department = "Q" }
            };
            await repository.CreateManyAsync(people);

            int streamed = 0;
            await foreach (Person person in repository.Query().Where(p => p.Department == "Q").ExecuteAsyncEnumerable())
            {
                Assert.Equal("Q", person.Department);
                streamed++;
            }

            Assert.Equal(2, streamed);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
