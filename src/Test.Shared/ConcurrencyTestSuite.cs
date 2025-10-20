namespace Test.Shared
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Test suite for validating concurrency control and version column functionality.
    /// </summary>
    public class ConcurrencyTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        public ConcurrencyTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests version column increment on updates.
        /// </summary>
        [Fact]
        public async Task VersionColumnIncrementsOnUpdate()
        {
            IRepository<Author> repository = _Provider.CreateRepository<Author>();
            await repository.ExecuteSqlAsync("DELETE FROM authors");

            Author author = new Author
            {
                Name = "Test Author"
            };

            Author created = await repository.CreateAsync(author);
            Assert.NotNull(created);

            int initialVersion = created.Version;
            Console.WriteLine($"     Initial version: {initialVersion}");

            created.Name = "Updated Author";
            Author updated = await repository.UpdateAsync(created);

            Assert.True(updated.Version > initialVersion, $"Expected version > {initialVersion}, got {updated.Version}");
            Console.WriteLine($"     Updated version: {updated.Version}");
        }

        /// <summary>
        /// Tests that concurrent updates throw concurrency exception.
        /// </summary>
        [Fact]
        public async Task ConcurrentUpdatesThrowException()
        {
            IRepository<Author> repository = _Provider.CreateRepository<Author>();
            await repository.ExecuteSqlAsync("DELETE FROM authors");

            Author author = new Author
            {
                Name = "Test Author"
            };

            Author created = await repository.CreateAsync(author);

            Author? firstCopy = await repository.ReadByIdAsync(created.Id);
            Author? secondCopy = await repository.ReadByIdAsync(created.Id);

            Assert.NotNull(firstCopy);
            Assert.NotNull(secondCopy);

            firstCopy.Name = "First Update";
            await repository.UpdateAsync(firstCopy);

            secondCopy.Name = "Second Update";

            await Assert.ThrowsAsync<OptimisticConcurrencyException>(async () =>
            {
                await repository.UpdateAsync(secondCopy);
            });

            Console.WriteLine($"     Concurrency exception thrown as expected");
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
