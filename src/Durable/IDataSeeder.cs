namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Interface for seeding initial/default data into database tables
    /// </summary>
    /// <typeparam name="T">The entity type to seed</typeparam>
    public interface IDataSeeder<T> where T : class, new()
    {
        /// <summary>
        /// Gets the seed data to insert into the database
        /// </summary>
        /// <returns>Collection of entities to insert</returns>
        IEnumerable<T> GetSeedData();

        /// <summary>
        /// Asynchronously gets the seed data to insert into the database
        /// </summary>
        /// <param name="cancellationToken">A cancellation token to cancel the operation</param>
        /// <returns>A task representing the asynchronous operation with a collection of entities to insert</returns>
        Task<IEnumerable<T>> GetSeedDataAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Determines whether seeding should occur based on the current state of the repository
        /// </summary>
        /// <param name="repository">The repository to check</param>
        /// <returns>True if seeding should occur; otherwise, false</returns>
        bool ShouldSeed(IRepository<T> repository);

        /// <summary>
        /// Asynchronously determines whether seeding should occur based on the current state of the repository
        /// </summary>
        /// <param name="repository">The repository to check</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operation</param>
        /// <returns>A task representing the asynchronous operation with a boolean indicating whether to seed</returns>
        Task<bool> ShouldSeedAsync(IRepository<T> repository, CancellationToken cancellationToken = default);
    }
}
