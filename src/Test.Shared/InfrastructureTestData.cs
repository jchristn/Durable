namespace Test.Shared
{
    using System;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// Shared helpers for the transaction and infrastructure suites. Rows are tagged with a department value so
    /// each test can isolate and remove its own data. Stateless and thread safe.
    /// </summary>
    public static class InfrastructureTestData
    {
        #region Public-Methods

        /// <summary>
        /// Creates an unsaved person tagged with <paramref name="department"/>.
        /// </summary>
        /// <param name="email">The email address; must not be null.</param>
        /// <param name="department">The department tag (max 32 characters); must not be null.</param>
        /// <param name="age">The age. Default: 30.</param>
        /// <returns>A new person.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public static Person NewPerson(string email, string department, int age = 30)
        {
            ArgumentNullException.ThrowIfNull(email);
            ArgumentNullException.ThrowIfNull(department);
            return new Person
            {
                FirstName = "Infra",
                LastName = "Test",
                Age = age,
                Email = email,
                Salary = 50000m,
                Department = department
            };
        }

        /// <summary>
        /// Deletes every person tagged with <paramref name="department"/>.
        /// </summary>
        /// <param name="repository">The repository to use; must not be null.</param>
        /// <param name="department">The department tag; must not be null.</param>
        /// <returns>A task that completes when the rows are deleted.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public static async Task ClearDepartmentAsync(ISqlRepository<Person> repository, string department)
        {
            ArgumentNullException.ThrowIfNull(repository);
            ArgumentNullException.ThrowIfNull(department);
            await repository.DeleteManyAsync(p => p.Department == department).ConfigureAwait(false);
        }

        #endregion
    }
}
