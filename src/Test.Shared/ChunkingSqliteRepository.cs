namespace Test.Shared
{
    using Durable.Sql;
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// SQLite repository using <see cref="SmallParameterSqliteDialect"/> so that Include IN lists are split into
    /// many chunks. Does not own the supplied connection factory.
    /// Thread safety: same guarantees as <see cref="SqlRepository{T}"/>.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class ChunkingSqliteRepository<T> : SqlRepository<T> where T : class, new()
    {
        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ChunkingSqliteRepository{T}"/> class.
        /// </summary>
        /// <param name="connectionFactory">Connection factory. Must not be null.</param>
        /// <param name="options">Options; may be null.</param>
        public ChunkingSqliteRepository(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null)
            : base(new SmallParameterSqliteDialect(), connectionFactory, false, typeof(SqliteConnection), null, options)
        {
        }

        #endregion
    }
}
