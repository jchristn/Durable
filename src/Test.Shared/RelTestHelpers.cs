namespace Test.Shared
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Durable.MySql;
    using Durable.Postgres;
    using Durable.Sql;
    using Durable.Sqlite;
    using Durable.SqlServer;

    /// <summary>
    /// Shared helpers for the relationship and write-feature suites: table recreation through the engine's DDL
    /// generation and construction of repositories with custom <see cref="SqlRepositoryOptions"/>.
    /// Thread safety: all members are stateless and safe for concurrent use.
    /// </summary>
    public static class RelTestHelpers
    {
        #region Public-Methods

        /// <summary>
        /// Builds a provider-appropriate DROP TABLE IF EXISTS statement (with CASCADE on PostgreSQL).
        /// </summary>
        /// <param name="dialect">Dialect used to quote the identifier. Must not be null.</param>
        /// <param name="tableName">Unquoted table name. Must not be null or empty.</param>
        /// <returns>The DROP statement.</returns>
        /// <exception cref="ArgumentNullException">Thrown when dialect or tableName is null.</exception>
        public static string DropTableSql(ISqlDialect dialect, string tableName)
        {
            ArgumentNullException.ThrowIfNull(dialect);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));
            string sql = "DROP TABLE IF EXISTS " + dialect.QuoteIdentifier(tableName);
            if (dialect.RepositoryType == RepositoryType.Postgres) sql += " CASCADE";
            return sql;
        }

        /// <summary>
        /// Drops the table mapped by <typeparamref name="T"/> (if it exists) and recreates it with InitializeTable.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="repository">Repository for the entity. Must not be null.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository is null.</exception>
        public static async Task RecreateTableAsync<T>(ISqlRepository<T> repository) where T : class, new()
        {
            ArgumentNullException.ThrowIfNull(repository);
            await repository.ExecuteSqlAsync(DropTableSql(repository.Dialect, repository.Metadata.TableName)).ConfigureAwait(false);
            repository.InitializeTable(typeof(T));
        }

        /// <summary>
        /// Creates a repository that shares the provider's connection factory but uses custom options.
        /// The underlying provider repository owns the connection factory and is intentionally not disposed.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="provider">Repository provider. Must not be null.</param>
        /// <param name="options">Options to apply. Must not be null.</param>
        /// <returns>A repository using the supplied options.</returns>
        /// <exception cref="ArgumentNullException">Thrown when provider or options is null.</exception>
        /// <exception cref="NotSupportedException">Thrown for an unknown provider.</exception>
        public static ISqlRepository<T> CreateRepository<T>(IRepositoryProvider provider, SqlRepositoryOptions options) where T : class, new()
        {
            ArgumentNullException.ThrowIfNull(provider);
            ArgumentNullException.ThrowIfNull(options);
            ISqlRepository<T> template = provider.CreateRepository<T>();
            RepositoryType type = template.Dialect.RepositoryType;
            if (type == RepositoryType.Sqlite) return new SqliteRepository<T>(template.ConnectionFactory, options);
            if (type == RepositoryType.MySql) return new MySqlRepository<T>(template.ConnectionFactory, options);
            if (type == RepositoryType.Postgres) return new PostgresRepository<T>(template.ConnectionFactory, options);
            if (type == RepositoryType.SqlServer) return new SqlServerRepository<T>(template.ConnectionFactory, options);
            throw new NotSupportedException("Unknown repository type " + type + ".");
        }

        /// <summary>
        /// Gets the repository type of the configured provider.
        /// </summary>
        /// <param name="provider">Repository provider. Must not be null.</param>
        /// <returns>The repository type.</returns>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public static RepositoryType TypeOf(IRepositoryProvider provider)
        {
            ArgumentNullException.ThrowIfNull(provider);
            return provider.CreateRepository<Author>().Dialect.RepositoryType;
        }

        #endregion
    }
}
